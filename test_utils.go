package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"io"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

var (
	// List of comma separated kafka servers to connect to, defaults to "localhost:9092",
	// or override with KAFKA_SERVERS env var
	KafkaServers = "localhost:9092"

	// List of comma separated zookeeper servers to connect to, defaults to "localhost:2081",
	// or override with ZOOKEEPER_SERVERS env var
	ZooKeeperServers = "localhost:2081"
)

func init() {
	if host := os.Getenv("KAFKA_SERVERS"); host != "" {
		KafkaServers = host
	}
	if port := os.Getenv("ZOOKEEPER_SERVERS"); port != "" {
		ZooKeeperServers = port
	}
}

type TestListener struct {
	t       *testing.T
	Events  []SubscriptionEvent
	WaitFor SubscriptionEventType
	Done    chan (bool)
}

func NewTestListener(t *testing.T, waitFor SubscriptionEventType, done chan (bool)) *TestListener {
	return &TestListener{t: t, Events: make([]SubscriptionEvent, 0), WaitFor: waitFor, Done: done}
}

func (tl *TestListener) SubscriptionEvent(event SubscriptionEvent) {
	tl.t.Logf("TestListener received event %s", event.Type)
	tl.Events = append(tl.Events, event)
	if tl.WaitFor == event.Type {
		tl.Done <- true
	}
}

type TestDestination struct {
	t       *testing.T
	Batches [][]*kafka.Message
	SendOK  func(*TestDestination, []*kafka.Message) bool
	Events  chan BatchEvent
}

type BatchEvent struct {
	Index    int
	Messages []*kafka.Message
}

func NewTestDest(t *testing.T) *TestDestination {
	td := TestDestination{
		t:       t,
		Batches: make([][]*kafka.Message, 0),
		Events:  make(chan BatchEvent),
	}
	td.SendOK = func(_ *TestDestination, _ []*kafka.Message) bool { return true }
	return &td
}

func (wh *TestDestination) Send(ctx context.Context, msgs []*kafka.Message) error {
	// Only save the messages if SendOK is true
	if !wh.SendOK(wh, msgs) {
		wh.t.Logf("TestDestination received batch %d with %d messages, ACK failed", len(wh.Batches), len(msgs))
		return fmt.Errorf("TestDestination NACK")
	}
	wh.t.Logf("TestDestination received batch %d with %d messages, ACK ok", len(wh.Batches), len(msgs))
	wh.Events <- BatchEvent{Index: len(wh.Batches), Messages: msgs}
	wh.Batches = append(wh.Batches, msgs)
	return nil
}

func testProducer(t *testing.T) *kafka.Producer {

	t.Logf("stating producer, bootstrap.servers: %s", KafkaServers)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaServers,
		"acks":              "all",
	})
	assert.NoError(t, err, "creating kafka producer")

	// Capture events from the logs channel
	lc := p.Logs()
	if lc != nil {
		t.Log("listening to producer logs")
		go func() {
			for {
				select {
				case e, ok := <-lc:
					if !ok {
						return
					}
					t.Logf("kafka producer %v", e)
				}
			}
		}()
	}
	return p
}

func sendMsgBlocking(t *testing.T, p *kafka.Producer, key, value, topic string) {
	m := &kafka.Message{
		Key:            []byte(key),
		Value:          []byte(value),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	}
	// Create a channel, used to very when msg has been delivered, or an error has occured
	deliveryChan := make(chan kafka.Event)

	err := p.Produce(m, deliveryChan)
	assert.NoError(t, err, "error calling produce")

	// Block waiting for delivery
	evt := <-deliveryChan
	msg := evt.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		t.Fatalf("message send failed! error: %v", msg.TopicPartition.Error)
	}
	// t.Logf("message sent ok, topic: %s, key: %s", topic, key)
	close(deliveryChan)
}

func testConnection(hostPorts string) bool {
	// hostPorts is a comma separated list of host:port pairs
	for _, address := range strings.Split(hostPorts, ",") {
		conn, err := net.DialTimeout("tcp", address, time.Second*1)
		if err != nil {
			return false
		}
		conn.Close()
	}
	return true
}

type ResponseOverrider interface {
	// Return true if the response should be overridden, and the new response code and message
	// return false to let the server handle the request
	OverrideResponse(*http.Request) (bool, int, string)
}

type DefaultResponseOverrider struct{}

func (d DefaultResponseOverrider) OverrideResponse(*http.Request) (bool, int, string) {
	return false, 0, ""
}

type FailEveryNthResponse struct {
	count      int
	N          int
	StatusCode int
	Message    string
}

func (d *FailEveryNthResponse) OverrideResponse(*http.Request) (bool, int, string) {
	d.count++
	if d.count%d.N == 0 {
		return true, d.StatusCode, d.Message
	}
	return false, 0, ""
}

// WebhookTestServer is a test webserver that can be used to receive webhook messages.
// It has a Messages field that can be used to inspect the messages received by the server.
type WebhookTestServer struct {
	Server   *httptest.Server
	t        *testing.T
	Messages []Message
	// ResponseOverrider is a function that can be used to override the response code returned by the server.
	// To have the server return a 500 Internal Server Error, use: func(*http.Request) (bool, int) { return true, 500, "Internal Server Error" }
	// To have the server return a 200 OK, use: func(*http.Request) (bool, int) { return true, 0, "" }
	ResponseOverrider ResponseOverrider

	// Set CheckAuthHeader to true to check that the Authorization header is set to the value in AuthHeaderVale
	CheckAuthHeader bool
	AuthHeaderValue string
}

// testWebserver creates a httptest.Server that has one endpoint /messages that will accept a POST request
// containing a list of messages in the format:
//
//	{
//	  "messages":
//	  [
//	    {
//	      "key": "key1",
//	      "value": "value1",
//	      "topic": "topic1"
//	      "headers": [ { "key": "header1", "value": "header1value" } ]
//	    },
//	    ...
//	   ]
//	}
//
// The webserver will return a 200 OK and save the messsages in memory, so they can be inspected
// by the test.  The function returns the httptest.Server object.
// Pass the done channel to the function, and it will close the channel when the expected number of
// messages have been received.
func testWebserver(t *testing.T, expectedMessages int, done chan bool) *WebhookTestServer {
	testServer := WebhookTestServer{t: t, Messages: make([]Message, 0)}
	// Create a test webserver
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Fatalf("test webhook unexpected request path %s", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Fatalf("test webhook unexpected request method %s", r.Method)
		}
		override, status, b := testServer.ResponseOverrider.OverrideResponse(r)
		if override {
			t.Logf("test webhook overriding response with %d", status)
			w.WriteHeader(status)
			w.Write([]byte(b))
			return
		}
		if testServer.CheckAuthHeader {
			expected := fmt.Sprintf("Bearer %s", testServer.AuthHeaderValue)
			if authHeader := r.Header.Get("Authorization"); authHeader != expected {
				t.Logf("test webhook got invalid auth header '%s', expected '%s'", authHeader, expected)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		t.Logf("test webhook got valid request %s", r.URL.Path)
		// Read the request body
		body, err := io.ReadAll(io.Reader(r.Body))
		assert.NoError(t, err, "test webhook reading request body")
		// Unmarshal the body into a list of messages
		var msgBatch MessageBatch
		err = json.Unmarshal(body, &msgBatch)
		assert.NoErrorf(t, err, "test webhook unmarshalling request body: '%s'", string(body))
		// Save the messages to a list
		testServer.Messages = append(testServer.Messages, msgBatch.Messages...)
		t.Logf("test webhook received %d messages", len(msgBatch.Messages))
		w.WriteHeader(http.StatusOK)
		if len(testServer.Messages) >= expectedMessages {
			t.Logf("test webhook sending done on channel")
			done <- true
		}
	}))
	testServer.Server = ts
	// By default, let the server handle all requests and return a 200 OK
	testServer.ResponseOverrider = DefaultResponseOverrider{}

	return &testServer
}

func (ts *WebhookTestServer) Close() {
	ts.Server.Close()
}

func (ts *WebhookTestServer) MesssageMap() map[string]string {
	msg := make(map[string]string)
	for _, m := range ts.Messages {
		msg[m.Key] = m.Value
	}
	return msg
}
