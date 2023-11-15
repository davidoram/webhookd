package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"io"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

const (
	KafkaHost     = "localhost"
	KafkaPort     = "9092"
	ZooKeeperPort = "2081"
)

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

func (wh *TestDestination) Send(msgs []*kafka.Message) bool {
	// Only save the messages if SendOK is true
	if !wh.SendOK(wh, msgs) {
		wh.t.Logf("TestDestination received batch %d with %d messages, ACK failed", len(wh.Batches), len(msgs))
		return false
	}
	wh.t.Logf("TestDestination received batch %d with %d messages, ACK ok", len(wh.Batches), len(msgs))
	wh.Events <- BatchEvent{Index: len(wh.Batches), Messages: msgs}
	wh.Batches = append(wh.Batches, msgs)
	return true
}

func testProducer(t *testing.T) *kafka.Producer {

	hostPort := fmt.Sprintf("%s:%s", KafkaHost, KafkaPort)
	t.Logf("stating producer, bootstrap.servers: %s", hostPort)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": hostPort,
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

func testConnection(host string, port string) bool {
	address := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", address, time.Second*1)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

type webhookTestServer struct {
	Server   *httptest.Server
	t        *testing.T
	Messages []Message
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
func testWebserver(t *testing.T, expectedMessages int, done chan bool) *webhookTestServer {
	testServer := webhookTestServer{t: t, Messages: make([]Message, 0)}
	// Create a test webserver
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Fatalf("unexpected request path %s", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Fatalf("unexpected request method %s", r.Method)
		}
		// Read the request body
		body, err := io.ReadAll(io.Reader(r.Body))
		assert.NoError(t, err, "reading request body")
		// Unmarshal the body into a list of messages
		var msgBatch MessageBatch
		err = json.Unmarshal(body, &msgBatch)
		assert.NoError(t, err, "unmarshalling request body")
		// Save the messages to a list
		testServer.Messages = append(testServer.Messages, msgBatch.Messages...)
		t.Logf("received %d messages", len(msgBatch.Messages))
		w.WriteHeader(http.StatusOK)
		if len(testServer.Messages) >= expectedMessages {
			t.Logf("sending done on channel")
			done <- true
		}
	}))
	testServer.Server = ts
	return &testServer
}

func (ts *webhookTestServer) Close() {
	ts.Server.Close()
}

func (ts *webhookTestServer) MesssageMap() map[string]string {
	msg := make(map[string]string)
	for _, m := range ts.Messages {
		msg[m.Key] = m.Value
	}
	return msg
}

func FixedRetrier(dur time.Duration) Retrier {
	return func(retries, maxretries int) time.Duration { return dur }
}
