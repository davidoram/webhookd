package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaRunning(t *testing.T) {
	if !testConnection("kafka", "9092") {
		t.Errorf("Can't connect to kafka:9092 - clients")
	}
	if !testConnection("kafka", "2081") {
		t.Errorf("Can't connect to kafka:2081 - zookeeper")
	}
}

type TestDestination struct {
	t       *testing.T
	Batches [][]*kafka.Message
	SendOK  bool
	Events  chan BatchEvent
}

type BatchEvent struct {
	Index    int
	Messages []*kafka.Message
}

func NewTestDest(t *testing.T) *TestDestination {
	return &TestDestination{
		t:       t,
		Batches: make([][]*kafka.Message, 0),
		SendOK:  true,
		Events:  make(chan BatchEvent),
	}
}
func (wh *TestDestination) Send(msgs []*kafka.Message) bool {
	wh.t.Logf("TestDestination received batch %d with %d messages", len(wh.Batches), len(msgs))
	wh.Events <- BatchEvent{Index: len(wh.Batches), Messages: msgs}
	wh.Batches = append(wh.Batches, msgs)
	return wh.SendOK
}

const (
	KafkaHost = "kafka"
	KafkaPort = "9092"
)

func TestSubscription(t *testing.T) {
	require.True(t, testConnection(KafkaHost, KafkaPort))

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := testProducer(t)
	defer producer.Close()

	dest := NewTestDest(t)
	var loggingLevel = new(slog.LevelVar)
	loggingLevel.Set(slog.LevelDebug)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: loggingLevel}))
	s := Subscription{
		Name: "sub-1",
		ID:   uuid.New(),
		Source: Source{
			Topic:      topic,
			JMESFilter: "",
		},
		Config: Config{
			BatchSize: 1,
		},
		Destination: dest,
	}.WithLogger(logger)

	err := s.Start(fmt.Sprintf("%s:%s", KafkaHost, KafkaPort))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Consume(ctx)

	key := uuid.NewString()
	sendMsgBlocking(t, producer, key, "msg-1", topic)

	t.Logf("wait for batch to arrive...")
	event := <-dest.Events
	t.Logf("checking batch")
	assert.Equal(t, 0, event.Index)
	require.Equal(t, 1, len(event.Messages))
	assert.Equal(t, key, string(event.Messages[0].Key))
	assert.Equal(t, "msg-1", string(event.Messages[0].Value))
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
	t.Logf("message sent ok, topic: %s, key: %s", topic, key)
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
