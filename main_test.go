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

const (
	KafkaHost     = "localhost"
	KafkaPort     = "9092"
	ZooKeeperPort = "2081"
)

func TestKafkaRunning(t *testing.T) {
	if !testConnection(KafkaHost, KafkaPort) {
		t.Errorf("Can't connect to %s:%s - clients", KafkaHost, KafkaPort)
	}
	if !testConnection(KafkaHost, ZooKeeperPort) {
		t.Errorf("Can't connect to %s:%s - zookeeper", KafkaHost, ZooKeeperPort)
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
		Topic: Topic{
			Topic: topic,
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

func TestMultipleBatches(t *testing.T) {
	batchSize := 10

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
		Topic: Topic{
			Topic: topic,
		},
		Config: Config{
			BatchSize: batchSize,
			MaxWait:   time.Millisecond * 10,
		},
		Destination: dest,
	}.WithLogger(logger)

	err := s.Start(fmt.Sprintf("%s:%s", KafkaHost, KafkaPort))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Consume(ctx)

	totalMessages := 111
	sent := map[string]string{}
	for i := 0; i < totalMessages; i++ {
		sent[fmt.Sprintf("%d", i)] = fmt.Sprintf("msg-%d", i)
	}

	for k, v := range sent {
		sendMsgBlocking(t, producer, k, v, topic)
	}

	received := map[string]string{}
	numBatches := totalMessages / batchSize
	if totalMessages%batchSize > 0 {
		numBatches++
	}
	t.Logf("wait for %d batches", numBatches)
	for i := 0; i < numBatches; i++ {
		t.Logf("wait for batch %d to arrive...", i)
		event := <-dest.Events
		for _, msg := range event.Messages {
			received[string(msg.Key)] = string(msg.Value)
		}
	}

	t.Logf("checking all messages received")
	assert.Equal(t, sent, received)
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
