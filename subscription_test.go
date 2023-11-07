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
	if !testConnection(KafkaHost, KafkaPort) {
		t.Errorf("Can't connect to kafka:9092 - clients")
	}
	if !testConnection(KafkaHost, ZookeeperPort) {
		t.Errorf("Can't connect to kafka:2081 - zookeeper")
	}
}

const (
	// Need to use localhost here, as the kafka container is not on the same network as the test
	KafkaHost     = "localhost"
	KafkaPort     = "9092"
	ZookeeperPort = "2081"
)

func TestSubscription(t *testing.T) {
	require.True(t, testConnection(KafkaHost, KafkaPort))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := testProducer(t)
	defer producer.Close()

	dest := NewBufferDestination(t)
	var loggingLevel = new(slog.LevelVar)
	loggingLevel.Set(slog.LevelDebug)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: loggingLevel}))

	s := NewSubscription("sub-1", uuid.New(), fmt.Sprintf("%s:%s", KafkaHost, KafkaPort)).
		WithDestination(dest).
		WithTopic(topic).
		WithConfig(Config{BatchSize: 1}).
		WithLogger(logger)
	// tel := TestEventListener{T: t}
	// tel.ListenAndLog(&s)
	// defer tel.Close()
	err := s.Start(ctx)
	require.NoError(t, err)
	go s.Consume(ctx)

	key := uuid.NewString()
	sendMsgBlocking(t, producer, key, "msg-1", topic)

	t.Logf("wait for batch to arrive...")
	dest.RequireBatchesArrived(t, 1, 5*time.Second)
	t.Logf("checking batch")
	assert.Equal(t, 1, len(dest.Batches[0]), "batch size mismatch")
	assert.Equal(t, key, string(dest.Batches[0][0].Key), "key mismatch")
	assert.Equal(t, key, string(dest.Batches[0][0].Value), "value mismatch")
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
