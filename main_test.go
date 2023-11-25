package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaRunning(t *testing.T) {
	if !testConnection(KafkaHost, KafkaPort) {
		t.Errorf("Can't connect to %s:%s - clients", KafkaHost, KafkaPort)
	}
	if !testConnection(KafkaHost, ZooKeeperPort) {
		t.Errorf("Can't connect to %s:%s - zookeeper", KafkaHost, ZooKeeperPort)
	}
}

func TestOneMessage(t *testing.T) {
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
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

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
		},
		Destination: dest,
	}.WithLogger(logger)
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

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
