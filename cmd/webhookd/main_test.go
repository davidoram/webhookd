package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/davidoram/webhookd/core"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaRunning(t *testing.T) {
	if !core.TestConnection(core.KafkaServers) {
		t.Errorf("Can't connect to %s - clients", core.KafkaServers)
	}
	if !core.TestConnection(core.ZooKeeperServers) {
		t.Errorf("Can't connect to %s - zookeeper", core.ZooKeeperServers)
	}
}

func TestOneMessage(t *testing.T) {
	require.True(t, core.TestConnection(core.KafkaServers))

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := core.TestProducer(t)
	defer producer.Close()

	dest := core.NewTestDest(t)
	s := core.Subscription{
		Name: "sub-1",
		ID:   uuid.New(),
		Topic: core.Topic{
			Topic: topic,
		},
		Config: core.Config{
			BatchSize: 1,
		},
		Destination: dest,
	}
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

	err := s.Start(core.KafkaServers)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Consume(ctx)

	key := uuid.NewString()
	core.SendMsgBlocking(t, producer, key, "msg-1", topic)

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

	require.True(t, core.TestConnection(core.KafkaServers))

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := core.TestProducer(t)
	defer producer.Close()

	dest := core.NewTestDest(t)
	s := core.Subscription{
		Name: "sub-1",
		ID:   uuid.New(),
		Topic: core.Topic{
			Topic: topic,
		},
		Config: core.Config{
			BatchSize: batchSize,
		},
		Destination: dest,
	}
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

	err := s.Start(core.KafkaServers)
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
		core.SendMsgBlocking(t, producer, k, v, topic)
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
