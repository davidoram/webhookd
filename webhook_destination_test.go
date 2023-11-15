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

func TestExponentialRetrier(t *testing.T) {
	assert.Equal(t, 10*time.Second, ExponentialRetrier(0, 5))
	assert.Equal(t, 20*time.Second, ExponentialRetrier(1, 5))
	assert.Equal(t, 40*time.Second, ExponentialRetrier(2, 5))
	assert.Equal(t, 80*time.Second, ExponentialRetrier(3, 5))
	assert.Equal(t, 160*time.Second, ExponentialRetrier(4, 5))
	assert.Equal(t, 320*time.Second, ExponentialRetrier(5, 5))
}

func TestFixedRetrier(t *testing.T) {
	r := FixedRetrier(time.Millisecond)
	assert.Equal(t, 1*time.Millisecond, r(0, 5))
	assert.Equal(t, 1*time.Millisecond, r(5, 5))
}

func TestSendToWebhook(t *testing.T) {
	batchSize := 100

	require.True(t, testConnection(KafkaHost, KafkaPort))

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := testProducer(t)
	defer producer.Close()

	totalMessages := 1000

	// Start a real webserver to receive the webhook
	done := make(chan bool)
	svr := testWebserver(t, totalMessages, done)
	defer svr.Close()

	// Create the destination
	dest := NewWebhook(fmt.Sprintf("%s/messages", svr.Server.URL))
	// Don't make the test wait for retries
	dest.Retry = FixedRetrier(time.Millisecond)

	// Setup the Subscription
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

	sent := map[string]string{}
	for i := 0; i < totalMessages; i++ {
		sent[fmt.Sprintf("%d", i)] = fmt.Sprintf("msg-%d", i)
	}

	for k, v := range sent {
		sendMsgBlocking(t, producer, k, v, topic)
	}

	// Wait for all messages to be received
	<-done

	t.Logf("checking all messages received")
	assert.Equal(t, sent, svr.MesssageMap())
}
