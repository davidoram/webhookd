package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSendToWebhook(t *testing.T) {
	TestTCPConnect(t, KafkaServers)

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := TestProducer(t)
	defer producer.Close()

	batchSize := 100
	totalMessages := 1000

	// Start a real webserver to receive the webhook
	done := make(chan bool)
	svr := testWebserver(t, totalMessages, done)
	defer svr.Close()

	// Create the destination
	dest := NewWebhook(fmt.Sprintf("%s/messages", svr.Server.URL))
	// Don't make the test wait for retries
	dest.Retry = FixedRetrier{Duration: time.Millisecond}

	// Setup the Subscription
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
	}
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

	// Start the subscription
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Start(ctx, KafkaServers)

	// Send some messages
	sent := map[string]string{}
	for i := 0; i < totalMessages; i++ {
		sent[fmt.Sprintf("%d", i)] = fmt.Sprintf("msg-%d", i)
	}

	for k, v := range sent {
		SendMsgBlocking(t, producer, k, v, topic)
	}

	// Wait for all messages to be received
	<-done

	t.Logf("checking all messages received")
	assert.Equal(t, sent, svr.MesssageMap())
}

func TestSendToWebhookWithRetry(t *testing.T) {
	TestTCPConnect(t, KafkaServers)

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := TestProducer(t)
	defer producer.Close()

	totalMessages := 1000
	batchSize := 100

	// Start a real webserver to receive the webhook
	done := make(chan bool)
	svr := testWebserver(t, totalMessages, done)
	// Fail every 2nd request with a 500 error
	svr.ResponseOverrider = &FailEveryNthResponse{N: 2, StatusCode: 500, Message: "Internal Server Error"}
	defer svr.Close()

	// Create the destination
	dest := NewWebhook(fmt.Sprintf("%s/messages", svr.Server.URL))
	// Don't make the test wait for retries
	dest.Retry = FixedRetrier{Duration: time.Millisecond}

	// Setup the Subscription
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
	}
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

	// Start the subscription
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Start(ctx, KafkaServers)

	// Send some messages
	sent := map[string]string{}
	for i := 0; i < totalMessages; i++ {
		sent[fmt.Sprintf("%d", i)] = fmt.Sprintf("msg-%d", i)
	}

	for k, v := range sent {
		SendMsgBlocking(t, producer, k, v, topic)
	}

	// Wait for all messages to be received
	<-done

	t.Logf("checking all messages received")
	assert.Equal(t, sent, svr.MesssageMap())
}

func TestSendToWebhookAuthToken(t *testing.T) {
	TestTCPConnect(t, KafkaServers)

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := TestProducer(t)
	defer producer.Close()

	totalMessages := 1000
	batchSize := 100

	// Start a real webserver to receive the webhook
	done := make(chan bool)
	svr := testWebserver(t, totalMessages, done)
	svr.CheckAuthHeader = true
	svr.AuthHeaderValue = uuid.NewString()

	defer svr.Close()

	// Create the destination
	dest := NewWebhook(fmt.Sprintf("%s/messages", svr.Server.URL))
	// Don't make the test wait for retries
	dest.Retry = FixedRetrier{Duration: time.Millisecond}
	// Set the auth token
	dest = dest.WithClient(NewAuthTokenClient(svr.AuthHeaderValue, time.Second*1))
	dest.Retry = FixedRetrier{Duration: time.Millisecond}

	// Setup the Subscription
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
	}
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

	// Start the subscription
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Start(ctx, KafkaServers)

	// Send some messages
	sent := map[string]string{}
	for i := 0; i < totalMessages; i++ {
		sent[fmt.Sprintf("%d", i)] = fmt.Sprintf("msg-%d", i)
	}

	for k, v := range sent {
		SendMsgBlocking(t, producer, k, v, topic)
	}

	// Wait for all messages to be received
	<-done

	t.Logf("checking all messages received")
	assert.Equal(t, sent, svr.MesssageMap())
}

func TestSendToWebhookBadAuthToken(t *testing.T) {
	TestTCPConnect(t, KafkaServers)

	topic := fmt.Sprintf("topic-1-%s", uuid.NewString())
	producer := TestProducer(t)
	defer producer.Close()

	batchSize := 1
	totalMessages := 1

	// Start a real webserver to receive the webhook
	done := make(chan bool)
	svr := testWebserver(t, totalMessages, done)
	svr.CheckAuthHeader = true
	svr.AuthHeaderValue = uuid.NewString()

	defer svr.Close()

	// Create the destination
	dest := NewWebhook(fmt.Sprintf("%s/messages", svr.Server.URL))
	// Don't make the test wait for retries
	dest.Retry = FixedRetrier{Duration: time.Millisecond}
	// Set the auth token
	dest = dest.WithClient(NewAuthTokenClient("wrong-auth-token", time.Millisecond*200))
	dest.Retry = FixedRetrier{Duration: time.Millisecond}

	// Setup the Subscription
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
	}
	tl := NewTestListener(t, SubscriptionEventBatchSentNACK, done)
	s.AddListener(tl)
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)

	// Start the subscription
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Start(ctx, KafkaServers)

	// Send some messages
	sent := map[string]string{}
	for i := 0; i < totalMessages; i++ {
		sent[fmt.Sprintf("%d", i)] = fmt.Sprintf("msg-%d", i)
	}

	for k, v := range sent {
		SendMsgBlocking(t, producer, k, v, topic)
	}

	// Wait to get a callback saying the messages couldnt be sent
	<-done

	t.Logf("checking no messages received")
	assert.Equal(t, 0, len(svr.MesssageMap()))
}
