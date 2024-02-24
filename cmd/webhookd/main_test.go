package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/davidoram/webhookd/adapter"
	"github.com/davidoram/webhookd/core"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaRunning(t *testing.T) {
	if !core.TCPConnect(core.KafkaServers) {
		t.Errorf("Can't connect to %s - clients", core.KafkaServers)
	}
	if !core.TCPConnect(core.ZooKeeperServers) {
		t.Errorf("Can't connect to %s - zookeeper", core.ZooKeeperServers)
	}
}

func TestOneMessage(t *testing.T) {
	require.True(t, core.TCPConnect(core.KafkaServers))

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Start(ctx, core.KafkaServers)

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
	require.True(t, core.TCPConnect(core.KafkaServers))

	topic, producer := NewTestProducer(t)
	defer producer.Close()

	dest := core.NewTestDest(t)
	batchSize := 10
	sub := NewTestSubscription(topic, batchSize, dest)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sub.Start(ctx, core.KafkaServers)

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

var (
	topicNum        = 0
	subscriptionNum = 0
)

func NextTopic() string            { topicNum++; return fmt.Sprintf("topic-%d-%s", topicNum, uuid.New().String()) }
func NextSubscriptionName() string { subscriptionNum++; return fmt.Sprintf("sub-%d", subscriptionNum) }

func NewTestProducer(t *testing.T) (topic string, producer *kafka.Producer) {
	topic = NextTopic()
	return topic, core.TestProducer(t)
}

func NewTestSubscription(topic string, batchSize int, dest *core.TestDestination) core.Subscription {
	s := core.Subscription{
		Name: NextSubscriptionName(),
		ID:   uuid.New(),
		Topic: core.Topic{
			Topic: topic,
		},
		Config: core.Config{
			BatchSize: batchSize,
		},
		Destination: dest,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	s.Config = s.Config.WithMaxWait(time.Millisecond * 10)
	return s
}

func TestGenerateSubscriptionChangesDatabaseEmpty(t *testing.T) {

	require.True(t, core.TCPConnect(core.KafkaServers))
	db := core.OpenTestDatabase(t)
	defer db.Close()

	subChanges := make(chan core.SubscriptionChangeEvent)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Run a goroutine to capture the events from subChanges
	events := make([]core.SubscriptionChangeEvent, 0)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-subChanges:
				t.Log("received event")
				events = append(events, evt)
			}
		}
	}()
	pollingPeriod := time.Duration(250 * time.Millisecond)

	go GenerateSubscriptionChanges(ctx, db, subChanges, pollingPeriod)

	// Wait for the context to timeout
	<-ctx.Done()

	// Check that no events were received
	require.Len(t, events, 0)
}

func TestGenerateSubscriptionChangesReadFromDatabase(t *testing.T) {
	core.DefaultTest = t
	require.True(t, core.TCPConnect(core.KafkaServers))
	db := core.OpenTestDatabase(t)
	defer db.Close()

	// Create a subscription
	csub := NewTestSubscription(NextTopic(), 10, core.NewTestDest(t))
	vsub, err := adapter.CoreToViewAdapter(csub)
	require.NoError(t, err)

	// save the subscription to the database
	err = core.InsertSubscription(context.Background(), db, vsub)
	require.NoError(t, err)

	subChanges := make(chan core.SubscriptionChangeEvent)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Run a goroutine to capture the events from subChanges
	events := make([]core.SubscriptionChangeEvent, 0)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-subChanges:
				t.Log("received event")
				events = append(events, evt)
			}
		}
	}()
	pollingPeriod := time.Duration(250 * time.Millisecond)

	go GenerateSubscriptionChanges(ctx, db, subChanges, pollingPeriod)

	// Wait for the context to timeout
	<-ctx.Done()

	// Check that the correct event was received
	require.Len(t, events, 1)
	assert.Equal(t, csub.ID, events[0].Subscription.ID)
}

func TestGenerateSubscriptionChangesReadFromDatabaseMany(t *testing.T) {
	core.DefaultTest = t
	require.True(t, core.TCPConnect(core.KafkaServers))
	db := core.OpenTestDatabase(t)
	defer db.Close()

	// Create 100 subscriptions
	createdIds := []uuid.UUID{}
	for i := 0; i < 100; i++ {
		csub := NewTestSubscription(NextTopic(), 10, core.NewTestDest(t))
		vsub, err := adapter.CoreToViewAdapter(csub)
		require.NoError(t, err)

		// save the subscription to the database
		err = core.InsertSubscription(context.Background(), db, vsub)
		require.NoError(t, err)
		createdIds = append(createdIds, csub.ID)
	}

	subChanges := make(chan core.SubscriptionChangeEvent)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Run a goroutine to capture the events from subChanges
	events := make([]core.SubscriptionChangeEvent, 0)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-subChanges:
				t.Log("received event")
				events = append(events, evt)
			}
		}
	}()
	pollingPeriod := time.Duration(250 * time.Millisecond)

	go GenerateSubscriptionChanges(ctx, db, subChanges, pollingPeriod)

	// Wait for the context to timeout
	<-ctx.Done()

	// Check that the correct event was received
	require.Len(t, events, 100)
	for i, evt := range events {
		// Check that the ID is in the list of created IDs
		found := false
		for _, id := range createdIds {
			if id == evt.Subscription.ID {
				found = true
				break
			}
		}
		assert.True(t, found, "event %d, id %s not found in createdIds", i, evt.Subscription.ID)
	}
}
