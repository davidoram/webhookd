package core

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"

	"github.com/google/uuid"
)

type SubscriptionManager struct {
	KafkaBootstrapServers string
	subs                  map[uuid.UUID]*Subscription
	subsMutex             sync.Mutex
}

func NewSubscriptionManager(kafkaServers string) *SubscriptionManager {
	return &SubscriptionManager{
		KafkaBootstrapServers: kafkaServers,
		subs:                  map[uuid.UUID]*Subscription{},
	}
}

func (sm *SubscriptionManager) Start(ctx context.Context, events chan SubscriptionSetEvent) {
	// Loop forever, processing subscription set events until the context is cancelled
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			sm.processEvent(ctx, event)
		}
	}
}

func (sm *SubscriptionManager) Close() {
	sm.subsMutex.Lock()
	defer sm.subsMutex.Unlock()
	// Stop all subscriptions
	for _, sub := range sm.subs {
		sub.Stop()
	}
	// Clear the map
	sm.subs = map[uuid.UUID]*Subscription{}
}

func (sm *SubscriptionManager) processEvent(ctx context.Context, event SubscriptionSetEvent) error {
	sm.subsMutex.Lock()
	defer sm.subsMutex.Unlock()

	switch event.Type {
	case NewSubscriptionEvent:
		// Double check that the subscription doesn't already exist
		if oldSub, ok := sm.subs[event.Subscription.ID]; ok {
			sm.stop(oldSub)
		}
		// Start the new subscription
		sm.start(ctx, event.Subscription)
		sm.subs[event.Subscription.ID] = event.Subscription

	case UpdatedSubscriptionEvent:
		// Remove the existing subscription if it exists
		if oldSub, ok := sm.subs[event.Subscription.ID]; ok {
			sm.stop(oldSub)
		}
		// Start the updated subscription
		sm.start(ctx, event.Subscription)
	case DeletedSubscriptionEvent:
		// Remove the existing subscription if it exists
		if oldSub, ok := sm.subs[event.Subscription.ID]; ok {
			sm.stop(oldSub)
		}
	default:
		panic("unknown subscription set event type " + event.Type)
	}
	return nil
}

func (sm *SubscriptionManager) start(ctx context.Context, sub *Subscription) {
	sm.subs[sub.ID] = sub
	go sm.consumeWithRecovery(ctx, sub)
}

func (sm *SubscriptionManager) stop(sub *Subscription) {
	sub.Stop()
	delete(sm.subs, sub.ID)
}

// consumeWithRecovery runs the consume function in a goroutine and recovers from any panics / errors that occur
// It will exit the process if a panic or error occurs
func (sm *SubscriptionManager) consumeWithRecovery(ctx context.Context, sub *Subscription) {
	var wg sync.WaitGroup
	wg.Add(1)
	errorOrPanic := false

	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("consumer recovered from panic",
					slog.Any("error", r),
					slog.String("consumer_id", sub.ID.String()),
					slog.String("group_id", sub.GroupID()),
					slog.String("subscription.id", sub.ID.String()),
					slog.String("subscription.name", sub.Name),
				)
				debug.PrintStack()
				errorOrPanic = true
			}
		}()
		sub.Start(ctx, sm.KafkaBootstrapServers)
	}()

	wg.Wait()
	if errorOrPanic {
		os.Exit(1) // TODO Wrap for testing
	}
}
