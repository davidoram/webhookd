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

func (sm *SubscriptionManager) Start(ctx context.Context, events chan SubscriptionChangeEvent) {
	// Loop forever, processing subscription set events until the context is cancelled
	var err error
	for err == nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case event := <-events:
			sm.processEvent(ctx, event)
		}
	}

	// Stop all subscriptions before exiting
	for _, sub := range sm.subs {
		sub.Stop()
	}
	// Clear the map
	sm.subs = map[uuid.UUID]*Subscription{}
}

func (sm *SubscriptionManager) processEvent(ctx context.Context, event SubscriptionChangeEvent) error {
	sm.subsMutex.Lock()
	defer sm.subsMutex.Unlock()

	sm.stop(event.Subscription)
	if event.Subscription.IsActive() {
		sm.start(ctx, event.Subscription)
	}
	return nil
}

func (sm *SubscriptionManager) start(ctx context.Context, sub *Subscription) {
	sm.subs[sub.ID] = sub
	go sm.consumeWithRecovery(ctx, sub)
}

func (sm *SubscriptionManager) stop(sub *Subscription) {
	if oldSub, ok := sm.subs[sub.ID]; ok {
		oldSub.Stop()
		delete(sm.subs, sub.ID)
	}
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
