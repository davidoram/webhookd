package core

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"

	"github.com/google/uuid"
)

type SubscriptionContext struct {
	Subscription *Subscription
	Context      context.Context
	CancelFunc   context.CancelFunc
}

type SubscriptionManager struct {
	KafkaBootstrapServers string
	subs                  map[uuid.UUID]*SubscriptionContext
	subsMutex             sync.Mutex
}

func NewSubscriptionManager(kafkaServers string) *SubscriptionManager {
	return &SubscriptionManager{
		KafkaBootstrapServers: kafkaServers,
		subs:                  map[uuid.UUID]*SubscriptionContext{},
	}
}

func (sm *SubscriptionManager) Start(ctx context.Context, events chan SubscriptionSetEvent) {
	// Loop forever, processing subscription set events until the context is cancelled
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			sm.processEvent(event)
		}
	}
}

func (sm *SubscriptionManager) Close() {
	sm.subsMutex.Lock()
	defer sm.subsMutex.Unlock()
	// Stop all subscriptions
	for _, subCtx := range sm.subs {
		sm.stop(subCtx)
	}
}

func (sm *SubscriptionManager) processEvent(event SubscriptionSetEvent) error {
	sm.subsMutex.Lock()
	defer sm.subsMutex.Unlock()

	switch event.Type {
	case NewSubscriptionEvent:
		return sm.start(event.Subscription)
	case UpdatedSubscriptionEvent:
		subCtx, exists := sm.subs[event.Subscription.ID]
		if exists {
			sm.stop(subCtx)
		}
		return sm.start(event.Subscription)
	case DeletedSubscriptionEvent:
		subCtx, exists := sm.subs[event.Subscription.ID]
		if exists {
			sm.stop(subCtx)
		}
	default:
		panic("unknown subscription set event type " + event.Type)
	}
	return nil
}

func (sm *SubscriptionManager) start(sub *Subscription) error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	sc := &SubscriptionContext{
		Subscription: sub,
		Context:      ctx,
		CancelFunc:   cancelFunc,
	}
	sm.subs[sub.ID] = sc
	err := sc.Subscription.Start(sm.KafkaBootstrapServers)
	if err != nil {
		return err
	}
	go sm.consumeWithRecovery(sc)
	return nil
}

// consumeWithRecovery runs the consume function in a goroutine and recovers from any panics / errors that occur
// It will exit the process if a panic or error occurs
func (sm *SubscriptionManager) consumeWithRecovery(sc *SubscriptionContext) {
	var wg sync.WaitGroup
	wg.Add(1)
	errorOrPanic := false

	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("consumer recovered from panic",
					slog.Any("error", r),
					slog.String("consumer_id", sc.Subscription.ID.String()),
					slog.String("group_id", sc.Subscription.GroupID()),
					slog.String("subscription.id", sc.Subscription.ID.String()),
					slog.String("subscription.name", sc.Subscription.Name),
				)
				debug.PrintStack()
				errorOrPanic = true
			}
		}()
		sc.Subscription.Consume(sc.Context)
	}()

	wg.Wait()
	if errorOrPanic {
		os.Exit(1)
	}
}

func (sm *SubscriptionManager) stop(subCtx *SubscriptionContext) error {
	subCtx.CancelFunc()
	delete(sm.subs, subCtx.Subscription.ID)
	return nil
}
