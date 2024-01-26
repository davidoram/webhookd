package core

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type Subscription struct {
	Name      string
	ID        uuid.UUID
	Topic     Topic
	Filter    Filter
	Config    Config
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime // NULL if not deleted, ie: still 'active'

	// Runtime elements
	Destination Destination
	consumer    *kafka.Consumer
	listeners   []SubscriptionListener
	ctx         context.Context
	done        context.CancelCauseFunc
}

var ErrSendFailed = errors.New("send transactionally failed")
var ErrContextCancelled = errors.New("context cancelled")

func (s Subscription) IsActive() bool {
	return !s.DeletedAt.Valid
}

func (s Subscription) ResourcePath() string {
	return fmt.Sprintf("1/subscriptions/%s", s.ID)
}

func (s *Subscription) AddListener(l SubscriptionListener) {
	s.listeners = append(s.listeners, l)
}

func (s Subscription) GroupID() string {
	return fmt.Sprintf("webhookd-%s", s.ID)
}

// Start starts the subscription, this is a blocking call, so run it in a go routine
// it will return when the context is cancelled. Call context.Cause(ctx) that will wither return
// the error that caused the context to be cancelled or context.Cancelled if the subscription was stopped
func (s *Subscription) Start(ctx context.Context, kafkaServers string) {
	var err error
	s.ctx, s.done = context.WithCancelCause(ctx)
	s.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServers,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"group.id":              s.GroupID(),
		"auto.offset.reset":     "earliest", // TODO is this correct? Start reading from the end of the topic
		// "debug":                    "msg",
		// "session.timeout.ms":   6000,
		// "max.poll.interval.ms": 6000,
		// The best way to achieve at least once semantics is to
		// disable `enable.auto.offset.store`, which marks a message as eligible
		// for commit as soon as it's delivered to the application
		// and use `StoreOffsets` to manually indicate this instead.
		// Leaving `enable.auto.commit` as true to avoid blocking the poll loop.
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
	})
	if err != nil {
		slog.Error("failed to create consumer", slog.Any("error", err))
		s.done(err)
		return
	}
	slog.Info("start consumer", slog.String("bootstrap.servers", kafkaServers), slog.Any("consumer_id", s.ID), slog.String("group_id", s.GroupID()), slog.String("topic", s.Topic.Topic))
	err = s.consumer.SubscribeTopics([]string{s.Topic.Topic}, nil)
	if err != nil {
		slog.Error("failed to subscribe to topics", slog.Any("error", err))
		s.done(err)
		return
	}
	for l := range s.listeners {
		s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventStart, Subscription: s})
	}
	batch := make([]*kafka.Message, 0)
	run := true

	pushTicker := time.NewTicker(s.Config.MaxWait)
	for run {
		select {
		case <-ctx.Done():
			slog.Info("consume loop terminating", slog.String("consumer_id", s.ID.String()))
			run = false
		case <-pushTicker.C:
			if len(batch) > 0 {
				var err error
				err = s.sendBatch(s.ctx, batch)
				if err != nil {
					run = false
				} else {
					// Reset batch
					batch = batch[:0]
				}
			}
		default:
			// fmt.Printf("Webhook consumer %+v\n", s.consumer)
			ev := s.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				slog.Debug("received msg", slog.String("consumer_id", s.ID.String()), slog.String("topic", *e.TopicPartition.Topic), slog.String("key", string(e.Key)))
				batch = append(batch, e)
				if len(batch) < s.Config.BatchSize {
					continue
				}
				var err error
				slog.Debug("sending batch", slog.String("consumer_id", s.ID.String()), slog.Any("batch_size", len(batch)))
				err = s.sendBatch(s.ctx, batch)
				if err != nil {
					run = false
				} else {
					// Reset batch
					batch = batch[:0]
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
					slog.Info("consumer existing, all brokers down", slog.Any("kafka_error", ev.(kafka.Error)))
				} else {
					slog.Debug("ignore", slog.String("consumer_id", s.ID.String()), slog.Any("kafka_error", ev.(kafka.Error)))
				}

			default:
				slog.Debug("ignore kafka event", slog.String("consumer_id", s.ID.String()), slog.Any("kafka_event", ev.(kafka.Error)))
			}
		}
	}

	for l := range s.listeners {
		s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventStop, Subscription: s})
	}
	if err != nil {
		slog.Error("consumer stopped with error", slog.String("consumer_id", s.ID.String()), slog.Any("error", err))
	} else {
		slog.Info("consumer stopped", slog.String("consumer_id", s.ID.String()))
	}
	s.done(err)
	return
}

func (s *Subscription) Stop() {
	s.done(nil)
	<-s.ctx.Done()
}

// sendBatch sends a batch of messages to the destination and commits the offset, if it returns an error
// caller should check the run flag to see if the consumer should continue
func (s *Subscription) sendBatch(ctx context.Context, batch []*kafka.Message) error {
	err := s.SendTransactionally(ctx, batch)
	if err != nil {
		for l := range s.listeners {
			s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventError, Subscription: s, Error: err})
		}
		slog.Info("consumer stopping, send to destination failed", slog.Any("error", err))
	} else {
		for _, msg := range batch {
			if _, err = s.consumer.CommitMessage(msg); err != nil {
				break
			}
		}
		if err != nil {
			for l := range s.listeners {
				s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventError, Subscription: s, Error: err})
			}
			slog.Info("consumer stopping, unable to store offset", slog.Any("error", err))
		}
	}
	return err
}

// SendTransactionally sends a batch of messages to the destination,
// returns ErrSendFailed if the messages were not sent, because of a client side error
func (s *Subscription) SendTransactionally(ctx context.Context, msgs []*kafka.Message) error {
	select {
	case <-ctx.Done():
		return ErrContextCancelled
	default:
		err := s.Destination.Send(ctx, msgs)
		if err == nil {
			for l := range s.listeners {
				s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventBatchSentACK, Subscription: s})
			}
			slog.Info("consumer sent batch ok", slog.String("consumer_id", s.ID.String()))
			return nil
		}
		for l := range s.listeners {
			s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventBatchSentNACK, Subscription: s, Error: err})
		}
		return err
	}
}
