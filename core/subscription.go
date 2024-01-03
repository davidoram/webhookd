package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/davidoram/webhookd/configuration"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type Subscription struct {
	Name   string    `json:"name" validate:"required|min_len:5" message:"required:{field} is required, with min length 5"`
	ID     uuid.UUID `json:"id" validate:"required"`
	Topic  Topic     `json:"source" validate:"required"                               `
	Filter Filter    `json:"filter"`
	Config Config    `json:"config"`

	// Runtime elements
	Destination Destination
	consumer    *kafka.Consumer
	logger      *slog.Logger
	listeners   []SubscriptionListener
}

var ErrSendFailed = errors.New("send transactionally failed")
var ErrContextCancelled = errors.New("context cancelled")

func NewSubscription() Subscription {
	return Subscription{logger: slog.Default()}
}

func NewSubscriptionFromJSON(data []byte) (Subscription, error) {

	var s configuration.Subscription
	err := json.Unmarshal(data, &s)
	if err != nil {
		return Subscription{}, err
	}
	topic := Topic{Topic: s.Source[0].Topic}
	filter := Filter{JMESFilter: s.Source[0].JmesFilters[0]}

	sub := Subscription{
		Name:   s.Name,
		ID:     uuid.New(),
		Topic:  topic,
		Filter: filter,
		Config: Config{
			MaxWait:   time.Duration(s.Configuration.Batching.MaxBatchIntervalSeconds) * time.Second,
			BatchSize: s.Configuration.Batching.MaxBatchSize,
		},
	}
	return sub, err
}

func (s Subscription) WithLogger(logger *slog.Logger) Subscription {
	s.logger = logger
	return s
}

func (s *Subscription) AddListener(l SubscriptionListener) {
	s.listeners = append(s.listeners, l)
}

func (s Subscription) GroupID() string {
	return fmt.Sprintf("webhookd-%s", s.ID)
}

func (s *Subscription) Start(kafkaServers string) error {
	var err error
	s.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServers,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"group.id":              s.GroupID(),
		"auto.offset.reset":     "earliest", // Start reading from the end of the topic
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
		return err
	}
	s.logger.Info("start consumer", slog.String("bootstrap.servers", kafkaServers), slog.String("consumer_id", s.ID.String()), slog.String("group_id", s.GroupID()), slog.String("topic", s.Topic.Topic))
	return s.consumer.SubscribeTopics([]string{s.Topic.Topic}, nil)
}

func (s *Subscription) Consume(ctx context.Context) {
	s.logger.Info("consume loop started", slog.String("consumer_id", s.ID.String()))
	defer s.logger.Info("consume loop finished", slog.String("consumer_id", s.ID.String()))
	for l := range s.listeners {
		s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventStart, Subscription: s})
	}
	batch := make([]*kafka.Message, 0)
	run := true

	pushTicker := time.NewTicker(s.Config.MaxWait)
	for run {
		select {
		case <-ctx.Done():
			s.logger.Info("consume loop terminating", slog.String("consumer_id", s.ID.String()))
			return
		case <-pushTicker.C:
			if len(batch) > 0 {
				var err error
				err = s.sendBatch(ctx, batch)
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
				// s.logger.Debug("received msg", slog.String("consumer_id", s.ID.String()), slog.String("topic", *e.TopicPartition.Topic), slog.String("key", string(e.Key)))
				batch = append(batch, e)
				if len(batch) < s.Config.BatchSize {
					continue
				}
				var err error
				err = s.sendBatch(ctx, batch)
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
					s.logger.Info("consumer existing, all brokers down", slog.Any("kafka_error", ev.(kafka.Error)))
				} else {
					s.logger.Debug("ignore", slog.String("consumer_id", s.ID.String()), slog.Any("kafka_error", ev.(kafka.Error)))
				}

			default:
				s.logger.Debug("ignore kafka event", slog.String("consumer_id", s.ID.String()), slog.Any("kafka_event", ev.(kafka.Error)))
			}
		}
	}

	for l := range s.listeners {
		s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventStop, Subscription: s})
	}
}

// sendBatch sends a batch of messages to the destination and commits the offset, if it returns an error
// caller should check the run flag to see if the consumer should continue
func (s *Subscription) sendBatch(ctx context.Context, batch []*kafka.Message) error {
	err := s.SendTransactionally(ctx, batch)
	if err != nil {
		for l := range s.listeners {
			s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventError, Subscription: s, Error: err})
		}
		s.logger.Info("consumer stopping, send to destination failed", slog.Any("error", err))
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
			s.logger.Info("consumer stopping, unable to store offset", slog.Any("error", err))
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
			s.logger.Info("consumer sent batch ok", slog.String("consumer_id", s.ID.String()))
			return nil
		}
		for l := range s.listeners {
			s.listeners[l].SubscriptionEvent(SubscriptionEvent{Type: SubscriptionEventBatchSentNACK, Subscription: s, Error: err})
		}
		return err
	}
}
