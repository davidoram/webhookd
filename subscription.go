package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type Subscription struct {
	Name        string    `json:"name"`
	ID          uuid.UUID `json:"id"`
	Destination Destination
	Topic       string `json:"topic"`
	Config      Config `json:"config"`

	// Runtime elements
	consumer *kafka.Consumer
	logger   *slog.Logger
	emitter  Emitter
}

func NewSubscription(name string, id uuid.UUID, kafkaServers string) Subscription {
	return Subscription{
		Name: name,
		ID:   id,
		Config: Config{
			BatchSize:    50,
			KafkaServers: kafkaServers,
		},
		logger: slog.Default(),
	}
}

func (s Subscription) WithLogger(l *slog.Logger) Subscription {
	s.logger = l
	return s
}

func (s Subscription) WithDestination(d Destination) Subscription {
	s.Destination = d
	return s
}

func (s Subscription) WithTopic(t string) Subscription {
	s.Topic = t
	return s
}

func (s Subscription) WithConfig(c Config) Subscription {
	s.Config = c
	return s
}

func (s Subscription) GroupID() string {
	return fmt.Sprintf("webhookd-%s", s.ID)
}

func (s Subscription) EventHook() *Emitter {
	return &s.emitter
}

func (s *Subscription) Start(ctx context.Context) error {
	var err error
	s.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": s.Config.KafkaServers,
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
	err = s.consumer.SubscribeTopics([]string{s.Topic}, nil)
	if err != nil {
		return err
	}
	//s.logger.Info("start consumer", slog.String("bootstrap.servers", kafkaServers), slog.String("consumer_id", s.ID.String()), slog.String("group_id", s.GroupID()), slog.String("topic", s.Source.Topic))
	s.emitter.Emit(Event{SubscriptionID: s.ID, Type: ConsumerCreated, CreatedAt: time.Now()})
	return nil
}

func (s *Subscription) Consume(ctx context.Context) {
	// s.logger.Info("consume loop started", slog.String("consumer_id", s.ID.String()))
	s.emitter.Emit(Event{SubscriptionID: s.ID, Type: ConsumerStarted, CreatedAt: time.Now()})
	defer s.emitter.Emit(Event{SubscriptionID: s.ID, Type: ConsumerStopped, CreatedAt: time.Now()})

	// defer s.logger.Info("consume loop finished", slog.String("consumer_id", s.ID.String()))

	batch := make([]*kafka.Message, 0)
	run := true
	for run {
		select {
		case <-ctx.Done():
			return
		default:
			ev := s.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				s.emitter.Emit(Event{SubscriptionID: s.ID, Type: MessageReceived, CreatedAt: time.Now()})
				batch = append(batch, e)
				if len(batch) < s.Config.BatchSize {
					continue
				}
				err := s.SendTransactionally(ctx, batch)
				if err != nil {
					run = false
					s.emitter.Emit(Event{SubscriptionID: s.ID, Type: BatchSentError, CreatedAt: time.Now(), Error: err})

				} else {
					_, err := s.consumer.CommitMessage(e)
					if err != nil {
						run = false
						s.emitter.Emit(Event{SubscriptionID: s.ID, Type: BatchCommitError, CreatedAt: time.Now(), Error: err})
					}
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
					s.emitter.Emit(Event{SubscriptionID: s.ID, Type: KafkaOffline, CreatedAt: time.Now(), Error: ev.(kafka.Error)})
				} else {
					if s.logger.Enabled(ctx, slog.LevelDebug) {
						s.logger.Debug("kafka_error ignored", slog.String("consumer_id", s.ID.String()), slog.Any("kafka_error", ev.(kafka.Error)))
					}
				}

			default:
				if s.logger.Enabled(ctx, slog.LevelDebug) {
					s.logger.Debug("kafka_event ignored", slog.String("consumer_id", s.ID.String()), slog.Any("kafka_error", ev.(kafka.Error)))
				}
			}
		}
	}
}

func (s *Subscription) SendTransactionally(ctx context.Context, msgs []*kafka.Message) error {
	// Add a Timeout to the context so we don't send forever
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if s.Destination.Send(msgs) {
				s.emitter.Emit(Event{SubscriptionID: s.ID, Type: BatchSentOK, CreatedAt: time.Now()})
				return nil
			}
			s.emitter.Emit(Event{SubscriptionID: s.ID, Type: BatchSentError, CreatedAt: time.Now()})
			time.Sleep(2 * time.Second)
		}
	}
}
