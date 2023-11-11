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
	Topic       Topic  `json:"source"`
	Filter      Filter `json:"filter"`
	Config      Config `json:"config"`

	// Runtime elements
	consumer *kafka.Consumer
	logger   *slog.Logger
}

func NewSubscription() Subscription {
	return Subscription{logger: slog.Default()}
}

func (s Subscription) WithLogger(logger *slog.Logger) Subscription {
	s.logger = logger
	return s
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

	batch := make([]*kafka.Message, 0)
	run := true

	pushTicker := time.NewTicker(s.Config.MaxWait)
	for run == true {
		select {
		case <-ctx.Done():
			s.logger.Info("consume loop terminating", slog.String("consumer_id", s.ID.String()))
			return
		case <-pushTicker.C:
			if len(batch) > 0 {
				var err error
				err, run = s.sendBatch(ctx, batch)
				if err == nil {
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
				s.logger.Info("received msg", slog.String("consumer_id", s.ID.String()), slog.String("topic", *e.TopicPartition.Topic), slog.String("key", string(e.Key)))
				batch = append(batch, e)
				if len(batch) < s.Config.BatchSize {
					continue
				}
				var err error
				err, run = s.sendBatch(ctx, batch)
				if err == nil {
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
}

// sendBatch sends a batch of messages to the destination and commits the offset, if it returns an error
// caller should check the run flag to see if the consumer should continue
func (s *Subscription) sendBatch(ctx context.Context, batch []*kafka.Message) (error, bool) {
	run := true
	err := s.SendTransactionally(ctx, batch)
	if err != nil {
		run = false
		s.logger.Info("consumer stopping, send to destination failed", slog.Any("error", err))
	} else {
		for _, msg := range batch {
			if _, err = s.consumer.CommitMessage(msg); err != nil {
				break
			}
		}
		if err != nil {
			run = false
			s.logger.Info("consumer stopping, unable to store offset", slog.Any("error", err))
		}
	}
	return err, run
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
				// TODO Emit event - Send succesfull
				s.logger.Info("consumer sent batch ok", slog.String("consumer_id", s.ID.String()))
				return nil
			}
			// TODO Emit event - Send failed
			slog.Info("consumer sent batch failed, retrying", slog.String("consumer_id", s.ID.String()))
			time.Sleep(2 * time.Second)
		}
	}
}
