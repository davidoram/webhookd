package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type Subscription struct {
	Name        string    `json:"name"`
	ID          uuid.UUID `json:"id"`
	Destination Destination
	Source      Source `json:"source"`
	Config      Config `json:"config"`

	// Runtime elements
	consumer *kafka.Consumer
	logger   *slog.Logger
}

type Destination interface {
	// Send messages and return if the desination has accepted the messages, and is ready for the next batch
	// Will be called repeatedly with the same messages until the Destination returns true to indicate that
	// the destination has accepted the messages, and is ready to receive the next batch
	Send([]*kafka.Message) bool
}

type Source struct {
	Topic      string `json:"kind"`
	JMESFilter string `json:"jmes_filter"`
}

type Config struct {
	BatchSize int `json:"batch_size"`
}

type Actvity struct {
	CreatedAt time.Time `json:"created_at"`
	Message   string    `json:"message"`
}

type Options struct {
	KafkaBootstrapServers string
}

func main() {

	// Parse command line arguments into Options struct
	options, err := parseArgs()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Get subscriptions from the database
	subscription := NewSubscription()

	subscription.Name = "test"
	subscription.ID = uuid.New()
	subscription.Destination = NewWebhook("http://localhost:8080/1/EventNotifications")
	subscription.Source = Source{
		Topic:      "topic-1",
		JMESFilter: "",
	}
	subscription.Config = Config{
		BatchSize: 10,
	}

	subscriptions := []Subscription{subscription}
	StartSubscriptions(ctx, subscriptions, options.KafkaBootstrapServers)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

}

func parseArgs() (*Options, error) {
	options := &Options{}

	flag.StringVar(&options.KafkaBootstrapServers, "kafka-bootstrap-servers", "", "Kafka bootstrap servers")
	flag.Parse()

	if options.KafkaBootstrapServers == "" {
		return options, fmt.Errorf("error: Kafka bootstrap servers must be specified\n")
	}

	return options, nil
}

func StartSubscriptions(ctx context.Context, subscriptions []Subscription, kafkaServers string) error {
	for _, s := range subscriptions {
		if err := s.Start(kafkaServers); err != nil {
			return err
		}
		go s.Consume(ctx)
	}
	return nil
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
	s.logger.Info("start consumer", slog.String("bootstrap.servers", kafkaServers), slog.String("consumer_id", s.ID.String()), slog.String("group_id", s.GroupID()), slog.String("topic", s.Source.Topic))
	return s.consumer.SubscribeTopics([]string{s.Source.Topic}, nil)
}

func (s *Subscription) Consume(ctx context.Context) {
	s.logger.Info("consume loop started", slog.String("consumer_id", s.ID.String()))
	defer s.logger.Info("consume loop finished", slog.String("consumer_id", s.ID.String()))

	batch := make([]*kafka.Message, 0)
	run := true
	for run == true {
		select {
		case <-ctx.Done():
			s.logger.Info("consume loop terminating", slog.String("consumer_id", s.ID.String()))
			return
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
				err := s.SendTransactionally(ctx, batch)
				if err != nil {
					run = false
					s.logger.Info("consumer stopping, send to destination failed", slog.Any("error", err))
				} else {
					s.logger.Info("consumer committing offsets")
					_, err := s.consumer.CommitMessage(e)
					if err != nil {
						run = false
						s.logger.Info("consumer stopping, unable to store offset", slog.Any("error", err))
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
