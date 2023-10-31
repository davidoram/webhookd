package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type Subscription struct {
	Name        string      `json:"name"`
	ID          uuid.UUID   `json:"id"`
	Destination Destination `json:"destination"`
	Sources     []Source    `json:"sources"`
	Config      Config      `json:"config"`

	// Runtime elements
	consumer *kafka.Consumer
}

type Destination struct {
	Kind       string `json:"kind"`
	WebhookURL string `json:"url"`
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
	subscription := Subscription{
		Name: "test",
		ID:   uuid.New(),
		Destination: Destination{
			Kind:       "Webhook",
			WebhookURL: "http://localhost:8080/1/EventNotifications",
		},
		Sources: []Source{
			Source{
				Topic:      "topic-1",
				JMESFilter: "",
			},
		},
		Config: Config{
			BatchSize: 10,
		},
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
		return options, fmt.Errorf("Error: Kafka bootstrap servers must be specified\n")
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

func (s Subscription) GroupID() string {
	return fmt.Sprintf("webhookd-%s", s.ID)
}

func (s Subscription) Topics() []string {
	// return a list of topics to subscribe to
	topics := []string{}
	for _, source := range s.Sources {
		topics = append(topics, source.Topic)
	}
	return topics
}

func (s Subscription) Start(kafkaServers string) error {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaServers,
		"group.id":           s.GroupID(),
		"enable.auto.commit": "false",
	})
	if err != nil {
		return err
	}

	return consumer.SubscribeTopics(s.Topics(), nil)
}

func (s Subscription) processBatch(messages []*kafka.Message) error {
	// Process the messages here
	// Batch the messages
	// Send the batch through the webhook
	// Return an error if there was a problem
	return nil
}

func (s Subscription) Consume(ctx context.Context) {
	var batch []*kafka.Message
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Webhook %s terminated\n", s.ID)
			return
		default:
			ev := s.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var payload map[string]interface{}
				if err := json.Unmarshal(e.Value, &payload); err != nil {
					fmt.Printf("Error unmarshalling message: %v\n", err)
					continue
				}

				batch = append(batch, e)
				if len(batch) < s.Config.BatchSize {
					continue
				}
				err := s.processBatch(batch)
				if err == nil {
					offsets := make([]kafka.TopicPartition, len(batch))
					for _, e := range batch {
						offsets = append(offsets, e.TopicPartition)
					}
					s.consumer.CommitOffsets(offsets)
				}
			case kafka.Error:
				fmt.Printf("Ignore Kafka error: %v\n", e)

			default:
				fmt.Printf("Ignore Kafka event: %v", e)
			}
		}
	}
}
