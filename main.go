package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
)

type Source struct {
	Topic      string `json:"kind"`
	JMESFilter string `json:"jmes_filter"`
}

type Config struct {
	BatchSize int `json:"batch_size"`
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
