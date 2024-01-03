package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"log/slog"
	_ "net/http/pprof" // Register the pprof handlers. Run 'go tool pprof "http://localhost:8080/debug/pprof/profile?seconds=30"' to capture profiles
	"os"
	"os/signal"
	"syscall"
)

type Options struct {
	KafkaBootstrapServers string
}

func main() {

	slog.Info("starting webhookd")

	// Parse command line arguments
	dbFile := flag.String("db", "input.db", "Path to the database file")
	// kafkaBootstrapServers := flag.String("kafka", "localhost:9092", "Kafka bootstrap servers")
	flag.Parse()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Open the SQLite database
	db, err := sql.Open("sqlite3", *dbFile)
	if err != nil {
		slog.Error("Error opening the database", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("database open")

	// Migrate the database
	err = MigrateDB(ctx, db)
	if err != nil {
		log.Printf("Error migrating database: %s", err)
		os.Exit(1)
	}
	slog.Info("database migrated")

	// // Get subscriptions from the database
	// subscription := NewSubscription()

	// subscription.Name = "test"
	// subscription.ID = uuid.New()
	// subscription.Destination = NewWebhook("http://localhost:8080/1/EventNotifications")
	// subscription.Topic = Topic{
	// 	Topic: "topic-1",
	// }
	// subscription.Config = Config{
	// 	BatchSize: 10,
	// }.WithMaxWait(time.Second * 10)

	// subscriptions := []Subscription{subscription}
	// StartSubscriptions(ctx, subscriptions, options.KafkaBootstrapServers)

	// signals := make(chan os.Signal, 1)
	// signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Exit on these signals
	signals := []os.Signal{syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT}
	done := make(chan os.Signal, 1)
	signal.Notify(done, signals...)
	go func() {
		sig := <-done
		slog.Info("exiting with signal", slog.String("signal", sig.String()))
	}()

	// Wait for a signal to exit
	<-done

}

// func StartSubscriptions(ctx context.Context, subscriptions []Subscription, kafkaServers string) error {
// 	for _, s := range subscriptions {
// 		if err := s.Start(kafkaServers); err != nil {
// 			return err
// 		}
// 		go s.Consume(ctx)
// 	}
// 	return nil
// }
