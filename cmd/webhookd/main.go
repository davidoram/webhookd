package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // Register the pprof handlers. Run 'go tool pprof "http://localhost:8080/debug/pprof/profile?seconds=30"' to capture profiles
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davidoram/webhookd/core"
	"github.com/google/uuid"

	"github.com/jba/muxpatterns" // Routing proposal extension to stdlib https://github.com/golang/go/issues/61410
)

func main() {

	slog.Info("webhookd start")
	defer slog.Info("webhookd exited")

	// Parse command line arguments
	dbURL := flag.String("db", "file:webhookd.db?vacuum=1", "URL connection to the SQLite database")
	kafkaBootstrapServers := flag.String("kafka", "localhost:9092", "Kafka bootstrap servers")
	pollingPeriod := flag.Duration("sub-poll", time.Second*10, "How long to wait between polling the database for changes to subscriptions, defaults to 10s")
	httpAddress := flag.String("http-address", ":8080", "Host and port to start webserver on, defaults to ':8080'")
	flag.Parse()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Open the SQLite database
	db, err := sql.Open("sqlite3", *dbURL)
	if err != nil {
		slog.Error("open db", slog.Any("error", err), slog.String("url", *dbURL))
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("db open ok")

	// Migrate the database
	err = core.MigrateDB(ctx, db)
	if err != nil {
		slog.Error("migrating db", slog.Any("error", err))
		os.Exit(1)
	}
	slog.Info("db migrated ok")

	// Create a subscription change channel that will be used to notify
	// the subscription manager of changes to the subscriptions
	subChanges := make(chan core.SubscriptionSetEvent)
	go GenerateSubscriptionChanges(ctx, db, subChanges, *pollingPeriod)

	// Create a subscription manager that will manage the subscriptions
	// and their lifecycle, and start it
	manager := core.NewSubscriptionManager(*kafkaBootstrapServers)
	manager.Start(ctx, subChanges)
	defer manager.Close()

	// Start a web server to handle API requests, that will quit when the context is cancelled
	hctx := core.HandlerContext{Db: db}
	mux := muxpatterns.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, r *http.Request) {
		// Pass the context to the handler
		hctx.PostSubscriptionHandler(w, r, ctx)
	})

	slog.Info("http server starting", slog.Any("address", httpAddress))
	server := &http.Server{
		Addr:    *httpAddress,
		Handler: mux,
	}
	go server.ListenAndServe()

	// Exit on these signals
	signals := []os.Signal{syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT}
	done := make(chan os.Signal, 1)
	signal.Notify(done, signals...)
	go func() {
		sig := <-done
		slog.Info("got signal", slog.String("signal", sig.String()))

		// Create a context with a timeout to force a graceful shutdown of the server
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Attempt to gracefully shut down the server
		if err := server.Shutdown(ctx); err != nil {
			slog.Info("failed http server shutdown", slog.Any("error", err))
		}
		server.Shutdown(ctx)
	}()

	// Wait for a signal to exit
	<-done
}

// This function takes a database connection and channel of core.SubscriptionSetEvent,
// it retrieves all the subscriptions in the database and sending them to the channel as NewSubscriptionEvent
// then it loops, detecting any changes to the subscriptions in the database and sends them to the channel
// as NewSubscriptionEvent, UpdatedSubscriptionEvent or DeletedSubscriptionEvent.
// It exists when the context is cancelled, or an error occurs.
func GenerateSubscriptionChanges(ctx context.Context, db *sql.DB, subChanges chan core.SubscriptionSetEvent, pollingPeriod time.Duration) error {

	// Get all the subscriptions from the database
	subs, err := core.GetActiveSubscriptions(ctx, db)
	if err != nil {
		return err
	}

	// Create a map of the subscriptions by ID
	subMap := map[uuid.UUID]core.Subscription{}

	// Save each subscription to the map, send each subscription to the channel as a NewSubscriptionEvent,
	// and find the max updated_at time/ so we can detect any new subscriptions added to the database
	maxUpdatedAt := time.Date(1990, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, sub := range subs {

		subMap[sub.ID] = sub

		subChanges <- core.SubscriptionSetEvent{
			Type:         core.NewSubscriptionEvent,
			Subscription: &sub,
		}

		if sub.UpdatedAt.After(maxUpdatedAt) {
			maxUpdatedAt = sub.UpdatedAt
		}
	}

	// start a loop that will run until the context is cancelled, or an error occurs
	// the loop will detect any changes to the subscriptions in the database and send them to the channel
	// as NewSubscriptionEvent, UpdatedSubscriptionEvent or DeletedSubscriptionEvent
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Get all the subscriptions from the database
		subs, err := core.GetSubscriptionsUpdatedSince(ctx, db, maxUpdatedAt)
		if err != nil {
			return err
		}

		// Create a map of the 'updated' subscriptions by ID
		changedSubMap := map[uuid.UUID]core.Subscription{}
		for _, sub := range subs {
			changedSubMap[sub.ID] = sub
		}

		// Process any changes to the subscriptions
		for id, sub := range changedSubMap {

			// New if not in the subMap
			if _, ok := subMap[id]; !ok {

				// Update the map
				subMap[sub.ID] = sub

				// Publish the change to the channel
				subChanges <- core.SubscriptionSetEvent{
					Type:         core.NewSubscriptionEvent,
					Subscription: &sub,
				}

			} else {
				// Updated if IsActive
				if sub.IsActive() {
					// Update the map
					subMap[sub.ID] = sub

					// Publish the change to the channel
					subChanges <- core.SubscriptionSetEvent{
						Type:         core.UpdatedSubscriptionEvent,
						Subscription: &sub,
					}

				} else {
					// Subscription deleted, remove from the map
					delete(subMap, sub.ID)

					// Publish the change to the channel
					subChanges <- core.SubscriptionSetEvent{
						Type:         core.DeletedSubscriptionEvent,
						Subscription: &sub,
					}

				}
			}
			// Update the maxUpdatedAt time
			if sub.UpdatedAt.After(maxUpdatedAt) {
				maxUpdatedAt = sub.UpdatedAt
			}
		}
		// Sleep for the polling period
		time.Sleep(pollingPeriod)
	}
}
