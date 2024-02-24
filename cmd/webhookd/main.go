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
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/davidoram/webhookd/adapter"
	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/web"
	_ "github.com/mattn/go-sqlite3"

	// Routing proposal extension to go stdlib in go 1.22
	// See: https://github.com/golang/go/issues/61410
	//      https://benhoyt.com/writings/go-servemux-enhancements/
	"github.com/jba/muxpatterns"
)

func main() {

	slog.Info("webhookd start")
	defer slog.Info("webhookd exited")

	// Parse command line arguments
	dbURL := flag.String("db", "file:/data/webhookd.db?vacuum=1", "URL connection to the SQLite database")
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
	slog.Info("db open ok", slog.String("url", *dbURL))

	// Migrate the database
	err = core.MigrateDB(ctx, db)
	if err != nil {
		slog.Error("migrating db", slog.Any("error", err))
		os.Exit(1)
	}
	slog.Info("db migrated ok")

	// Create a subscription change channel that will be used to notify
	// the subscription manager of changes to the subscriptions
	subChanges := make(chan core.SubscriptionChangeEvent)

	// Create a subscription manager that will manage the subscriptions
	// and their lifecycle, and start it
	manager := core.NewSubscriptionManager(*kafkaBootstrapServers)
	// start the subscription manager in a goroutine
	go manager.Start(ctx, subChanges)
	slog.Info("started subscription manager", slog.String("kafka", *kafkaBootstrapServers))

	// Start a goroutine that will poll the database for changes to the subscriptions
	go GenerateSubscriptionChangesWithRecovery(ctx, db, subChanges, *pollingPeriod)
	slog.Info("listening for subscription changes")

	// Start a web server to handle API requests, that will quit when the context is cancelled
	hctx := web.HandlerContext{Db: db}
	mux := muxpatterns.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) {
		// Return a 200 OK
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
		slog.Info("GET /ping ok")
	})

	mux.HandleFunc("POST /1/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		// Pass the context to the handler
		hctx.PostSubscriptionHandler(w, r, ctx)
	})
	mux.HandleFunc("GET /1/subscriptions/{id}", func(w http.ResponseWriter, r *http.Request) {
		// Pass the context to the handler
		hctx.ShowSubscriptionHandler(w, r, ctx)
	})
	mux.HandleFunc("DELETE /1/subscriptions/{id}", func(w http.ResponseWriter, r *http.Request) {
		// Pass the context to the handler
		hctx.DeleteSubscriptionHandler(w, r, ctx)
	})
	mux.HandleFunc("GET /1/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		// Pass the context to the handler
		hctx.ListSubscriptionsHandler(w, r, ctx)
	})

	slog.Info("http server starting", slog.Any("address", *httpAddress))
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

func GenerateSubscriptionChangesWithRecovery(ctx context.Context, db *sql.DB, subChanges chan core.SubscriptionChangeEvent, pollingPeriod time.Duration) {
	var wg sync.WaitGroup
	errorOrPanic := false
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Recovered from panic", slog.Any("error", r))
				debug.PrintStack()
				errorOrPanic = true
			}
		}()
		GenerateSubscriptionChanges(ctx, db, subChanges, pollingPeriod)
	}()

	wg.Wait()
	if errorOrPanic {
		os.Exit(1) // TODO wrap in interface
	}
}

// This function takes a database connection and channel of core.SubscriptionSetEvent,
// it retrieves all the subscriptions in the database and sending them to the channel as NewSubscriptionEvent
// then it loops, detecting any changes to the subscriptions in the database and sends them to the channel
// as NewSubscriptionEvent, UpdatedSubscriptionEvent or DeletedSubscriptionEvent.
// It exists when the context is cancelled, or an error occurs.
func GenerateSubscriptionChanges(ctx context.Context, db *sql.DB, subChanges chan core.SubscriptionChangeEvent, pollingPeriod time.Duration) error {

	// Read the subscriptions in batches, until all subscriptions have been read
	maxUpdatedAt := time.Time{}
	offset := int64(0)
	limit := int64(100)
	slog.Info("reading active subscriptions from database")
	for {
		// Check if the context has been cancelled
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		// Get all the subscriptions from the database
		subs, err := core.GetActiveSubscriptions(ctx, db, offset, limit)
		if err != nil {
			return err
		}

		// Send each subscription to the channel,
		// and find the max updated_at time/ so we can detect any new subscriptions added to the database
		for _, vsub := range subs.Subscriptions {
			csub, err := adapter.ViewToCoreAdapter(vsub)
			if err != nil {
				return err
			}
			subChanges <- core.SubscriptionChangeEvent{Subscription: &csub}

			if vsub.UpdatedAt.After(maxUpdatedAt) {
				maxUpdatedAt = vsub.UpdatedAt
			}
		}

		// If we have read all the subscriptions, exit the loop
		if int64(len(subs.Subscriptions)) < limit {
			break
		}

		// Update the offset, so we can read the next batch of subscriptions
		offset = offset + limit
	}

	// start a loop that will run until the context is cancelled, or an error occurs
	// the loop will detect any changes to the subscriptions in the database and send them to the channel
	slog.Info("polling for updated subscriptions from database")
	offset = int64(0)
	limit = int64(100)
	for {
		// Check if the context has been cancelled
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		// Get all the subscriptions from the database, updated since the maxUpdatedAt time
		subs, err := core.GetSubscriptionsUpdatedSince(ctx, db, maxUpdatedAt, offset, limit)
		if err != nil {
			return err
		}

		if len(subs.Subscriptions) > 0 {
			slog.Info("found subscription changes in db", slog.Any("found", len(subs.Subscriptions)))

			for _, vsub := range subs.Subscriptions {
				csub, err := adapter.ViewToCoreAdapter(vsub)
				if err != nil {
					return err
				}

				slog.Info("subscription change event", slog.Any("id", csub.ID), slog.String("name", csub.Name), slog.Any("active", csub.IsActive()), slog.Any("updated_at", csub.UpdatedAt))
				// Publish the change to the channel
				subChanges <- core.SubscriptionChangeEvent{Subscription: &csub}

				// Update the maxUpdatedAt time
				if csub.UpdatedAt.After(maxUpdatedAt) {
					maxUpdatedAt = csub.UpdatedAt
				}
			}
		} else {
			// Only sleep if we didn't find any changes
			// Sleep for the polling period
			time.Sleep(pollingPeriod)
		}
	}
}
