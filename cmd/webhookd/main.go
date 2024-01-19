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

	"github.com/davidoram/webhookd/adapter"
	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/view"
	"github.com/davidoram/webhookd/web"
	"github.com/google/uuid"
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
	subChanges := make(chan core.SubscriptionSetEvent)

	// Create a subscription manager that will manage the subscriptions
	// and their lifecycle, and start it
	manager := core.NewSubscriptionManager(*kafkaBootstrapServers)
	// start the subscription manager in a goroutine
	go manager.Start(ctx, subChanges)
	defer manager.Close()
	slog.Info("started subscription manager", slog.String("kafka", *kafkaBootstrapServers))

	// Start a goroutine that will poll the database for changes to the subscriptions
	go GenerateSubscriptionChanges(ctx, db, subChanges, *pollingPeriod)
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

// This function takes a database connection and channel of core.SubscriptionSetEvent,
// it retrieves all the subscriptions in the database and sending them to the channel as NewSubscriptionEvent
// then it loops, detecting any changes to the subscriptions in the database and sends them to the channel
// as NewSubscriptionEvent, UpdatedSubscriptionEvent or DeletedSubscriptionEvent.
// It exists when the context is cancelled, or an error occurs.
func GenerateSubscriptionChanges(ctx context.Context, db *sql.DB, subChanges chan core.SubscriptionSetEvent, pollingPeriod time.Duration) error {

	vSubs := view.SubscriptionCollection{
		Subscriptions: []view.Subscription{},
		Offset:        0,
		Limit:         100,
	}

	// Create a map of the subscriptions by ID
	cSubMap := map[uuid.UUID]core.Subscription{}

	// Read the subscriptions in batches, until all subscriptions have been read
	maxUpdatedAt := time.Time{}
	var err error
	for {
		// Get all the subscriptions from the database
		vSubs, err = core.GetActiveSubscriptions(ctx, db, vSubs.Offset, vSubs.Limit)
		if err != nil {
			return err
		}
		slog.Info("reading subscriptions from database",
			slog.Any("count", len(vSubs.Subscriptions)),
			slog.Any("offset", vSubs.Offset),
			slog.Any("limit", vSubs.Limit))

		// Save each subscription to the map, send each subscription to the channel as a NewSubscriptionEvent,
		// and find the max updated_at time/ so we can detect any new subscriptions added to the database
		for _, vsub := range vSubs.Subscriptions {
			csub, err := adapter.ViewToCoreAdapter(vsub)
			if err != nil {
				return err
			}
			cSubMap[vsub.ID] = csub

			slog.Info("subscription set event", slog.String("type", string(core.NewSubscriptionEvent)), slog.Any("id", csub.ID), slog.String("name", csub.Name))
			subChanges <- core.SubscriptionSetEvent{
				Type:         core.NewSubscriptionEvent,
				Subscription: &csub,
			}

			if vsub.UpdatedAt.After(maxUpdatedAt) {
				maxUpdatedAt = vsub.UpdatedAt
			}
		}

		// If we have read all the subscriptions, exit the loop
		if len(vSubs.Subscriptions) == 0 {
			break
		}

		// Update the offset, so we can read the next batch of subscriptions
		vSubs.Offset = vSubs.Offset + vSubs.Limit
	}

	// start a loop that will run until the context is cancelled, or an error occurs
	// the loop will detect any changes to the subscriptions in the database and send them to the channel
	// as NewSubscriptionEvent, UpdatedSubscriptionEvent or DeletedSubscriptionEvent
	for {
		slog.Info("polling database for subscription changes")
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Get all the subscriptions from the database, updated since the maxUpdatedAt time
		vSubs, err := core.GetSubscriptionsUpdatedSince(ctx, db, maxUpdatedAt, 0, 100)
		if err != nil {
			return err
		}

		// Create a map of the 'updated' subscriptions by ID
		changedSubMap := map[uuid.UUID]core.Subscription{}
		for _, vSub := range vSubs.Subscriptions {
			csub, err := adapter.ViewToCoreAdapter(vSub)
			if err != nil {
				return err
			}
			changedSubMap[vSub.ID] = csub
		}

		// Process any changes to the subscriptions
		for id, csub := range changedSubMap {

			// New if not in the subMap
			if _, ok := cSubMap[id]; !ok {

				// Update the map
				cSubMap[csub.ID] = csub

				slog.Info("subscription set event", slog.String("type", string(core.NewSubscriptionEvent)), slog.Any("id", csub.ID), slog.String("name", csub.Name))
				// Publish the change to the channel
				subChanges <- core.SubscriptionSetEvent{
					Type:         core.NewSubscriptionEvent,
					Subscription: &csub,
				}

			} else {
				// Updated if IsActive
				if csub.IsActive() {
					// Update the map
					cSubMap[csub.ID] = csub

					slog.Info("subscription set event", slog.String("type", string(core.UpdatedSubscriptionEvent)), slog.Any("id", csub.ID), slog.String("name", csub.Name))
					// Publish the change to the channel
					subChanges <- core.SubscriptionSetEvent{
						Type:         core.UpdatedSubscriptionEvent,
						Subscription: &csub,
					}

				} else {
					// Subscription deleted, remove from the map
					delete(cSubMap, csub.ID)

					slog.Info("subscription set event", slog.String("type", string(core.DeletedSubscriptionEvent)), slog.Any("id", csub.ID), slog.String("name", csub.Name))
					// Publish the change to the channel
					subChanges <- core.SubscriptionSetEvent{
						Type:         core.DeletedSubscriptionEvent,
						Subscription: &csub,
					}

				}
			}
			// Update the maxUpdatedAt time
			if csub.UpdatedAt.After(maxUpdatedAt) {
				maxUpdatedAt = csub.UpdatedAt
			}
		}
		// Sleep for the polling period
		time.Sleep(pollingPeriod)
	}
}
