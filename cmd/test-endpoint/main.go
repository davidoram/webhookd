// A simple HTTP server that accepts POST requests with a JSON body containing a batch of messages, and saves them to an SQLite database.
// It also has a graceful shutdown mechanism that will wait for the server to finish handling requests before exiting.
// The database is initialised and truncated on startup.
// It exposes the following endpoints:
// POST /data - Save a batch of messages to the database
// GET /total - Get the total number of messages saved to the database
// GET /progress?duration=1m - Get the number of messages saved to the database in the last minute where '1m' can be replaced by any duration string parsable by time.ParseDuration
// POST /shutdown - Gracefully shutdown the server
// POST /subscribe?topics=3 - Subscribe to  'topic.1,topic.2,topic.3' where '3' can be replaced by any number of topics

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/davidoram/webhookd/view"
	_ "github.com/mattn/go-sqlite3"

	// Routing proposal extension to go stdlib in go 1.22
	// See: https://github.com/golang/go/issues/61410
	//      https://benhoyt.com/writings/go-servemux-enhancements/
	"github.com/jba/muxpatterns"
)

var (
	sma30     = SimpleMovingAverage(30)
	newMsgCnt = 0
)

func main() {

	slog.Info("test-endpoint start")
	defer slog.Info("test-endpoint exited")

	// Parse command line arguments
	dbURL := flag.String("db", "file:/data/test-endpoint.db?vacuum=1", "URL connection to the SQLite database")
	httpAddress := flag.String("http-address", ":8081", "Host and port to start webserver on, defaults to ':8081'")
	webhookdAddress := flag.String("webhookd-endpoint", "http://webhookd:8080", "Host and port of the webhookd server, defaults to 'http://webhookd:8080'")
	topics := flag.Int("topics", 3, "Subscribe to 'topic.1,topic.2,topic.3' where '3' can be replaced by any number of topics, default is 3")
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
	err = MigrateDB(ctx, db)
	if err != nil {
		slog.Error("migrating db", slog.Any("error", err))
		os.Exit(1)
	}
	slog.Info("db migrated ok")

	err = TruncateDB(ctx, db)
	if err != nil {
		slog.Error("truncate db", slog.Any("error", err))
		os.Exit(1)
	}

	// Ping the webhookd server to check it is running
	_, err = http.Get(*webhookdAddress + "/ping")
	if err != nil {
		slog.Error("ping webhookd server", slog.Any("error", err), slog.String("url", *webhookdAddress))
		os.Exit(1)
	}

	// Set up a channel to receive signals
	done := make(chan os.Signal, 1)

	// Start a web server to handle API requests, that will quit when the context is cancelled
	hctx := HandlerContext{Db: db, done: done}
	mux := muxpatterns.NewServeMux()
	mux.HandleFunc("POST /data", func(w http.ResponseWriter, r *http.Request) {
		// Pass the context to the handler
		hctx.SaveMessages(w, r, ctx)
	})
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		hctx.done <- syscall.SIGTERM
		slog.Info("shutdown from API call")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("shutting down"))
		// Send a signal to the done channel to trigger a graceful shutdown
	})
	mux.HandleFunc("GET /statistics", func(w http.ResponseWriter, r *http.Request) {
		// Pass the context to the handler
		hctx.GetStatistics(w, r, ctx)
	})

	slog.Info("http server starting", slog.Any("address", *httpAddress))
	server := &http.Server{
		Addr:    *httpAddress,
		Handler: mux,
	}
	go server.ListenAndServe()

	// Subscribe to the topics
	slog.Info("subscribing to topics", slog.String("topics", fmt.Sprintf("topic.1 ... topic.%d", *topics)))
	for i := 1; i <= *topics; i++ {
		topic := fmt.Sprintf("topic.%d", i)
		sub := view.Subscription{
			SubscriptionData: view.SubscriptionData{
				Name:   fmt.Sprintf("Test - subscription %d", i),
				Active: true,
				Destination: view.Destination{
					Kind: "webhook",
					Webhook: view.Webhook{
						URL: "http://" + server.Addr + "/data",
					},
				},
				Source: []view.Source{
					{
						Topic: topic,
					},
				},
				Configuration: view.Configuration{
					Batching: view.Batching{
						MaxBatchSize:            50,
						MaxBatchIntervalSeconds: 10,
					},
					Retry: view.Retry{
						MaxRetries:     3,
						RetryAlgorithm: "exponential_backoff",
					},
					Alerting: view.Alerting{
						AlertChannel: "none",
					},
				},
			},
		}
		body, err := json.Marshal(sub)
		if err != nil {
			slog.Error("Error marshalling subscription", slog.Any("error", err))
			os.Exit(1)
		}
		url := *webhookdAddress + "/1/subscriptions"
		resp, err := http.Post(url, "application+json", bytes.NewReader(body))
		if err != nil {
			slog.Error("subscription API call returned error", slog.Any("url", url), slog.Any("error", err))
			os.Exit(1)
		}
		if resp.StatusCode != http.StatusCreated {
			slog.Error("subscription API call returned unexpected status", slog.Any("url", url), slog.Any("status", resp.StatusCode))
			os.Exit(1)
		}
		// Read the response body to ensure the connection is closed
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			slog.Error("error reading response body", slog.Any("url", url), slog.Any("error", err))
			os.Exit(1)
		}
		resp.Body.Close()

		slog.Info("subscription API call ok", slog.Any("url", url), slog.Any("status", resp.StatusCode), slog.String("body", string(body)))
	}

	// Start a ticker to log the new messages saved to the database in the last period
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				sma30(float64(newMsgCnt))
				newMsgCnt = 0
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	// Exit on these signals
	signals := []os.Signal{syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT}
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

type Message struct {
	Topic   string            `json:"topic"`
	Key     string            `json:"key"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers"`
}

func (m Message) HeadersAsStr() string {
	// sort the headers by key
	keys := make([]string, 0, len(m.Headers))
	for k := range m.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// build the string
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, m.Headers[k])
	}
	return b.String()
}

type MessageBatch struct {
	Messages []Message `json:"messages"`
}

type HandlerContext struct {
	Db   *sql.DB
	done chan os.Signal
}

func MigrateDB(ctx context.Context, db *sql.DB) error {

	// Create the messages table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			key TEXT PRIMARY KEY,
			topic TEXT NOT NULL,  
			value TEXT NOT NULL,
			headers TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS duplicate_messages (
			key TEXT NOT NULL,
			topic TEXT NOT NULL,  
			value TEXT NOT NULL,
			headers TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}

func TruncateDB(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DELETE FROM messages`)
	return err
}

func InsertMessageBatch(ctx context.Context, db *sql.DB, batch MessageBatch) error {
	// Start a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Insert each message in the batch
	for _, msg := range batch.Messages {
		// Check if the message already exists
		var exists bool
		err = tx.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM messages WHERE key = ?)`,
			msg.Key).Scan(&exists)
		if err != nil {
			tx.Rollback()
			return err
		}
		if exists {
			// Insert the message into the duplicate_messages table
			_, err = tx.ExecContext(ctx,
				`INSERT INTO duplicate_messages (topic, key, value, headers) VALUES (?, ?, ?, ?)`,
				msg.Topic,
				msg.Key,
				msg.Value,
				msg.HeadersAsStr())
			if err != nil {
				tx.Rollback()
				return err
			}
			continue
		}
		// Insert the message into the database
		_, err = tx.ExecContext(ctx,
			`INSERT INTO messages (topic, key, value, headers) VALUES (?, ?, ?, ?)`,
			msg.Topic,
			msg.Key,
			msg.Value,
			msg.HeadersAsStr())
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	// Commit the transaction
	err = tx.Commit()
	return err
}

// SaveMessages handles POST requests to save batches of messages to the database
// The request body should be a JSON array of messages
// It returns a 201 Created response with the messages in the body.
func (hctx HandlerContext) SaveMessages(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		slog.Error("Error reading request body", slog.Any("error", err))
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	var batch MessageBatch
	err = json.Unmarshal(body, &batch)
	if err != nil {
		slog.Error("Error unmarshalling request body", slog.Any("error", err))
		http.Error(w, "Error unmarshalling request body", http.StatusBadRequest)
		return
	}

	// Save the messagebatch to the database
	err = InsertMessageBatch(ctx, hctx.Db, batch)
	if err != nil {
		slog.Error("Error inserting message batch", slog.Any("error", err))
		http.Error(w, "Error inserting message batch", http.StatusInternalServerError)
		return
	}

	// Update the message count
	newMsgCnt += len(batch.Messages)
	slog.Info("received batch of messages", slog.Any("batch_size", len(batch.Messages)))
	w.WriteHeader(http.StatusCreated)
	w.Write(body)
}

// GetStatistics returns the total number of messages saved to the database &
// the average number of messages saved per second over the last 30 seconds
func (hctx HandlerContext) GetStatistics(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	tot, err := CountMessages(ctx, hctx.Db)
	if err != nil {
		slog.Error("Error counting messages", slog.Any("error", err))
		http.Error(w, "Error counting messages", http.StatusInternalServerError)
		return
	}

	payload := fmt.Sprintf(`{"total": %d, "average": %f}`, tot, sma30(0))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(payload))
}

// CountMessages returns the total number of messages saved to the database
func CountMessages(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&count)
	return count, err
}

// SimpleMovingAverage takes a period, it keeps a record of that many values, and
// returns a function that will calculate the simple moving average over that period
// See https://rosettacode.org/wiki/Averages/Simple_moving_average
func SimpleMovingAverage(period int) func(float64) float64 {
	var i int
	var sum float64
	var storage = make([]float64, 0, period)
	var m = sync.Mutex{}

	return func(input float64) (avrg float64) {
		m.Lock()
		defer m.Unlock()
		if len(storage) < period {
			sum += input
			storage = append(storage, input)
		}

		sum += input - storage[i]
		storage[i], i = input, (i+1)%period
		avrg = sum / float64(len(storage))

		return
	}
}
