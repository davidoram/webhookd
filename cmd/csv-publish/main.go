package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/davidoram/webhookd/core"
	_ "github.com/mattn/go-sqlite3"
)

// Message struct represents a message in the database
type Message struct {
	ID                  int
	Topic               string
	Payload             string
	Key                 string
	PublishOffsetMillis int
	PublishedAt         sql.NullTime
}

func populateDatabase(db *sql.DB, csvFile string) (int, []string, error) {
	topics := []string{}
	file, err := os.Open(csvFile)
	if err != nil {
		return 0, topics, err
	}
	defer file.Close()

	// Read the CSV file
	reader := csv.NewReader(file)

	// Ignore the header row
	_, err = reader.Read()
	if err != nil {
		return 0, topics, err
	}

	records, err := reader.ReadAll()
	if err != nil {
		return 0, topics, err
	}

	// Create the messages table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY,
			topic TEXT,
			payload TEXT,
			key TEXT,
			publish_offset_millis INTEGER,
			published_at DATETIME
		)
	`)
	if err != nil {
		return 0, topics, err
	}

	// Truncate the messages table
	_, err = db.Exec(`DELETE FROM messages`)
	if err != nil {
		return 0, topics, err
	}

	// Ensure uniqueness of the 'key' column
	_, err = db.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_key ON messages (key)
	`)
	if err != nil {
		return 0, topics, err
	}

	// Insert records into the database
	const batchSize = 500
	var values []string
	var args []interface{}

	for i, record := range records {
		values = append(values, "(?, ?, ?, ?, NULL)")
		args = append(args, record[0], record[1], record[2], record[3])

		// If we have hit the batch size or we are at the end of the records slice, insert the batch
		if (i+1)%batchSize == 0 || i+1 == len(records) {
			query := fmt.Sprintf(`
				INSERT INTO messages (topic, payload, key, publish_offset_millis, published_at)
				VALUES %s
			`, strings.Join(values, ","))

			_, err := db.Exec(query, args...)
			if err != nil {
				return 0, topics, err
			}

			// Reset values and args
			values = values[:0]
			args = args[:0]
		}
	}

	// Fetch the distinct topics
	rows, err := db.Query(`SELECT DISTINCT topic FROM messages ORDER BY topic ASC`)
	if err != nil {
		return 0, topics, err
	}
	defer rows.Close()
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			return 0, topics, err
		}
		topics = append(topics, topic)
	}

	return len(records), topics, nil
}

func logProducerEvents(ctx context.Context, producer *kafka.Producer) {
	for {
		select {
		case le, ok := <-producer.Logs():
			if ok {
				slog.Info("producer log", slog.String("event", le.String()))
			}

		case <-ctx.Done():
			return
		}
	}
}

func logDeliveryReports(ctx context.Context, producer *kafka.Producer) {
	for {
		select {
		case e := <-producer.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					slog.Info("message delivery failed",
						slog.String("key", string(m.Key)),
						slog.String("topic", *m.TopicPartition.Topic),
						slog.String("error", m.TopicPartition.Error.Error()))
				}
			default:
				if e != nil {
					if ev.(kafka.Error).Code() == kafka.ErrAllBrokersDown {
						slog.Error("All brokers down", slog.String("error", ev.(kafka.Error).Error()))
						os.Exit(1)
					}
					slog.Info("Event ignored", slog.String("event", e.String()))
				}
			}
		case <-ctx.Done():
			return
		}

	}
}

func publishToKafka(producer *kafka.Producer, messages []Message) error {
	for _, message := range messages {
		topic := message.Topic
		payload := message.Payload
		key := message.Key

		// Construct the Kafka message
		kafkaMessage := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(payload),
			Key:            []byte(key),
		}

		// Produce the message to Kafka
		if err := producer.Produce(kafkaMessage, nil); err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				slog.Info("Queue full, pausing and retrying...")
				// If the queue is full, wait for 1 second and try again
				time.Sleep(1 * time.Second)
				if err := producer.Produce(kafkaMessage, nil); err != nil {
					slog.Info("Retry failed", slog.String("error", err.Error()))
					return err
				}
				slog.Info("... retried ok")
			} else {
				slog.Info("Kafka produce error", slog.String("error", err.Error()))
				return err
			}
		}
	}
	return nil
}

func fetchMessages(db *sql.DB, offsetMillis int64) ([]Message, error) {
	// Update published_at and publish to Kafka
	var messagesToPublish []Message

	// Fetch 100 rows where published_at is null and NOW() < start_time + publish_offset_millis
	rows, err := db.Query(`
	SELECT * FROM messages
	WHERE published_at IS NULL AND publish_offset_millis <= ?
	LIMIT 100
`, offsetMillis)
	if err != nil {
		slog.Info("Error querying the database", slog.String("error", err.Error()))
		return messagesToPublish, err
	}
	defer rows.Close()

	for rows.Next() {
		var message Message
		if err := rows.Scan(
			&message.ID,
			&message.Topic,
			&message.Payload,
			&message.Key,
			&message.PublishOffsetMillis,
			&message.PublishedAt,
		); err != nil {
			slog.Info("Error scanning rows", slog.String("error", err.Error()))
			return messagesToPublish, err
		}

		messagesToPublish = append(messagesToPublish, message)
	}
	return messagesToPublish, err
}

func main() {
	ctx := context.Background()
	// Read dbURL, csvFile, and kafkaBootstrapServers from command line arguments
	dbURL := flag.String("db", "file:/data/csv-publish.db", "Path to the database file, defaults to /data/csv-publish.db")
	csvFile := flag.String("csv", "/data/input.csv", "Path to the CSV file, defaults to /data/input.csv")
	kafkaBootstrapServers := flag.String("kafka", "localhost:9092", "Kafka bootstrap servers")
	profile := flag.Bool("profile", false, "Enable profiling")
	profileFile := flag.String("profile-file", "/data/csv-publish-cpu.prof", "Profiling output file, defaults to /data/csv-publish-cpu.prof")

	flag.Parse()
	slog.Info("csv-publish started")

	// Wait for Kafka to be available
	connected := core.TestConnection(*kafkaBootstrapServers)
	waitUntil := time.Now().Add(60 * time.Second)
	for !connected && time.Now().Before(waitUntil) {
		slog.Info("Waiting for Kafka to be available", slog.String("servers", *kafkaBootstrapServers))
		time.Sleep(5 * time.Second)
		connected = core.TestConnection(*kafkaBootstrapServers)
	}
	if !connected {
		slog.Error("Kafka not available", slog.String("servers", *kafkaBootstrapServers))
		os.Exit(1)
	}

	// Start profiling if enabled
	if *profile {
		f, err := os.Create(*profileFile)
		if err != nil {
			slog.Error("Error creating profiling file", slog.Any("error", err), slog.String("file", *profileFile))
			os.Exit(1)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			slog.Error("Error starting profile", slog.String("error", err.Error()))
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
		slog.Info("Profiling saved", slog.String("file", *profileFile))
	}

	// Open the SQLite database
	db, err := sql.Open("sqlite3", *dbURL)
	if err != nil {
		slog.Info("Error opening the database", slog.String("error", err.Error()))
		return
	}
	slog.Info("db open ok", slog.String("url", *dbURL))

	defer db.Close()

	// Populate the SQLite database
	total, topics, err := populateDatabase(db, *csvFile)
	if err != nil {
		slog.Info("Error populating the database: %s", err)
		return
	}
	slog.Info("loaded messages for publishing", slog.Int("total_msgs", total), slog.Int("total_topics", len(topics)))
	slog.Info("topics", slog.String("topics", strings.Join(topics, ", ")))

	// Set up Kafka producer configuration
	slog.Info("Connecting to Kafka", slog.String("servers", *kafkaBootstrapServers))
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"client.id":              "csv-publish",
		"bootstrap.servers":      *kafkaBootstrapServers,
		"linger.ms":              1000,
		"compression.type":       "none",
		"retries":                2,
		"go.batch.producer":      true,
		"acks":                   "all",
		"go.logs.channel.enable": true,
		//"debug":                  "broker,topic,msg", // To see all debug messages, uncomment this line
	})
	if err != nil {
		slog.Error("Error creating Kafka producer", slog.Any("error", err))
		return
	}

	// Start a goroutine to handle delivery reports
	go logDeliveryReports(ctx, producer)
	go logProducerEvents(ctx, producer)

	// Wait for message deliveries before shutting down producer
	defer func() {
		slog.Info("Flushing producer")
		unpublished := producer.Flush(15 * 1000) // 15 seconds
		slog.Info("Closing producer", slog.Int("unpublished_messages", unpublished))
		producer.Close()
	}()

	// Note the start time
	startTime := time.Now()
	nextLogTime := startTime.Add(10 * time.Second)

	// Loop to publish messages
	for {
		offsetTime := time.Now()
		offsetMillis := offsetTime.Sub(startTime).Milliseconds()

		// Fetch 100 rows where published_at is null and NOW() < start_time + publish_offset_millis
		messagesToPublish, err := fetchMessages(db, offsetMillis)
		if err != nil {
			slog.Error("Error fetchMessages", slog.Any("error", err))
			return
		}

		// Publish messages to Kafka
		if err := publishToKafka(producer, messagesToPublish); err != nil {
			slog.Info("Error publishing messages to Kafka", slog.Any("error", err))
			return
		}

		// Update published_at for the messages
		if err := markMessagesPublished(db, messagesToPublish, offsetTime); err != nil {
			slog.Info("Error marking as published", slog.Any("error", err))
			return
		}

		// Count how many rows remain unpublished, ie: published_at is null
		unpublishedRowsCount, err := countUnpublished(db)
		if err != nil {
			slog.Info("Error counting unpublished", slog.Any("error", err))
			return
		}
		if time.Now().After(nextLogTime) {

			slog.Info("updat", slog.Int("published", total-unpublishedRowsCount), slog.Int("total", total))
			nextLogTime = time.Now().Add(10 * time.Second)
		}
		if len(messagesToPublish) == 0 {
			time.Sleep(3 * time.Second)
		}

		// Exit the loop if there are no more rows to publish & all messages have been flushed
		if unpublishedRowsCount == 0 {
			break
		}
	}
	slog.Info("Finished")
}

func countUnpublished(db *sql.DB) (int, error) {
	var unpublishedRowsCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages WHERE published_at IS NULL`).Scan(&unpublishedRowsCount); err != nil {
		slog.Error("Error querying the database", slog.Any("error", err))
		return 0, err
	}
	return unpublishedRowsCount, nil
}

func markMessagesPublished(db *sql.DB, messages []Message, published_at time.Time) error {
	if len(messages) == 0 {
		return nil
	}

	// Start building the SQL query
	query := "UPDATE messages SET published_at = ? WHERE id IN ("

	// Add placeholders for each message ID
	for i := 0; i < len(messages); i++ {
		if i > 0 {
			query += ", "
		}
		query += "?"
	}

	query += ")"

	// Prepare the statement
	stmt, err := db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Convert messages to a slice of IDs
	ids := make([]interface{}, len(messages)+1)
	ids[0] = published_at
	for i, message := range messages {
		ids[i+1] = message.ID
	}

	// Execute the statement
	_, err = stmt.Exec(ids...)
	return err
}
