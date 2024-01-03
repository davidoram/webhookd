package core

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

type WebhookDestination struct {
	URL        string
	MaxRetries int
	Retry      Retrier
	client     *http.Client
}

var WebhookSendFailed = errors.New("webhook send failed")

// NewWebhook creates a new WebhookDestination using the given URL, and a default retry policy
// it also uses the default http.Client
func NewWebhook(url string) WebhookDestination {
	return WebhookDestination{URL: url, MaxRetries: 5, Retry: ExponentialRetrier, client: http.DefaultClient}
}

func (wh WebhookDestination) WithClient(client *http.Client) WebhookDestination {
	wh.client = client
	return wh
}

// Send messages and return if the desination has accepted the messages, and is ready for the next batch
// Will be called repeatedly with the same messages until the Destination returns nil to indicate that
// the destination has accepted the messages, and is ready to receive the next batch
func (wh WebhookDestination) Send(ctx context.Context, msgs []*kafka.Message) error {
	batch := encode(msgs)
	buf, err := json.Marshal(batch)
	if err != nil {
		log.Printf("Error encoding JSON: %v", err)
		return err
	}

	// Start a retry loop with exponential backoff
	for i := 0; i < wh.MaxRetries; i++ {
		// Check if the context has been cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Send the batch to the webhook destination
			resp, err := wh.client.Post(wh.URL, "application/json", bytes.NewBuffer(buf))
			if err != nil {
				delay := wh.Retry(i, wh.MaxRetries)
				log.Printf("Error sending batch to webhook destination: %v", err)
				time.Sleep(delay)
				continue
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				delay := wh.Retry(i, wh.MaxRetries)
				log.Printf("Webhook returned status code %s, delaying for %s", resp.Status, delay.String())
				time.Sleep(delay)
				continue
			}
			// If we get here, the batch was sent successfully
			return nil
		}
	}
	// If we get here, we retried and failed
	log.Printf("Too many retries %d to webhook destination", wh.MaxRetries)
	return WebhookSendFailed
}

func encode(msgs []*kafka.Message) MessageBatch {
	mb := MessageBatch{Messages: make([]Message, len(msgs))}
	for i, msg := range msgs {
		mb.Messages[i] = Message{
			Topic:   *msg.TopicPartition.Topic,
			Key:     string(msg.Key),
			Value:   string(msg.Value),
			Headers: make(map[string]string),
		}
		// TODO Should headers be filtered?
		for _, h := range msg.Headers {
			mb.Messages[i].Headers[h.Key] = string(h.Value)
		}
	}
	return mb
}
