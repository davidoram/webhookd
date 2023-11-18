package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Retrier func(retries, maxretries int) time.Duration

// ExponentialRetrier returns a function that returns a duration that increases exponentially
// with each retry, up to maxretries, with a base of 10 seconds
// This yields the following reties: sec ratio.
// 0: 10
// 1: 20
// 2: 40
// 3: 80
// 4: 160
// 5: 320
// 6: 640
// 7: 1280 ~ 21 min
// 8: 2560 ~ 42 min
// 9: 5120 ~ 85 min
// 10: 10240 ~ 2.8 hours
// 11: 20480 ~ 5.6 hours
// 12: 40960 ~ 11.3 hours
// 13: 81920 ~ 22.7 hours
// 14: 163840 ~ 45.5 hours
// 15: 327680 ~ 91 hours
func ExponentialRetrier(retries, maxretries int) time.Duration {
	return time.Duration(math.Pow(2, float64(retries))) * (10 * time.Second)
}

type WebhookDestination struct {
	URL        string
	MaxRetries int
	Retry      Retrier
}

func NewWebhook(url string) WebhookDestination {
	return WebhookDestination{URL: url, MaxRetries: 5, Retry: ExponentialRetrier}
}

// Send messages and return if the desination has accepted the messages, and is ready for the next batch
// Will be called repeatedly with the same messages until the Destination returns true to indicate that
// the destination has accepted the messages, and is ready to receive the next batch
func (wh WebhookDestination) Send(msgs []*kafka.Message) bool {
	batch := encode(msgs)
	buf, err := json.Marshal(batch)
	if err != nil {
		log.Printf("Error encoding JSON: %v", err)
		return false
	}

	// Start a retry loop with exponential backoff
	for i := 0; i < wh.MaxRetries; i++ {
		// Send the batch to the webhook destination
		resp, err := http.Post(wh.URL, "application/json", bytes.NewBuffer(buf))
		if err != nil {
			log.Printf("Error sending batch to webhook destination: %v", err)
			time.Sleep(time.Duration(math.Pow(2, float64(i))) * time.Second)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			delay := time.Duration(math.Pow(2, float64(i))) * time.Second
			log.Printf("Webhook returned status code %s, delaying for %s", resp.Status, delay.String())
			time.Sleep(delay)
			continue
		}
		// If we get here, the batch was sent successfully
		return true
	}
	// If we get here, we retried and failed
	log.Printf("Too many retries %d to webhook destination", wh.MaxRetries)
	return false
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
