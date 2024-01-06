// configuration package contains structs that map to the JSON configuration.
package configuration

import (
	"encoding/json"
)

type Subscription struct {
	Name          string        `json:"name" valid:"alphanum,required"`
	Active        bool          `json:"active" valid:"required"`
	Destination   Destination   `json:"destination" valid:"required"`
	Source        []Source      `json:"source" valid:"required"`
	Configuration Configuration `json:"configuration"`
}

type Destination struct {
	Kind    string  `json:"kind" valid:"required,in(webhook)"`
	Webhook Webhook `json:"webhook" valid:"required"`
}

type Webhook struct {
	URL     string   `json:"url" valid:"url,required"`
	Headers []string `json:"headers" valid:"string"`
}

type Source struct {
	Topic       string   `json:"topic" valid:"required" `
	JmesFilters []string `json:"jmes_filters" valid:"-"`
}

type Configuration struct {
	Batching    Batching    `json:"batching" valid:"required"`
	PayloadSize PayloadSize `json:"payload_size" valid:"-"`
	Retry       Retry       `json:"retry" valid:"-"`
	Alerting    Alerting    `json:"alerting" valid:"-"`
}

type Batching struct {
	MaxBatchSize            int `json:"max_batch_size" valid:"type(int),range(1|1000)"`
	MaxBatchIntervalSeconds int `json:"max_batch_interval_seconds" valid:"type(int),range(1|300)"`
}

type PayloadSize struct {
	MaxPayloadSizeKb int `json:"max_payload_size_kb" valid:"type(int),range(1|5000)"`
}

type Retry struct {
	MaxRetries     int    `json:"max_retries" valid:"range(1,10)"`
	RetryAlgorithm string `json:"retry_algorithm" valid:"required"`
}

type Alerting struct {
	AlertChannel string   `json:"alert_channel" valid:"required,in(email,none)"`
	AlertEmails  []string `json:"alert_emails" valid:"email"`
}

func UnmarshalSubscription(data []byte) (Subscription, error) {
	s := NewSubscription()
	err := json.Unmarshal(data, &s)
	return s, err
}

func NewSubscription() Subscription {
	return Subscription{
		Active: true,
		Configuration: Configuration{
			Batching: Batching{
				MaxBatchSize:            50,
				MaxBatchIntervalSeconds: 30,
			},
			PayloadSize: PayloadSize{
				MaxPayloadSizeKb: 1024,
			},
			Retry: Retry{
				MaxRetries:     3,
				RetryAlgorithm: "exponential_backoff",
			},
			Alerting: Alerting{
				AlertChannel: "none",
				AlertEmails:  []string{},
			},
		},
	}
}
