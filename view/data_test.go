package view

import (
	"testing"

	"github.com/asaskevich/govalidator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalSubscription(t *testing.T) {
	testData := []byte(`
	{
		"name": "Subscription 1",
		"active": true,
		"destination": {
			"kind": "webhook",
			"webhook": {
				"url": "https://example.com/webhook",
				"headers": [
					"X-API-Key: 33139801-5796-4450-8de1-3d303cdb39f2"
				]
			}
		},
		"source": [
			{
				"topic": "path.to.my.topic",
				"jmes_filters": [
					"transaction_type == 'credit'"
				]
			}
		],
		"configuration": {
			"batching": {
				"max_batch_size": 50,
				"max_batch_interval_seconds": 30
			},
			"payload_size": {
				"max_payload_size_kb": 1024
			},
			"retry": {
				"max_retries": 1,
				"retry_algorithm": "exponential_backoff"
			},
			"alerting": {
				"alert_channel": "email",
				"alert_emails": [
					"dave@foo.com",
					"sid@moo.com"
				]
			}
		}
	}`)
	s, err := UnmarshalSubscription(testData)
	require.NoError(t, err)
	assert.Equal(t, "Subscription 1", s.Name)
	assert.Equal(t, true, s.Active)
	assert.Equal(t, "webhook", s.Destination.Kind)
	assert.Equal(t, "https://example.com/webhook", s.Destination.Webhook.URL)
	assert.Equal(t, "X-API-Key: 33139801-5796-4450-8de1-3d303cdb39f2", s.Destination.Webhook.Headers[0])
	assert.Equal(t, 1, len(s.Destination.Webhook.Headers))
	assert.Equal(t, "path.to.my.topic", s.Source[0].Topic)
	assert.Equal(t, "transaction_type == 'credit'", s.Source[0].JmesFilters[0])
	assert.Equal(t, 1, len(s.Source[0].JmesFilters))
	assert.Equal(t, 50, s.Configuration.Batching.MaxBatchSize)
	assert.Equal(t, 30, s.Configuration.Batching.MaxBatchIntervalSeconds)
	assert.Equal(t, 1024, s.Configuration.PayloadSize.MaxPayloadSizeKb)
	assert.Equal(t, 1, s.Configuration.Retry.MaxRetries)
	assert.Equal(t, "exponential_backoff", s.Configuration.Retry.RetryAlgorithm)
	assert.Equal(t, "email", s.Configuration.Alerting.AlertChannel)
	assert.Equal(t, "dave@foo.com", s.Configuration.Alerting.AlertEmails[0])
	assert.Equal(t, "sid@moo.com", s.Configuration.Alerting.AlertEmails[1])
	assert.Equal(t, 2, len(s.Configuration.Alerting.AlertEmails))

	// Check is valid
	result, err := govalidator.ValidateStruct(t)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestUnmarshalSubscriptionWithDefaults(t *testing.T) {
	testData := []byte(`
	{
		"name": "Subscription 1",
		"destination": {
			"kind": "webhook",
			"webhook": {
				"url": "https://example.com/webhook"
			}
		},
		"source": [
			{
				"topic": "path.to.my.topic"
			}
		]
	}`)
	s, err := UnmarshalSubscription(testData)
	require.NoError(t, err)
	assert.Equal(t, "Subscription 1", s.Name)
	assert.Equal(t, true, s.Active)
	assert.Equal(t, "webhook", s.Destination.Kind)
	assert.Equal(t, "https://example.com/webhook", s.Destination.Webhook.URL)
	assert.Equal(t, 0, len(s.Destination.Webhook.Headers))
	assert.Equal(t, "path.to.my.topic", s.Source[0].Topic)
	assert.Equal(t, 0, len(s.Source[0].JmesFilters))
	assert.Equal(t, 50, s.Configuration.Batching.MaxBatchSize)
	assert.Equal(t, 30, s.Configuration.Batching.MaxBatchIntervalSeconds)
	assert.Equal(t, 1024, s.Configuration.PayloadSize.MaxPayloadSizeKb)
	assert.Equal(t, 3, s.Configuration.Retry.MaxRetries)
	assert.Equal(t, "exponential_backoff", s.Configuration.Retry.RetryAlgorithm)
	assert.Equal(t, "none", s.Configuration.Alerting.AlertChannel)
	assert.Equal(t, 0, len(s.Configuration.Alerting.AlertEmails))

	// Check is valid
	result, err := govalidator.ValidateStruct(t)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestValidateSubscriptions(t *testing.T) {
	tests := map[string]struct {
		json     string
		field    string
		errorStr string
	}{
		"empty name": {
			`{"name":"","destination":{"kind":"webhook","webhook":{"url":"https://example.com/webhook"}},"source":[{"topic":"path.to.my.topic"}]}`,
			"name",
			"SubscriptionData.name: non zero value required",
		},
		"webhook URL invalid": {
			`{"name":"s1","destination":{"kind":"webhook","webhook":{"url":"not-a-url"}},"source":[{"topic":"path.to.my.topic"}]}`,
			"webhook",
			"SubscriptionData.Destination.Webhook.url: not-a-url does not validate as url",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := UnmarshalSubscription([]byte(test.json))
			assert.NoError(t, err)
			result, err := govalidator.ValidateStruct(s)
			assert.NotNil(t, err)
			assert.False(t, result)
			assert.Equal(t, test.errorStr, err.Error())
		})
	}
}
