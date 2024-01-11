package adapter

import (
	"testing"
	"time"

	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/view"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestViewToCoreAdapter(t *testing.T) {
	id := uuid.New()
	// Create a test subscription
	vSub := view.Subscription{
		ID: id,
		SubscriptionData: view.SubscriptionData{
			Name: "test-subscription",
			Source: []view.Source{
				{
					Topic: "test-topic",
				},
			},
			Destination: view.Destination{
				Kind: "webhook",
				Webhook: view.Webhook{
					URL: "http://test-webhook",
				},
			},
			Configuration: view.Configuration{
				Batching: view.Batching{
					MaxBatchIntervalSeconds: 60,
					MaxBatchSize:            100,
				},
				Retry: view.Retry{
					MaxRetries:     3,
					RetryAlgorithm: "exponential_backoff",
				},
			},
		},
	}

	// Convert the view.Subscription to a core.Subscription
	cSub, err := ViewToCoreAdapter(vSub)
	assert.NoError(t, err)

	// Check the core.Subscription
	assert.Equal(t, id, cSub.ID)
	assert.Equal(t, "test-subscription", cSub.Name)
	assert.Equal(t, "test-topic", cSub.Topic.Topic)
	assert.Equal(t, "http://test-webhook", cSub.Destination.(core.WebhookDestination).URL)
	assert.Equal(t, time.Duration(60*time.Second), cSub.Config.MaxWait)
	assert.Equal(t, 100, cSub.Config.BatchSize)
	assert.Equal(t, 3, cSub.Destination.(core.WebhookDestination).MaxRetries)
}

// TODO Add tests for conversion CoreToViewAdapter
