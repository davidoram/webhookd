package adapter

import (
	"database/sql"
	"testing"
	"time"

	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/view"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"gopkg.in/guregu/null.v4"
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
					Topic:       "test-topic",
					JmesFilters: []string{"jmes-filter"},
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
		CreatedAt: time.Date(2024, time.January, 11, 12, 20, 0, 0, time.UTC),
		UpdatedAt: time.Date(2024, time.January, 11, 12, 21, 0, 0, time.UTC),
		DeletedAt: null.Time{
			NullTime: sql.NullTime{
				Time:  time.Date(2024, time.January, 11, 12, 22, 0, 0, time.UTC),
				Valid: true,
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
	assert.Equal(t, "jmes-filter", cSub.Filter.JMESFilter)
	assert.Equal(t, "http://test-webhook", cSub.Destination.(core.WebhookDestination).URL)
	assert.Equal(t, time.Duration(60*time.Second), cSub.Config.MaxWait)
	assert.Equal(t, 100, cSub.Config.BatchSize)
	assert.Equal(t, 3, cSub.Destination.(core.WebhookDestination).MaxRetries)
	assert.Equal(t, "2024-01-11T12:20:00Z", cSub.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2024-01-11T12:21:00Z", cSub.UpdatedAt.Format(time.RFC3339))
	assert.Equal(t, true, cSub.DeletedAt.Valid)
	assert.Equal(t, "2024-01-11T12:22:00Z", cSub.DeletedAt.Time.Format(time.RFC3339))
}

func TestCoreToViewAdapter(t *testing.T) {
	cSub := core.Subscription{
		Name: "test-subscription",
		ID:   uuid.MustParse("8283c807-7266-4976-9618-7925d99cc527"),
		Topic: core.Topic{
			Topic: "test-topic",
		},
		Filter: core.Filter{
			JMESFilter: "jmes-filter",
		},
		Config: core.Config{
			BatchSize: 100,
			MaxWait:   time.Duration(60) * time.Second,
		},
		CreatedAt: time.Date(2024, time.January, 11, 12, 20, 0, 0, time.UTC),
		UpdatedAt: time.Date(2024, time.January, 11, 12, 21, 0, 0, time.UTC),
		DeletedAt: sql.NullTime{
			Time:  time.Date(2024, time.January, 11, 12, 22, 0, 0, time.UTC),
			Valid: true,
		},
		Destination: core.WebhookDestination{
			URL:        "http://test-webhook",
			MaxRetries: 3,
			Retry: core.FixedRetrier{
				Duration: time.Duration(15) * time.Second,
			},
		},
	}

	// Convert the core.Subscription to a view.Subscription
	vSub, err := CoreToViewAdapter(cSub)
	assert.NoError(t, err)

	// Check the core.Subscription
	assert.Equal(t, "8283c807-7266-4976-9618-7925d99cc527", vSub.ID.String())
	assert.Equal(t, "test-subscription", vSub.Name)
	assert.Equal(t, "test-topic", vSub.Source[0].Topic)
	assert.Equal(t, "jmes-filter", vSub.Source[0].JmesFilters[0])
	assert.Equal(t, "http://test-webhook", vSub.Destination.Webhook.URL)
	assert.Equal(t, 60, vSub.Configuration.Batching.MaxBatchIntervalSeconds)
	assert.Equal(t, 100, vSub.Configuration.Batching.MaxBatchSize)
	assert.Equal(t, 3, vSub.Configuration.Retry.MaxRetries)
	assert.Equal(t, "fixed", vSub.Configuration.Retry.RetryAlgorithm)
	assert.Equal(t, "2024-01-11T12:20:00Z", vSub.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2024-01-11T12:21:00Z", vSub.UpdatedAt.Format(time.RFC3339))
	assert.Equal(t, true, vSub.DeletedAt.Valid)
	assert.Equal(t, "2024-01-11T12:22:00Z", vSub.DeletedAt.Time.Format(time.RFC3339))
}
