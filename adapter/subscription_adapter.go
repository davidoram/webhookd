package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/view"
	"github.com/pkg/errors"
	"gopkg.in/guregu/null.v4"
)

// ViewToCoreAdapter converts a view.Subscription to a core.Subscription
func ViewToCoreAdapter(vSub view.Subscription) (core.Subscription, error) {
	var cSub core.Subscription
	cSub.ID = vSub.ID
	cSub.Name = vSub.Name
	cSub.CreatedAt = vSub.CreatedAt
	cSub.UpdatedAt = vSub.UpdatedAt
	cSub.DeletedAt = sql.NullTime{Time: vSub.DeletedAt.Time, Valid: vSub.DeletedAt.Valid}

	// Convert the Topic
	cSub.Topic = core.Topic{}
	if len(vSub.Source) == 1 {
		cSub.Topic = core.Topic{Topic: vSub.Source[0].Topic}
	} else {
		return cSub, errors.New("invalid number of topics, must be exactly 1")
	}

	// Convert the Filter
	cSub.Filter = core.Filter{}
	cSub.Filter.JMESFilter = ""
	if len(vSub.Source[0].JmesFilters) == 1 {
		cSub.Filter.JMESFilter = vSub.Source[0].JmesFilters[0]
	} else if len(vSub.Source[0].JmesFilters) > 1 {
		return cSub, errors.New("multiple JMES filters not supported")
	}

	// Convert the Config
	cSub.Config = core.Config{}
	cSub.Config.MaxWait = time.Duration(vSub.Configuration.Batching.MaxBatchIntervalSeconds) * time.Second
	cSub.Config.BatchSize = vSub.Configuration.Batching.MaxBatchSize
	cSub.Config.Alerting = core.Alerting{
		AlertChannel: vSub.Configuration.Alerting.AlertChannel,
		AlertEmails:  vSub.Configuration.Alerting.AlertEmails,
	}

	// Convert the Destination
	switch vSub.Destination.Kind {
	case "webhook":
		webhook := core.NewWebhook(vSub.Destination.Webhook.URL)
		webhook.MaxRetries = vSub.Configuration.Retry.MaxRetries

		// Convert the Headers
		for key, values := range vSub.Destination.Webhook.Headers {
			for _, value := range strings.Split(values, ";") {
				webhook.Headers.Add(key, value)
			}
		}

		// Set the retrier
		switch vSub.Configuration.Retry.RetryAlgorithm {
		case "exponential_backoff":
			webhook.Retry = core.ExponentialRetrier{}
		case "fixed":
			interval := time.Duration(60 * time.Second)
			if vSub.Configuration.Retry.FixedRetryIntervalDuration != "" {
				// Parse the interval, or default to 1 minute
				var err error
				interval, err = time.ParseDuration(vSub.Configuration.Retry.FixedRetryIntervalDuration)
				if err != nil {
					return cSub, errors.Wrap(err, "invalid fixed retry interval")
				}
			}
			webhook.Retry = core.FixedRetrier{Duration: interval}
		default:
			return cSub, fmt.Errorf("missing or invalid retry algorithm: '%s'", vSub.Configuration.Retry.RetryAlgorithm)
		}

		cSub.Destination = webhook
	case "test_destination": // Used by test cases only
		cSub.Destination = core.NewTestDest(nil)
	default:
		return cSub, errors.New("invalid destination kind")
	}
	return cSub, nil
}

// CoreToViewAdapter converts a core.Subscription to a view.Subscription
func CoreToViewAdapter(cSub core.Subscription) (view.Subscription, error) {
	vSub := view.NewSubscription()
	vSub.ID = cSub.ID
	vSub.CreatedAt = cSub.CreatedAt
	vSub.UpdatedAt = cSub.UpdatedAt
	vSub.DeletedAt = null.NewTime(cSub.DeletedAt.Time, cSub.DeletedAt.Valid)
	vSub.Name = cSub.Name
	vSub.Active = cSub.IsActive()
	vSub.Source = []view.Source{
		{
			Topic:       cSub.Topic.Topic,
			JmesFilters: []string{},
		},
	}
	if cSub.Filter.JMESFilter != "" {
		vSub.Source[0].JmesFilters = []string{cSub.Filter.JMESFilter}
	}
	vSub.Configuration.Batching.MaxBatchIntervalSeconds = int(cSub.Config.MaxWait.Seconds())
	vSub.Configuration.Batching.MaxBatchSize = cSub.Config.BatchSize
	vSub.Configuration.Alerting.AlertChannel = cSub.Config.Alerting.AlertChannel
	vSub.Configuration.Alerting.AlertEmails = cSub.Config.Alerting.AlertEmails

	// Convert the Destination
	switch cSub.Destination.TypeName() {
	case "webhook":
		wndest := cSub.Destination.(core.WebhookDestination)
		vSub.Destination.Kind = "webhook"
		vSub.Destination.Webhook.URL = wndest.URL
		vSub.Destination.Webhook.Headers = map[string]string{}
		for key, values := range wndest.Headers {
			vSub.Destination.Webhook.Headers[key] = strings.Join(values, ";")
		}
	case "test_destination": // Used by test cases only
		vSub.Destination.Kind = "test_destination"
	default:
		return vSub, fmt.Errorf("invalid destination type: '%s'", cSub.Destination.TypeName())
	}

	// Convert the Retry
	switch cSub.Destination.TypeName() {
	case "webhook":
		wndest := cSub.Destination.(core.WebhookDestination)
		vSub.Configuration.Retry.MaxRetries = wndest.MaxRetries

		// Convert the retrier config
		switch wndest.Retry.Name() {
		case "exponential_backoff":
			vSub.Configuration.Retry.RetryAlgorithm = "exponential_backoff"
		case "fixed":
			vSub.Configuration.Retry.RetryAlgorithm = "fixed"
			retrier := wndest.Retry.(core.FixedRetrier)
			vSub.Configuration.Retry.FixedRetryIntervalDuration = retrier.Duration.String()
		default:
			return vSub, errors.New("invalid retry algorithim")
		}
	}
	return vSub, nil
}
