package core

// SubscriptionSetEventType is an enum for the different types of events representing changes to the set of subscriptions
type SubscriptionSetEventType string

const (
	// NewSubscriptionEvent is sent when a new subscription is added to the set
	NewSubscriptionEvent SubscriptionSetEventType = "new-sub"
	// UpdatedSubscriptionEvent is sent when an existing subscription is updated
	UpdatedSubscriptionEvent SubscriptionSetEventType = "updated-sub"
	// DeletedSubscriptionEvent is sent when an existing subscription is removed/deleted
	DeletedSubscriptionEvent SubscriptionSetEventType = "deleted-sub"
)

type SubscriptionSetEvent struct {
	Type         SubscriptionSetEventType
	Subscription *Subscription
}
