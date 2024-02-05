package core

// SubscriptionChangeEvent is the event that is published to the channel when a subscription is created, updated, or deleted.
type SubscriptionChangeEvent struct {
	Subscription *Subscription
}
