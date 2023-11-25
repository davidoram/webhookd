package main

// SubscriptionEventType is an enum for the different types of events that can be sent to the subscription event channel.
type SubscriptionEventType string

const (
	// SubscriptionEventStart is sent when the subscription is started
	SubscriptionEventStart SubscriptionEventType = "start"
	// SubscriptionEventStop is sent when the subscription is stopped
	SubscriptionEventStop SubscriptionEventType = "stop"
	// SubscriptionEventError is sent when the subscription encounters an error
	SubscriptionEventError SubscriptionEventType = "error"

	// SubscriptionEventBatchSentACK is sent when when a batch of message is sent
	// to the destination, and the ACK is received
	SubscriptionEventBatchSentACK SubscriptionEventType = "batch_sent_ack"

	// SubscriptionEventBatchSentACK is sent when when a batch of message is sent
	// to the destination, and the client side ACK fails
	SubscriptionEventBatchSentNACK SubscriptionEventType = "batch_sent_nack"
)

type SubscriptionEvent struct {
	Type         SubscriptionEventType
	Subscription *Subscription
	Error        error
}

type SubscriptionListener interface {
	SubscriptionEvent(event SubscriptionEvent)
}
