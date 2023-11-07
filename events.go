package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	ConsumerCreated EventType = "ConsumerCreated"

	ConsumerStarted EventType = "ConsumerStarted"
	ConsumerStopped EventType = "ConsumerStopped"

	KafkaOffline    EventType = "KafkaOffline"
	MessageReceived EventType = "MessageReceived"

	BatchSentOK      EventType = "BatchSentOK"
	BatchSentError   EventType = "BatchSentError"
	BatchCommitError EventType = "BatchCommitError"
	BatchSentRetry   EventType = "BatchSentRetry"
)

type Event struct {
	Type           EventType
	SubscriptionID uuid.UUID
	CreatedAt      time.Time
	Error          error
}

func (e Event) String() string {
	sb := &strings.Builder{}
	sb.WriteString("Event{")
	sb.WriteString(fmt.Sprintf("Type: %s, ", e.Type))
	sb.WriteString(fmt.Sprintf("SubscriptionID: %s, ", e.SubscriptionID))
	sb.WriteString(fmt.Sprintf("CreatedAt: %s, ", e.CreatedAt))
	if e.Error != nil {
		sb.WriteString(fmt.Sprintf("Error: %s", e.Error))
	}
	return sb.String()
}

type EventChannel chan Event

type Emitter struct {
	listeners []EventChannel
}

// AddListener adds a listener to the event emitter.
//
// To listen for events and block until available you would do something like this:
//
//	event := <-dest.Events.
//
// ... or if you want to avoid blocking wrap it in a select statement, like this:
//
// select {
// case event := <-dest.Events:
//
//	// handle event
//
// default:
//
//	    // do something else if no event is available
//	}
func (e *Emitter) AddListener() EventChannel {
	listener := make(EventChannel, 10)
	e.listeners = append(e.listeners, listener)
	return listener
}

// RemoveListener removes a listener from the event emitter.
func (e *Emitter) RemoveListener(listenerToRemove EventChannel) {
	for i, listener := range e.listeners {
		if listener == listenerToRemove {
			// Remove the listener from the slice
			e.listeners = append(e.listeners[:i], e.listeners[i+1:]...)
			break
		}
	}
}

// Emit emits an event to all listeners.
func (e *Emitter) Emit(event Event) {
	// for _, listener := range e.listeners {
	// 	select {
	// 	case listener <- event: // Write was successful
	// 	default: // Channel was full
	// 	}
	// }
	slog.Info("emit", slog.Any("event", event))
}
