package main

import (
	"fmt"
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

type EventCallback func(Event)
type Closer func()

type Emitter struct {
	listeners []EventCallback
}

func NewEmitter() *Emitter {
	return &Emitter{
		listeners: make([]EventCallback, 0),
	}
}

func (e *Emitter) AddListener(cb EventCallback) Closer {
	e.listeners = append(e.listeners, cb)

	// determine the index of the callback
	index := len(e.listeners) - 1

	// return a function that will remove the callback from the slice
	return func() {
		e.listeners = append(e.listeners[:index], e.listeners[index+1:]...)
	}
}

// Emit sends an event to all listeners.
func (e *Emitter) Emit(event Event) {
	for _, cb := range e.listeners {
		// cope with panics in the callback
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("recovered from panic in event listener: %v\n", r)
			}
		}()
		// make a copy of the event, so that the listener can't modify it
		eventCopy := event
		cb(eventCopy)
	}
}
