package main

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

// BufferDestination
type BufferDestination struct {
	t       *testing.T
	Batches [][]*kafka.Message
	SendOK  bool
	Events  chan BatchEvent
}

type BatchEvent struct {
	Index    int
	Messages []*kafka.Message
}

func NewBufferDestination(t *testing.T) *BufferDestination {
	return &BufferDestination{
		t:       t,
		Batches: make([][]*kafka.Message, 0),
		SendOK:  true,
		Events:  make(chan BatchEvent),
	}
}
func (wh *BufferDestination) Send(msgs []*kafka.Message) bool {
	wh.Events <- BatchEvent{Index: len(wh.Batches), Messages: msgs}
	wh.Batches = append(wh.Batches, msgs)
	return wh.SendOK
}

func (wh *BufferDestination) AssertMsgCount(count int) {
	tot := 0
	for _, batch := range wh.Batches {
		tot += len(batch)
	}
	assert.Equal(wh.t, count, tot, "Message count mismatch")
}

func (wh *BufferDestination) AssertBatchCount(count int) {
	assert.Equal(wh.t, count, len(wh.Batches), "Batch count mismatch")
}

type TestEventListener struct {
	T *testing.T
}

func (tel TestEventListener) ListenAndLog(s *Subscription) {
	events := s.EventHook().AddListener()
	go func() {
		for {
			select {
			case event, ok := <-events:
				if !ok {
					return
				}
				tel.T.Logf("event: %s", event)
			}
		}
	}()
}
