package main

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BufferDestination
type BufferDestination struct {
	t       *testing.T
	Batches [][]*kafka.Message
	SendOK  bool
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
	}
}
func (wh *BufferDestination) Send(msgs []*kafka.Message) bool {
	wh.Batches = append(wh.Batches, msgs)
	return wh.SendOK
}

func (wh *BufferDestination) RequireBatchesArrived(t *testing.T, count int, timeout time.Duration) {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	done := make(chan bool)
	go func() {
		time.Sleep(timeout)
		done <- true
	}()
	for {
		select {
		case <-done:
			t.Log("Timeout waiting for batches")
			require.GreaterOrEqual(t, count, len(wh.Batches), "Too few batches delivered before timeout")
			return
		case <-ticker.C:
			if len(wh.Batches) >= count {
				t.Log("Batches delivered")
				return
			}
		}
	}
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
	T      *testing.T
	closer Closer
}

func (tel TestEventListener) Close() {
	tel.closer()
}

// func (tel TestEventListener) ListenAndLog(s *Subscription) {
// 	tel.closer = s.EventHook().AddListener(func(e Event) {
// 		tel.T.Logf("event: %s", e)
// 	})
// }
