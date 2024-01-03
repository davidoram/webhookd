package core

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Destination interface {
	// Send messages and return if the desination has accepted the messages, and is ready for the next batch
	// Will be called repeatedly with the same messages until the Destination returns nil to indicate that
	// the destination has accepted the messages, and is ready to receive the next batch
	Send(context.Context, []*kafka.Message) error
}
