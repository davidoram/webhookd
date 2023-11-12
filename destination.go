package main

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Destination interface {
	// Send messages and return if the desination has accepted the messages, and is ready for the next batch
	// Will be called repeatedly with the same messages until the Destination returns true to indicate that
	// the destination has accepted the messages, and is ready to receive the next batch
	Send([]*kafka.Message) bool
}
