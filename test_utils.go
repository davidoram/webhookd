package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

type TestDestination struct {
	t       *testing.T
	Batches [][]*kafka.Message
	SendOK  func(*TestDestination, []*kafka.Message) bool
	Events  chan BatchEvent
}

type BatchEvent struct {
	Index    int
	Messages []*kafka.Message
}

func NewTestDest(t *testing.T) *TestDestination {
	td := TestDestination{
		t:       t,
		Batches: make([][]*kafka.Message, 0),
		Events:  make(chan BatchEvent),
	}
	td.SendOK = func(_ *TestDestination, _ []*kafka.Message) bool { return true }
	return &td
}

func (wh *TestDestination) Send(msgs []*kafka.Message) bool {
	// Only save the messages if SendOK is true
	if !wh.SendOK(wh, msgs) {
		wh.t.Logf("TestDestination received batch %d with %d messages, ACK failed", len(wh.Batches), len(msgs))
		return false
	}
	wh.t.Logf("TestDestination received batch %d with %d messages, ACK ok", len(wh.Batches), len(msgs))
	wh.Events <- BatchEvent{Index: len(wh.Batches), Messages: msgs}
	wh.Batches = append(wh.Batches, msgs)
	return true
}

func testProducer(t *testing.T) *kafka.Producer {

	hostPort := fmt.Sprintf("%s:%s", KafkaHost, KafkaPort)
	t.Logf("stating producer, bootstrap.servers: %s", hostPort)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": hostPort,
		"acks":              "all",
	})
	assert.NoError(t, err, "creating kafka producer")

	// Capture events from the logs channel
	lc := p.Logs()
	if lc != nil {
		t.Log("listening to producer logs")
		go func() {
			for {
				select {
				case e, ok := <-lc:
					if !ok {
						return
					}
					t.Logf("kafka producer %v", e)
				}
			}
		}()
	}
	return p
}

func sendMsgBlocking(t *testing.T, p *kafka.Producer, key, value, topic string) {
	m := &kafka.Message{
		Key:            []byte(key),
		Value:          []byte(value),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	}
	// Create a channel, used to very when msg has been delivered, or an error has occured
	deliveryChan := make(chan kafka.Event)

	err := p.Produce(m, deliveryChan)
	assert.NoError(t, err, "error calling produce")

	// Block waiting for delivery
	evt := <-deliveryChan
	msg := evt.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		t.Fatalf("message send failed! error: %v", msg.TopicPartition.Error)
	}
	// t.Logf("message sent ok, topic: %s, key: %s", topic, key)
	close(deliveryChan)
}

func testConnection(host string, port string) bool {
	address := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", address, time.Second*1)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
