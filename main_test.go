package main

import (
	"net"
	"testing"
	"time"
)

func TestKafkaRunning(t *testing.T) {
	if !testConnection("kafka", "9092") {
		t.Errorf("Can't connect to kafka:9092 - clients")
	}
	if !testConnection("kafka", "2081") {
		t.Errorf("Can't connect to kafka:2081 - zookeeper")
	}
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
