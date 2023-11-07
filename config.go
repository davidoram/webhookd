package main

type Config struct {
	BatchSize    int    `json:"batch_size"`
	KafkaServers string `json:"kafka_servers"`
}
