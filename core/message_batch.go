package core

type Message struct {
	Topic   string            `json:"topic"`
	Key     string            `json:"key"`
	Value   string            `json:"value"`
	Headers map[string]string `json:"headers"`
}

type MessageBatch struct {
	Messages []Message `json:"messages"`
}
