package main

import "time"

const MinWait = time.Millisecond * 100

type Config struct {
	BatchSize int           `json:"batch_size"`
	maxWait   time.Duration `json:"max_wait"` // Maximum time to wait for a batch to fill up
}

func (c Config) WithMaxWait(wait time.Duration) Config {
	c.maxWait = MinWait
	if c.maxWait > MinWait {
		c.maxWait = wait
	}
	return c
}
