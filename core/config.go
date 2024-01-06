package core

import "time"

const MinWait = time.Millisecond * 100

type Config struct {
	BatchSize int           `json:"batch_size" `
	MaxWait   time.Duration `json:"max_wait"` // Maximum time to wait for a batch to fill up
}

func (c Config) WithMaxWait(wait time.Duration) Config {
	c.MaxWait = MinWait
	if c.MaxWait > MinWait {
		c.MaxWait = wait
	}
	return c
}
