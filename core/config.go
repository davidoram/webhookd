package core

import "time"

const MinWait = time.Millisecond * 100

type Config struct {
	BatchSize int
	MaxWait   time.Duration // Maximum time to wait for a batch to fill up
	Alerting  Alerting
}

type Alerting struct {
	AlertChannel string
	AlertEmails  []string
}

func (c Config) WithMaxWait(wait time.Duration) Config {
	c.MaxWait = MinWait
	if c.MaxWait > MinWait {
		c.MaxWait = wait
	}
	return c
}
