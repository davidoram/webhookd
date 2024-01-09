package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExponentialRetrier(t *testing.T) {
	assert.Equal(t, 10*time.Second, ExponentialRetrier{}.RetryIn(0, 5))
	assert.Equal(t, 20*time.Second, ExponentialRetrier{}.RetryIn(1, 5))
	assert.Equal(t, 40*time.Second, ExponentialRetrier{}.RetryIn(2, 5))
	assert.Equal(t, 80*time.Second, ExponentialRetrier{}.RetryIn(3, 5))
	assert.Equal(t, 160*time.Second, ExponentialRetrier{}.RetryIn(4, 5))
	assert.Equal(t, 320*time.Second, ExponentialRetrier{}.RetryIn(5, 5))
	// No change once you hit maxretries
	assert.Equal(t, 320*time.Second, ExponentialRetrier{}.RetryIn(15, 5))
	assert.Equal(t, 327680*time.Second, ExponentialRetrier{}.RetryIn(15, 15))
	// Check the name
	assert.Equal(t, "exponential", ExponentialRetrier{}.Name())
}

func TestFixedRetrier(t *testing.T) {
	r := FixedRetrier{Duration: time.Millisecond}
	assert.Equal(t, 1*time.Millisecond, r.RetryIn(0, 5))
	assert.Equal(t, 1*time.Millisecond, r.RetryIn(5, 5))
	assert.Equal(t, 1*time.Millisecond, r.RetryIn(50000000, 5))
	// Check the name
	assert.Equal(t, "fixed", ExponentialRetrier{}.Name())
}
