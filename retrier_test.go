package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExponentialRetrier(t *testing.T) {
	assert.Equal(t, 10*time.Second, ExponentialRetrier(0, 5))
	assert.Equal(t, 20*time.Second, ExponentialRetrier(1, 5))
	assert.Equal(t, 40*time.Second, ExponentialRetrier(2, 5))
	assert.Equal(t, 80*time.Second, ExponentialRetrier(3, 5))
	assert.Equal(t, 160*time.Second, ExponentialRetrier(4, 5))
	assert.Equal(t, 320*time.Second, ExponentialRetrier(5, 5))
	// No change once you hit maxretries
	assert.Equal(t, 320*time.Second, ExponentialRetrier(15, 5))

	assert.Equal(t, 327680*time.Second, ExponentialRetrier(15, 15))
}

func TestFixedRetrier(t *testing.T) {
	r := FixedRetrier(time.Millisecond)
	assert.Equal(t, 1*time.Millisecond, r(0, 5))
	assert.Equal(t, 1*time.Millisecond, r(5, 5))
}
