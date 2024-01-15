package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleMovingAverage(t *testing.T) {
	sma3 := SimpleMovingAverage(3)
	sma5 := SimpleMovingAverage(5)

	sma3Expected := []float64{1, 1.5, 2, 3, 4, 4.666666666666667, 4.666666666666667, 4, 3, 2}
	sma5Expected := []float64{1, 1.5, 2, 2.5, 3, 3.8, 4.2, 4.2, 3.8, 3}

	for i, v := range []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1} {
		val := sma3(v)
		assert.Equal(t, sma3Expected[i], val)
		val = sma5(v)
		assert.Equal(t, sma5Expected[i], val)
	}

}
