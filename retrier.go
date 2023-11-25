package main

import (
	"math"
	"time"
)

type Retrier func(retries, maxretries int) time.Duration

// ExponentialRetrier returns a duration that increases exponentially
// with each retry, up to maxretries, with a base of 10 seconds
// This yields the following reties: sec ratio.
// 0: 10
// 1: 20
// 2: 40
// 3: 80
// 4: 160
// 5: 320
// 6: 640
// 7: 1280 ~ 21 min
// 8: 2560 ~ 42 min
// 9: 5120 ~ 85 min
// 10: 10240 ~ 2.8 hours
// 11: 20480 ~ 5.6 hours
// 12: 40960 ~ 11.3 hours
// 13: 81920 ~ 22.7 hours
// 14: 163840 ~ 45.5 hours
// 15: 327680 ~ 91 hours
func ExponentialRetrier(retries, maxretries int) time.Duration {
	if retries > maxretries {
		return time.Duration(math.Pow(2, float64(maxretries))) * (10 * time.Second)
	}
	return time.Duration(math.Pow(2, float64(retries))) * (10 * time.Second)
}

// FixedRetrier function returns a fixed duration
func FixedRetrier(dur time.Duration) Retrier {
	return func(retries, maxretries int) time.Duration { return dur }
}
