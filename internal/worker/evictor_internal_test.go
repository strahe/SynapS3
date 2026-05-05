package worker

import (
	"testing"
	"time"
)

func TestEvictPollSleepDurationAddsBoundedJitter(t *testing.T) {
	interval := 20 * time.Millisecond
	maxDuration := interval + interval/evictPollJitterDivisor

	for range 100 {
		got := evictPollSleepDuration(interval)
		if got < interval || got > maxDuration {
			t.Fatalf("evict poll sleep duration = %s, want between %s and %s", got, interval, maxDuration)
		}
	}
}

func TestEvictPollSleepDurationKeepsNonPositiveAndTinyIntervals(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second, time.Nanosecond} {
		if got := evictPollSleepDuration(interval); got != interval {
			t.Fatalf("evict poll sleep duration = %s, want %s", got, interval)
		}
	}
}
