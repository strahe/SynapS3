package worker

import (
	"testing"
	"time"
)

func TestWorkerPollSleepDurationAddsBoundedJitter(t *testing.T) {
	interval := 20 * time.Millisecond
	maxDuration := interval + interval/workerPollJitterDivisor

	for range 100 {
		got := workerPollSleepDuration(interval)
		if got < interval || got > maxDuration {
			t.Fatalf("worker poll sleep duration = %s, want between %s and %s", got, interval, maxDuration)
		}
	}
}

func TestWorkerPollSleepDurationKeepsNonPositiveAndTinyIntervals(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second, time.Nanosecond} {
		if got := workerPollSleepDuration(interval); got != interval {
			t.Fatalf("worker poll sleep duration = %s, want %s", got, interval)
		}
	}
}
