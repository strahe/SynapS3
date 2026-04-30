package worker

import (
	"testing"
	"time"
)

func TestUploadPollSleepDurationAddsPositiveJitter(t *testing.T) {
	interval := 100 * time.Millisecond
	max := interval + interval/uploadPollJitterDivisor

	for range 50 {
		got := uploadPollSleepDuration(interval)
		if got < interval {
			t.Fatalf("sleep duration = %s, want at least %s", got, interval)
		}
		if got > max {
			t.Fatalf("sleep duration = %s, want at most %s", got, max)
		}
	}
}
