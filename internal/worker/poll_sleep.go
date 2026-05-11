package worker

import (
	"context"
	"math/rand"
	"time"
)

const workerPollJitterDivisor = 5

func sleepUntilNextWorkerPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(workerPollSleepDuration(interval))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func workerPollSleepDuration(interval time.Duration) time.Duration {
	if interval <= 0 {
		return interval
	}

	maxJitter := interval / workerPollJitterDivisor
	if maxJitter <= 0 {
		return interval
	}
	return interval + time.Duration(rand.Int63n(int64(maxJitter)+1))
}
