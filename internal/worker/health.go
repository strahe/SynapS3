package worker

import (
	"sync/atomic"
	"time"
)

// livenessTracker tracks worker liveness based on last tick time.
type livenessTracker struct {
	lastTick     atomic.Int64 // unix nanos
	pollInterval time.Duration
}

func newLivenessTracker(pollInterval time.Duration) *livenessTracker {
	lt := &livenessTracker{pollInterval: pollInterval}
	lt.recordTick() // mark as healthy at creation time
	return lt
}

func (lt *livenessTracker) recordTick() {
	lt.lastTick.Store(time.Now().UnixNano())
}

func (lt *livenessTracker) healthy() bool {
	last := time.Unix(0, lt.lastTick.Load())
	return time.Since(last) < 3*lt.pollInterval
}
