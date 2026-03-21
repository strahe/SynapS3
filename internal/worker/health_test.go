package worker

import (
	"testing"
	"time"
)

func TestLivenessTracker_Healthy(t *testing.T) {
	lt := newLivenessTracker(10 * time.Second)
	if !lt.healthy() {
		t.Fatal("new tracker should be healthy")
	}
}

func TestLivenessTracker_Stale(t *testing.T) {
	lt := newLivenessTracker(1 * time.Millisecond)
	// Force the lastTick to be far in the past
	lt.lastTick.Store(time.Now().Add(-10 * time.Second).UnixNano())
	if lt.healthy() {
		t.Fatal("stale tracker should be unhealthy")
	}
}

func TestLivenessTracker_RecordTick(t *testing.T) {
	lt := newLivenessTracker(1 * time.Millisecond)
	lt.lastTick.Store(time.Now().Add(-10 * time.Second).UnixNano())
	if lt.healthy() {
		t.Fatal("should be unhealthy before recordTick")
	}
	lt.recordTick()
	if !lt.healthy() {
		t.Fatal("should be healthy after recordTick")
	}
}
