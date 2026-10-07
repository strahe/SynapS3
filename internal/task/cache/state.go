package cache

import (
	"sync"
	"sync/atomic"
)

type pressureSnapshot struct{ targetBytes int64 }

// State holds cache pressure and the concurrent LRU deletion budget.
type State struct {
	pressure           atomic.Pointer[pressureSnapshot]
	lruCapacityMu      sync.Mutex
	lruProjectedBytes  int64
	lruInFlightDeletes int
}

func NewState() *State { return &State{} }
func (s *State) TargetBytes() (int64, bool) {
	if s == nil {
		return 0, false
	}
	snapshot := s.pressure.Load()
	if snapshot == nil {
		return 0, false
	}
	return snapshot.targetBytes, true
}

func lruLowBytes(maxBytes, maxWriteBytes int64, lowPercent int) int64 {
	return max(0, min(cacheWatermarkBytes(maxBytes, lowPercent), maxBytes-maxWriteBytes))
}

func cacheWatermarkBytes(maxBytes int64, percent int) int64 {
	if maxBytes <= 0 || percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return maxBytes
	}
	return (maxBytes/100)*int64(percent) + (maxBytes%100)*int64(percent)/100
}
