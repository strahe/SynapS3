package worker

import (
	"context"

	"github.com/strahe/synaps3/internal/cache"
)

type cachePressureState struct{ targetBytes int64 }

func (h *TaskHandlers) updateCachePressure(ctx context.Context, targetBytes int64, previous *cachePressureState) error {
	active, err := h.deps.Repositories.CacheEvictions.ActiveEvictionBytes(ctx)
	if err != nil {
		return err
	}
	deficit := h.deps.Cache.CapacitySnapshot().OccupiedBytes() - targetBytes - active
	if deficit <= 0 {
		return nil
	}
	if h.deps.EvictionPolicy == cache.EvictionPolicyLRU {
		available, err := h.deps.Repositories.CacheEvictions.ReclaimableLRUBytes(ctx)
		if err != nil {
			return err
		}
		deficit -= available
	}
	if deficit <= 0 {
		return nil
	}
	h.cachePressure.Store(&cachePressureState{targetBytes: targetBytes})
	if previous == nil {
		if err := h.deps.Repositories.Contents.WakeCacheDependentCommitTasks(ctx); err != nil {
			h.cachePressure.Store(nil)
			return err
		}
	}
	return nil
}

func (h *TaskHandlers) commitCachePressure(ctx context.Context, requestID string) (bool, error) {
	if !h.deps.CommitSealOnCachePressure || h.deps.EvictionPolicy == cache.EvictionPolicyNone {
		return false, nil
	}
	pressure := h.cachePressure.Load()
	if pressure == nil || h.deps.Cache.CapacitySnapshot().OccupiedBytes() <= pressure.targetBytes {
		return false, nil
	}
	return h.deps.Repositories.Contents.HasCacheDependentCommitMembers(ctx, requestID)
}
