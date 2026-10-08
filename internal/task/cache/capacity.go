package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type (
	PressureListener     interface{ OnCachePressure(context.Context) error }
	CapacityDependencies struct {
		Repositories              *repository.Repositories
		Cache                     cache.Cache
		CacheTracker              *cacheaccess.Tracker
		Scheduler                 *taskengine.Scheduler
		State                     *State
		PressureListener          PressureListener
		EvictionPolicy            cache.EvictionPolicy
		MaxCacheBytes             int64
		MaxWriteBytes             int64
		LRUHighPercent            int
		LRULowPercent             int
		CommitSealOnCachePressure bool
	}
)

type CapacityHandler struct {
	*taskengine.FuncHandler
	deps CapacityDependencies
}

func NewCapacityHandler(deps CapacityDependencies) (*CapacityHandler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil || deps.State == nil {
		return nil, errors.New("cache capacity handler requires repositories and state")
	}
	if deps.CommitSealOnCachePressure && deps.EvictionPolicy != cache.EvictionPolicyNone && deps.PressureListener == nil {
		return nil, errors.New("cache pressure sealing requires a listener")
	}
	if deps.EvictionPolicy == cache.EvictionPolicyLRU && (deps.MaxCacheBytes <= 0 || deps.LRULowPercent < 0 || deps.LRULowPercent > 100 || deps.LRUHighPercent < 0 || deps.LRUHighPercent > 100 || deps.LRUHighPercent <= deps.LRULowPercent) {
		return nil, errors.New("LRU cache capacity requires valid size and watermarks")
	}
	h := &CapacityHandler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}

func (h *CapacityHandler) lruLowBytes() int64 {
	return lruLowBytes(h.deps.MaxCacheBytes, h.deps.MaxWriteBytes, h.deps.LRULowPercent)
}

const (
	dependencyWait        = time.Minute
	externalPollInterval  = 5 * time.Second
	disabledCycleInterval = time.Hour
)

type cacheCapacityCheckpoint struct {
	CycleActive       bool  `json:"cycle_active"`
	RefusedWriteBytes int64 `json:"refused_write_bytes,omitempty"`
}

func (h *CapacityHandler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeCacheCapacityReconcile, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec:  taskengine.StrictJSONCodec(func(input *systemtask.Input) error { return systemtask.ValidateInput(*input) }),
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
		NextCycleCheckpoint: func(previous *model.Task) json.RawMessage {
			var cp cacheCapacityCheckpoint
			if previous == nil || json.Unmarshal(previous.Checkpoint, &cp) != nil {
				return nil
			}
			if !cp.CycleActive && cp.RefusedWriteBytes == 0 {
				return nil
			}
			raw, _ := json.Marshal(cp)
			return raw
		},
	}
	var pendingWriteRefusal atomic.Int64
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		previousPressure := h.deps.State.pressure.Swap(nil)
		if h.deps.EvictionPolicy == cache.EvictionPolicyNone ||
			(h.deps.EvictionPolicy == cache.EvictionPolicyAfterUpload && !h.deps.CommitSealOnCachePressure) {
			return taskengine.CompleteCycle(disabledCycleInterval, "Automatic cache cleanup is disabled", nil)
		}
		if h.deps.Cache == nil || h.deps.CacheTracker == nil || h.deps.Scheduler == nil || h.deps.MaxCacheBytes <= 0 {
			return taskengine.Fail(errors.New("cache capacity dependencies are unavailable"), "dependency_unavailable", nil)
		}
		checkpoint, _, err := taskengine.DecodeCheckpoint[cacheCapacityCheckpoint](execution)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		if h.deps.EvictionPolicy == cache.EvictionPolicyLRU && !h.deps.CacheTracker.SafeForLRU() {
			return taskengine.Wait(model.TaskResumeModeRecover, dependencyWait, "cache_access", "Waiting for reliable cache access records", nil)
		}
		usedBytes := h.deps.Cache.CapacitySnapshot().OccupiedBytes()
		highBytes := cacheWatermarkBytes(h.deps.MaxCacheBytes, h.deps.LRUHighPercent)
		lowBytes := h.lruLowBytes()
		// A refused write starts a cycle below the high watermark too: when the
		// headroom above it is smaller than the write, usage would otherwise never
		// reach the watermark and the write would be refused indefinitely.
		writeRefused := max(pendingWriteRefusal.Swap(0), h.deps.Cache.ConsumeRefusedWriteBytes())
		next := checkpoint
		next.RefusedWriteBytes = max(next.RefusedWriteBytes, writeRefused)
		if h.deps.EvictionPolicy == cache.EvictionPolicyAfterUpload {
			lowBytes = max(0, h.deps.MaxCacheBytes-next.RefusedWriteBytes)
			if usedBytes <= lowBytes {
				next.RefusedWriteBytes = 0
			}
		} else {
			switch {
			case usedBytes <= lowBytes:
				next.CycleActive, next.RefusedWriteBytes = false, 0
			case usedBytes >= highBytes, next.RefusedWriteBytes > 0:
				next.CycleActive = true
			}
		}
		if next != checkpoint {
			if err := execution.WriteCheckpoint(ctx, next); err != nil {
				pendingWriteRefusal.Store(max(writeRefused, next.RefusedWriteBytes))
				return retryTask(err, "cache_capacity_checkpoint_failed")
			}
		}
		cycleActive := next.CycleActive
		if h.deps.EvictionPolicy == cache.EvictionPolicyAfterUpload {
			cycleActive = next.RefusedWriteBytes > 0
		}
		if !cycleActive {
			return taskengine.CompleteCycle(externalPollInterval, "Local cache usage is within its target", nil)
		}
		activeBytes, err := h.deps.Repositories.CacheEvictions.ActiveEvictionBytes(ctx)
		if err != nil {
			return retryTask(err, "cache_capacity_scan_failed")
		}
		bytesToPlan := usedBytes - lowBytes - activeBytes
		var plannedBytes int64
		var plannedTasks int
		var failedTasks []int64
		if bytesToPlan > 0 && h.deps.EvictionPolicy == cache.EvictionPolicyLRU {
			plannedBytes, plannedTasks, failedTasks, err = h.planLRUEvictions(ctx, bytesToPlan)
			if err != nil {
				return retryTask(err, "cache_capacity_plan_failed")
			}
		}
		if len(failedTasks) > 0 {
			return taskengine.RetryBackoff(errors.New("cache cleanup work failed"), "cache_cleanup_retry", nil).WithRetrySettlements(func(ctx context.Context, repos *repository.Repositories, availableAt time.Time) error {
				for _, id := range failedTasks {
					if _, err := h.deps.Scheduler.RetryInTransaction(ctx, repos, id, availableAt); err != nil {
						return err
					}
				}
				return nil
			}, nil)
		}
		if h.deps.CommitSealOnCachePressure {
			if err := h.updateCachePressure(ctx, lowBytes, previousPressure); err != nil {
				return retryTask(err, "cache_pressure_scan_failed")
			}
		}
		message := "Waiting for remotely safe cached data"
		if bytesToPlan <= 0 {
			message = "Local cache cleanup is in progress"
		}
		if plannedTasks > 0 {
			message = fmt.Sprintf("Scheduled cleanup for %d cached items (%d bytes)", plannedTasks, plannedBytes)
		}
		return taskengine.Wait(model.TaskResumeModeExecute, externalPollInterval, "cache_cleanup", message, nil)
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

func (h *CapacityHandler) planLRUEvictions(ctx context.Context, bytesToPlan int64) (int64, int, []int64, error) {
	const candidateBatchSize = 100
	var plannedBytes int64
	var plannedTasks int
	var failedTasks []int64
	for bytesToPlan > 0 {
		candidates, err := h.deps.Repositories.CacheEvictions.ListLRUCandidates(ctx, candidateBatchSize)
		if err != nil {
			return plannedBytes, plannedTasks, failedTasks, err
		}
		if len(candidates) == 0 {
			break
		}
		createdThisBatch := 0
		for i := range candidates {
			candidate := candidates[i]
			entry, err := h.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, candidate.ContentID)
			if err != nil {
				return plannedBytes, plannedTasks, failedTasks, err
			}
			if entry != nil && entry.CacheActiveTaskID != nil {
				source, err := h.deps.Repositories.Tasks.GetByID(ctx, *entry.CacheActiveTaskID)
				if err != nil {
					return plannedBytes, plannedTasks, failedTasks, err
				}
				if source != nil && source.Status == model.TaskStatusFailed {
					failedTasks = append(failedTasks, source.ID)
					bytesToPlan -= candidate.Size
				}
				continue
			}
			scheduled := false
			err = h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
				accessedAt := cacheeviction.NormalizeAccessTime(candidate.AccessedAt)
				var err error
				scheduled, err = enqueueEviction(ctx, repos, h.deps.Scheduler, candidate.ContentID, &accessedAt)
				return err
			})
			if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
				continue
			}
			if err != nil {
				return plannedBytes, plannedTasks, failedTasks, fmt.Errorf("planning cache cleanup for content %d: %w", candidate.ContentID, err)
			}
			if !scheduled {
				continue
			}
			plannedBytes += candidate.Size
			bytesToPlan -= candidate.Size
			plannedTasks++
			createdThisBatch++
			if bytesToPlan <= 0 {
				break
			}
		}
		if len(candidates) < candidateBatchSize || createdThisBatch == 0 {
			break
		}
	}
	return plannedBytes, plannedTasks, failedTasks, nil
}

func (h *CapacityHandler) updateCachePressure(ctx context.Context, targetBytes int64, previous *pressureSnapshot) error {
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
	h.deps.State.pressure.Store(&pressureSnapshot{targetBytes: targetBytes})
	if previous == nil {
		if err := h.deps.PressureListener.OnCachePressure(ctx); err != nil {
			h.deps.State.pressure.Store(nil)
			return err
		}
	}
	return nil
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
