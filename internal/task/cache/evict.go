package cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type EvictDependencies struct {
	Repositories   *repository.Repositories
	Cache          cache.Cache
	CacheGate      *cacheaccess.Gate
	CacheTracker   *cacheaccess.Tracker
	State          *State
	EvictionPolicy cache.EvictionPolicy
	MaxCacheBytes  int64
	MaxWriteBytes  int64
	LRULowPercent  int
}
type EvictHandler struct {
	*taskengine.FuncHandler
	deps EvictDependencies
}

func NewEvictHandler(deps EvictDependencies) (*EvictHandler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil || deps.State == nil {
		return nil, errors.New("cache eviction handler requires repositories and state")
	}
	h := &EvictHandler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}

func (h *EvictHandler) lruLowBytes() int64 {
	return lruLowBytes(h.deps.MaxCacheBytes, h.deps.MaxWriteBytes, h.deps.LRULowPercent)
}

type cacheEvictionCheckpoint struct {
	AttemptedAt time.Time `json:"attempted_at"`
}

func (h *EvictHandler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeCacheEvict, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		Codec:  taskengine.StrictJSONCodec(cacheeviction.ValidateEvictInput),
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
		Subject: taskengine.SubjectFromInput(model.TaskSubjectStorageContent, func(input cacheeviction.EvictInput) int64 {
			return input.ContentID
		}),
	}
	return taskengine.NewFuncHandler(definition, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runCacheEviction(ctx, execution, true)
	},
		func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runCacheEviction(ctx, execution, false)
		},
	)
}

func (h *EvictHandler) runCacheEviction(
	ctx context.Context,
	execution taskengine.Execution,
	allowDelete bool,
) taskengine.Result {
	input, err := taskengine.DecodeInput[cacheeviction.EvictInput](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeCacheEvict), err)
	}
	if h.deps.Cache == nil || h.deps.CacheGate == nil || h.deps.CacheTracker == nil {
		return taskengine.Fail(
			errors.New("cache deletion dependencies are unavailable"),
			"dependency_unavailable",
			nil,
		)
	}
	if _, _, checkpointErr := taskengine.DecodeCheckpoint[cacheEvictionCheckpoint](execution); checkpointErr != nil {
		return taskengine.Fail(checkpointErr, "invalid_checkpoint", nil)
	}
	if allowDelete {
		checkpoint := cacheEvictionCheckpoint{AttemptedAt: time.Now().UTC()}
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_checkpoint_failed")
		}
	}
	var result taskengine.Result
	h.deps.CacheGate.GuardDeletion(model.ContentCacheKey(input.ContentID), func() {
		if input.AccessedAt != nil {
			if h.deps.EvictionPolicy != cache.EvictionPolicyLRU || !h.deps.CacheTracker.SafeForLRU() {
				result = h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
				return
			}
			if h.deps.Cache.CapacitySnapshot().OccupiedBytes() <= h.lruLowBytes() {
				result = h.cancelCacheEviction(input, execution.ID(), "Local cache usage reached its target")
				return
			}
		}
		result = h.deleteAuthorizedCacheEntry(ctx, execution, input, allowDelete)
	})
	return result
}

func (h *EvictHandler) deleteAuthorizedCacheEntry(
	ctx context.Context,
	execution taskengine.Execution,
	input cacheeviction.EvictInput,
	allowDelete bool,
) (result taskengine.Result) {
	var workStartedAt time.Time
	defer func() { result = result.WithWorkStartedAt(workStartedAt) }()
	deletionSucceeded := false
	if input.AccessedAt != nil && allowDelete {
		if !h.deps.CacheTracker.SafeForLRU() {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		entry, err := h.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_entry_load_failed")
		}
		content, err := h.deps.Repositories.Contents.GetByID(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_content_load_failed")
		}
		if entry == nil || content == nil || entry.CacheAccessedAt == nil ||
			!cacheeviction.NormalizeAccessTime(*entry.CacheAccessedAt).Equal(*input.AccessedAt) ||
			cacheeviction.NormalizeAccessTime(h.deps.CacheTracker.Latest(input.ContentID)).After(*input.AccessedAt) {
			if entry != nil && entry.CacheAccessedAt != nil &&
				cacheeviction.NormalizeAccessTime(h.deps.CacheTracker.Latest(input.ContentID)).After(cacheeviction.NormalizeAccessTime(*entry.CacheAccessedAt)) {
				if flushErr := h.deps.CacheTracker.FlushWhileGuarded(ctx, input.ContentID); flushErr != nil {
					return h.retryCacheEviction(execution, input, flushErr, "cache_access_flush_failed")
				}
			}
			return h.cancelCacheEviction(input, execution.ID(), "Cached data was used after cleanup was scheduled")
		}
		if !h.reserveLRUDeletion(content.ContentSize) {
			return h.cancelCacheEviction(input, execution.ID(), "Local cache usage reached its target")
		}
		defer func() { h.finishLRUDeletion(content.ContentSize, deletionSucceeded) }()
	}
	if !allowDelete {
		entry, err := h.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_entry_load_failed")
		}
		if entry == nil {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		content, err := h.deps.Repositories.Contents.GetByID(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_content_load_failed")
		}
		if content == nil {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, content.BucketID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_bucket_load_failed")
		}
		if bucket == nil {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		body, _, err := h.deps.Cache.Get(ctx, bucket.Name, model.ContentCacheKey(input.ContentID))
		switch {
		case err == nil:
			if body == nil {
				return h.retryCacheEviction(execution, input, errors.New("cache returned an empty read handle"), "cache_observation_failed")
			}
			if closeErr := body.Close(); closeErr != nil {
				return h.retryCacheEviction(execution, input, closeErr, "cache_observation_failed")
			}
			return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Local cache removal is ready", nil)
		case os.IsNotExist(err):
			finalizeErr := h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
				if err := repos.Tasks.ValidateClaim(ctx, execution.ID(), execution.ClaimGeneration()); err != nil {
					return err
				}
				return repos.CacheEvictions.RecordDeletion(ctx, input.ContentID, input.Generation, execution.ID())
			})
			if errors.Is(finalizeErr, repository.ErrConflict) || errors.Is(finalizeErr, repository.ErrNotFound) {
				return h.cancelCacheEviction(input, execution.ID(), "Cache removal was superseded")
			}
			if finalizeErr != nil {
				return h.retryCacheEviction(execution, input, finalizeErr, "cache_record_failed")
			}
			h.deps.CacheTracker.Forget(input.ContentID)
			return taskengine.Complete("Local cache removed", nil)
		default:
			return h.retryCacheEviction(execution, input, err, "cache_observation_failed")
		}
	}
	deleteErr := h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
		if err := repos.Tasks.ValidateClaim(ctx, execution.ID(), execution.ClaimGeneration()); err != nil {
			return err
		}
		authorized, err := repos.CacheEvictions.AuthorizeDeletion(
			ctx, input.ContentID, input.Generation, execution.ID(), input.AccessedAt,
		)
		if err != nil {
			return err
		}
		workStartedAt = time.Now().UTC()
		if err := h.deps.Cache.Delete(ctx, authorized.BucketName, model.ContentCacheKey(authorized.Content.ID)); err != nil {
			return fmt.Errorf("deleting cache file: %w", err)
		}
		return repos.CacheEvictions.RecordDeletion(ctx, input.ContentID, input.Generation, execution.ID())
	})
	if deleteErr != nil {
		recorded, checkErr := h.deps.Repositories.CacheEvictions.DeletionRecorded(ctx, input.ContentID, input.Generation)
		if checkErr == nil && recorded {
			h.deps.CacheTracker.Forget(input.ContentID)
			if input.AccessedAt != nil {
				deletionSucceeded = true
			}
			return taskengine.Complete("Local cache removed", nil)
		}
		switch {
		case errors.Is(deleteErr, cacheeviction.ErrDurabilityThreshold) && input.AccessedAt == nil:
			return taskengine.Wait(model.TaskResumeModeExecute, dependencyWait, "durability", "Waiting for durable storage", nil)
		case errors.Is(deleteErr, cacheeviction.ErrDurabilityThreshold), errors.Is(deleteErr, cacheeviction.ErrNoLongerEligible),
			errors.Is(deleteErr, cacheeviction.ErrAccessChanged), errors.Is(deleteErr, repository.ErrNotFound), errors.Is(deleteErr, repository.ErrConflict):
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		default:
			return h.retryCacheEviction(execution, input, errors.Join(deleteErr, checkErr), "cache_delete_failed")
		}
	}
	h.deps.CacheTracker.Forget(input.ContentID)
	if input.AccessedAt != nil {
		deletionSucceeded = true
	}
	return taskengine.Complete("Local cache removed", nil)
}

func (h *EvictHandler) cancelCacheEviction(
	input cacheeviction.EvictInput,
	taskID int64,
	message string,
) taskengine.Result {
	return taskengine.Cancel(message, h.releaseCacheEvictionSettlement(input, taskID))
}

func (h *EvictHandler) releaseCacheEvictionSettlement(input cacheeviction.EvictInput, taskID int64) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		err := repos.CacheEvictions.ReleaseEviction(ctx, input.ContentID, input.Generation, taskID)
		if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
			return nil
		}
		return err
	}
}

func (h *EvictHandler) retryCacheEviction(
	execution taskengine.Execution,
	input cacheeviction.EvictInput,
	err error,
	reason string,
) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func (h *EvictHandler) reserveLRUDeletion(size int64) bool {
	h.deps.State.lruCapacityMu.Lock()
	defer h.deps.State.lruCapacityMu.Unlock()
	if h.deps.State.lruInFlightDeletes == 0 {
		h.deps.State.lruProjectedBytes = h.deps.Cache.CapacitySnapshot().OccupiedBytes()
	}
	if h.deps.State.lruProjectedBytes <= h.lruLowBytes() {
		return false
	}
	h.deps.State.lruProjectedBytes -= size
	h.deps.State.lruInFlightDeletes++
	return true
}

func (h *EvictHandler) finishLRUDeletion(size int64, deleted bool) {
	h.deps.State.lruCapacityMu.Lock()
	defer h.deps.State.lruCapacityMu.Unlock()
	if !deleted {
		h.deps.State.lruProjectedBytes += size
	}
	h.deps.State.lruInFlightDeletes--
	if h.deps.State.lruInFlightDeletes <= 0 {
		h.deps.State.lruInFlightDeletes = 0
		h.deps.State.lruProjectedBytes = 0
	}
}
