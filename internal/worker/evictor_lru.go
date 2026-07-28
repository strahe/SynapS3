package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
)

func (e *Evictor) processLRUEviction(
	ctx context.Context,
	task *model.Task,
) *evictionDecision {
	if e.policy != cache.EvictionPolicyLRU {
		return cancelEviction("Cache eviction policy no longer uses LRU")
	}
	if !e.lruAccessTrackingSafe() {
		return cancelEviction("LRU eviction is paused because recent cache access could not be retained")
	}
	if e.cache.UsedBytes() <= watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent) {
		return cancelEviction("LRU cache usage already reached the low watermark")
	}
	payload, err := cacheeviction.ParseLRUTaskPayload(task)
	if err != nil {
		return cancelEviction("LRU eviction plan is no longer valid and can be replanned")
	}

	var decision *evictionDecision
	e.cacheGate.GuardDeletion(task.RefVersionID, func() {
		decision = e.finalizeLRUEviction(ctx, task, payload.AccessedAt)
	})
	return decision
}

func (e *Evictor) finalizeLRUEviction(
	ctx context.Context,
	task *model.Task,
	accessSnapshot time.Time,
) *evictionDecision {
	if !e.lruAccessTrackingSafe() {
		return cancelEviction("LRU eviction is paused because recent cache access could not be retained")
	}

	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil {
		return retryEviction(err, "loading object version for LRU cache eviction")
	}
	if version == nil {
		return cancelEviction("Object version no longer exists")
	}
	if !version.InCache {
		return cancelEviction("Object is no longer present in the local cache")
	}
	if version.State != model.ObjectStateStored &&
		version.State != model.ObjectStateCacheEvicted {
		return cancelEviction("Object is no longer eligible for LRU eviction")
	}

	durableAccess := effectiveLRUAccessTime(version)
	inMemoryAccess := e.cacheAccessTracker.Latest(task.RefVersionID)
	if !cacheeviction.NormalizeAccessTime(durableAccess).Equal(accessSnapshot) ||
		cacheeviction.NormalizeAccessTime(inMemoryAccess).After(accessSnapshot) {
		if newerInMemoryAccess(version.CacheAccessedAt, inMemoryAccess).IsZero() {
			return cancelEviction("Object was accessed after this LRU eviction was planned")
		}
		if err := e.cacheAccessTracker.FlushWhileGuarded(ctx, task.RefVersionID); err != nil {
			e.taskLogger(task).Warn(
				"persisting recent cache access before cancelling LRU eviction",
				"error",
				err,
			)
		}
		return cancelEviction("Object was accessed after this LRU eviction was planned")
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil {
		return retryEviction(err, "loading object bucket for LRU cache eviction")
	}
	if bucket == nil {
		return cancelEviction("Object bucket no longer exists")
	}

	readable, err := e.hasReadableRemoteCopy(ctx, version)
	if err != nil {
		return retryEviction(err, "checking readable remote copies before LRU cache eviction")
	}
	if !readable {
		return cancelEviction("Object no longer has a readable committed remote copy")
	}

	if !e.reserveLRUDeletion(version.Size) {
		return cancelEviction("LRU cache usage already reached the low watermark")
	}
	defer e.releaseLRUDeletion(version.Size)
	return e.deleteCacheEntry(ctx, task, bucket.Name, version)
}

func effectiveLRUAccessTime(version *model.ObjectVersion) time.Time {
	if version == nil {
		return time.Time{}
	}
	if version.CacheAccessedAt != nil {
		return cacheeviction.NormalizeAccessTime(*version.CacheAccessedAt)
	}
	return cacheeviction.NormalizeAccessTime(version.CreatedAt)
}

func newerInMemoryAccess(durable *time.Time, inMemory time.Time) time.Time {
	inMemory = cacheeviction.NormalizeAccessTime(inMemory)
	if inMemory.IsZero() {
		return time.Time{}
	}
	if durable != nil &&
		!inMemory.After(cacheeviction.NormalizeAccessTime(*durable)) {
		return time.Time{}
	}
	return inMemory
}

func (e *Evictor) runLRUPlanner(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		e.recordTick()
		if err := e.planLRUEvictions(ctx); err != nil && ctx.Err() == nil {
			e.logger.Error("planning LRU cache evictions", "error", err)
		}
		if !sleepUntilNextWorkerPoll(ctx, e.pollInterval) {
			return
		}
	}
}

func (e *Evictor) planLRUEvictions(ctx context.Context) error {
	if e.policy != cache.EvictionPolicyLRU || e.maxCacheBytes <= 0 {
		return nil
	}
	if !e.lruAccessTrackingSafe() {
		return nil
	}
	usedBytes := e.cache.UsedBytes()
	highBytes := watermarkBytes(e.maxCacheBytes, e.highWatermarkPercent)
	if usedBytes < highBytes {
		return nil
	}
	lowBytes := watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent)
	activeBytes, err := e.repos.CacheEvictions.ActiveLRUBytes(ctx)
	if err != nil {
		return err
	}
	bytesToPlan := usedBytes - lowBytes - activeBytes
	if bytesToPlan <= 0 {
		return nil
	}

	var plannedBytes int64
	plannedTasks := 0
	for bytesToPlan > 0 {
		createdThisBatch := 0
		terminalSince := time.Now().Add(-lruTerminalRetryDelay)
		candidates, err := e.repos.CacheEvictions.ListLRUCandidates(
			ctx,
			terminalSince,
			lruCandidateBatchSize,
		)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			activated, err := e.repos.CacheEvictions.PlanLRU(
				ctx,
				candidate,
				e.maxRetries,
				terminalSince,
			)
			if err != nil {
				return fmt.Errorf("creating LRU eviction task for version %s: %w", candidate.VersionID, err)
			}
			if !activated {
				continue
			}
			plannedTasks++
			createdThisBatch++
			plannedBytes += candidate.Size
			bytesToPlan -= candidate.Size
			if bytesToPlan <= 0 {
				break
			}
		}
		if len(candidates) < lruCandidateBatchSize || createdThisBatch == 0 {
			break
		}
	}

	if plannedTasks == 0 {
		e.logger.Warn(
			"cache is above the LRU high watermark but no remotely safe entries can be evicted",
			"usedBytes",
			usedBytes,
			"highBytes",
			highBytes,
			"lowBytes",
			lowBytes,
		)
		return nil
	}
	e.logger.Info(
		"planned LRU cache evictions",
		"tasks",
		plannedTasks,
		"plannedBytes",
		plannedBytes,
		"usedBytes",
		usedBytes,
		"targetBytes",
		lowBytes,
	)
	return nil
}

func watermarkBytes(maxBytes int64, percent int) int64 {
	if maxBytes <= 0 || percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return maxBytes
	}
	return (maxBytes/100)*int64(percent) + (maxBytes%100)*int64(percent)/100
}
