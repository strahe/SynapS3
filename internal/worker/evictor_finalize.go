package worker

import (
	"context"
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
)

type evictionAction uint8

const (
	evictionComplete evictionAction = iota + 1
	evictionCancel
	evictionRetry
	evictionFail
	evictionWait
)

type evictionDecision struct {
	action        evictionAction
	reason        string
	logMessage    string
	cause         error
	accessToStore time.Time
}

type preparedEviction struct {
	version           *model.ObjectVersion
	bucketName        string
	lruAccessSnapshot time.Time
}

func (e *Evictor) prepareEviction(
	ctx context.Context,
	task *model.Task,
	stagePolicy evictionStagePolicy,
) (*preparedEviction, *evictionDecision) {
	if e.policy != stagePolicy.requiredPolicy {
		return nil, cancelEviction(stagePolicy.policyMismatchReason)
	}
	if stagePolicy.capacityBound &&
		e.cache.UsedBytes() <= watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent) {
		return nil, cancelEviction("LRU cache usage already reached the low watermark")
	}

	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil {
		return nil, retryEviction(err, "loading object version for cache eviction")
	}
	if version == nil {
		return nil, stagePolicy.reject(
			"object not found",
			"object version not found for cache eviction",
		)
	}
	if decision := stagePolicy.eligibility(version); decision != nil {
		return nil, decision
	}

	var snapshot time.Time
	if stagePolicy.capacityBound {
		payload, err := cacheeviction.ParseLRUTaskPayload(task)
		if err != nil {
			return nil, cancelEviction("LRU eviction plan is no longer valid and can be replanned")
		}
		snapshot = payload.AccessedAt
	}

	if decision := e.remoteSafetyDecision(
		ctx,
		stagePolicy,
		version,
		"checking readable remote copies before cache eviction",
	); decision != nil {
		return nil, decision
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil {
		return nil, retryEviction(err, "loading object bucket for cache eviction")
	}
	if bucket == nil {
		return nil, stagePolicy.reject("bucket not found", "bucket not found for cache eviction")
	}
	return &preparedEviction{
		version:           version,
		bucketName:        bucket.Name,
		lruAccessSnapshot: snapshot,
	}, nil
}

func (e *Evictor) finalizePreparedEviction(
	ctx context.Context,
	task *model.Task,
	prepared *preparedEviction,
	stagePolicy evictionStagePolicy,
	lastAccess time.Time,
) (*evictionDecision, bool) {
	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil {
		return retryEviction(err, "reloading object version before cache eviction"), false
	}
	if version == nil {
		return stagePolicy.reject(
			"object not found",
			"object version not found before cache eviction",
		), false
	}
	if decision := stagePolicy.eligibility(version); decision != nil {
		return decision, false
	}
	if !sameEvictionTarget(prepared.version, version) {
		return retryEviction(
			errors.New("object version changed while preparing cache eviction"),
			"object version changed before cache eviction",
		), false
	}
	if stagePolicy.capacityBound {
		durableAccess := effectiveLRUAccessTime(version)
		if !cacheeviction.NormalizeAccessTime(durableAccess).Equal(prepared.lruAccessSnapshot) ||
			cacheeviction.NormalizeAccessTime(lastAccess).After(prepared.lruAccessSnapshot) {
			decision := cancelEviction("Object was accessed after this LRU eviction was planned")
			decision.accessToStore = newerInMemoryAccess(version.CacheAccessedAt, lastAccess)
			return decision, false
		}
		if !e.reserveLRUDeletion(version.Size) {
			return cancelEviction("LRU cache usage already reached the low watermark"), false
		}
		defer e.releaseLRUDeletion(version.Size)
	}

	if err := e.cache.Delete(ctx, prepared.bucketName, version.CacheKey); err != nil {
		return retryEviction(err, "deleting cache entry"), false
	}
	if err := e.recordDeletedCacheState(ctx, task, version); err != nil {
		return retryEviction(err, "recording cache eviction state"), true
	}
	return completeEviction(), true
}

func (e *Evictor) remoteSafetyDecision(
	ctx context.Context,
	stagePolicy evictionStagePolicy,
	version *model.ObjectVersion,
	logMessage string,
) *evictionDecision {
	if version.StorageUploadID == nil {
		return stagePolicy.reject(
			"no accepted upload",
			"object version has no accepted upload, refusing to evict",
		)
	}
	readable, err := e.repos.Uploads.HasReadableCommittedCopy(ctx, *version.StorageUploadID)
	if err != nil {
		return retryEviction(err, logMessage)
	}
	if !readable {
		return stagePolicy.reject(
			"no readable upload copies",
			"object version has no readable upload copies, refusing to evict",
		)
	}
	return nil
}

func (e *Evictor) applyEvictionDecision(
	ctx context.Context,
	task *model.Task,
	decision *evictionDecision,
) {
	if decision == nil {
		return
	}
	if !decision.accessToStore.IsZero() {
		if err := e.repos.Objects.RecordVersionCacheAccess(
			ctx,
			task.RefVersionID,
			decision.accessToStore,
		); err != nil {
			e.taskLogger(task).Warn(
				"persisting recent cache access before cancelling LRU eviction",
				"error",
				err,
			)
		}
	}

	switch decision.action {
	case evictionComplete:
		e.completeTask(ctx, task, e.taskLogger(task), "cache eviction completed")
	case evictionCancel:
		e.cancelTask(ctx, task, decision.reason)
	case evictionRetry:
		e.retryTask(ctx, task, decision.cause, decision.logMessage)
	case evictionFail:
		e.failTask(ctx, task, decision.reason, decision.logMessage)
	case evictionWait:
		e.deferReplicatingEviction(ctx, task)
	default:
		e.failTask(ctx, task, "invalid cache eviction decision", "cache eviction produced an invalid decision")
	}
}

func sameEvictionTarget(before, after *model.ObjectVersion) bool {
	if before == nil || after == nil ||
		before.BucketID != after.BucketID ||
		before.CacheKey != after.CacheKey {
		return false
	}
	if before.StorageUploadID == nil || after.StorageUploadID == nil {
		return before.StorageUploadID == nil && after.StorageUploadID == nil
	}
	return *before.StorageUploadID == *after.StorageUploadID
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

func completeEviction() *evictionDecision {
	return &evictionDecision{action: evictionComplete}
}

func cancelEviction(reason string) *evictionDecision {
	return &evictionDecision{action: evictionCancel, reason: reason}
}

func retryEviction(err error, logMessage string) *evictionDecision {
	return &evictionDecision{
		action:     evictionRetry,
		cause:      err,
		logMessage: logMessage,
	}
}

func failEviction(reason, logMessage string) *evictionDecision {
	return &evictionDecision{
		action:     evictionFail,
		reason:     reason,
		logMessage: logMessage,
	}
}

func waitForEvictionDependency() *evictionDecision {
	return &evictionDecision{action: evictionWait}
}
