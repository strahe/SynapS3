package worker

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

const bucketDurabilityBatchSize = 100

func (e *Evictor) processBucketDurabilityReconciliation(ctx context.Context, task *model.Task) {
	logger := e.taskLogger(task)
	processed := 0
	for processed < bucketDurabilityBatchSize {
		authorized, err := cacheeviction.DeleteAuthorized(task)
		if err != nil {
			e.cancelTask(ctx, task, "Cache deletion authorization is invalid")
			return
		}
		if authorized {
			if decision := e.resumeBucketDurabilityDeletion(ctx, task); decision != nil {
				e.applyEvictionDecision(ctx, task, decision)
				return
			}
			processed++
			continue
		}

		candidate, err := e.repos.CacheEvictions.NextBucketDurabilityCandidate(ctx, task.RefID)
		if err != nil {
			e.applyEvictionDecision(ctx, task, retryEviction(err, "selecting bucket durability candidate"))
			return
		}
		if candidate == nil {
			completed, err := e.repos.CacheEvictions.CompleteBucketDurabilityReconciliation(ctx, task)
			switch {
			case err == nil && completed:
				admin.WorkerTasksProcessed.WithLabelValues("evictor", "success").Inc()
				logger.Info("bucket cache policy applied", "versions", processed)
				return
			case err == nil:
				continue
			case errors.Is(err, cacheeviction.ErrNoLongerEligible):
				e.cancelTask(ctx, task, "Bucket no longer exists")
				return
			default:
				e.applyEvictionDecision(ctx, task, retryEviction(err, "completing bucket durability reconciliation"))
				return
			}
		}

		promoted, decision := e.promoteBucketDurabilityCandidate(ctx, task, candidate.VersionID)
		if decision != nil {
			e.applyEvictionDecision(ctx, task, decision)
			return
		}
		if promoted {
			processed++
		}
	}

	if err := e.repos.Tasks.ReleaseRunning(ctx, task); err != nil {
		e.applyEvictionDecision(ctx, task, retryEviction(err, "requeueing bucket durability reconciliation"))
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("evictor", "success").Inc()
	logger.Debug("requeued bucket cache policy batch", "versions", processed)
}

func (e *Evictor) promoteBucketDurabilityCandidate(
	ctx context.Context,
	task *model.Task,
	versionID string,
) (bool, *evictionDecision) {
	deleteAfterPromotion := e.policy == cache.EvictionPolicyAfterUpload
	if !deleteAfterPromotion {
		_, err := e.repos.CacheEvictions.PromoteBucketDurabilityCandidate(ctx, task, versionID, false)
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, cacheeviction.ErrNoLongerEligible),
			errors.Is(err, cacheeviction.ErrDurabilityThreshold):
			return false, nil
		default:
			return false, retryEviction(err, "promoting bucket durability candidate")
		}
	}

	var (
		promoted bool
		decision *evictionDecision
	)
	e.cacheGate.GuardDeletion(versionID, func() {
		deletion, err := e.repos.CacheEvictions.PromoteBucketDurabilityCandidate(ctx, task, versionID, true)
		switch {
		case err == nil:
			if err := e.deleteCoordinatorCacheEntry(ctx, task, deletion); err != nil {
				decision = retryEviction(err, "deleting reconciled cache entry")
				return
			}
			promoted = true
		case errors.Is(err, cacheeviction.ErrNoLongerEligible),
			errors.Is(err, cacheeviction.ErrDurabilityThreshold):
			return
		default:
			decision = retryEviction(err, "promoting bucket durability candidate")
		}
	})
	return promoted, decision
}

func (e *Evictor) resumeBucketDurabilityDeletion(
	ctx context.Context,
	task *model.Task,
) *evictionDecision {
	var decision *evictionDecision
	e.cacheGate.GuardDeletion(task.RefVersionID, func() {
		deletion, err := e.repos.CacheEvictions.AuthorizeDeletion(ctx, task, nil)
		switch {
		case err == nil:
			if err := e.deleteCoordinatorCacheEntry(ctx, task, deletion); err != nil {
				decision = retryEviction(err, "resuming reconciled cache deletion")
			}
		case errors.Is(err, repository.ErrNotFound):
			switch clearErr := e.repos.CacheEvictions.RecordAuthorizedDeletion(ctx, task); {
			case clearErr == nil:
			case errors.Is(clearErr, cacheeviction.ErrNoLongerEligible):
				decision = cancelEviction("Bucket no longer exists")
			default:
				decision = retryEviction(clearErr, "clearing completed cache deletion authorization")
			}
		case errors.Is(err, cacheeviction.ErrNoLongerEligible):
			decision = cancelEviction("Authorized cache entry no longer exists")
		default:
			decision = retryEviction(err, "loading authorized cache deletion")
		}
	})
	return decision
}
