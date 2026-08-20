package worker

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func (e *Evictor) processAfterUploadEviction(
	ctx context.Context,
	task *model.Task,
) *evictionDecision {
	authorized, err := cacheeviction.DeleteAuthorized(task)
	if err != nil {
		return cancelEviction("Cache deletion authorization is invalid")
	}
	if !authorized && e.policy != cache.EvictionPolicyAfterUpload {
		return cancelEviction("Cache eviction policy no longer removes objects after upload")
	}

	var decision *evictionDecision
	e.cacheGate.GuardDeletion(task.RefVersionID, func() {
		decision = e.finalizeAfterUploadEviction(ctx, task)
	})
	return decision
}

func (e *Evictor) finalizeAfterUploadEviction(
	ctx context.Context,
	task *model.Task,
) *evictionDecision {
	deletion, err := e.repos.CacheEvictions.AuthorizeDeletion(ctx, task, nil)
	switch {
	case err == nil:
		return e.deleteCacheEntry(ctx, task, deletion)
	case errors.Is(err, cacheeviction.ErrDurabilityThreshold):
		return waitForEvictionDependency()
	case errors.Is(err, repository.ErrNotFound):
		return failEviction("object not found", "object version not found for after-upload cache eviction")
	case errors.Is(err, cacheeviction.ErrNoLongerEligible), errors.Is(err, cacheeviction.ErrAccessChanged):
		return failEviction("not stored", "object version not in stored state")
	default:
		return retryEviction(err, "authorizing after-upload cache eviction")
	}
}
