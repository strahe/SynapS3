package worker

import (
	"context"
	"fmt"

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
	action     evictionAction
	reason     string
	logMessage string
	cause      error
}

func (e *Evictor) deleteCacheEntry(
	ctx context.Context,
	task *model.Task,
	deletion *cacheeviction.AuthorizedDeletion,
) *evictionDecision {
	if deletion == nil {
		return failEviction("cache deletion was not authorized", "cache eviction has no authorized target")
	}
	if deletion.Version.InCache {
		if err := e.cache.Delete(ctx, deletion.BucketName, deletion.Version.CacheKey); err != nil {
			return retryEviction(err, "deleting cache entry")
		}
	}
	return e.recordCacheEntryDeleted(ctx, task, &deletion.Version)
}

func (e *Evictor) deleteCoordinatorCacheEntry(
	ctx context.Context,
	task *model.Task,
	deletion *cacheeviction.AuthorizedDeletion,
) error {
	if deletion == nil {
		return cacheeviction.ErrNoLongerEligible
	}
	if deletion.Version.InCache {
		if err := e.cache.Delete(ctx, deletion.BucketName, deletion.Version.CacheKey); err != nil {
			return err
		}
	}
	e.cacheAccessTracker.Forget(deletion.Version.VersionID)
	if err := e.repos.CacheEvictions.RecordAuthorizedDeletion(ctx, task); err != nil {
		return fmt.Errorf("recording cache eviction state: %w", err)
	}
	return nil
}

func (e *Evictor) recordCacheEntryDeleted(
	ctx context.Context,
	task *model.Task,
	version *model.ObjectVersion,
) *evictionDecision {
	e.cacheAccessTracker.Forget(version.VersionID)
	if err := e.repos.CacheEvictions.RecordAuthorizedDeletion(ctx, task); err != nil {
		return retryEviction(err, "recording cache eviction state")
	}
	return completeEviction()
}

func (e *Evictor) applyEvictionDecision(
	ctx context.Context,
	task *model.Task,
	decision *evictionDecision,
) {
	if decision == nil {
		return
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

func taskStage(task *model.Task) string {
	if task == nil || task.Stage == nil {
		return ""
	}
	return *task.Stage
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
