package worker

import (
	"context"

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
	bucketName string,
	version *model.ObjectVersion,
) *evictionDecision {
	if err := e.cache.Delete(ctx, bucketName, version.CacheKey); err != nil {
		return retryEviction(err, "deleting cache entry")
	}
	e.cacheAccessTracker.Forget(version.VersionID)
	if err := e.recordDeletedCacheState(ctx, task, version); err != nil {
		return retryEviction(err, "recording cache eviction state")
	}
	return completeEviction()
}

func (e *Evictor) hasReadableRemoteCopy(
	ctx context.Context,
	version *model.ObjectVersion,
) (bool, error) {
	if version.StorageUploadID == nil {
		return false, nil
	}
	return e.repos.Uploads.HasReadableCommittedCopy(ctx, *version.StorageUploadID)
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
