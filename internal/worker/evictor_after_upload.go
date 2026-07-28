package worker

import (
	"context"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
)

func (e *Evictor) processAfterUploadEviction(
	ctx context.Context,
	task *model.Task,
) *evictionDecision {
	if e.policy != cache.EvictionPolicyAfterUpload {
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
	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil {
		return retryEviction(err, "loading object version for after-upload cache eviction")
	}
	if version == nil {
		return failEviction("object not found", "object version not found for after-upload cache eviction")
	}
	switch version.State {
	case model.ObjectStateReplicating:
		return waitForEvictionDependency()
	case model.ObjectStateStored:
	default:
		return failEviction("not stored", "object version not in stored state")
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil {
		return retryEviction(err, "loading object bucket for after-upload cache eviction")
	}
	if bucket == nil {
		return failEviction("bucket not found", "bucket not found for after-upload cache eviction")
	}

	readable, err := e.hasReadableRemoteCopy(ctx, version)
	if err != nil {
		return retryEviction(err, "checking readable remote copies before after-upload cache eviction")
	}
	if !readable {
		if version.StorageUploadID == nil {
			return failEviction(
				"no accepted upload",
				"object version has no accepted upload, refusing after-upload cache eviction",
			)
		}
		return failEviction(
			"no readable upload copies",
			"object version has no readable upload copies, refusing after-upload cache eviction",
		)
	}

	return e.deleteCacheEntry(ctx, task, bucket.Name, version)
}
