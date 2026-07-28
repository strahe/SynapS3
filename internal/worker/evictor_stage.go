package worker

import (
	"context"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type evictionStagePolicy struct {
	requiredPolicy       cache.EvictionPolicy
	policyMismatchReason string
	capacityBound        bool
	eligibility          func(*model.ObjectVersion) guardedEvictionResult
	reject               func(*Evictor, context.Context, *model.Task, string, string)
}

var (
	lruEvictionStagePolicy = evictionStagePolicy{
		requiredPolicy:       cache.EvictionPolicyLRU,
		policyMismatchReason: "Cache eviction policy no longer uses LRU",
		capacityBound:        true,
		eligibility:          lruEvictionEligibility,
		reject:               cancelRejectedEviction,
	}
	afterUploadEvictionStagePolicy = evictionStagePolicy{
		requiredPolicy:       cache.EvictionPolicyAfterUpload,
		policyMismatchReason: "Cache eviction policy no longer removes objects after upload",
		eligibility:          afterUploadEvictionEligibility,
		reject:               failRejectedEviction,
	}
)

func evictionPolicyForTask(task *model.Task) (evictionStagePolicy, bool) {
	switch taskStage(task) {
	case repository.CacheEvictionStageLRU:
		return lruEvictionStagePolicy, true
	case repository.CacheEvictionStageAfterUpload:
		return afterUploadEvictionStagePolicy, true
	default:
		return evictionStagePolicy{}, false
	}
}

func taskStage(task *model.Task) string {
	if task == nil || task.Stage == nil {
		return ""
	}
	return *task.Stage
}

func lruEvictionEligibility(version *model.ObjectVersion) guardedEvictionResult {
	if !version.InCache {
		return guardedEvictionResult{
			action: guardedEvictionReject,
			reason: "Object is no longer present in the local cache",
		}
	}
	if version.State != model.ObjectStateStored &&
		version.State != model.ObjectStateCacheEvicted {
		return guardedEvictionResult{
			action: guardedEvictionReject,
			reason: "Object is no longer eligible for LRU eviction",
		}
	}
	return guardedEvictionResult{}
}

func afterUploadEvictionEligibility(version *model.ObjectVersion) guardedEvictionResult {
	if version.State == model.ObjectStateReplicating {
		return guardedEvictionResult{action: guardedEvictionDefer}
	}
	if version.State != model.ObjectStateStored {
		return guardedEvictionResult{
			action:     guardedEvictionReject,
			reason:     "not stored",
			logMessage: "object version not in stored state",
		}
	}
	return guardedEvictionResult{}
}

func cancelRejectedEviction(
	evictor *Evictor,
	ctx context.Context,
	task *model.Task,
	reason string,
	_ string,
) {
	evictor.cancelTask(ctx, task, reason)
}

func failRejectedEviction(
	evictor *Evictor,
	ctx context.Context,
	task *model.Task,
	reason string,
	logMessage string,
) {
	evictor.failTask(ctx, task, reason, logMessage)
}
