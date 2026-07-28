package worker

import (
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
)

type evictionStagePolicy struct {
	requiredPolicy       cache.EvictionPolicy
	policyMismatchReason string
	capacityBound        bool
	eligibility          func(*model.ObjectVersion) *evictionDecision
	rejectionAction      evictionAction
}

var (
	lruEvictionStagePolicy = evictionStagePolicy{
		requiredPolicy:       cache.EvictionPolicyLRU,
		policyMismatchReason: "Cache eviction policy no longer uses LRU",
		capacityBound:        true,
		eligibility:          lruEvictionEligibility,
		rejectionAction:      evictionCancel,
	}
	afterUploadEvictionStagePolicy = evictionStagePolicy{
		requiredPolicy:       cache.EvictionPolicyAfterUpload,
		policyMismatchReason: "Cache eviction policy no longer removes objects after upload",
		eligibility:          afterUploadEvictionEligibility,
		rejectionAction:      evictionFail,
	}
)

func evictionPolicyForTask(task *model.Task) (evictionStagePolicy, bool) {
	switch taskStage(task) {
	case cacheeviction.StageLRU:
		return lruEvictionStagePolicy, true
	case cacheeviction.StageAfterUpload:
		return afterUploadEvictionStagePolicy, true
	default:
		return evictionStagePolicy{}, false
	}
}

func (p evictionStagePolicy) reject(reason, logMessage string) *evictionDecision {
	if p.rejectionAction == evictionCancel {
		return cancelEviction(reason)
	}
	return failEviction(reason, logMessage)
}

func taskStage(task *model.Task) string {
	if task == nil || task.Stage == nil {
		return ""
	}
	return *task.Stage
}

func lruEvictionEligibility(version *model.ObjectVersion) *evictionDecision {
	if !version.InCache {
		return cancelEviction("Object is no longer present in the local cache")
	}
	if version.State != model.ObjectStateStored &&
		version.State != model.ObjectStateCacheEvicted {
		return cancelEviction("Object is no longer eligible for LRU eviction")
	}
	return nil
}

func afterUploadEvictionEligibility(version *model.ObjectVersion) *evictionDecision {
	if version.State == model.ObjectStateReplicating {
		return waitForEvictionDependency()
	}
	if version.State != model.ObjectStateStored {
		return failEviction("not stored", "object version not in stored state")
	}
	return nil
}
