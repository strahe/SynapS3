package storagepipeline

import "github.com/strahe/synaps3/internal/model"

// CopyRetryBlock describes why a failed replica cannot start fresh work.
type CopyRetryBlock string

const (
	CopyRetryObjectDeleted             CopyRetryBlock = "object_deleted"
	CopyRetryReplacementInProgress     CopyRetryBlock = "replacement_in_progress"
	CopyRetryStorageServiceUnavailable CopyRetryBlock = "storage_service_unavailable"
	CopyRetryNoSource                  CopyRetryBlock = "no_source"
	CopyRetryRecoveryRequiresAttention CopyRetryBlock = "recovery_requires_attention"
)

func (b CopyRetryBlock) Valid() bool {
	//exhaustive:enforce
	switch b {
	case CopyRetryObjectDeleted, CopyRetryReplacementInProgress, CopyRetryStorageServiceUnavailable, CopyRetryNoSource, CopyRetryRecoveryRequiresAttention:
		return true
	default:
		return false
	}
}

// CopyRetryFacts is shared by read-only presentation and locked admission.
type CopyRetryFacts struct {
	Status                model.StorageCopyStatus
	Owned                 bool
	Sealed                bool
	Method                model.StorageCopyTransferMethod
	Current               bool
	DataSetStatus         model.StorageDataSetStatus
	ObjectDeleted         bool
	ReplacementInProgress bool
	UnresolvedPull        bool
	ReadableSources       int
	CacheAvailable        bool
	OtherIngress          bool
}

func DecideCopyRetry(f CopyRetryFacts) (model.StorageCopyTransferMethod, CopyRetryBlock, bool) {
	if f.Status != model.StorageCopyStatusFailed || f.Owned || f.Sealed || !f.Method.Valid() {
		return "", "", false
	}
	if f.ObjectDeleted {
		return "", CopyRetryObjectDeleted, false
	}
	if f.UnresolvedPull {
		return "", CopyRetryRecoveryRequiresAttention, false
	}
	if f.ReplacementInProgress {
		return "", CopyRetryReplacementInProgress, false
	}
	if !f.Current || f.DataSetStatus != model.StorageDataSetStatusReady {
		return "", CopyRetryStorageServiceUnavailable, false
	}
	if f.ReadableSources > 0 {
		return model.StorageCopyTransferMethodPeerPull, "", true
	}
	if !f.CacheAvailable {
		return "", CopyRetryNoSource, false
	}
	if f.Method == model.StorageCopyTransferMethodIngress && !f.OtherIngress {
		return model.StorageCopyTransferMethodIngress, "", true
	}
	return model.StorageCopyTransferMethodCacheRestore, "", true
}
