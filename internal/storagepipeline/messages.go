package storagepipeline

import "time"

const (
	MessageEnsureDataSet            = "storage.ensure_dataset"
	MessageStartCopyTransfer        = "storage.start_copy_transfer"
	MessageFailCopy                 = "storage.fail_copy"
	MessageJoinCommit               = "storage.join_commit"
	MessageEvictCacheContent        = "storage.evict_cache_content"
	MessageDataSetReady             = "storage.dataset_ready"
	MessageContentCommitted         = "storage.content_committed"
	MessageContentDurabilityReached = "storage.content_durability_reached"
)

type EnsureDataSet struct{ BindingID int64 }

func (EnsureDataSet) MessageType() string { return MessageEnsureDataSet }

type StartCopyTransfer struct {
	CopyID      int64
	AvailableAt time.Time
}

func (StartCopyTransfer) MessageType() string { return MessageStartCopyTransfer }

type FailCopy struct {
	CopyID        int64
	Message       string
	PullAttemptID string
}

func (FailCopy) MessageType() string { return MessageFailCopy }

type JoinCommit struct{ CopyID int64 }

func (JoinCommit) MessageType() string { return MessageJoinCommit }

type EvictCacheContent struct{ ContentID int64 }

func (EvictCacheContent) MessageType() string { return MessageEvictCacheContent }

type DataSetReady struct{ BindingID int64 }

func (DataSetReady) MessageType() string { return MessageDataSetReady }

type ContentCommitted struct {
	ContentID        int64
	BucketID         int64
	IngressCommitted bool
}

func (ContentCommitted) MessageType() string { return MessageContentCommitted }

type ContentDurabilityReached struct{ ContentIDs []int64 }

func (ContentDurabilityReached) MessageType() string { return MessageContentDurabilityReached }
