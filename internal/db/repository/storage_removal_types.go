package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storageremoval"
)

type EnqueueRemovalTask func(context.Context, *Repositories, storageremoval.Input) (int64, error)

type JoinRemovalInput struct {
	ContentID        int64
	Generation       int64
	ContentTaskID    int64
	StorageDataSetID int64
	CopyIDs          []int64
	Identity         storagepipeline.ExecutionIdentity
	Enqueue          EnqueueRemovalTask
	Now              time.Time
}

type RemovalRequestInput struct {
	RequestID string
	TaskID    int64
	Message   string
	Now       time.Time
}

type SealRemovalInput struct {
	RequestID string
	TaskID    int64
	MaxPieces int
	Now       time.Time
}

type RemovalMembersInput struct {
	RequestID string
	TaskID    int64
	CopyIDs   []int64
	Message   string
	Detach    bool
	Now       time.Time
}

type AdmitRemovalSendInput struct {
	RequestID     string
	TaskID        int64
	SendNo        int
	CopyIDs       []int64
	AttemptedAt   time.Time
	QueueLength   int
	SnapshotBlock uint64
}

type RemovalSendEvidenceInput struct {
	RequestID       string
	TaskID          int64
	SendNo          int
	TxHash          string
	ConfirmedTxHash string
	ReceiptStatus   *int64
	Message         string
	Now             time.Time
}

type RemovalQueuedInput struct {
	RequestID   string
	TaskID      int64
	QueuedAt    time.Time
	QueuedBlock uint64
}

type RemovalAttentionInput struct {
	RequestID string
	TaskID    int64
	Code      storageremoval.AttentionCode
	Message   string
	Now       time.Time
}

type AdoptLegacyCleanupCopyInput struct {
	ContentID       int64
	Generation      int64
	ContentTaskID   int64
	CopyID          int64
	AttemptedAt     time.Time
	RetryOfTxHash   string
	TransactionHash string
	Identity        storagepipeline.ExecutionIdentity
	Enqueue         EnqueueRemovalTask
	Now             time.Time
}
