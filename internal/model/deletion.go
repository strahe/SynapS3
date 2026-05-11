package model

import (
	"time"

	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type CacheCleanupStatus string

const (
	CacheCleanupStatusPending CacheCleanupStatus = "pending"
	CacheCleanupStatusDeleted CacheCleanupStatus = "deleted"
	CacheCleanupStatusSkipped CacheCleanupStatus = "skipped"
	CacheCleanupStatusFailed  CacheCleanupStatus = "failed"
)

type StorageCleanupCopyStatus string

const (
	StorageCleanupCopyStatusPending         StorageCleanupCopyStatus = "pending"
	StorageCleanupCopyStatusDeleteScheduled StorageCleanupCopyStatus = "delete_scheduled"
	StorageCleanupCopyStatusRemoved         StorageCleanupCopyStatus = "removed"
	StorageCleanupCopyStatusFailed          StorageCleanupCopyStatus = "failed"
	StorageCleanupCopyStatusUnsupported     StorageCleanupCopyStatus = "unsupported"
)

// ObjectDeletion records an accepted permanent deletion of one object version.
type ObjectDeletion struct {
	bun.BaseModel `bun:"table:object_deletions"`

	ID                 int64              `bun:",pk,autoincrement"`
	BucketID           int64              `bun:",notnull"`
	ObjectID           int64              `bun:",notnull"`
	Key                string             `bun:",notnull"`
	VersionID          string             `bun:",unique,notnull"`
	CacheKey           string             `bun:",notnull"`
	StorageUploadID    *int64             `bun:",nullzero"`
	Size               int64              `bun:",notnull"`
	Checksum           string             `bun:",notnull"`
	CacheCleanupStatus CacheCleanupStatus `bun:",notnull,default:'pending'"`
	CacheError         *string            `bun:",nullzero"`
	CacheCleanedAt     *time.Time         `bun:",nullzero"`
	CreatedAt          time.Time          `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt          time.Time          `bun:",nullzero,notnull,default:current_timestamp"`
	DeletedAt          time.Time          `bun:",nullzero,notnull,default:current_timestamp"`
}

// StorageCleanupCopy tracks PDP cleanup for one committed storage copy.
type StorageCleanupCopy struct {
	bun.BaseModel `bun:"table:storage_cleanup_copies"`

	ID               int64                    `bun:",pk,autoincrement"`
	TaskID           int64                    `bun:",notnull"`
	UploadID         int64                    `bun:",notnull"`
	CopyIndex        int                      `bun:",notnull"`
	ProviderID       *types.OnChainID         `bun:"type:text"`
	StorageDataSetID *int64                   `bun:",nullzero"`
	DataSetID        *types.OnChainID         `bun:"type:text"`
	ClientDataSetID  *types.OnChainID         `bun:"type:text"`
	PieceID          *types.OnChainID         `bun:"type:text"`
	PieceCID         string                   `bun:",notnull"`
	RetrievalURL     *string                  `bun:",nullzero"`
	Status           StorageCleanupCopyStatus `bun:",notnull,default:'pending'"`
	DeleteTxHash     *string                  `bun:",nullzero"`
	LastError        *string                  `bun:",nullzero"`
	ScheduledAt      *time.Time               `bun:",nullzero"`
	RemovedAt        *time.Time               `bun:",nullzero"`
	CreatedAt        time.Time                `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt        time.Time                `bun:",nullzero,notnull,default:current_timestamp"`

	Task *Task `bun:"rel:belongs-to,join:task_id=id"`
}
