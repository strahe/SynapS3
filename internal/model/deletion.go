package model

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type StorageCleanupCopyStatus string

const (
	StorageCleanupCopyStatusPending         StorageCleanupCopyStatus = "pending"
	StorageCleanupCopyStatusDeleteScheduled StorageCleanupCopyStatus = "delete_scheduled"
	StorageCleanupCopyStatusRemoved         StorageCleanupCopyStatus = "removed"
	StorageCleanupCopyStatusFailed          StorageCleanupCopyStatus = "failed"
	StorageCleanupCopyStatusUnsupported     StorageCleanupCopyStatus = "unsupported"
)

// ObjectDeletion is an append-only tombstone for one permanently deleted
// object version. Cache cleanup is not tracked here: residency is keyed by
// content, so the trigger is the content's reference count reaching zero
// rather than the removal of any single version.
type ObjectDeletion struct {
	bun.BaseModel `bun:"table:object_deletions"`

	ID        int64     `bun:",pk,autoincrement,identity"`
	BucketID  int64     `bun:",notnull"`
	ObjectID  int64     `bun:",notnull"`
	Key       string    `bun:"type:text,notnull"`
	VersionID string    `bun:"type:text,unique,notnull"`
	ContentID *int64    `bun:",nullzero"`
	Size      int64     `bun:",notnull"`
	DeletedAt time.Time `bun:",nullzero,notnull"`
}

// StorageCleanupCopy tracks PDP cleanup for one committed storage copy.
type StorageCleanupCopy struct {
	bun.BaseModel `bun:"table:storage_cleanup_copies"`

	ID               int64                    `bun:",pk,autoincrement,identity"`
	ContentID        int64                    `bun:",notnull"`
	BucketID         int64                    `bun:",notnull"`
	CopyIndex        int                      `bun:"type:integer,notnull"`
	ProviderID       types.OnChainID          `bun:"type:text,notnull"`
	StorageDataSetID int64                    `bun:",notnull"`
	DataSetID        *types.OnChainID         `bun:"type:text"`
	ClientDataSetID  *types.OnChainID         `bun:"type:text"`
	PieceID          types.OnChainID          `bun:"type:text,notnull"`
	PieceCID         string                   `bun:"type:text,notnull"`
	RetrievalURL     *string                  `bun:"type:text,nullzero"`
	Status           StorageCleanupCopyStatus `bun:"type:text,notnull,default:'pending'"`
	DeleteTxHash     *string                  `bun:"type:text,nullzero"`
	LastError        *string                  `bun:"type:text,nullzero"`
	ScheduledAt      *time.Time               `bun:",nullzero"`
	RemovedAt        *time.Time               `bun:",nullzero"`
	CreatedAt        time.Time                `bun:",nullzero,notnull"`
	UpdatedAt        time.Time                `bun:",nullzero,notnull"`
}

var _ bun.BeforeAppendModelHook = (*StorageCleanupCopy)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (s *StorageCleanupCopy) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = now
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = now
	}
	return nil
}
