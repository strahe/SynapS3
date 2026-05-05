package model

import (
	"encoding/json"
	"time"

	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type StorageUploadStatus string

const (
	StorageUploadStatusRunning            StorageUploadStatus = "running"
	StorageUploadStatusStoredOnPrimary    StorageUploadStatus = "stored_on_primary"
	StorageUploadStatusPrimaryCommitted   StorageUploadStatus = "primary_committed"
	StorageUploadStatusPartial            StorageUploadStatus = "partial"
	StorageUploadStatusAllCopiesCommitted StorageUploadStatus = "all_copies_committed"
	StorageUploadStatusComplete           StorageUploadStatus = StorageUploadStatusAllCopiesCommitted
	StorageUploadStatusFailed             StorageUploadStatus = "failed"
	StorageUploadStatusRejected           StorageUploadStatus = "rejected"
	StorageUploadStatusSuperseded         StorageUploadStatus = "superseded"
)

type StorageDataSetStatus string

const (
	StorageDataSetStatusPending  StorageDataSetStatus = "pending"
	StorageDataSetStatusCreating StorageDataSetStatus = "creating"
	StorageDataSetStatusReady    StorageDataSetStatus = "ready"
	StorageDataSetStatusFailed   StorageDataSetStatus = "failed"
)

type StorageUploadCopyStatus string

const (
	StorageUploadCopyStatusPending    StorageUploadCopyStatus = "pending"
	StorageUploadCopyStatusPieceReady StorageUploadCopyStatus = "piece_ready"
	StorageUploadCopyStatusCommitting StorageUploadCopyStatus = "committing"
	StorageUploadCopyStatusCommitted  StorageUploadCopyStatus = "committed"
	StorageUploadCopyStatusFailed     StorageUploadCopyStatus = "failed"
)

// StorageUpload records one SDK upload attempt and its persisted outcome.
type StorageUpload struct {
	bun.BaseModel `bun:"table:storage_uploads"`

	ID              int64               `bun:",pk,autoincrement"`
	BucketID        int64               `bun:",notnull"`
	SourceTaskID    *int64              `bun:",nullzero"`
	SourceVersionID string              `bun:",nullzero"`
	ContentSize     int64               `bun:",notnull"`
	Checksum        string              `bun:",notnull"`
	Status          StorageUploadStatus `bun:",notnull,default:'running'"`
	PieceCID        *string             `bun:",nullzero"`
	RequestedCopies int                 `bun:",notnull,default:0"`
	RawResultJSON   json.RawMessage     `bun:"type:jsonb,nullzero"`
	ErrorMessage    *string             `bun:",nullzero"`
	AcceptError     *string             `bun:",nullzero"`
	AcceptedAt      *time.Time          `bun:",nullzero"`
	CreatedAt       time.Time           `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt       time.Time           `bun:",nullzero,notnull,default:current_timestamp"`

	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
	Task   *Task   `bun:"rel:belongs-to,join:source_task_id=id"`
}

// StorageDataSet records the bucket ownership of a provider-scoped data set.
type StorageDataSet struct {
	bun.BaseModel `bun:"table:storage_data_sets"`

	ID                  int64                `bun:",pk,autoincrement"`
	BucketID            int64                `bun:",notnull"`
	ProviderID          types.OnChainID      `bun:"type:text,notnull"`
	CopyIndex           int                  `bun:",notnull"`
	DataSetID           *types.OnChainID     `bun:"type:text"`
	ClientDataSetID     *types.OnChainID     `bun:"type:text"`
	Status              StorageDataSetStatus `bun:",notnull,default:'pending'"`
	CreateTransactionID *string              `bun:",nullzero"`
	CreateStatusURL     *string              `bun:",nullzero"`
	CreatedByUploadID   *int64               `bun:",nullzero"`
	LastUsedUploadID    *int64               `bun:",nullzero"`
	LastError           *string              `bun:",nullzero"`
	CreatedAt           time.Time            `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt           time.Time            `bun:",nullzero,notnull,default:current_timestamp"`

	Bucket          *Bucket        `bun:"rel:belongs-to,join:bucket_id=id"`
	CreatedByUpload *StorageUpload `bun:"rel:belongs-to,join:created_by_upload_id=id"`
	LastUsedUpload  *StorageUpload `bun:"rel:belongs-to,join:last_used_upload_id=id"`
}

// StorageUploadCopy stores one successful copy returned by the SDK.
type StorageUploadCopy struct {
	bun.BaseModel `bun:"table:storage_upload_copies"`

	ID                  int64                   `bun:",pk,autoincrement"`
	UploadID            int64                   `bun:",notnull"`
	CopyIndex           int                     `bun:",notnull"`
	ProviderID          *types.OnChainID        `bun:"type:text"`
	DataSetID           *types.OnChainID        `bun:"type:text,scanonly"`
	PieceID             *types.OnChainID        `bun:"type:text"`
	Role                string                  `bun:",notnull"`
	Status              StorageUploadCopyStatus `bun:",notnull,default:'pending'"`
	RetrievalURL        *string                 `bun:",nullzero"`
	StorageDataSetID    *int64                  `bun:",nullzero"`
	CommitExtraDataHex  *string                 `bun:",nullzero"`
	CommitTransactionID *string                 `bun:",nullzero"`
	LastError           *string                 `bun:",nullzero"`
	CreatedAt           time.Time               `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt           time.Time               `bun:",nullzero,notnull,default:current_timestamp"`

	Upload     *StorageUpload  `bun:"rel:belongs-to,join:upload_id=id"`
	StorageSet *StorageDataSet `bun:"rel:belongs-to,join:storage_data_set_id=id"`
}

// StorageUploadFailure stores one failed provider attempt returned by the SDK.
type StorageUploadFailure struct {
	bun.BaseModel `bun:"table:storage_upload_failures"`

	ID           int64            `bun:",pk,autoincrement"`
	UploadID     int64            `bun:",notnull"`
	AttemptIndex int              `bun:",notnull"`
	ProviderID   *types.OnChainID `bun:"type:text"`
	Role         string           `bun:",notnull"`
	Stage        *string          `bun:",nullzero"`
	ErrorMessage *string          `bun:",nullzero"`
	Explicit     bool             `bun:",notnull,default:false"`
	CreatedAt    time.Time        `bun:",nullzero,notnull,default:current_timestamp"`

	Upload *StorageUpload `bun:"rel:belongs-to,join:upload_id=id"`
}
