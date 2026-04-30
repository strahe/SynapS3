package model

import (
	"encoding/json"
	"time"

	"github.com/uptrace/bun"
)

type StorageUploadStatus string

const (
	StorageUploadStatusRunning    StorageUploadStatus = "running"
	StorageUploadStatusComplete   StorageUploadStatus = "complete"
	StorageUploadStatusPartial    StorageUploadStatus = "partial"
	StorageUploadStatusFailed     StorageUploadStatus = "failed"
	StorageUploadStatusRejected   StorageUploadStatus = "rejected"
	StorageUploadStatusSuperseded StorageUploadStatus = "superseded"
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

	ID                int64     `bun:",pk,autoincrement"`
	BucketID          int64     `bun:",notnull"`
	ProviderID        string    `bun:",notnull"`
	DataSetID         string    `bun:",notnull"`
	FirstSeenUploadID int64     `bun:",notnull"`
	LastSeenUploadID  int64     `bun:",notnull"`
	CreatedAt         time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt         time.Time `bun:",nullzero,notnull,default:current_timestamp"`

	Bucket          *Bucket        `bun:"rel:belongs-to,join:bucket_id=id"`
	FirstSeenUpload *StorageUpload `bun:"rel:belongs-to,join:first_seen_upload_id=id"`
	LastSeenUpload  *StorageUpload `bun:"rel:belongs-to,join:last_seen_upload_id=id"`
}

// StorageUploadCopy stores one successful copy returned by the SDK.
type StorageUploadCopy struct {
	bun.BaseModel `bun:"table:storage_upload_copies"`

	ID               int64     `bun:",pk,autoincrement"`
	UploadID         int64     `bun:",notnull"`
	CopyIndex        int       `bun:",notnull"`
	ProviderID       *string   `bun:",nullzero"`
	DataSetID        *string   `bun:",nullzero"`
	PieceID          *string   `bun:",nullzero"`
	Role             string    `bun:",notnull"`
	RetrievalURL     *string   `bun:",nullzero"`
	IsNewDataSet     bool      `bun:",notnull,default:false"`
	StorageDataSetID *int64    `bun:",nullzero"`
	CreatedAt        time.Time `bun:",nullzero,notnull,default:current_timestamp"`

	Upload     *StorageUpload  `bun:"rel:belongs-to,join:upload_id=id"`
	StorageSet *StorageDataSet `bun:"rel:belongs-to,join:storage_data_set_id=id"`
}

// StorageUploadFailure stores one failed provider attempt returned by the SDK.
type StorageUploadFailure struct {
	bun.BaseModel `bun:"table:storage_upload_failures"`

	ID           int64     `bun:",pk,autoincrement"`
	UploadID     int64     `bun:",notnull"`
	AttemptIndex int       `bun:",notnull"`
	ProviderID   *string   `bun:",nullzero"`
	Role         string    `bun:",notnull"`
	Stage        *string   `bun:",nullzero"`
	ErrorMessage *string   `bun:",nullzero"`
	Explicit     bool      `bun:",notnull,default:false"`
	CreatedAt    time.Time `bun:",nullzero,notnull,default:current_timestamp"`

	Upload *StorageUpload `bun:"rel:belongs-to,join:upload_id=id"`
}
