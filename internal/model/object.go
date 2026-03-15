package model

import (
	"time"

	"github.com/uptrace/bun"
)

// ObjectState represents the lifecycle state of an object.
// Designed to be extensible — new states can be added without
// breaking existing transitions.
// Note: Object visibility is controlled by DeletedAt, NOT by State.
// State tracks the storage pipeline lifecycle only.
type ObjectState string

const (
	ObjectStateCached       ObjectState = "cached"
	ObjectStateUploading    ObjectState = "uploading"
	ObjectStateUploaded     ObjectState = "uploaded"
	ObjectStateOnChaining   ObjectState = "onchaining"
	ObjectStateOnChained    ObjectState = "onchained"
	ObjectStateFailed       ObjectState = "failed"
	ObjectStateCacheEvicted ObjectState = "cache_evicted"
)

// Object stores metadata and state for an S3 object backed by Filecoin.
type Object struct {
	bun.BaseModel `bun:"table:objects"`

	ID            int64             `bun:",pk,autoincrement"`
	BucketID      int64             `bun:",notnull"`
	Key           string            `bun:",notnull"`
	Generation    int64             `bun:",notnull,default:1"`
	Size          int64             `bun:",notnull"`
	ETag          string            `bun:",notnull"`
	Checksum      string            `bun:",notnull"`
	ContentType   string            `bun:",notnull,default:'application/octet-stream'"`
	Metadata      map[string]string `bun:"type:jsonb"`
	CachePath     string            `bun:",notnull"`
	PieceCID      *string           `bun:",nullzero"`
	State         ObjectState       `bun:",notnull,default:'cached'"`
	FailedAtState *ObjectState      `bun:",nullzero"`
	RetryCount    int               `bun:",notnull,default:0"`
	MaxRetries    int               `bun:",notnull,default:5"`
	LastError     *string           `bun:",nullzero"`
	DeletedAt     *time.Time        `bun:",soft_delete,nullzero"`
	CreatedAt     time.Time         `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt     time.Time         `bun:",nullzero,notnull,default:current_timestamp"`

	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}
