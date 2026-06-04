package model

import (
	"time"

	"github.com/uptrace/bun"
)

// ObjectState represents the lifecycle state of an object.
// State tracks the storage pipeline lifecycle only.
type ObjectState string

const (
	ObjectStateCached       ObjectState = "cached"
	ObjectStateUploading    ObjectState = "uploading"
	ObjectStateCommitting   ObjectState = "committing"
	ObjectStateReplicating  ObjectState = "replicating"
	ObjectStateStored       ObjectState = "stored"
	ObjectStateFailed       ObjectState = "failed"
	ObjectStateCacheEvicted ObjectState = "cache_evicted"
)

// Object stores the stable identity for an S3 object key.
type Object struct {
	bun.BaseModel `bun:"table:objects"`

	ID        int64     `bun:",pk,autoincrement"`
	BucketID  int64     `bun:",notnull"`
	Key       string    `bun:",notnull"`
	CreatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`

	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}

// ObjectVersion stores immutable per-version identity plus mutable lifecycle state.
type ObjectVersion struct {
	bun.BaseModel `bun:"table:object_versions"`

	VersionID         string            `bun:",pk"`
	ObjectID          int64             `bun:",notnull"`
	BucketID          int64             `bun:",notnull"`
	Key               string            `bun:",notnull"`
	Size              int64             `bun:",notnull"`
	ETag              string            `bun:",notnull"`
	Checksum          string            `bun:",notnull"`
	ContentType       string            `bun:",notnull,default:'application/octet-stream'"`
	Metadata          map[string]string `bun:"type:jsonb"`
	CacheKey          string            `bun:",notnull"`
	MultipartUploadID *string           `bun:",nullzero"`
	StorageUploadID   *int64            `bun:",nullzero"`
	PieceCID          *string           `bun:",scanonly"`
	RetrievalURL      *string           `bun:",scanonly"`
	InCache           bool              `bun:",notnull,default:true"`
	InFilecoin        bool              `bun:",scanonly"`
	IsCurrent         bool              `bun:",notnull,default:false"`
	IsDeleteMarker    bool              `bun:",notnull,default:false"`
	State             ObjectState       `bun:",notnull,default:'cached'"`
	FailedAtState     *ObjectState      `bun:",nullzero"`
	LastError         *string           `bun:",nullzero"`
	CreatedAt         time.Time         `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt         time.Time         `bun:",nullzero,notnull,default:current_timestamp"`

	Object *Object `bun:"rel:belongs-to,join:object_id=id"`
	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}
