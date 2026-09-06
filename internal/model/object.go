package model

import (
	"context"
	"path"
	"strconv"
	"time"

	"github.com/uptrace/bun"
)

// ObjectState is a derived view of how far a version's content has travelled
// through the storage pipeline. It is computed from the content's copy rows on
// read and is deliberately not a stored column: persisting it would duplicate
// facts the copies already own, and the two would drift.
type ObjectState string

const (
	ObjectStateCached      ObjectState = "cached"
	ObjectStateUploading   ObjectState = "uploading"
	ObjectStateCommitting  ObjectState = "committing"
	ObjectStateReplicating ObjectState = "replicating"
	ObjectStateStored      ObjectState = "stored"
	ObjectStateFailed      ObjectState = "failed"
)

// ContentCacheKey returns the local cache path for one content payload. Cache
// residency is content-addressed, so versions that share bytes share a file
// and the key is derived rather than stored.
func ContentCacheKey(contentID int64) string {
	return path.Join(".contents", strconv.FormatInt(contentID, 10))
}

// Object stores the stable identity for an S3 object key.
type Object struct {
	bun.BaseModel `bun:"table:objects"`

	ID       int64  `bun:",pk,autoincrement,identity"`
	BucketID int64  `bun:",notnull"`
	Key      string `bun:"type:text,notnull"`
	// CurrentVersionID names the version reads serve. It is the single authority
	// for "current"; a version carries no flag of its own.
	CurrentVersionID *string   `bun:"type:text,nullzero"`
	CreatedAt        time.Time `bun:",nullzero,notnull"`
	UpdatedAt        time.Time `bun:",nullzero,notnull"`

	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}

// ObjectVersion stores immutable per-version identity plus mutable lifecycle state.
type ObjectVersion struct {
	bun.BaseModel `bun:"table:object_versions"`

	VersionID         string            `bun:"type:text,pk"`
	ObjectID          int64             `bun:",notnull"`
	BucketID          int64             `bun:",notnull"`
	Key               string            `bun:"type:text,notnull"`
	ContentID         *int64            `bun:",nullzero"`
	Size              int64             `bun:",notnull"`
	ETag              string            `bun:"type:text,notnull"`
	ContentType       string            `bun:"type:text,notnull,default:'application/octet-stream'"`
	Metadata          map[string]string `bun:"type:jsonb,notnull,default:'{}'"`
	MultipartUploadID *string           `bun:"type:text,nullzero"`
	IsDeleteMarker    bool              `bun:",notnull,default:false"`
	CreatedAt         time.Time         `bun:",nullzero,notnull"`
	UpdatedAt         time.Time         `bun:",nullzero,notnull"`

	// Everything below is projected by repository reads rather than stored.
	// Durability and pipeline position are functions of the copy rows, cache
	// residency belongs to object_cache, and "current" is the object's pointer.
	IsCurrent       bool        `bun:",scanonly"`
	Checksum        string      `bun:",scanonly"`
	PieceCID        *string     `bun:"type:text,scanonly"`
	RetrievalURL    *string     `bun:"type:text,scanonly"`
	InCache         bool        `bun:",scanonly"`
	CacheAccessedAt *time.Time  `bun:",scanonly"`
	InFilecoin      bool        `bun:",scanonly"`
	State           ObjectState `bun:",scanonly"`

	Object *Object `bun:"rel:belongs-to,join:object_id=id"`
	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}

// CacheKey returns the local cache path backing this version. Residency is
// content-addressed, so a delete marker has no key and versions sharing bytes
// share one.
func (v *ObjectVersion) CacheKey() string {
	if v == nil || v.ContentID == nil {
		return ""
	}
	return ContentCacheKey(*v.ContentID)
}

// ObjectCache records local cache residency for one content payload. Two
// versions of identical bytes share a single entry, so eviction is driven by
// the content's reference count rather than by any one version.
type ObjectCache struct {
	bun.BaseModel `bun:"table:object_cache"`

	ContentID                int64      `bun:",pk"`
	InCache                  bool       `bun:",notnull"`
	CacheAccessedAt          *time.Time `bun:",nullzero"`
	CachePresenceGeneration  int64      `bun:",notnull,default:0"`
	CacheOperationGeneration int64      `bun:",notnull,default:0"`
	CacheActiveTaskID        *int64     `bun:",nullzero"`
	CreatedAt                time.Time  `bun:",nullzero,notnull"`
	UpdatedAt                time.Time  `bun:",nullzero,notnull"`
}

var _ bun.BeforeAppendModelHook = (*Object)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (o *Object) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if o.CreatedAt.IsZero() {
		o.CreatedAt = now
	}
	if o.UpdatedAt.IsZero() {
		o.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*ObjectVersion)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (o *ObjectVersion) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if o.CreatedAt.IsZero() {
		o.CreatedAt = now
	}
	if o.UpdatedAt.IsZero() {
		o.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*ObjectCache)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (o *ObjectCache) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if o.CreatedAt.IsZero() {
		o.CreatedAt = now
	}
	if o.UpdatedAt.IsZero() {
		o.UpdatedAt = now
	}
	return nil
}
