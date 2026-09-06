package model

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// MultipartStatus represents the lifecycle state of a multipart upload.
type MultipartStatus string

const (
	MultipartStatusInitiated  MultipartStatus = "initiated"
	MultipartStatusCompleting MultipartStatus = "completing"
	MultipartStatusCompleted  MultipartStatus = "completed"
	MultipartStatusAborted    MultipartStatus = "aborted"
)

// MultipartUpload tracks an in-progress multipart upload session.
type MultipartUpload struct {
	bun.BaseModel `bun:"table:multipart_uploads"`

	UploadID    string            `bun:"type:text,pk"`
	BucketID    int64             `bun:",notnull"`
	Key         string            `bun:"type:text,notnull"`
	ContentType string            `bun:"type:text,notnull,default:'application/octet-stream'"`
	Metadata    map[string]string `bun:"type:jsonb,notnull,default:'{}'"`
	Status      MultipartStatus   `bun:"type:text,notnull,default:'initiated'"`
	CreatedAt   time.Time         `bun:",nullzero,notnull"`
	UpdatedAt   time.Time         `bun:",nullzero,notnull"`

	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}

// MultipartPart stores metadata for a single part within a multipart upload.
type MultipartPart struct {
	bun.BaseModel `bun:"table:multipart_parts"`

	ID         int64     `bun:",pk,autoincrement,identity"`
	UploadID   string    `bun:"type:text,notnull"`
	PartNumber int       `bun:"type:integer,notnull"`
	Size       int64     `bun:",notnull"`
	ETag       string    `bun:"type:text,notnull"`
	Checksum   *string   `bun:"type:text,nullzero"`
	CreatedAt  time.Time `bun:",nullzero,notnull"`

	Upload *MultipartUpload `bun:"rel:belongs-to,join:upload_id=upload_id"`
}

var _ bun.BeforeAppendModelHook = (*MultipartUpload)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (m *MultipartUpload) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	if m.UpdatedAt.IsZero() {
		m.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*MultipartPart)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (m *MultipartPart) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	return nil
}
