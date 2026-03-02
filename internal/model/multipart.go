package model

import (
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

	ID          int64             `bun:",pk,autoincrement"`
	BucketID    int64             `bun:",notnull"`
	Key         string            `bun:",notnull"`
	UploadID    string            `bun:",notnull,unique"`
	ContentType string            `bun:",notnull,default:'application/octet-stream'"`
	Metadata    map[string]string `bun:"type:jsonb"`
	Status      MultipartStatus   `bun:",notnull,default:'initiated'"`
	CreatedAt   time.Time         `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt   time.Time         `bun:",nullzero,notnull,default:current_timestamp"`

	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}

// MultipartPart stores metadata for a single part within a multipart upload.
type MultipartPart struct {
	bun.BaseModel `bun:"table:multipart_parts"`

	ID         int64     `bun:",pk,autoincrement"`
	UploadID   string    `bun:",notnull"`
	PartNumber int       `bun:",notnull"`
	Size       int64     `bun:",notnull"`
	ETag       string    `bun:",notnull"`
	CreatedAt  time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}
