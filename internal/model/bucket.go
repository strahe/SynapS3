package model

import (
	"time"

	"github.com/uptrace/bun"
)

// BucketStatus represents the lifecycle state of a bucket.
type BucketStatus string

const (
	BucketStatusActive       BucketStatus = "active"
	BucketStatusCreating     BucketStatus = "creating"
	BucketStatusDeleting     BucketStatus = "deleting"
	BucketStatusDeleted      BucketStatus = "deleted"
	BucketStatusCreateFailed BucketStatus = "create_failed"
	BucketStatusDeleteFailed BucketStatus = "delete_failed"
)

// IsVisible returns true for bucket statuses that should be visible to S3 clients
// (i.e., for HeadBucket, GetObject read operations).
func (s BucketStatus) IsVisible() bool {
	switch s {
	case BucketStatusActive, BucketStatusCreating, BucketStatusDeleting:
		return true
	default:
		return false
	}
}

// IsWritable returns true for bucket statuses that allow write operations
// (PutObject, multipart uploads, etc). Only active buckets accept writes.
func (s BucketStatus) IsWritable() bool {
	return s == BucketStatusActive
}

// Bucket maps an S3 bucket to a Filecoin ProofSet.
type Bucket struct {
	bun.BaseModel `bun:"table:buckets"`

	ID         int64        `bun:",pk,autoincrement"`
	Name       string       `bun:",unique,notnull"`
	ProofSetID *string      `bun:",nullzero"` // assigned after on-chain creation
	Status     BucketStatus `bun:",notnull,default:'active'"`
	CreatedAt  time.Time    `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt  time.Time    `bun:",nullzero,notnull,default:current_timestamp"`
}
