package model

import (
	"time"

	"github.com/uptrace/bun"
)

// BucketStatus represents the lifecycle state of a bucket.
type BucketStatus string

const (
	BucketStatusActive   BucketStatus = "active"
	BucketStatusCreating BucketStatus = "creating"
	BucketStatusDeleting BucketStatus = "deleting"
	BucketStatusDeleted  BucketStatus = "deleted"
)

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
