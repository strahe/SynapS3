package model

import (
	"time"

	"github.com/uptrace/bun"
)

// BucketStatus represents the lifecycle state of a bucket.
type BucketStatus string

const (
	BucketStatusActive BucketStatus = "active"
)

// IsVisible returns true — all buckets are active and visible.
func (s BucketStatus) IsVisible() bool { return true }

// IsAdminVisible returns true — all buckets are visible to admin.
func (s BucketStatus) IsAdminVisible() bool { return true }

// IsWritable returns true — all active buckets accept writes.
func (s BucketStatus) IsWritable() bool { return true }

// Bucket maps an S3 bucket to a Filecoin ProofSet.
type Bucket struct {
	bun.BaseModel `bun:"table:buckets"`

	ID             int64        `bun:",pk,autoincrement"`
	Name           string       `bun:",unique,notnull"`
	ProofSetID     *string      `bun:",nullzero"` // assigned after on-chain creation
	ACL            []byte       `bun:",nullzero"`
	OwnerAccessKey *string      `bun:",nullzero"`
	Status         BucketStatus `bun:",notnull,default:'active'"`
	CreatedAt      time.Time    `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt      time.Time    `bun:",nullzero,notnull,default:current_timestamp"`

	Owner *S3Account `bun:"rel:belongs-to,join:owner_access_key=access_key,on_update:cascade,on_delete:restrict"`
}
