package model

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// BucketStatus represents the lifecycle state of a bucket.
type BucketStatus string

const (
	BucketStatusProvisioning BucketStatus = "provisioning"
	BucketStatusReady        BucketStatus = "ready"

	// BucketStatusActive is kept as a source-compatible alias for callers that
	// create already-provisioned test fixtures.
	BucketStatusActive = BucketStatusReady
)

// IsVisible reports whether S3 clients may discover the bucket namespace.
func (s BucketStatus) IsVisible() bool {
	return s == BucketStatusProvisioning || s == BucketStatusReady
}

// IsAdminVisible reports whether the bucket belongs in the admin inventory.
func (s BucketStatus) IsAdminVisible() bool { return s.IsVisible() }

// IsWritable reports whether provider storage is ready for object writes.
func (s BucketStatus) IsWritable() bool { return s == BucketStatusReady }

// Bucket stores S3 bucket metadata.
type Bucket struct {
	bun.BaseModel `bun:"table:buckets"`

	ID             int64   `bun:",pk,autoincrement,identity"`
	Name           string  `bun:"type:text,unique,notnull"`
	ACL            []byte  `bun:",nullzero"`
	OwnerAccessKey *string `bun:"type:text,nullzero"`
	// The durability policy is materialised at creation from configuration, so
	// every bucket answers "how many replicas" without consulting config.
	DefaultCopies        int          `bun:"type:integer,notnull"`
	MinimumDurableCopies int          `bun:"type:integer,notnull"`
	DurabilityGeneration int64        `bun:",notnull,default:0"`
	DurabilityTaskID     *int64       `bun:",nullzero"`
	Status               BucketStatus `bun:"type:text,notnull,default:'provisioning'"`
	CreatedAt            time.Time    `bun:",nullzero,notnull"`
	UpdatedAt            time.Time    `bun:",nullzero,notnull"`

	Owner *S3Account `bun:"rel:belongs-to,join:owner_access_key=access_key,on_update:restrict,on_delete:restrict"`
}

// BucketReplicaSlotStatus tracks whether a slot still accepts new writes. A
// closed slot is decommissioned rather than deleted: retired data set
// generations still reference it, and this schema never deletes that history.
//
// Nothing writes Decommissioned yet. Closing a slot only makes sense together
// with retiring the paid storage service that sits on it, and that retirement
// path does not exist, so lowering a bucket's replica target is refused instead.
type BucketReplicaSlotStatus string

const (
	BucketReplicaSlotStatusActive         BucketReplicaSlotStatus = "active"
	BucketReplicaSlotStatusDecommissioned BucketReplicaSlotStatus = "decommissioned"
)

// BucketReplicaSlot is one logical replica position in a bucket. Data sets,
// copies, replacements, cleanups and observability rows all reference it, so a
// copy index cannot name a slot the bucket never opened.
type BucketReplicaSlot struct {
	bun.BaseModel `bun:"table:bucket_replica_slots"`

	ID        int64                   `bun:",pk,autoincrement,identity"`
	BucketID  int64                   `bun:",notnull"`
	CopyIndex int                     `bun:"type:integer,notnull"`
	Status    BucketReplicaSlotStatus `bun:"type:text,notnull,default:'active'"`
	CreatedAt time.Time               `bun:",nullzero,notnull"`
	UpdatedAt time.Time               `bun:",nullzero,notnull"`
}

var _ bun.BeforeAppendModelHook = (*Bucket)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (b *Bucket) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if b.CreatedAt.IsZero() {
		b.CreatedAt = now
	}
	if b.UpdatedAt.IsZero() {
		b.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*BucketReplicaSlot)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (b *BucketReplicaSlot) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if b.CreatedAt.IsZero() {
		b.CreatedAt = now
	}
	if b.UpdatedAt.IsZero() {
		b.UpdatedAt = now
	}
	return nil
}
