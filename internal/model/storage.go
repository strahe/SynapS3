package model

import (
	"context"
	"math/bits"
	"time"

	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

// UploadProgressPercent returns a byte-based integer percent when the total is known.
func UploadProgressPercent(uploaded, total int64) *int {
	if total <= 0 {
		return nil
	}
	if uploaded < 0 {
		uploaded = 0
	}
	if uploaded > total {
		uploaded = total
	}
	hi, lo := bits.Mul64(uint64(uploaded), 100)
	value64, _ := bits.Div64(hi, lo, uint64(total))
	value := int(value64)
	return &value
}

type StorageDataSetStatus string

const (
	StorageDataSetStatusPending  StorageDataSetStatus = "pending"
	StorageDataSetStatusCreating StorageDataSetStatus = "creating"
	StorageDataSetStatusReady    StorageDataSetStatus = "ready"
	StorageDataSetStatusFailed   StorageDataSetStatus = "failed"
	StorageDataSetStatusDraining StorageDataSetStatus = "draining"
	StorageDataSetStatusRetired  StorageDataSetStatus = "retired"
)

type StorageCopyTransferMethod string

const (
	StorageCopyTransferMethodIngress  StorageCopyTransferMethod = "ingress"
	StorageCopyTransferMethodPeerPull StorageCopyTransferMethod = "peer_pull"
)

type StorageCopyStatus string

const (
	StorageCopyStatusPending    StorageCopyStatus = "pending"
	StorageCopyStatusPieceReady StorageCopyStatus = "piece_ready"
	StorageCopyStatusCommitting StorageCopyStatus = "committing"
	StorageCopyStatusCommitted  StorageCopyStatus = "committed"
	StorageCopyStatusFailed     StorageCopyStatus = "failed"
)

// StorageContent is the identity of one bucket-scoped byte payload. Object
// versions point at it, copies place it with providers, and dedup is a lookup
// on (bucket_id, checksum, content_size) rather than a scan.
type StorageContent struct {
	bun.BaseModel `bun:"table:storage_contents"`

	ID          int64   `bun:",pk,autoincrement,identity"`
	BucketID    int64   `bun:",notnull"`
	Checksum    string  `bun:"type:text,notnull"`
	ContentSize int64   `bun:",notnull"`
	PieceCID    *string `bun:"type:text,nullzero"`
	// RequestedCopies is the durability target frozen when the content is first
	// created. Later bucket-policy changes do not rewrite this target, and a
	// deduplicated write inherits it.
	RequestedCopies   int        `bun:"type:integer,notnull"`
	ErrorMessage      *string    `bun:"type:text,nullzero"`
	AcceptedAt        *time.Time `bun:",nullzero"`
	CleanupGeneration int64      `bun:",notnull,default:0"`
	CleanupTaskID     *int64     `bun:",nullzero"`
	CreatedAt         time.Time  `bun:",nullzero,notnull"`
	UpdatedAt         time.Time  `bun:",nullzero,notnull"`

	Bucket *Bucket `bun:"rel:belongs-to,join:bucket_id=id"`
}

// StorageDataSet records the bucket ownership of a provider-scoped data set.
// CopyIndex names the logical replica slot, which can own several physical
// generations while a provider replacement is in flight. IsCurrent selects the
// generation that accepts new writes; replaced generations stay readable until
// they are verifiably retired.
type StorageDataSet struct {
	bun.BaseModel `bun:"table:storage_data_sets"`

	ID                   int64                `bun:",pk,autoincrement,identity"`
	BucketID             int64                `bun:",notnull"`
	ProviderID           types.OnChainID      `bun:"type:text,notnull"`
	CopyIndex            int                  `bun:"type:integer,notnull"`
	Generation           int64                `bun:",notnull,default:1"`
	IsCurrent            bool                 `bun:",notnull"`
	DataSetID            *types.OnChainID     `bun:"type:text"`
	ClientDataSetID      *types.OnChainID     `bun:"type:text"`
	Status               StorageDataSetStatus `bun:"type:text,notnull,default:'pending'"`
	CreateTransactionID  *string              `bun:"type:text,nullzero"`
	CreateStatusURL      *string              `bun:"type:text,nullzero"`
	CreatedByContentID   *int64               `bun:",nullzero"`
	LastUsedContentID    *int64               `bun:",nullzero"`
	LastError            *string              `bun:"type:text,nullzero"`
	EnsureTaskID         *int64               `bun:",nullzero"`
	RetirementGeneration int64                `bun:",notnull,default:0"`
	RetirementTaskID     *int64               `bun:",nullzero"`
	CreatedAt            time.Time            `bun:",nullzero,notnull"`
	UpdatedAt            time.Time            `bun:",nullzero,notnull"`

	Bucket           *Bucket         `bun:"rel:belongs-to,join:bucket_id=id"`
	CreatedByContent *StorageContent `bun:"rel:belongs-to,join:created_by_content_id=id"`
	LastUsedContent  *StorageContent `bun:"rel:belongs-to,join:last_used_content_id=id"`
}

// StorageCopy places one content payload on one data set generation. Ingress
// progress lives here rather than on the content because it belongs to the
// concrete transfer that produced it.
type StorageCopy struct {
	bun.BaseModel `bun:"table:storage_copies"`

	ID        int64 `bun:",pk,autoincrement,identity"`
	ContentID int64 `bun:",notnull"`
	BucketID  int64 `bun:",notnull"`
	// ContentSize repeats the content size so the ingress bound stays a local
	// check; a composite foreign key keeps the repetition from drifting.
	ContentSize      int64                     `bun:",notnull"`
	StorageDataSetID int64                     `bun:",notnull"`
	CopyIndex        int                       `bun:"type:integer,notnull"`
	ProviderID       types.OnChainID           `bun:"type:text,notnull"`
	PieceID          *types.OnChainID          `bun:"type:text"`
	TransferMethod   StorageCopyTransferMethod `bun:"type:text,notnull"`
	Status           StorageCopyStatus         `bun:"type:text,notnull,default:'pending'"`
	RetrievalURL     *string                   `bun:"type:text,nullzero"`
	// IsNewDataSet is derived by repository reads from the data set's creator.
	IsNewDataSet       bool       `bun:",scanonly"`
	CommitExtraDataHex *string    `bun:"type:text,nullzero"`
	CommitReadyAt      *time.Time `bun:",nullzero"`
	// ConfirmedAttemptID and ConfirmedAttemptStatus project the ledger row that
	// proves this copy is committed. A composite foreign key requires the named
	// attempt to actually be confirmed, so the projection cannot drift.
	ConfirmedAttemptID      *string    `bun:"type:text,nullzero"`
	ConfirmedAttemptStatus  *string    `bun:"type:text,nullzero"`
	IngressBytesTransferred int64      `bun:",notnull,default:0"`
	IngressStoreAttempt     int        `bun:"type:integer,notnull,default:0"`
	ProgressUpdatedAt       *time.Time `bun:",nullzero"`
	WorkGeneration          int64      `bun:",notnull,default:0"`
	ActiveTaskID            *int64     `bun:",nullzero"`
	LastError               *string    `bun:"type:text,nullzero"`
	CreatedAt               time.Time  `bun:",nullzero,notnull"`
	UpdatedAt               time.Time  `bun:",nullzero,notnull"`

	DataSetID *types.OnChainID `bun:"type:text,scanonly"`

	// Commit evidence is projected from storage_commit_attempts by repository
	// reads. These fields are not columns on the copy table.
	CommitAttemptID              *string    `bun:",scanonly"`
	CommitAttemptedAt            *time.Time `bun:",scanonly"`
	CommitTransactionID          *string    `bun:",scanonly"`
	CommitStatusURL              *string    `bun:",scanonly"`
	CommitConfirmedTransactionID *string    `bun:",scanonly"`
	CommitAttentionCode          *string    `bun:",scanonly"`
	CommitAttentionAt            *time.Time `bun:",scanonly"`

	Content    *StorageContent `bun:"rel:belongs-to,join:content_id=id"`
	StorageSet *StorageDataSet `bun:"rel:belongs-to,join:storage_data_set_id=id"`
}

var _ bun.BeforeAppendModelHook = (*StorageContent)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (s *StorageContent) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = now
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*StorageDataSet)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (s *StorageDataSet) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = now
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*StorageCopy)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (s *StorageCopy) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = now
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = now
	}
	return nil
}
