package storagereplacement

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

// Replacement is one operator-approved provider replacement for a single
// bucket replica slot. It is created only by an explicit confirmation and
// authorizes a new paid service, the topology switch, migration, and
// termination of the old service once the safety gate passes.
type Replacement struct {
	bun.BaseModel `bun:"table:storage_replacements,alias:storage_replacement"`

	ID       int64 `bun:",pk,autoincrement,identity"`
	BucketID int64 `bun:",notnull"`
	// CopyIndex is the logical replica slot both generations belong to.
	CopyIndex           int              `bun:"type:integer,notnull"`
	SourceDataSetID     int64            `bun:",notnull"`
	TargetDataSetID     int64            `bun:",notnull"`
	SelectionMode       SelectionMode    `bun:"type:text,notnull"`
	RequestedProviderID *types.OnChainID `bun:"type:text"`
	ClientRequestID     string           `bun:"type:text,notnull"`
	Status              Status           `bun:"type:text,notnull"`
	WaitReason          *WaitReason      `bun:"type:text,nullzero"`
	FailureReason       *FailureReason   `bun:"type:text,nullzero"`
	LastError           *string          `bun:"type:text,nullzero"`
	// ItemsTotal and ItemsCopied are maintained inside the transactions that
	// seed and complete items, so progress never needs a history-sized count.
	ItemsTotal  int `bun:"type:integer,notnull,default:0"`
	ItemsCopied int `bun:"type:integer,notnull,default:0"`
	// SeedCursorContentID advances through storage uploads in bounded batches so
	// no single transaction scales with retained bucket history.
	SeedCursorContentID int64     `bun:",notnull,default:0"`
	SeedingComplete     bool      `bun:",notnull,default:false"`
	TaskGeneration      int64     `bun:",notnull,default:1"`
	TaskID              *int64    `bun:",nullzero"`
	SupersededByID      *int64    `bun:",nullzero"`
	CreatedAt           time.Time `bun:",nullzero,notnull"`
	UpdatedAt           time.Time `bun:",nullzero,notnull"`

	// The termination fields below are projected from storage_data_set_terminations
	// on read. A termination is a ledger row, so a third kind of termination adds
	// a row rather than another repeated column group here.
	TerminationTxHash          *string `bun:",scanonly"`
	TerminationEpoch           *int64  `bun:",scanonly"`
	AbandonedTerminationTxHash *string `bun:",scanonly"`
	AbandonedTerminationEpoch  *int64  `bun:",scanonly"`
}

// TerminationRole names which of a replacement's two data sets a termination
// ended. The source is terminated once migration is safe; the target is
// terminated only after the replacement is superseded and its target abandoned.
type TerminationRole string

const (
	TerminationRoleSource          TerminationRole = "source"
	TerminationRoleAbandonedTarget TerminationRole = "abandoned_target"
)

// Termination records the end of term paid for on one data set. It is written
// before the remote service is treated as terminated, so a crash in between
// re-reads it instead of paying for a second termination.
type Termination struct {
	bun.BaseModel `bun:"table:storage_data_set_terminations,alias:storage_data_set_termination"`

	ID            int64           `bun:",pk,autoincrement,identity"`
	ReplacementID int64           `bun:",notnull"`
	Role          TerminationRole `bun:"type:text,notnull"`
	// Exactly one of the data set columns is set, chosen by Role. Each carries a
	// composite foreign key back to the matching column on the replacement, so a
	// row cannot name a data set the replacement never held in that role.
	SourceDataSetID          *int64    `bun:",nullzero"`
	AbandonedTargetDataSetID *int64    `bun:",nullzero"`
	TxHash                   *string   `bun:"type:text,nullzero"`
	Epoch                    int64     `bun:",notnull"`
	CreatedAt                time.Time `bun:",nullzero,notnull"`
	UpdatedAt                time.Time `bun:",nullzero,notnull"`
}

// Item is one unit of migration work. Items are keyed by storage upload, not by
// object version, so content shared by many versions is copied once.
type Item struct {
	bun.BaseModel `bun:"table:storage_replacement_items,alias:storage_replacement_item"`

	ID            int64 `bun:",pk,autoincrement,identity"`
	ReplacementID int64 `bun:",notnull"`
	ContentID     int64 `bun:",notnull"`
	// TargetDataSetID identifies the target generation. Together with ContentID
	// it resolves exactly one bound copy without retaining a surrogate copy ID.
	// It is NOT NULL because a nullable column would make the composite foreign
	// key to that copy skip validation whenever it was unset.
	TargetDataSetID int64      `bun:",notnull"`
	Status          ItemStatus `bun:"type:text,notnull,default:'pending'"`
	LastError       *string    `bun:"type:text,nullzero"`
	CreatedAt       time.Time  `bun:",nullzero,notnull"`
	UpdatedAt       time.Time  `bun:",nullzero,notnull"`
}

// ProgressSnapshot is the UI-neutral aggregate for one replacement. Total is
// provisional until SeedingComplete is true.
type ProgressSnapshot struct {
	ReplacementID       int64
	Phase               Phase
	SeedingComplete     bool
	ItemsTotal          int
	ItemsProcessed      int
	ItemsCopied         int
	ItemsNoLongerNeeded int
	ItemsPending        int
	ItemsActive         int
	ItemsAttention      int
	ItemsRetrying       int
	ItemsWaitingSource  int
	ItemsFailed         int
	Percent             *int
	NextRetryAt         *time.Time
}

// ExecutionSnapshot is the bounded coordinator view of one replacement. It
// intentionally reports presence rather than item counts; full aggregates are
// reserved for operator-facing progress reads.
type ExecutionSnapshot struct {
	ReplacementID   int64 `bun:"replacement_id"`
	SeedingComplete bool  `bun:"seeding_complete"`
	HasPending      bool  `bun:"has_pending"`
	HasFailed       bool  `bun:"has_failed"`
}

var _ bun.BeforeAppendModelHook = (*Replacement)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (r *Replacement) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	if r.UpdatedAt.IsZero() {
		r.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*Termination)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (t *Termination) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = now
	}
	return nil
}

var _ bun.BeforeAppendModelHook = (*Item)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (i *Item) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if i.CreatedAt.IsZero() {
		i.CreatedAt = now
	}
	if i.UpdatedAt.IsZero() {
		i.UpdatedAt = now
	}
	return nil
}
