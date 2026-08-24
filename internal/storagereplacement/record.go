package storagereplacement

import (
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

	ID       int64 `bun:",pk,autoincrement"`
	BucketID int64 `bun:",notnull"`
	// CopyIndex is the logical replica slot both generations belong to.
	CopyIndex           int              `bun:",notnull"`
	SourceDataSetID     int64            `bun:",notnull"`
	TargetDataSetID     int64            `bun:",notnull"`
	SelectionMode       SelectionMode    `bun:",notnull"`
	RequestedProviderID *types.OnChainID `bun:"type:text"`
	ClientRequestID     string           `bun:",notnull"`
	Status              Status           `bun:",notnull"`
	WaitReason          *WaitReason      `bun:",nullzero"`
	FailureReason       *FailureReason   `bun:",nullzero"`
	LastError           *string          `bun:",nullzero"`
	// ItemsTotal and ItemsCopied are maintained inside the transactions that
	// seed and complete items, so progress never needs a history-sized count.
	ItemsTotal  int `bun:",notnull,default:0"`
	ItemsCopied int `bun:",notnull,default:0"`
	// SeedCursorUploadID advances through storage uploads in bounded batches so
	// no single transaction scales with retained bucket history.
	SeedCursorUploadID int64 `bun:",notnull,default:0"`
	SeedingComplete    bool  `bun:",notnull,default:false"`
	// StateVersion fences item-worker pause requests from a later coordinator
	// recovery. LastDispatchedAt is the durable fairness cursor used by the
	// global item queue.
	StateVersion     int64      `bun:",notnull,default:1"`
	LastDispatchedAt *time.Time `bun:",nullzero"`
	// TerminationEpoch is recorded before the old service is treated as
	// terminated, so a crash between termination and observation re-reads it
	// instead of terminating twice.
	TerminationTxHash     *string    `bun:",nullzero"`
	TerminationEpoch      *int64     `bun:",nullzero"`
	TerminationObservedAt *time.Time `bun:",nullzero"`
	// AbandonedTerminationEpoch records termination of a superseded target.
	// It is separate from the source termination fields above because the two
	// services belong to opposite generations.
	AbandonedTerminationTxHash     *string    `bun:",nullzero"`
	AbandonedTerminationEpoch      *int64     `bun:",nullzero"`
	AbandonedTerminationObservedAt *time.Time `bun:",nullzero"`
	SupersededByID                 *int64     `bun:",nullzero"`
	ConfirmedAt                    time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	CreatedAt                      time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt                      time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
}

// Item is one unit of migration work. Items are keyed by storage upload, not by
// object version, so content shared by many versions is copied once.
type Item struct {
	bun.BaseModel `bun:"table:storage_replacement_items,alias:storage_replacement_item"`

	ID            int64 `bun:",pk,autoincrement"`
	ReplacementID int64 `bun:",notnull"`
	UploadID      int64 `bun:",notnull"`
	// TargetCopyID is the concrete copy row on the target generation. Tasks
	// address it directly so they can never write the wrong generation.
	TargetCopyID *int64     `bun:",nullzero"`
	Status       ItemStatus `bun:",notnull"`
	Attempts     int        `bun:",notnull,default:0"`
	ScheduledAt  time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	RetryCount   int        `bun:",notnull,default:0"`
	// MaxRetries is nullable only for rows created before the durable item
	// queue migration. Startup recovery initializes it exactly once.
	MaxRetries *int       `bun:",nullzero"`
	ClaimedAt  *time.Time `bun:",nullzero"`
	LeaseUntil *time.Time `bun:",nullzero"`
	LastError  *string    `bun:",nullzero"`
	CreatedAt  time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt  time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
}

// ClaimToken fences lifecycle updates made by one item worker lease.
type ClaimToken struct {
	ItemID    int64
	ClaimedAt time.Time
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
	ReplacementID    int64 `bun:"replacement_id"`
	SeedingComplete  bool  `bun:"seeding_complete"`
	ItemsTotal       int   `bun:"items_total"`
	ItemsCopied      int   `bun:"items_copied"`
	HasPending       bool  `bun:"has_pending"`
	HasActive        bool  `bun:"has_active"`
	HasRetrying      bool  `bun:"has_retrying"`
	HasWaitingSource bool  `bun:"has_waiting_source"`
	HasFailed        bool  `bun:"has_failed"`
}
