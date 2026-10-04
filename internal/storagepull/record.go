// Package storagepull owns the ledger of provider-side copy attempts. A pull
// asks a target provider to fetch one piece from a source provider. The target
// request is idempotent through its authorization extra data; AttemptID names
// the internal ledger row across recovery.
package storagepull

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

// AttemptStatus distinguishes replayable requests from abandoned ones.
// ResolvedAt records completion: a resolved attempted row succeeded, while a
// resolved abandoned row will no longer be observed or replayed.
type AttemptStatus string

const (
	AttemptStatusAttempted AttemptStatus = "attempted"
	AttemptStatusAbandoned AttemptStatus = "abandoned"
)

// These task reasons preserve the request and copy owner until recovery can
// establish the provider's outcome.
const (
	FailureOutcomeUnknown       = "pull_outcome_unknown"
	FailureCancelOutcomeUnknown = "pull_cancel_outcome_unknown"
	FailureRecoveryBlocked      = "pull_recovery_blocked"
	WaitQueueFull               = "pull_queue_full"
)

// Attempt records the source and authorization before a request reaches the
// target provider. Recovery replays this request without changing its identity.
type Attempt struct {
	bun.BaseModel `bun:"table:storage_pull_attempts,alias:storage_pull_attempt"`

	AttemptID        string        `bun:"type:text,pk"`
	ContentID        int64         `bun:",notnull"`
	StorageDataSetID int64         `bun:",notnull"`
	Status           AttemptStatus `bun:"type:text,notnull"`
	// The source the piece was pulled from, recorded before the request is sent
	// so recovery never invents a different one.
	SourceProviderID types.OnChainID `bun:"type:text,notnull"`
	SourceDataSetID  types.OnChainID `bun:"type:text,notnull"`
	SourcePieceID    types.OnChainID `bun:"type:text,notnull"`
	// SourcePieceCID names the piece itself, distinct from the content's own CID.
	SourcePieceCID     string     `bun:"type:text,notnull"`
	SourceRetrievalURL string     `bun:"type:text,notnull"`
	ExtraDataHex       string     `bun:"type:text,notnull"`
	LastError          *string    `bun:"type:text,nullzero"`
	AttemptedAt        time.Time  `bun:",nullzero,notnull"`
	ResolvedAt         *time.Time `bun:",nullzero"`
	CreatedAt          time.Time  `bun:",nullzero,notnull"`
	UpdatedAt          time.Time  `bun:",nullzero,notnull"`
}

var _ bun.BeforeAppendModelHook = (*Attempt)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (a *Attempt) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	}
	if a.UpdatedAt.IsZero() {
		a.UpdatedAt = now
	}
	return nil
}
