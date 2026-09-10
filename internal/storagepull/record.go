// Package storagepull owns the ledger of provider-side copy attempts. A pull
// asks a target provider to fetch one piece from a source provider. The target
// request is idempotent through its commit extra data, while AttemptID identifies
// the internal ledger row across recovery.
package storagepull

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

// AttemptStatus is deliberately two-valued. A commit needs five states because
// its effect is an on-chain transaction that can hang and cost gas; a pull's
// effect is a transfer request the target can be asked about directly, and
// success is recorded on the copy reaching piece_ready. Termination is
// resolved_at, so a resolved attempt is a success and a resolved abandoned one
// is a request nobody will observe again.
type AttemptStatus string

const (
	AttemptStatusAttempted AttemptStatus = "attempted"
	AttemptStatusAbandoned AttemptStatus = "abandoned"
)

// Attempt is one request sent to a target provider. Every source field is NOT
// NULL: an attempt that exists is fully identified, which is what the old
// all-or-nothing check constraint tried to express across nullable columns.
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
