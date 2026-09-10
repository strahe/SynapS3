package storagecommit

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// AttemptStatus is the durable lifecycle of one possible external commit
// effect. Terminal attempts remain as evidence after the copy is re-queued.
type AttemptStatus string

const (
	AttemptStatusReserved  AttemptStatus = "reserved"
	AttemptStatusAttempted AttemptStatus = "attempted"
	AttemptStatusConfirmed AttemptStatus = "confirmed"
	AttemptStatusReleased  AttemptStatus = "released"
	AttemptStatusRejected  AttemptStatus = "rejected"
)

// Attempt is the append-preserving ledger for storage commit side effects.
// ContentID and StorageDataSetID are the stable business key of the bound copy.
type Attempt struct {
	bun.BaseModel `bun:"table:storage_commit_attempts"`

	AttemptID              string        `bun:"attempt_id,type:text,pk"`
	ContentID              int64         `bun:"content_id,notnull"`
	StorageDataSetID       int64         `bun:"storage_data_set_id,notnull"`
	Status                 AttemptStatus `bun:"status,type:text,notnull,default:'reserved'"`
	ExtraDataHex           *string       `bun:"extra_data_hex,type:text,nullzero"`
	TransactionID          *string       `bun:"transaction_id,type:text,nullzero"`
	SubmissionJSON         *string       `bun:"submission_json,type:text,nullzero"`
	ConfirmedTransactionID *string       `bun:"confirmed_transaction_id,type:text,nullzero"`
	AttentionCode          *string       `bun:"attention_code,type:text,nullzero"`
	AttentionAt            *time.Time    `bun:"attention_at,nullzero"`
	ReleaseReason          *string       `bun:"release_reason,type:text,nullzero"`
	LastError              *string       `bun:"last_error,type:text,nullzero"`
	AttemptedAt            *time.Time    `bun:"attempted_at,nullzero"`
	ResolvedAt             *time.Time    `bun:"resolved_at,nullzero"`
	CreatedAt              time.Time     `bun:"created_at,nullzero,notnull"`
	UpdatedAt              time.Time     `bun:"updated_at,nullzero,notnull"`
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
