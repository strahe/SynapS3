package model

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

type WalletOperationType string

const (
	WalletOperationTypeFund     WalletOperationType = "fund"
	WalletOperationTypeWithdraw WalletOperationType = "withdraw"
	WalletOperationTypeApprove  WalletOperationType = "approve"
)

type WalletOperationStatus string

const (
	WalletOperationStatusPending   WalletOperationStatus = "pending"
	WalletOperationStatusSubmitted WalletOperationStatus = "submitted"
	WalletOperationStatusConfirmed WalletOperationStatus = "confirmed"
	WalletOperationStatusFailed    WalletOperationStatus = "failed"
	WalletOperationStatusUnknown   WalletOperationStatus = "unknown"
)

type WalletOperation struct {
	bun.BaseModel `bun:"table:wallet_operations"`

	ID                   int64                 `bun:",pk,autoincrement,identity"`
	Type                 WalletOperationType   `bun:"type:text,notnull"`
	ClientRequestID      string                `bun:"type:text,notnull"`
	Amount               string                `bun:"type:text,notnull"`
	Status               WalletOperationStatus `bun:"type:text,notnull,default:'pending'"`
	TxHash               *string               `bun:"type:text,nullzero"`
	LastError            *string               `bun:"type:text,nullzero"`
	BroadcastAttemptedAt *time.Time            `bun:",nullzero"`
	TaskID               *int64                `bun:",nullzero"`
	StartedAt            *time.Time            `bun:",nullzero"`
	SubmittedAt          *time.Time            `bun:",nullzero"`
	CompletedAt          *time.Time            `bun:",nullzero"`
	CreatedAt            time.Time             `bun:",nullzero,notnull"`
	UpdatedAt            time.Time             `bun:",nullzero,notnull"`
}

var _ bun.BeforeAppendModelHook = (*WalletOperation)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (w *WalletOperation) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if w.CreatedAt.IsZero() {
		w.CreatedAt = now
	}
	if w.UpdatedAt.IsZero() {
		w.UpdatedAt = now
	}
	return nil
}
