package model

import (
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
	WalletOperationStatusRunning   WalletOperationStatus = "running"
	WalletOperationStatusSubmitted WalletOperationStatus = "submitted"
	WalletOperationStatusConfirmed WalletOperationStatus = "confirmed"
	WalletOperationStatusFailed    WalletOperationStatus = "failed"
	WalletOperationStatusUnknown   WalletOperationStatus = "unknown"
)

type WalletOperation struct {
	bun.BaseModel `bun:"table:wallet_operations"`

	ID              int64                 `bun:",pk,autoincrement"`
	Type            WalletOperationType   `bun:",notnull"`
	ClientRequestID string                `bun:",notnull"`
	Amount          string                `bun:",notnull"`
	Status          WalletOperationStatus `bun:",notnull,default:'pending'"`
	TxHash          *string               `bun:",nullzero"`
	LastError       *string               `bun:",nullzero"`
	LeaseUntil      *time.Time            `bun:",nullzero"`
	StartedAt       *time.Time            `bun:",nullzero"`
	SubmittedAt     *time.Time            `bun:",nullzero"`
	CompletedAt     *time.Time            `bun:",nullzero"`
	CreatedAt       time.Time             `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt       time.Time             `bun:",nullzero,notnull,default:current_timestamp"`
}
