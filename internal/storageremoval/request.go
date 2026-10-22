// Package storageremoval owns deletion batches and their durable send evidence.
package storageremoval

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type RequestStatus string

const (
	RequestStatusCollecting RequestStatus = "collecting"
	RequestStatusReady      RequestStatus = "ready"
	RequestStatusSubmitted  RequestStatus = "submitted"
	RequestStatusQueued     RequestStatus = "queued"
	RequestStatusSettled    RequestStatus = "settled"
	RequestStatusAbandoned  RequestStatus = "abandoned"
)

func (s RequestStatus) Valid() bool {
	//exhaustive:enforce
	switch s {
	case RequestStatusCollecting, RequestStatusReady, RequestStatusSubmitted, RequestStatusQueued, RequestStatusSettled, RequestStatusAbandoned:
		return true
	default:
		return false
	}
}

func (s RequestStatus) Terminal() bool {
	return s == RequestStatusSettled || s == RequestStatusAbandoned
}

type Request struct {
	bun.BaseModel    `bun:"table:storage_removal_requests,alias:removal_request"`
	RequestID        string        `bun:"request_id,type:text,pk"`
	StorageDataSetID int64         `bun:"storage_data_set_id,notnull"`
	Status           RequestStatus `bun:"status,type:text,notnull"`
	TaskID           *int64        `bun:"task_id,nullzero"`
	SealedAt         *time.Time    `bun:"sealed_at,nullzero"`
	QueuedAt         *time.Time    `bun:"queued_at,nullzero"`
	QueuedBlock      *int64        `bun:"queued_block,nullzero"`
	AttentionCode    *string       `bun:"attention_code,type:text,nullzero"`
	AttentionAt      *time.Time    `bun:"attention_at,nullzero"`
	LastError        *string       `bun:"last_error,type:text,nullzero"`
	CreatedAt        time.Time     `bun:"created_at,nullzero,notnull"`
	UpdatedAt        time.Time     `bun:"updated_at,nullzero,notnull"`
}

func (r *Request) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); ok {
		now := time.Now().UTC()
		if r.CreatedAt.IsZero() {
			r.CreatedAt = now
		}
		if r.UpdatedAt.IsZero() {
			r.UpdatedAt = now
		}
	}
	return nil
}

// Send preserves one admission, including authorizations refused by a provider.
type Send struct {
	bun.BaseModel   `bun:"table:storage_removal_sends,alias:removal_send"`
	RequestID       string      `bun:"request_id,type:text,pk"`
	SendNo          int         `bun:"send_no,type:integer,pk"`
	TaskID          *int64      `bun:"task_id,nullzero"`
	AttemptedAt     *time.Time  `bun:"attempted_at,nullzero"`
	TxHash          *string     `bun:"tx_hash,type:text,nullzero"`
	ConfirmedTxHash *string     `bun:"confirmed_tx_hash,type:text,nullzero"`
	ReceiptStatus   *int64      `bun:"receipt_status,type:smallint,nullzero"`
	RefusedAt       *time.Time  `bun:"refused_at,nullzero"`
	UnsentAt        *time.Time  `bun:"unsent_at,nullzero"`
	LastError       *string     `bun:"last_error,type:text,nullzero"`
	CreatedAt       time.Time   `bun:"created_at,nullzero,notnull"`
	UpdatedAt       time.Time   `bun:"updated_at,nullzero,notnull"`
	Pieces          []SendPiece `bun:"-"`
}

func (s *Send) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); ok {
		now := time.Now().UTC()
		if s.CreatedAt.IsZero() {
			s.CreatedAt = now
		}
		if s.UpdatedAt.IsZero() {
			s.UpdatedAt = now
		}
	}
	return nil
}

func (s Send) Exposed() bool          { return s.UnsentAt == nil }
func (s Send) PossiblyAccepted() bool { return s.UnsentAt == nil && s.RefusedAt == nil }
func (s Send) InFlight() bool {
	return s.PossiblyAccepted() && (s.ReceiptStatus == nil || *s.ReceiptStatus != 0)
}

type SendPiece struct {
	bun.BaseModel `bun:"table:storage_removal_send_pieces"`
	RequestID     string            `bun:"request_id,type:text,pk"`
	SendNo        int               `bun:"send_no,type:integer,pk"`
	CleanupCopyID int64             `bun:"cleanup_copy_id,pk"`
	PieceID       idtypes.OnChainID `bun:"piece_id,type:text,notnull"`
	CreatedAt     time.Time         `bun:"created_at,nullzero,notnull"`
}

func (p *SendPiece) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); ok && p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now().UTC()
	}
	return nil
}

// TaskBatch is the operator-facing projection of a task's current batch.
type TaskBatch struct {
	TaskID        int64              `bun:"task_id"`
	RequestID     string             `bun:"request_id"`
	Status        RequestStatus      `bun:"status"`
	AttentionCode *string            `bun:"attention_code"`
	AttentionAt   *time.Time         `bun:"attention_at"`
	ProviderID    idtypes.OnChainID  `bun:"provider_id,type:text"`
	DataSetID     *idtypes.OnChainID `bun:"data_set_id,type:text"`
	PieceCount    int                `bun:"piece_count"`
	TxHash        *string            `bun:"tx_hash"`
	LastError     *string            `bun:"last_error"`
}

func NewRequestID() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generating removal request ID: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}
