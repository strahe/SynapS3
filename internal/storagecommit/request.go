package storagecommit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

// RequestStatus is the lifecycle of one signed add-pieces request.
type RequestStatus string

const (
	// RequestStatusCollecting gathers transferred copies of one data set. Nothing
	// has been signed, so members can still leave.
	RequestStatusCollecting RequestStatus = "collecting"
	// RequestStatusReady is signed. No send of it can be pending.
	RequestStatusReady RequestStatus = "ready"
	// RequestStatusSubmitted has been sent at least once in this period, and a
	// send may still land.
	RequestStatusSubmitted RequestStatus = "submitted"
	// RequestStatusConfirmed is proven on chain.
	RequestStatusConfirmed RequestStatus = "confirmed"
	// RequestStatusAbandoned can never land.
	RequestStatusAbandoned RequestStatus = "abandoned"
)

// Terminal reports whether the request has been settled.
func (s RequestStatus) Terminal() bool {
	return s == RequestStatusConfirmed || s == RequestStatusAbandoned
}

// MaxSubmittedRequestsPerDataSet bounds the requests of one data set that may
// be on chain without a known outcome.
const MaxSubmittedRequestsPerDataSet = 4

// Request is the ledger of one signed add-pieces request: the data set it
// names, every send of it, and its outcome. It outlives the copies it
// registers.
type Request struct {
	bun.BaseModel `bun:"table:storage_commit_requests,alias:commit_request"`

	RequestID              string             `bun:"request_id,type:text,pk"`
	StorageDataSetID       int64              `bun:"storage_data_set_id,notnull"`
	Status                 RequestStatus      `bun:"status,type:text,notnull"`
	TaskID                 *int64             `bun:"task_id,nullzero"`
	PieceCount             int                `bun:"piece_count,type:integer,notnull"`
	ExtraDataHex           *string            `bun:"extra_data_hex,type:text,nullzero"`
	SealedAt               *time.Time         `bun:"sealed_at,nullzero"`
	FirstSentAt            *time.Time         `bun:"first_sent_at,nullzero"`
	Sends                  int                `bun:"sends,type:integer,notnull"`
	SubmittedAt            *time.Time         `bun:"submitted_at,nullzero"`
	LastSentAt             *time.Time         `bun:"last_sent_at,nullzero"`
	TransactionID          *string            `bun:"transaction_id,type:text,nullzero"`
	StatusURL              *string            `bun:"status_url,type:text,nullzero"`
	SubmitError            *string            `bun:"submit_error,type:text,nullzero"`
	Refusals               int                `bun:"refusals,type:integer,notnull"`
	RetryAt                *time.Time         `bun:"retry_at,nullzero"`
	FirstPieceID           *idtypes.OnChainID `bun:"first_piece_id,type:text"`
	ConfirmedTransactionID *string            `bun:"confirmed_transaction_id,type:text,nullzero"`
	ConfirmedAt            *time.Time         `bun:"confirmed_at,nullzero"`
	LastError              *string            `bun:"last_error,type:text,nullzero"`
	AttentionCode          *string            `bun:"attention_code,type:text,nullzero"`
	AttentionAt            *time.Time         `bun:"attention_at,nullzero"`
	CreatedAt              time.Time          `bun:"created_at,nullzero,notnull"`
	UpdatedAt              time.Time          `bun:"updated_at,nullzero,notnull"`
}

var _ bun.BeforeAppendModelHook = (*Request)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (r *Request) BeforeAppendModel(_ context.Context, query bun.Query) error {
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

// Sent reports whether a send of the request may ever have reached the
// provider. Its members are then pinned until the request is settled.
func (r Request) Sent() bool { return r.FirstSentAt != nil }

// RequestPiece names one piece of a sealed request at its position.
type RequestPiece struct {
	bun.BaseModel `bun:"table:storage_commit_request_pieces"`

	ID               int64     `bun:"id,pk,autoincrement,identity"`
	RequestID        string    `bun:"request_id,type:text,notnull"`
	Position         int       `bun:"position,type:integer,notnull"`
	ContentID        int64     `bun:"content_id,notnull"`
	StorageDataSetID int64     `bun:"storage_data_set_id,notnull"`
	PieceCID         string    `bun:"piece_cid,type:text,notnull"`
	CreatedAt        time.Time `bun:"created_at,nullzero,notnull"`
}

var _ bun.BeforeAppendModelHook = (*RequestPiece)(nil)

func (p *RequestPiece) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); ok && p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now().UTC()
	}
	return nil
}

// NewRequestID returns a random identifier for a new commit request.
func NewRequestID() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generating storage commit request ID: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

// ErrNotEligible means a ready request cannot be sent yet: its data set has no
// room, an older request goes first, it is backing off, or a member is still
// being transferred.
var ErrNotEligible = errors.New("storage commit request cannot be sent yet")

// ProviderRejectedWaitReason is the wait reason of a request backing off after
// the provider refused it.
const ProviderRejectedWaitReason = "provider_rejected"
