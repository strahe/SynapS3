package storagecommit

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

const MaxActiveAttemptsPerDataSet = 4

type CopyIdentity struct {
	StorageCopyID       int64
	ContentID           int64
	CopyIndex           int
	StorageDataSetID    int64
	RequireEligibleCopy bool
}

type ReservationState string

const (
	ReservationAcquired ReservationState = "acquired"
	ReservationWaiting  ReservationState = "waiting"
)

type ReserveInput struct {
	Copy      CopyIdentity
	AttemptID string
	Now       time.Time
}

type ReserveResult struct {
	State ReservationState
	Copy  model.StorageCopy
	// AttentionHeld counts the data set's active attempts already flagged for
	// operator attention. It is set only when capacity turned the reservation
	// away, so a waiting result with zero here is queued behind work that is
	// still moving.
	AttentionHeld int
}

type AttemptInput struct {
	Copy         CopyIdentity
	AttemptID    string
	ExtraDataHex string
	Now          time.Time
}

type AttemptResult struct {
	Entered bool
	Copy    model.StorageCopy
}

type EvidenceInput struct {
	Copy          CopyIdentity
	AttemptID     string
	TransactionID string
	StatusURL     string
	Now           time.Time
}

type AttentionInput struct {
	Copy      CopyIdentity
	AttemptID string
	Code      AttentionCode
	Now       time.Time
}

type ResetInput struct {
	Copy      CopyIdentity
	AttemptID string
	LastError string
	Now       time.Time
}

type ReleaseInput struct {
	Copy              CopyIdentity
	AttemptID         string
	Reason            ReleaseReason
	KnownNotSubmitted bool
	ClearReadyAt      bool
	ClearExtraData    bool
	Now               time.Time
}

type ReservationReleaseInput struct {
	Copy           CopyIdentity
	ClearReadyAt   bool
	ClearExtraData bool
	Now            time.Time
}

type Store interface {
	ReserveCommitAttempt(context.Context, ReserveInput) (ReserveResult, error)
	MarkCommitAttempted(context.Context, AttemptInput) (AttemptResult, error)
	RecordCommitSubmission(context.Context, EvidenceInput) error
	MarkCommitAttention(context.Context, AttentionInput) error
	ResetCommitAttempt(context.Context, ResetInput) error
	ReleaseCommitAttempt(context.Context, ReleaseInput) error
	ReleaseCommitReservation(context.Context, ReservationReleaseInput) error
}
