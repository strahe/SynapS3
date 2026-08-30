package storagecommit

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

const MaxActiveAttemptsPerDataSet = 4

type CopyIdentity struct {
	StorageUploadCopyID int64
	UploadID            int64
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
	Copy  model.StorageUploadCopy
}

type AttemptInput struct {
	Copy         CopyIdentity
	AttemptID    string
	ExtraDataHex string
	Now          time.Time
}

type AttemptResult struct {
	Entered bool
	Copy    model.StorageUploadCopy
}

type EvidenceInput struct {
	Copy           CopyIdentity
	AttemptID      string
	TransactionID  string
	SubmissionJSON string
	Now            time.Time
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
	AllowAttempted    bool
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
	RecordCommitTransaction(context.Context, EvidenceInput) error
	RecordCommitSubmission(context.Context, EvidenceInput) error
	MarkCommitAttention(context.Context, AttentionInput) error
	ResetCommitAttempt(context.Context, ResetInput) error
	ReleaseCommitAttempt(context.Context, ReleaseInput) error
	ReleaseCommitReservation(context.Context, ReservationReleaseInput) error
}
