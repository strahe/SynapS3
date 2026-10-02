package storagecommit

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

const MaxActiveAttemptsPerDataSet = 4

// MaxReadyCopiesPerDataSet bounds the transferred copies waiting to be
// committed to one data set. A provider deletes an uploaded piece that has not
// joined a data set within hours, so transfers wait instead of queueing more
// pieces than the data set can commit in that time.
const MaxReadyCopiesPerDataSet = 32

// ProviderRejectedWaitReason is the wait reason of a commit task backing off
// after the provider refused its submission. Freed commit capacity does not
// wake it early.
const ProviderRejectedWaitReason = "provider_rejected"

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

// SubmitFailureInput records why a submission whose outcome is unknown failed.
type SubmitFailureInput struct {
	Copy      CopyIdentity
	AttemptID string
	Message   string
	Now       time.Time
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
	// Unacknowledged resolves an attempted request the provider never
	// acknowledged although an earlier send of it may still land. The reason
	// must keep that possibility visible to request history.
	Unacknowledged bool
	ClearReadyAt   bool
	ClearExtraData bool
	// SubmitError keeps the provider's reply to a submission it refused.
	SubmitError string
	Now         time.Time
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
	RecordCommitSubmitFailure(context.Context, SubmitFailureInput) error
	MarkCommitAttention(context.Context, AttentionInput) error
	ResetCommitAttempt(context.Context, ResetInput) error
	ReleaseCommitAttempt(context.Context, ReleaseInput) error
	ReleaseCommitReservation(context.Context, ReservationReleaseInput) error
	ListCommitRequests(context.Context, CopyIdentity) ([]CommitRequestHistory, error)
	// CountConsecutiveCommitRejections counts the copy's most recent resolved
	// attempts the provider refused, up to the first one it did not.
	CountConsecutiveCommitRejections(context.Context, CopyIdentity) (int, error)
}
