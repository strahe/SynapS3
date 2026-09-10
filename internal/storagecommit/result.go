package storagecommit

import "github.com/strahe/synapse-go/storage"

type AdvanceState string

const (
	AdvanceWaitingCapacity AdvanceState = "waiting_capacity"
	AdvanceSubmitted       AdvanceState = "submitted"
	AdvancePending         AdvanceState = "pending"
	AdvanceReleased        AdvanceState = "released"
	AdvanceConfirmed       AdvanceState = "confirmed"
	AdvanceRejected        AdvanceState = "rejected"
	AdvanceNeedsAttention  AdvanceState = "needs_attention"
)

// Valid reports whether the application can write this release reason. The
// database keeps the column open so newer binaries can add reasons safely.
func (r ReleaseReason) Valid() bool {
	switch r {
	case ReleaseBeforeSubmitCanceled, ReleaseDataSetUnavailable, ReleaseOwnerTerminal, ReleaseManualDuplicateAck:
		return true
	default:
		return false
	}
}

type ReleaseReason string

const (
	ReleaseBeforeSubmitCanceled ReleaseReason = "before_submit_canceled"
	ReleaseDataSetUnavailable   ReleaseReason = "data_set_unavailable"
	ReleaseOwnerTerminal        ReleaseReason = "owner_terminal"
	ReleaseManualDuplicateAck   ReleaseReason = "manual_duplicate_acknowledgement"
)

type AdvanceResult struct {
	State         AdvanceState
	ReleaseReason ReleaseReason
	AttemptID     string
	Confirmation  *storage.CommitResult
	AttentionCode AttentionCode
	Continue      bool
	// Cause carries the provider error behind a release so the caller can record
	// why the data set failed instead of a generic sentinel. It is set only on
	// ReleaseDataSetUnavailable and is nil everywhere else.
	Cause error
	// AttentionHeld carries the reservation's flagged-attempt count so the caller
	// can say why capacity is unavailable. It is set only on
	// AdvanceWaitingCapacity and is zero everywhere else.
	AttentionHeld int
}
