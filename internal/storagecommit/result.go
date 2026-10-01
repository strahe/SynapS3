package storagecommit

import (
	"time"

	"github.com/strahe/synapse-go/storage"
)

type AdvanceState string

const (
	AdvanceWaitingCapacity AdvanceState = "waiting_capacity"
	AdvanceSubmitted       AdvanceState = "submitted"
	AdvancePending         AdvanceState = "pending"
	AdvanceReleased        AdvanceState = "released"
	AdvanceConfirmed       AdvanceState = "confirmed"
	AdvanceRejected        AdvanceState = "rejected"
	AdvanceNeedsAttention  AdvanceState = "needs_attention"
	// AdvanceDeferred released an attempt that provably never reached the
	// chain; the copy keeps its signed request and tries again after
	// RetryAfter.
	AdvanceDeferred AdvanceState = "deferred"
	// AdvanceResendDue means the chain shows the attempt's request unused and
	// long enough has passed for execute to send the same request again.
	AdvanceResendDue AdvanceState = "resend_due"
)

// Valid reports whether the application can write this release reason. The
// database keeps the column open so newer binaries can add reasons safely.
func (r ReleaseReason) Valid() bool {
	//exhaustive:enforce
	switch r {
	case ReleaseBeforeSubmitCanceled, ReleaseDataSetUnavailable, ReleaseOwnerTerminal, ReleaseProviderRejected:
		return true
	case ReleaseManualDuplicateAck:
		return false
	default:
		return false
	}
}

type ReleaseReason string

const (
	ReleaseBeforeSubmitCanceled ReleaseReason = "before_submit_canceled"
	ReleaseDataSetUnavailable   ReleaseReason = "data_set_unavailable"
	ReleaseOwnerTerminal        ReleaseReason = "owner_terminal"
	// ReleaseProviderRejected means the provider refused the submission with a
	// 4xx before sending any transaction.
	ReleaseProviderRejected ReleaseReason = "provider_rejected"
	// ReleaseManualDuplicateAck names attempts an operator released by hand in
	// earlier versions. Nothing writes it any more.
	ReleaseManualDuplicateAck ReleaseReason = "manual_duplicate_acknowledgement"
)

type AdvanceResult struct {
	State         AdvanceState
	ReleaseReason ReleaseReason
	AttemptID     string
	Confirmation  *storage.CommitResult
	// ProvenByNonce marks a confirmation read from the FWSS nonce record rather
	// than from the provider; it carries no confirmed transaction.
	ProvenByNonce bool
	AttentionCode AttentionCode
	Continue      bool
	// Cause carries the error behind a release so the caller can record why it
	// happened instead of a generic sentinel. It is set on
	// ReleaseDataSetUnavailable and AdvanceDeferred and is nil everywhere else.
	Cause error
	// RetryAfter is how long to wait before advancing again. It is set on
	// AdvanceDeferred, and on AdvancePending while a resend is not yet due or
	// the chain could not be read.
	RetryAfter time.Duration
	// AttentionHeld carries the reservation's flagged-attempt count so the caller
	// can say why capacity is unavailable. It is set only on
	// AdvanceWaitingCapacity and is zero everywhere else.
	AttentionHeld int
}
