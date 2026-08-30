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

type ReleaseReason string

const (
	ReleaseBeforeSubmitCanceled ReleaseReason = "before_submit_canceled"
	ReleaseDataSetUnavailable   ReleaseReason = "data_set_unavailable"
	ReleaseOwnerTerminal        ReleaseReason = "owner_terminal"
)

type AdvanceResult struct {
	State         AdvanceState
	ReleaseReason ReleaseReason
	AttemptID     string
	Confirmation  *storage.CommitResult
	AttentionCode AttentionCode
	Continue      bool
}
