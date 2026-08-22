package storagereplacement

import "errors"

// Stable machine-readable codes returned alongside API errors. Clients branch
// on these, so treat them as part of the public contract.
const (
	CodeActive              = "replacement_active"
	CodeSuperseded          = "replacement_superseded"
	CodeNotRetryable        = "replacement_not_retryable"
	CodeTaskRunning         = "replacement_task_running"
	CodeTargetInvalid       = "replacement_target_invalid"
	CodeTargetInUse         = "replacement_target_in_use"
	CodeNoEligibleProvider  = "replacement_no_eligible_provider"
	CodeTargetUnavailable   = "replacement_target_unavailable"
	CodeIdempotencyConflict = "replacement_idempotency_conflict"
	CodeSourceNotCurrent    = "replacement_source_not_current"

	// CodeTaskRetryUnsupported is returned by the generic exhausted-task retry
	// endpoint when the task belongs to a replacement.
	CodeTaskRetryUnsupported = "replacement_task_retry_unsupported"
)

// Code maps a replacement error to its stable API code. It returns an empty
// string for errors that carry no client-facing code.
func Code(err error) string {
	switch {
	case errors.Is(err, ErrActiveReplacement):
		return CodeActive
	case errors.Is(err, ErrSuperseded):
		return CodeSuperseded
	case errors.Is(err, ErrNotRetryable):
		return CodeNotRetryable
	case errors.Is(err, ErrTaskRunning):
		return CodeTaskRunning
	case errors.Is(err, ErrInvalidTarget):
		return CodeTargetInvalid
	case errors.Is(err, ErrTargetInUse):
		return CodeTargetInUse
	case errors.Is(err, ErrNoEligibleProvider):
		return CodeNoEligibleProvider
	case errors.Is(err, ErrTargetUnavailable):
		return CodeTargetUnavailable
	case errors.Is(err, ErrIdempotencyConflict):
		return CodeIdempotencyConflict
	case errors.Is(err, ErrSourceNotCurrent):
		return CodeSourceNotCurrent
	default:
		return ""
	}
}
