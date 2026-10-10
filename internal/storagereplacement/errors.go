package storagereplacement

import "errors"

// SourceOutcomeError supplies a safe explanation while preserving the 409 code.
type SourceOutcomeError struct{ Message string }

func (e *SourceOutcomeError) Error() string { return e.Message }
func (e *SourceOutcomeError) Unwrap() error { return ErrSourceOutcomeUnknown }

var (
	// ErrSourceRunning blocks replacement while creation work holds a live claim.
	ErrSourceRunning = errors.New("data set setup is still running")
	// ErrSourceOutcomeUnknown requires an observed result before replacement.
	ErrSourceOutcomeUnknown = errors.New("data set creation outcome is not confirmed")
	// ErrActiveReplacement means this data set is reserved by an unfinished
	// replacement; only stopped outgoing work permits another provider choice.
	ErrActiveReplacement = errors.New("data set already has an active replacement")

	// ErrTargetCreating means an earlier replacement of this replica may still
	// get its target storage service created on chain, so superseding it would
	// leave that service untracked.
	ErrTargetCreating = errors.New("an earlier replacement target is still being created")

	// ErrSuperseded means a later confirmation took ownership of this work.
	ErrSuperseded = errors.New("replacement has been superseded")

	// ErrNotRetryable means the replacement is progressing or finished, so the
	// dedicated retry action does not apply.
	ErrNotRetryable = errors.New("replacement is not in a retryable state")

	// ErrTaskRunning means a coordinator task still holds this replacement.
	ErrTaskRunning = errors.New("replacement task is still running")

	// ErrInvalidTarget means the requested provider cannot serve as the target,
	// for example because it is the source itself.
	ErrInvalidTarget = errors.New("requested provider cannot replace this data set")

	// ErrTargetInUse means the requested provider already owns another current
	// data set in the same bucket.
	ErrTargetInUse = errors.New("requested provider already serves this bucket")

	// ErrNoEligibleProvider means automatic selection found no provider the
	// bucket has not already used.
	ErrNoEligibleProvider = errors.New("no eligible replacement provider is available")

	// ErrTargetUnavailable means the requested provider is not currently an
	// active PDP-capable replacement candidate.
	ErrTargetUnavailable = errors.New("requested provider is not available for replacement")

	// ErrIdempotencyConflict means a client request id was reused with different
	// replacement parameters.
	ErrIdempotencyConflict = errors.New("replacement idempotency key conflicts with an earlier request")

	// ErrSourceNotCurrent means the data set no longer owns its replica slot,
	// so replacing it would not change where writes go.
	ErrSourceNotCurrent = errors.New("data set is not the current replica")

	// ErrItemCancelled means this migration item no longer has executable work.
	ErrItemCancelled = errors.New("replacement item is no longer executable")

	// ErrItemDeferred means the item cannot run yet but must be revisited. The
	// coordinator moves on to other content rather than blocking on it.
	ErrItemDeferred = errors.New("replacement item is waiting for its source")

	// ErrPrematureComplete means a retirement safety gate still reports a
	// blocker. Repositories return it even when called outside the worker.
	ErrPrematureComplete = errors.New("replacement cannot complete while a safety gate blocks it")

	// ErrIllegalTransition means the requested state change is not in the
	// state machine.
	ErrIllegalTransition = errors.New("illegal replacement state transition")
)

// NotRetryableError supplies the operator action for a stopped replacement.
type NotRetryableError struct{ Message string }

func (e *NotRetryableError) Error() string { return e.Message }
func (e *NotRetryableError) Unwrap() error { return ErrNotRetryable }
