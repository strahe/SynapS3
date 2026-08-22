// Package storagereplacement owns the operator-approved provider replacement
// record: its state machine, wait reasons, stable API codes, and task payload
// contract. It holds no database or provider dependencies.
package storagereplacement

// Status is the lifecycle of one approved replacement.
type Status string

const (
	// StatusPreparingTarget means the approved target service is being created.
	// Writes still go to the source.
	StatusPreparingTarget Status = "preparing_target"
	// StatusMigrating means the target owns the slot for new writes and stored
	// content is being copied across.
	StatusMigrating Status = "migrating"
	// StatusWaiting means progress is blocked on a recoverable dependency. It
	// consumes no retry budget and is never a failure.
	StatusWaiting Status = "waiting"
	// StatusRetiring means migration finished and the source is going through
	// the retirement safety gate.
	StatusRetiring Status = "retiring"
	// StatusCleanupAttention means retirement cannot proceed without an
	// operator decision. Automatic task retry is suppressed.
	StatusCleanupAttention Status = "cleanup_attention"
	// StatusFailed means the work exhausted its retries and needs the operator
	// to retry it from the Data Sets surface.
	StatusFailed Status = "failed"
	// StatusCompleted means the source service was terminated and observed as
	// terminated.
	StatusCompleted Status = "completed"
	// StatusSuperseded means a later confirmation replaced this one.
	StatusSuperseded Status = "superseded"
)

// Active reports whether a coordinator should still advance this replacement.
func (s Status) Active() bool {
	switch s {
	case StatusPreparingTarget, StatusMigrating, StatusWaiting, StatusRetiring:
		return true
	default:
		return false
	}
}

// Terminal reports whether the replacement can never change again.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusSuperseded
}

// Retryable reports whether the dedicated retry action accepts this status.
// Waiting work resumes on its own and is deliberately excluded.
func (s Status) Retryable() bool {
	return s == StatusFailed || s == StatusCleanupAttention
}

// HoldsSource reports whether the replacement still owns its source data set,
// which is what stops a second replacement from starting on the same source.
func (s Status) HoldsSource() bool {
	return !s.Terminal()
}

// Valid reports whether the value is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPreparingTarget, StatusMigrating, StatusWaiting, StatusRetiring,
		StatusCleanupAttention, StatusFailed, StatusCompleted, StatusSuperseded:
		return true
	default:
		return false
	}
}

// SelectionMode records how the operator chose the target provider.
type SelectionMode string

const (
	// SelectionModeAutomatic picks an eligible provider the bucket has not used.
	SelectionModeAutomatic SelectionMode = "automatic"
	// SelectionModeManual uses exactly the provider the operator supplied.
	SelectionModeManual SelectionMode = "manual"
)

// Valid reports whether the value is a known selection mode.
func (m SelectionMode) Valid() bool {
	return m == SelectionModeAutomatic || m == SelectionModeManual
}

// ItemStatus is the lifecycle of one unit of migration work. An item is keyed
// by stored content, not by object version, so shared content migrates once.
type ItemStatus string

const (
	// ItemStatusPending is seeded work not yet attempted.
	ItemStatusPending ItemStatus = "pending"
	// ItemStatusRunning is the one item currently held by the coordinator.
	ItemStatusRunning ItemStatus = "running"
	// ItemStatusWaitingSource means no readable copy and no cached content is
	// available yet. The coordinator moves on and revisits it later.
	ItemStatusWaitingSource ItemStatus = "waiting_source"
	// ItemStatusCopied means the target holds a committed readable copy.
	ItemStatusCopied ItemStatus = "copied"
	// ItemStatusCancelled means the content no longer needs migrating.
	ItemStatusCancelled ItemStatus = "cancelled"
)

// Executable reports whether the coordinator may pick this item up.
func (s ItemStatus) Executable() bool {
	return s == ItemStatusPending || s == ItemStatusRunning || s == ItemStatusWaitingSource
}

// Blocking reports whether the item prevents the source from being retired.
func (s ItemStatus) Blocking() bool {
	return s.Executable()
}

// Valid reports whether the value is a known item status.
func (s ItemStatus) Valid() bool {
	switch s {
	case ItemStatusPending, ItemStatusRunning, ItemStatusWaitingSource,
		ItemStatusCopied, ItemStatusCancelled:
		return true
	default:
		return false
	}
}
