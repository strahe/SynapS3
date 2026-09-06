package storagereplacement

// Phase is the worker responsibility a status maps to. It keeps the coordinator
// dispatch and the state machine from drifting apart.
type Phase string

const (
	// PhasePrepare creates the approved target service and activates it.
	PhasePrepare Phase = "prepare"
	// PhaseMigrate copies stored content to the target.
	PhaseMigrate Phase = "migrate"
	// PhaseRetire runs the safety gate and terminates the old service.
	PhaseRetire Phase = "retire"
	// PhaseNone means no coordinator should act.
	PhaseNone Phase = "none"
)

// allowedTransitions is the complete state machine. A transition absent here is
// a bug, not an edge case, and the repository refuses it.
var allowedTransitions = map[Status]map[Status]bool{
	StatusPreparingTarget: {
		StatusMigrating:  true,
		StatusWaiting:    true,
		StatusFailed:     true,
		StatusSuperseded: true,
	},
	StatusMigrating: {
		StatusWaiting:    true,
		StatusRetiring:   true,
		StatusFailed:     true,
		StatusSuperseded: true,
	},
	StatusWaiting: {
		StatusPreparingTarget:  true,
		StatusMigrating:        true,
		StatusRetiring:         true,
		StatusFailed:           true,
		StatusCleanupAttention: true,
		StatusSuperseded:       true,
	},
	StatusRetiring: {
		StatusWaiting:          true,
		StatusCompleted:        true,
		StatusCleanupAttention: true,
		StatusSuperseded:       true,
	},
	// Retry re-derives the phase from data, so a failed replacement can resume
	// at whichever stage it actually reached.
	StatusFailed: {
		StatusPreparingTarget: true,
		StatusMigrating:       true,
		StatusRetiring:        true,
		StatusSuperseded:      true,
	},
	StatusCleanupAttention: {
		StatusRetiring:   true,
		StatusSuperseded: true,
	},
	StatusCompleted:  {},
	StatusSuperseded: {},
}

// Allowed reports whether the state machine permits this transition. A status
// never transitions to itself; callers update fields in place instead.
func Allowed(from, to Status) bool {
	return allowedTransitions[from][to]
}

// NextStates lists the statuses reachable from one status.
func NextStates(from Status) []Status {
	targets := allowedTransitions[from]
	out := make([]Status, 0, len(targets))
	for _, candidate := range []Status{
		StatusPreparingTarget, StatusMigrating, StatusWaiting, StatusRetiring,
		StatusCleanupAttention, StatusFailed, StatusCompleted, StatusSuperseded,
	} {
		if targets[candidate] {
			out = append(out, candidate)
		}
	}
	return out
}

// PhaseFor maps a status to the work a coordinator should perform.
func PhaseFor(status Status) Phase {
	switch status {
	case StatusPreparingTarget:
		return PhasePrepare
	case StatusMigrating:
		return PhaseMigrate
	case StatusRetiring:
		return PhaseRetire
	default:
		// Waiting work resumes through the status it was waiting in, which the
		// caller recovers from the record rather than from the status alone.
		return PhaseNone
	}
}
