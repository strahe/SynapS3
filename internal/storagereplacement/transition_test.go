package storagereplacement

import "testing"

var allStatuses = []Status{
	StatusPreparingTarget, StatusMigrating, StatusWaiting, StatusRetiring,
	StatusCleanupAttention, StatusFailed, StatusCompleted, StatusSuperseded,
}

func TestTerminalStatusesNeverTransition(t *testing.T) {
	for _, from := range []Status{StatusCompleted, StatusSuperseded} {
		for _, to := range allStatuses {
			if Allowed(from, to) {
				t.Fatalf("Allowed(%s, %s) = true, want false for a terminal status", from, to)
			}
		}
	}
}

func TestEveryUnfinishedStatusCanBeSuperseded(t *testing.T) {
	for _, from := range allStatuses {
		if from.Terminal() {
			continue
		}
		if !Allowed(from, StatusSuperseded) {
			t.Fatalf("Allowed(%s, superseded) = false, want true so a later confirmation can take over", from)
		}
	}
}

func TestNoStatusTransitionsToItself(t *testing.T) {
	for _, status := range allStatuses {
		if Allowed(status, status) {
			t.Fatalf("Allowed(%s, %s) = true, want false", status, status)
		}
	}
}

// Retirement is the only path to completion, so no earlier phase may shortcut
// the safety gate.
func TestOnlyRetiringReachesCompleted(t *testing.T) {
	for _, from := range allStatuses {
		want := from == StatusRetiring
		if got := Allowed(from, StatusCompleted); got != want {
			t.Fatalf("Allowed(%s, completed) = %v, want %v", from, got, want)
		}
	}
}

func TestForbiddenShortcuts(t *testing.T) {
	forbidden := []struct {
		from Status
		to   Status
		why  string
	}{
		{StatusPreparingTarget, StatusRetiring, "content must migrate before the source is retired"},
		{StatusPreparingTarget, StatusCleanupAttention, "cleanup attention belongs to retirement"},
		{StatusMigrating, StatusPreparingTarget, "the target is already activated"},
		{StatusMigrating, StatusCleanupAttention, "cleanup attention belongs to retirement"},
		{StatusCleanupAttention, StatusFailed, "cleanup attention is resolved by retrying retirement"},
		{StatusFailed, StatusCompleted, "a retry must re-run the safety gate"},
		{StatusFailed, StatusWaiting, "retry re-derives a working phase, not a wait"},
	}
	for _, tc := range forbidden {
		if Allowed(tc.from, tc.to) {
			t.Fatalf("Allowed(%s, %s) = true, want false: %s", tc.from, tc.to, tc.why)
		}
	}
}

// A failed replacement resumes at whatever phase it actually reached.
func TestFailedResumesAtAnyWorkingPhase(t *testing.T) {
	for _, to := range []Status{StatusPreparingTarget, StatusMigrating, StatusRetiring} {
		if !Allowed(StatusFailed, to) {
			t.Fatalf("Allowed(failed, %s) = false, want true", to)
		}
	}
}

func TestStatusClassification(t *testing.T) {
	cases := []struct {
		status    Status
		active    bool
		terminal  bool
		retryable bool
	}{
		{StatusPreparingTarget, true, false, false},
		{StatusMigrating, true, false, false},
		{StatusWaiting, true, false, false},
		{StatusRetiring, true, false, false},
		{StatusCleanupAttention, false, false, true},
		{StatusFailed, false, false, true},
		{StatusCompleted, false, true, false},
		{StatusSuperseded, false, true, false},
	}
	for _, tc := range cases {
		if got := tc.status.Active(); got != tc.active {
			t.Fatalf("%s.Active() = %v, want %v", tc.status, got, tc.active)
		}
		if got := tc.status.Terminal(); got != tc.terminal {
			t.Fatalf("%s.Terminal() = %v, want %v", tc.status, got, tc.terminal)
		}
		if got := tc.status.Retryable(); got != tc.retryable {
			t.Fatalf("%s.Retryable() = %v, want %v", tc.status, got, tc.retryable)
		}
		if !tc.status.Valid() {
			t.Fatalf("%s.Valid() = false, want true", tc.status)
		}
	}
	if Status("unknown").Valid() {
		t.Fatal("unknown status reported as valid")
	}
}

func TestPhaseForMatchesCoordinatorWork(t *testing.T) {
	cases := map[Status]Phase{
		StatusPreparingTarget:  PhasePrepare,
		StatusMigrating:        PhaseMigrate,
		StatusRetiring:         PhaseRetire,
		StatusWaiting:          PhaseNone,
		StatusCleanupAttention: PhaseNone,
		StatusFailed:           PhaseNone,
		StatusCompleted:        PhaseNone,
		StatusSuperseded:       PhaseNone,
	}
	for status, want := range cases {
		if got := PhaseFor(status); got != want {
			t.Fatalf("PhaseFor(%s) = %s, want %s", status, got, want)
		}
	}
}

func TestItemStatusBlocksRetirementWhileExecutable(t *testing.T) {
	cases := map[ItemStatus]bool{
		ItemStatusPending:   true,
		ItemStatusAttention: true,
		ItemStatusCopied:    false,
		ItemStatusCancelled: false,
	}
	for status, want := range cases {
		if got := status.Blocking(); got != want {
			t.Fatalf("%s.Blocking() = %v, want %v", status, got, want)
		}
		if !status.Valid() {
			t.Fatalf("%s.Valid() = false, want true", status)
		}
	}
}

func TestWaitReasonsCarryOperatorMessages(t *testing.T) {
	reasons := []WaitReason{
		WaitReasonReadableSource, WaitReasonTarget, WaitReasonTargetCreating, WaitReasonTargetWritable,
		WaitReasonFunding, WaitReasonProvider,
		WaitReasonTerminationEpoch, WaitReasonSourceWrites, WaitReasonCoverage,
	}
	seen := make(map[string]WaitReason, len(reasons))
	for _, reason := range reasons {
		if !reason.Valid() {
			t.Fatalf("%s.Valid() = false, want true", reason)
		}
		message := reason.Message()
		if message == "" {
			t.Fatalf("%s has no operator message", reason)
		}
		if other, ok := seen[message]; ok {
			t.Fatalf("%s and %s share the message %q, so an operator cannot tell them apart", reason, other, message)
		}
		seen[message] = reason
	}
	if WaitReason("unknown").Valid() {
		t.Fatal("unknown wait reason reported as valid")
	}
}
