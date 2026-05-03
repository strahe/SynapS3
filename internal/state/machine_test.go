package state

import (
	"sort"
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestObjectStateMachine_HappyPath(t *testing.T) {
	m := NewObjectStateMachine()

	transitions := []struct{ from, to model.ObjectState }{
		{model.ObjectStateCached, model.ObjectStateUploading},
		{model.ObjectStateUploading, model.ObjectStateCommitting},
		{model.ObjectStateCommitting, model.ObjectStateReplicating},
		{model.ObjectStateReplicating, model.ObjectStateStored},
		{model.ObjectStateStored, model.ObjectStateCacheEvicted},
	}
	for _, tt := range transitions {
		if err := m.Validate(string(tt.from), string(tt.to)); err != nil {
			t.Errorf("happy path %s→%s: unexpected error: %v", tt.from, tt.to, err)
		}
	}
}

func TestObjectStateMachine_FailureTransitions(t *testing.T) {
	m := NewObjectStateMachine()

	// States that can transition to failed.
	canFail := []model.ObjectState{
		model.ObjectStateUploading,
		model.ObjectStateCommitting,
	}
	for _, from := range canFail {
		if err := m.Validate(string(from), string(model.ObjectStateFailed)); err != nil {
			t.Errorf("%s→failed: unexpected error: %v", from, err)
		}
	}

	// States that cannot transition to failed.
	cannotFail := []model.ObjectState{
		model.ObjectStateCached,
		model.ObjectStateReplicating,
		model.ObjectStateStored,
		model.ObjectStateFailed,
		model.ObjectStateCacheEvicted,
	}
	for _, from := range cannotFail {
		if m.CanTransition(string(from), string(model.ObjectStateFailed)) {
			t.Errorf("%s→failed: should not be allowed", from)
		}
	}
}

func TestObjectStateMachine_RetryFromFailed(t *testing.T) {
	m := NewObjectStateMachine()

	// Allowed retries from failed.
	allowed := []model.ObjectState{
		model.ObjectStateUploading,
	}
	for _, to := range allowed {
		if err := m.Validate(string(model.ObjectStateFailed), string(to)); err != nil {
			t.Errorf("failed→%s: unexpected error: %v", to, err)
		}
	}

	// Disallowed retries.
	disallowed := []model.ObjectState{
		model.ObjectStateCached,
		model.ObjectStateStored,
		model.ObjectStateCacheEvicted,
	}
	for _, to := range disallowed {
		if m.CanTransition(string(model.ObjectStateFailed), string(to)) {
			t.Errorf("failed→%s: should not be allowed", to)
		}
	}
}

func TestObjectStateMachine_InvalidTransitions(t *testing.T) {
	m := NewObjectStateMachine()

	invalid := []struct{ from, to model.ObjectState }{
		{model.ObjectStateCached, model.ObjectStateStored},          // must go through uploading
		{model.ObjectStateUploading, model.ObjectStateReplicating},  // primary Store is not enough
		{model.ObjectStateCommitting, model.ObjectStateStored},      // secondary commits still pending
		{model.ObjectStateCached, model.ObjectStateCacheEvicted},    // can't evict from cached
		{model.ObjectStateCacheEvicted, model.ObjectStateCached},    // can't revive
		{model.ObjectStateStored, model.ObjectStateUploading},       // backwards
		{model.ObjectStateCached, model.ObjectStateFailed},          // can't fail from cached
		{model.ObjectStateStored, model.ObjectStateFailed},          // stored failures stay in task state
		{model.ObjectStateUploading, model.ObjectStateCacheEvicted}, // must go through stored
	}
	for _, tt := range invalid {
		if m.CanTransition(string(tt.from), string(tt.to)) {
			t.Errorf("invalid transition %s→%s: should not be allowed", tt.from, tt.to)
		}
	}
}

func TestObjectStateMachine_SameStateSelfTransition(t *testing.T) {
	m := NewObjectStateMachine()

	allStates := []model.ObjectState{
		model.ObjectStateCached,
		model.ObjectStateUploading,
		model.ObjectStateCommitting,
		model.ObjectStateReplicating,
		model.ObjectStateStored,
		model.ObjectStateFailed,
		model.ObjectStateCacheEvicted,
	}
	for _, s := range allStates {
		if m.CanTransition(string(s), string(s)) {
			t.Errorf("self-transition %s→%s: should not be allowed", s, s)
		}
	}
}

func TestObjectStateMachine_NextStates(t *testing.T) {
	m := NewObjectStateMachine()

	tests := []struct {
		from     model.ObjectState
		expected []string
	}{
		{model.ObjectStateCached, []string{"uploading"}},
		{model.ObjectStateUploading, []string{"committing", "failed"}},
		{model.ObjectStateCommitting, []string{"failed", "replicating"}},
		{model.ObjectStateReplicating, []string{"stored"}},
		{model.ObjectStateStored, []string{"cache_evicted"}},
		{model.ObjectStateFailed, []string{"uploading"}},
		{model.ObjectStateCacheEvicted, nil},
	}
	for _, tt := range tests {
		got := m.NextStates(string(tt.from))
		sort.Strings(got)
		expected := tt.expected
		sort.Strings(expected)

		if len(got) != len(expected) {
			t.Errorf("NextStates(%s) = %v, want %v", tt.from, got, expected)
			continue
		}
		for i := range got {
			if got[i] != expected[i] {
				t.Errorf("NextStates(%s) = %v, want %v", tt.from, got, expected)
				break
			}
		}
	}
}
