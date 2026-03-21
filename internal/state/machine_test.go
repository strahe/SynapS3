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
		{model.ObjectStateUploading, model.ObjectStateUploaded},
		{model.ObjectStateUploaded, model.ObjectStateOnChaining},
		{model.ObjectStateOnChaining, model.ObjectStateOnChained},
		{model.ObjectStateOnChained, model.ObjectStateCacheEvicted},
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
		model.ObjectStateUploaded,
		model.ObjectStateOnChaining,
	}
	for _, from := range canFail {
		if err := m.Validate(string(from), string(model.ObjectStateFailed)); err != nil {
			t.Errorf("%s→failed: unexpected error: %v", from, err)
		}
	}

	// States that cannot transition to failed.
	cannotFail := []model.ObjectState{
		model.ObjectStateCached,
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
		model.ObjectStateOnChaining,
	}
	for _, to := range allowed {
		if err := m.Validate(string(model.ObjectStateFailed), string(to)); err != nil {
			t.Errorf("failed→%s: unexpected error: %v", to, err)
		}
	}

	// Disallowed retries.
	disallowed := []model.ObjectState{
		model.ObjectStateCached,
		model.ObjectStateUploaded,
		model.ObjectStateOnChained,
		model.ObjectStateCacheEvicted,
	}
	for _, to := range disallowed {
		if m.CanTransition(string(model.ObjectStateFailed), string(to)) {
			t.Errorf("failed→%s: should not be allowed", to)
		}
	}
}

func TestObjectStateMachine_EvictionShortcuts(t *testing.T) {
	m := NewObjectStateMachine()

	// Early eviction from uploaded or onchaining.
	earlyEvict := []model.ObjectState{
		model.ObjectStateUploaded,
		model.ObjectStateOnChaining,
	}
	for _, from := range earlyEvict {
		if err := m.Validate(string(from), string(model.ObjectStateCacheEvicted)); err != nil {
			t.Errorf("%s→cache_evicted: unexpected error: %v", from, err)
		}
	}
}

func TestObjectStateMachine_InvalidTransitions(t *testing.T) {
	m := NewObjectStateMachine()

	invalid := []struct{ from, to model.ObjectState }{
		{model.ObjectStateCached, model.ObjectStateUploaded},      // skip uploading
		{model.ObjectStateCached, model.ObjectStateOnChained},     // skip everything
		{model.ObjectStateCacheEvicted, model.ObjectStateCached},  // can't revive
		{model.ObjectStateOnChained, model.ObjectStateUploading},  // backwards
		{model.ObjectStateCached, model.ObjectStateFailed},        // can't fail from cached
		{model.ObjectStateCached, model.ObjectStateCacheEvicted},  // can't evict uncached
		{model.ObjectStateUploading, model.ObjectStateOnChaining}, // skip uploaded
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
		model.ObjectStateUploaded,
		model.ObjectStateOnChaining,
		model.ObjectStateOnChained,
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
		{model.ObjectStateUploading, []string{"failed", "uploaded"}},
		{model.ObjectStateUploaded, []string{"cache_evicted", "failed", "onchaining"}},
		{model.ObjectStateOnChaining, []string{"cache_evicted", "failed", "onchained"}},
		{model.ObjectStateOnChained, []string{"cache_evicted", "failed"}},
		{model.ObjectStateFailed, []string{"onchaining", "uploading"}},
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
