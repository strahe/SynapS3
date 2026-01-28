package state

import "github.com/strahe/synaps3/internal/model"

// NewObjectStateMachine returns a state machine pre-configured with
// the standard object lifecycle transitions.
func NewObjectStateMachine() *Machine {
	m := New()
	m.Register(
		// Happy path
		Transition{From: string(model.ObjectStateCached), To: string(model.ObjectStateUploading)},
		Transition{From: string(model.ObjectStateUploading), To: string(model.ObjectStateUploaded)},
		Transition{From: string(model.ObjectStateUploaded), To: string(model.ObjectStateOnChaining)},
		Transition{From: string(model.ObjectStateOnChaining), To: string(model.ObjectStateOnChained)},
		Transition{From: string(model.ObjectStateOnChained), To: string(model.ObjectStateCacheEvicted)},

		// Failure transitions
		Transition{From: string(model.ObjectStateUploading), To: string(model.ObjectStateFailed)},
		Transition{From: string(model.ObjectStateUploaded), To: string(model.ObjectStateFailed)},
		Transition{From: string(model.ObjectStateOnChaining), To: string(model.ObjectStateFailed)},

		// Retry from failure: back to the step that failed
		Transition{From: string(model.ObjectStateFailed), To: string(model.ObjectStateUploading)},
		Transition{From: string(model.ObjectStateFailed), To: string(model.ObjectStateOnChaining)},

		// Cache eviction for uploaded objects (even if not yet on-chain)
		Transition{From: string(model.ObjectStateUploaded), To: string(model.ObjectStateCacheEvicted)},
		Transition{From: string(model.ObjectStateOnChaining), To: string(model.ObjectStateCacheEvicted)},
	)
	return m
}
