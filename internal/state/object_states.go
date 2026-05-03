package state

import "github.com/strahe/synaps3/internal/model"

// NewObjectStateMachine returns a state machine pre-configured with
// the standard object lifecycle transitions.
func NewObjectStateMachine() *Machine {
	m := New()
	m.Register(
		// Happy path
		Transition{From: string(model.ObjectStateCached), To: string(model.ObjectStateUploading)},
		Transition{From: string(model.ObjectStateUploading), To: string(model.ObjectStateCommitting)},
		Transition{From: string(model.ObjectStateCommitting), To: string(model.ObjectStateReplicating)},
		Transition{From: string(model.ObjectStateReplicating), To: string(model.ObjectStateStored)},
		Transition{From: string(model.ObjectStateStored), To: string(model.ObjectStateCacheEvicted)},

		// Failure
		Transition{From: string(model.ObjectStateUploading), To: string(model.ObjectStateFailed)},
		Transition{From: string(model.ObjectStateCommitting), To: string(model.ObjectStateFailed)},

		// Retry
		Transition{From: string(model.ObjectStateFailed), To: string(model.ObjectStateUploading)},
	)
	return m
}
