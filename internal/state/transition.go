package state

import (
	"context"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
)

// StateUpdater abstracts the DB compare-and-set for state transitions.
type StateUpdater interface {
	UpdateState(ctx context.Context, id int64, generation int64, from, to model.ObjectState) error
	UpdateStateToFailed(ctx context.Context, id int64, generation int64, from model.ObjectState, lastError string) error
}

// TransitionState validates a state change via the FSM, then atomically
// updates the database. This is the single entry point for all object
// state changes, providing dual protection:
//  1. FSM validation (catches programming errors early)
//  2. DB compare-and-set (prevents races at the data level)
//
// When transitioning to ObjectStateFailed, use TransitionToFailed instead
// to record the phase that failed and the error message.
func TransitionState(ctx context.Context, m *Machine, u StateUpdater, id int64, generation int64, from, to model.ObjectState) error {
	if to == model.ObjectStateFailed {
		return fmt.Errorf("use TransitionToFailed for failure transitions to record FailedAtState")
	}
	if err := m.Validate(string(from), string(to)); err != nil {
		return fmt.Errorf("state transition rejected: %w", err)
	}
	return u.UpdateState(ctx, id, generation, from, to)
}

// TransitionToFailed validates the transition to ObjectStateFailed via the
// FSM, then atomically updates the database, recording the source state
// (FailedAtState) and the error message. Workers use FailedAtState to
// determine the correct retry target.
func TransitionToFailed(ctx context.Context, m *Machine, u StateUpdater, id int64, generation int64, from model.ObjectState, lastError string) error {
	if err := m.Validate(string(from), string(model.ObjectStateFailed)); err != nil {
		return fmt.Errorf("state transition rejected: %w", err)
	}
	return u.UpdateStateToFailed(ctx, id, generation, from, lastError)
}
