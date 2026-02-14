package state

import (
	"context"
	"errors"
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

// mockStateUpdater implements StateUpdater for testing.
type mockStateUpdater struct {
	updateStateCalled     bool
	updateFailedCalled    bool
	lastID                int64
	lastFrom, lastTo      model.ObjectState
	lastError             string
	returnErr             error
}

func (m *mockStateUpdater) UpdateState(_ context.Context, id int64, from, to model.ObjectState) error {
	m.updateStateCalled = true
	m.lastID = id
	m.lastFrom = from
	m.lastTo = to
	return m.returnErr
}

func (m *mockStateUpdater) UpdateStateToFailed(_ context.Context, id int64, from model.ObjectState, lastError string) error {
	m.updateFailedCalled = true
	m.lastID = id
	m.lastFrom = from
	m.lastError = lastError
	return m.returnErr
}

func TestTransitionState_ValidTransition(t *testing.T) {
	m := NewObjectStateMachine()
	u := &mockStateUpdater{}
	ctx := context.Background()

	err := TransitionState(ctx, m, u, 42, model.ObjectStateCached, model.ObjectStateUploading)
	if err != nil {
		t.Fatalf("TransitionState: %v", err)
	}
	if !u.updateStateCalled {
		t.Error("UpdateState was not called")
	}
	if u.lastID != 42 || u.lastFrom != model.ObjectStateCached || u.lastTo != model.ObjectStateUploading {
		t.Errorf("wrong args: id=%d from=%s to=%s", u.lastID, u.lastFrom, u.lastTo)
	}
}

func TestTransitionState_InvalidTransition(t *testing.T) {
	m := NewObjectStateMachine()
	u := &mockStateUpdater{}
	ctx := context.Background()

	err := TransitionState(ctx, m, u, 1, model.ObjectStateCached, model.ObjectStateUploaded)
	if err == nil {
		t.Fatal("TransitionState should have failed for invalid transition")
	}
	if u.updateStateCalled {
		t.Error("UpdateState should not be called for invalid transition")
	}
}

func TestTransitionState_DBError(t *testing.T) {
	m := NewObjectStateMachine()
	dbErr := errors.New("db failure")
	u := &mockStateUpdater{returnErr: dbErr}
	ctx := context.Background()

	err := TransitionState(ctx, m, u, 1, model.ObjectStateCached, model.ObjectStateUploading)
	if !errors.Is(err, dbErr) {
		t.Errorf("expected db error, got: %v", err)
	}
}

func TestTransitionState_RejectsFailedTarget(t *testing.T) {
	m := NewObjectStateMachine()
	u := &mockStateUpdater{}
	ctx := context.Background()

	err := TransitionState(ctx, m, u, 1, model.ObjectStateUploading, model.ObjectStateFailed)
	if err == nil {
		t.Fatal("TransitionState should reject →failed; use TransitionToFailed instead")
	}
	if u.updateStateCalled {
		t.Error("UpdateState should not be called for →failed transition")
	}
}

func TestTransitionToFailed_Valid(t *testing.T) {
	m := NewObjectStateMachine()
	u := &mockStateUpdater{}
	ctx := context.Background()

	err := TransitionToFailed(ctx, m, u, 99, model.ObjectStateUploading, "upload timeout")
	if err != nil {
		t.Fatalf("TransitionToFailed: %v", err)
	}
	if !u.updateFailedCalled {
		t.Error("UpdateStateToFailed was not called")
	}
	if u.lastFrom != model.ObjectStateUploading {
		t.Errorf("lastFrom = %s, want uploading", u.lastFrom)
	}
	if u.lastError != "upload timeout" {
		t.Errorf("lastError = %q, want %q", u.lastError, "upload timeout")
	}
}

func TestTransitionToFailed_InvalidSource(t *testing.T) {
	m := NewObjectStateMachine()
	u := &mockStateUpdater{}
	ctx := context.Background()

	err := TransitionToFailed(ctx, m, u, 1, model.ObjectStateCached, "should not work")
	if err == nil {
		t.Fatal("TransitionToFailed should reject cached→failed")
	}
	if u.updateFailedCalled {
		t.Error("UpdateStateToFailed should not be called for invalid transition")
	}
}
