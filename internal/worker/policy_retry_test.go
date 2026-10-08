package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestRetryUsesCurrentPolicyWithoutChangingSourceSnapshot(t *testing.T) {
	limit := 5
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: func(context.Context, Execution) Result { return Fail(errors.New("stopped"), "stopped", nil) }}, nil)
	row := enqueueTestTask(t, h, "policy", "policy")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	registry := NewRegistry()
	newLimit := 1
	if err := registry.Register(scriptedHandler{definition: testDefinition(&newLimit, true)}); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(registry, h.repos)
	if err != nil {
		t.Fatal(err)
	}
	next, err := service.Retry(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	original, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	oldPolicy, err := DecodePolicy(original)
	if err != nil {
		t.Fatal(err)
	}
	newPolicy, err := DecodePolicy(next)
	if err != nil {
		t.Fatal(err)
	}
	if oldPolicy.MaxAttempts != 6 || newPolicy.MaxAttempts != 2 {
		t.Fatalf("policies old=%#v new=%#v", oldPolicy, newPolicy)
	}
}

func TestConcurrentManualRetriesReturnOneDirectSuccessor(t *testing.T) {
	assertConcurrentManualRetries(t, testutil.NewTestFileDB(t))
}

func assertConcurrentManualRetries(t *testing.T, db *bun.DB) {
	t.Helper()
	h := newTaskHarnessWithDB(t, db, scriptedHandler{definition: testDefinition(nil, true), execute: func(context.Context, Execution) Result { return Fail(errors.New("stopped"), "stopped", nil) }}, nil)
	row := enqueueTestTask(t, h, "concurrent-retry", "concurrent-retry")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	var wg sync.WaitGroup
	ids := make(chan int64, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			next, err := h.service.Retry(t.Context(), row.ID)
			if err != nil {
				errs <- err
				return
			}
			ids <- next.ID
		})
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var successorID int64
	for id := range ids {
		if successorID != 0 && successorID != id {
			t.Fatal("duplicate successors")
		}
		successorID = id
	}
	if successorID == 0 {
		t.Fatal("no successor")
	}
	claim := claimTestTask(t, h)
	if err := h.repos.Tasks.Settle(t.Context(), claim.ID, claim.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover}); err != nil {
		t.Fatal(err)
	}
	grandchild, err := h.service.Retry(t.Context(), successorID)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := h.service.Retry(t.Context(), row.ID)
	if err != nil || replayed.ID != successorID || replayed.ID == grandchild.ID {
		t.Fatalf("direct successor replay: %#v %v", replayed, err)
	}
}

func TestFinalOpportunityMayAdmitAndCompleteEffect(t *testing.T) {
	limit := 0
	called := false
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: func(ctx context.Context, e Execution) Result {
		attempted, err := e.WithCheckpointedEffect(ctx, ResourceProviderMutation, "one-operation", map[string]bool{"prepared": true}, nil, func(context.Context) error { called = true; return nil })
		if err != nil || !attempted {
			return Fail(errors.New("effect was not admitted"), "admission_failed", nil)
		}
		return Complete("Done", nil)
	}}, nil)
	row := enqueueTestTask(t, h, "last", "last")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	stored, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if !called || stored.Status != model.TaskStatusCompleted || stored.RetryCount != 0 {
		t.Fatalf("last opportunity: %#v", stored)
	}
}

func TestExhaustionSelectsOnlyExhaustedSettlement(t *testing.T) {
	limit := 0
	retried, exhausted := false, false
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: func(context.Context, Execution) Result {
		return RetryBackoff(errors.New("unknown result"), "temporary", nil).WithRetrySettlements(func(context.Context, *repository.Repositories, time.Time) error { retried = true; return nil }, func(context.Context, *repository.Repositories) error { exhausted = true; return nil })
	}}, nil)
	row := enqueueTestTask(t, h, "exhaust", "exhaust")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	stored, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if retried || !exhausted || stored.RetryCount != 0 || dereference(stored.FailureReason) != "attempts_exhausted" {
		t.Fatalf("exhaustion: %#v", stored)
	}
}

func TestMalformedPolicyNeverDispatchesHandler(t *testing.T) {
	for _, field := range []string{"unsupported_version", "invocation_timeout", "observation_window", "backoff"} {
		t.Run(field, func(t *testing.T) {
			called := false
			h := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true), execute: func(context.Context, Execution) Result { called = true; return Complete("Done", nil) }}, nil)
			row := enqueueTestTask(t, h, "corrupt-policy", "corrupt-policy")
			var snapshot map[string]json.RawMessage
			if err := json.Unmarshal(row.Policy, &snapshot); err != nil {
				t.Fatal(err)
			}
			if field == "unsupported_version" {
				snapshot["version"] = json.RawMessage(`999`)
			} else {
				delete(snapshot, field)
			}
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.db.NewUpdate().Model((*model.TaskPayload)(nil)).Set("policy_json = ?", json.RawMessage(raw)).Where("task_id = ?", row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			h.engine.executeClaim(t.Context(), claimTestTask(t, h))
			stored, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
			if called || stored.Status != model.TaskStatusFailed || dereference(stored.FailureReason) != "invalid_policy" {
				t.Fatalf("invalid policy: %#v", stored)
			}
		})
	}
}

func TestInvocationTimeoutAllocatesRecoveryButPreservesObservedCompletion(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiting", true: "completed"}[completed], func(t *testing.T) {
			definition := testDefinition(nil, true)
			definition.Policy.InvocationTimeout = 10 * time.Millisecond
			h := newTaskHarness(t, scriptedHandler{definition: definition, execute: func(ctx context.Context, _ Execution) Result {
				<-ctx.Done()
				if completed {
					return Complete("Done", nil)
				}
				return Wait(model.TaskResumeModeRecover, time.Minute, "observing", "Waiting", nil)
			}}, nil)
			row := enqueueTestTask(t, h, "timeout", "timeout")
			h.engine.executeClaim(t.Context(), claimTestTask(t, h))
			stored, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
			if completed {
				if stored.Status != model.TaskStatusCompleted || stored.RetryCount != 0 {
					t.Fatalf("observed completion: %#v", stored)
				}
			} else if stored.Status != model.TaskStatusPending || stored.RetryCount != 1 || dereference(stored.FailureReason) != "invocation_timeout" {
				t.Fatalf("timeout recovery: %#v", stored)
			}
		})
	}
}
