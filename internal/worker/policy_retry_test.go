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

func TestRetryUsesCurrentPolicy(t *testing.T) {
	old := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true), execute: func(context.Context, Execution) Result {
		return Fail(errors.New("stopped"), "stopped", nil)
	}}, nil)
	row := enqueueTestTask(t, old, "policy")
	old.engine.executeClaim(t.Context(), claimTestTask(t, old))
	limit := 1
	definition := testDefinition(&limit, true)
	definition.Policy.ObservationWindow = time.Minute
	definition.Policy.ObservationInterval = 30 * time.Second
	current := newTaskHarnessWithDB(t, old.db, scriptedHandler{definition: definition, recover: func(_ context.Context, execution Execution) Result {
		if execution.Policy() != definition.Policy {
			t.Fatalf("execution policy = %#v, want %#v", execution.Policy(), definition.Policy)
		}
		return Retry(errors.New("temporary"), "temporary", nil)
	}}, nil)
	if policy, ok := current.service.Policy(testTaskType); !ok || policy != definition.Policy {
		t.Fatalf("service policy = %#v, available=%v", policy, ok)
	}
	if _, ok := current.service.Policy("unregistered"); ok {
		t.Fatal("unregistered task has an execution policy")
	}
	next, err := current.service.Retry(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	current.engine.executeClaim(t.Context(), claimTestTask(t, current))
	scheduled, err := current.repos.Tasks.GetByID(t.Context(), next.ID)
	if err != nil || scheduled.Status != model.TaskStatusPending || scheduled.RetryCount != 1 {
		t.Fatalf("retry under current policy = %#v, err=%v", scheduled, err)
	}
	current.engine.executeClaim(t.Context(), claimTestTask(t, current))
	exhausted, err := current.repos.Tasks.GetByID(t.Context(), next.ID)
	if err != nil || exhausted.Status != model.TaskStatusFailed || dereference(exhausted.FailureReason) != "attempts_exhausted" || exhausted.RetryCount != 1 {
		t.Fatalf("current budget exhaustion = %#v, err=%v", exhausted, err)
	}
}

func TestConcurrentManualRetriesReturnOneDirectSuccessor(t *testing.T) {
	assertConcurrentManualRetries(t, testutil.NewTestDB(t))
}

func assertConcurrentManualRetries(t *testing.T, db *bun.DB) {
	t.Helper()
	h := newTaskHarnessWithDB(t, db, scriptedHandler{definition: testDefinition(nil, true), execute: func(context.Context, Execution) Result { return Fail(errors.New("stopped"), "stopped", nil) }}, nil)
	row := enqueueTestTask(t, h, "concurrent-retry")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	if err := h.service.Acknowledge(t.Context(), row.ID); err != nil {
		t.Fatal(err)
	}
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
	if err := h.service.Acknowledge(t.Context(), claim.ID); err != nil {
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
		attempted, err := e.WithCheckpointedEffect(ctx, "one-operation", map[string]bool{"prepared": true}, nil, func(context.Context) error { called = true; return nil })
		if err != nil || !attempted {
			return Fail(errors.New("effect was not admitted"), "admission_failed", nil)
		}
		return Complete("Done", nil)
	}}, nil)
	row := enqueueTestTask(t, h, "last")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	stored, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if !called || stored.Status != model.TaskStatusCompleted || stored.RetryCount != 0 {
		t.Fatalf("last opportunity: %#v", stored)
	}
}

func TestCurrentPolicyPreservesScheduledOpportunity(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		oldMax, scheduled, currentMax int
		mode                          model.TaskResumeMode
		outcome                       string
		wantStatus                    model.TaskStatus
		wantRetries                   int
		wantFailure                   string
	}{
		{"final retry", 1, 0, 1, model.TaskResumeModeExecute, "retry", model.TaskStatusFailed, 0, "attempts_exhausted"},
		{"decreased complete", 6, 3, 2, model.TaskResumeModeExecute, "complete", model.TaskStatusCompleted, 3, ""},
		{"decreased wait", 6, 3, 2, model.TaskResumeModeRecover, "wait", model.TaskStatusPending, 3, ""},
		{"decreased retry", 6, 3, 2, model.TaskResumeModeRecover, "retry", model.TaskStatusFailed, 3, "attempts_exhausted"},
		{"increased retry", 2, 1, 4, model.TaskResumeModeRecover, "retry", model.TaskStatusPending, 2, "temporary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldLimit := tc.oldMax - 1
			schedule := func(context.Context, Execution) Result {
				return Retry(errors.New("temporary"), "temporary", nil).WithResumeMode(tc.mode)
			}
			old := newTaskHarness(t, scriptedHandler{definition: testDefinition(&oldLimit, true), execute: schedule, recover: schedule}, nil)
			row := enqueueTestTask(t, old, "scheduled")
			for range tc.scheduled {
				old.engine.executeClaim(t.Context(), claimTestTask(t, old))
			}
			limit := tc.currentMax - 1
			definition := testDefinition(&limit, true)
			definition.Policy.ObservationInterval = 30 * time.Second
			called, retried, exhausted := false, false, false
			run := func(_ context.Context, execution Execution) Result {
				called = true
				if execution.Policy() != definition.Policy || execution.Mode() != tc.mode {
					t.Fatalf("scheduled invocation = policy:%#v mode:%s", execution.Policy(), execution.Mode())
				}
				switch tc.outcome {
				case "complete":
					return Complete("Done", nil)
				case "wait":
					return Wait(tc.mode, "observing", "Waiting", nil)
				default:
					return Retry(errors.New("temporary"), "temporary", nil).WithRetrySettlements(
						func(context.Context, *repository.Repositories, time.Time) error { retried = true; return nil },
						func(ctx context.Context, repos *repository.Repositories) error {
							exhausted = true
							return repos.Tasks.AppendEvent(ctx, execution.ID(), "exhaustion_settled", nil)
						},
					)
				}
			}
			current := newTaskHarnessWithDB(t, old.db, scriptedHandler{definition: definition, execute: run, recover: run}, nil)
			current.engine.executeClaim(t.Context(), claimTestTask(t, current))
			stored, err := current.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil || !called || stored.Status != tc.wantStatus || stored.RetryCount != tc.wantRetries || dereference(stored.FailureReason) != tc.wantFailure {
				t.Fatalf("scheduled opportunity = %#v called=%v err=%v", stored, called, err)
			}
			wantExhausted := tc.wantFailure == "attempts_exhausted"
			wantRetried := tc.outcome == "retry" && !wantExhausted
			if exhausted != wantExhausted || retried != wantRetried {
				t.Fatalf("settlements retry=%v exhausted=%v", retried, exhausted)
			}
			events, err := current.repos.Tasks.ListEvents(t.Context(), row.ID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			settled := false
			for _, event := range events {
				settled = settled || event.Type == "exhaustion_settled"
				if event.Type == "retry_scheduled" || event.Type == "failed" {
					var details struct {
						Error string `json:"error"`
					}
					if err := json.Unmarshal(event.Details, &details); err != nil {
						t.Fatal(err)
					}
					if details.Error != "temporary" {
						t.Fatalf("%s event error = %q, want temporary", event.Type, details.Error)
					}
				}
			}
			if settled != wantExhausted {
				t.Fatalf("exhaustion evidence = %v, want %v", settled, wantExhausted)
			}
		})
	}
}

func TestTaskContentionPreservesFinalOpportunityAndRecovery(t *testing.T) {
	for _, cause := range []error{repository.ErrTaskIdentityContended, repository.ErrRepositoryContended} {
		t.Run(cause.Error(), func(t *testing.T) {
			limit := 0
			settled := false
			h := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: func(context.Context, Execution) Result {
				return Retry(cause, "temporary", func(context.Context, *repository.Repositories) error {
					settled = true
					return nil
				})
			}}, nil)
			row := enqueueTestTask(t, h, "contention")
			before := time.Now()
			h.engine.executeClaim(t.Context(), claimTestTask(t, h))
			stored, err := h.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if settled || stored.Status != model.TaskStatusPending || stored.ResumeMode != model.TaskResumeModeRecover || stored.RetryCount != 0 || dereference(stored.WaitReason) != "database_contention" {
				t.Fatalf("contention consumed or settled a business opportunity: %#v", stored)
			}
			if stored.AvailableAt.Before(before.Add(h.engine.config.PollInterval)) || stored.AvailableAt.After(time.Now().Add(h.engine.config.PollInterval)) {
				t.Fatalf("contention wait did not use the poll interval: %s", stored.AvailableAt)
			}
		})
	}
}

func TestInvocationTimeoutAllocatesRecoveryButPreservesObservedCompletion(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiting", true: "completed"}[completed], func(t *testing.T) {
			old := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)}, nil)
			row := enqueueTestTask(t, old, "timeout")
			definition := testDefinition(nil, true)
			definition.Policy.InvocationTimeout = 10 * time.Millisecond
			h := newTaskHarnessWithDB(t, old.db, scriptedHandler{definition: definition, execute: func(ctx context.Context, _ Execution) Result {
				<-ctx.Done()
				if completed {
					return Complete("Done", nil)
				}
				return Wait(model.TaskResumeModeRecover, "observing", "Waiting", nil)
			}}, nil)
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
