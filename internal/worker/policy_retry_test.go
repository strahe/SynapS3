package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestObservationWindowUsesPersistedStageAndPolicy(t *testing.T) {
	definition := testDefinition(nil, true)
	definition.Policy.ObservationWindow = 5 * time.Minute
	definition.Policy.ObservationInterval = 30 * time.Second
	registry := NewRegistry()
	if err := registry.Register(scriptedHandler{definition: definition}); err != nil {
		t.Fatal(err)
	}
	engine := &Engine{registry: registry}
	now := time.Date(2026, time.October, 19, 1, 0, 0, 0, time.UTC)
	timeout := errors.New("transaction still has no receipt after the observation window")
	diagnostic := errors.New("provider returned HTTP 503: eid=receipt-unavailable")
	managed := WaitForObservation(model.TaskResumeModeRecover, "confirmation", "Waiting", nil).
		WithObservationWindow("tx:one", timeout, model.TaskResumeModeExecute).WithDiagnostic(diagnostic)
	for _, tc := range []struct {
		name      string
		state     *model.TaskObservation
		result    Result
		wantStart time.Time
		wantNext  time.Time
		wantRetry bool
	}{
		{"first wait", nil, managed, now, now.Add(30 * time.Second), false},
		{"same stage", &model.TaskObservation{Key: "tx:one", StartedAt: now.Add(-time.Minute)}, managed, now.Add(-time.Minute), now.Add(30 * time.Second), false},
		{"deadline bounds poll", &model.TaskObservation{Key: "tx:one", StartedAt: now.Add(-5*time.Minute + time.Second)}, managed, now.Add(-5*time.Minute + time.Second), now.Add(time.Second), false},
		{"deadline expires", &model.TaskObservation{Key: "tx:one", StartedAt: now.Add(-5 * time.Minute)}, managed, time.Time{}, time.Time{}, true},
		{"new stage", &model.TaskObservation{Key: "tx:other", StartedAt: now.Add(-time.Hour)}, managed, now, now.Add(30 * time.Second), false},
		{"legacy wait", &model.TaskObservation{Key: "tx:one", StartedAt: now.Add(-time.Hour)}, WaitForObservation(model.TaskResumeModeRecover, "confirmation", "Waiting", nil), time.Time{}, time.Time{}, false},
		{"completion wins", &model.TaskObservation{Key: "tx:one", StartedAt: now.Add(-time.Hour)}, Complete("Confirmed", nil), time.Time{}, time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(model.TaskRuntime{Observation: tc.state})
			if err != nil {
				t.Fatal(err)
			}
			row := &model.Task{Type: testTaskType, Runtime: raw}
			result, patch, err := engine.observationResult(row, tc.result, now)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantRetry {
				if result.diagnostic != nil || !errors.Is(result.err, diagnostic) {
					t.Fatalf("timeout dropped diagnostic: %#v", result)
				}
				if result.kind != resultRetry || result.resumeMode != model.TaskResumeModeExecute || result.failureReason != "observation_window_expired" || result.waitReason != "" || !errors.Is(result.err, timeout) || patch != nil {
					t.Fatalf("expired result=%#v patch=%#v", result, patch)
				}
			} else if tc.wantStart.IsZero() {
				if patch != nil || result.kind != tc.result.kind {
					t.Fatalf("unmanaged result=%#v patch=%#v", result, patch)
				}
			} else if patch == nil || patch.State == nil || !patch.State.StartedAt.Equal(tc.wantStart) || result.waitUntil == nil || !result.waitUntil.Equal(tc.wantNext) {
				t.Fatalf("observation result=%#v patch=%#v", result, patch)
			}
		})
	}
	for name, result := range map[string]Result{
		"ordinary wait":       Wait(model.TaskResumeModeRecover, "waiting", "Waiting", nil).WithObservationWindow("stage", timeout, model.TaskResumeModeRecover),
		"empty stage":         managed.WithObservationWindow(" ", timeout, model.TaskResumeModeRecover),
		"missing timeout":     managed.WithObservationWindow("stage", nil, model.TaskResumeModeRecover),
		"invalid mode":        managed.WithObservationWindow("stage", timeout, "invalid"),
		"terminal result":     Complete("Done", nil).WithObservationWindow("stage", timeout, model.TaskResumeModeRecover),
		"complete diagnostic": Complete("Done", nil).WithDiagnostic(diagnostic),
		"retry diagnostic":    Retry(timeout, "timeout", nil).WithDiagnostic(diagnostic),
	} {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(validateResult(result, nil), ErrInvalidResult) {
				t.Fatal("invalid observation contract accepted")
			}
		})
	}
	absolute := managed
	absolute.waitUntil = &now
	if !errors.Is(validateResult(absolute, nil), ErrInvalidResult) {
		t.Fatal("absolute and managed deadlines accepted together")
	}
}

func TestObservationWindowSettlementAndRetryAreAtomic(t *testing.T) {
	limit := 1
	definition := testDefinition(&limit, true)
	definition.Policy.ObservationWindow = 5 * time.Minute
	definition.Policy.ObservationInterval = 30 * time.Second
	currentResult := WaitForObservation(model.TaskResumeModeRecover, "confirmation", "Waiting", nil).
		WithObservationWindow("tx:one", errors.New("receipt was not available within the observation window"), model.TaskResumeModeExecute).
		WithDiagnostic(repository.ErrRepositoryContended)
	run := func(context.Context, Execution) Result { return currentResult }
	h := newTaskHarness(t, scriptedHandler{definition: definition, execute: run, recover: run}, nil)
	row := enqueueTestTask(t, h, "observation")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	load := func() (*model.Task, model.TaskRuntime) {
		t.Helper()
		stored, err := h.repos.Tasks.GetByID(t.Context(), row.ID)
		if err != nil {
			t.Fatal(err)
		}
		var runtime model.TaskRuntime
		if err := json.Unmarshal(stored.Runtime, &runtime); err != nil {
			t.Fatal(err)
		}
		return stored, runtime
	}
	stored, first := load()
	if first.Observation == nil || stored.RetryCount != 0 || dereference(stored.LastError) != repository.ErrRepositoryContended.Error() || dereference(stored.WaitReason) != "confirmation" {
		t.Fatalf("first wait=%#v runtime=%#v", stored, first)
	}
	// Rebuilding the engine exercises the durable clock rather than process memory.
	h = newTaskHarnessWithDB(t, h.db, scriptedHandler{definition: definition, execute: run, recover: run}, nil)
	claim := func() *model.Task {
		t.Helper()
		if _, err := h.repos.Tasks.WakePending(t.Context(), []int64{row.ID}); err != nil {
			t.Fatal(err)
		}
		return claimTestTask(t, h)
	}
	h.engine.executeClaim(t.Context(), claim())
	stored, repeated := load()
	if stored.RetryCount != 0 || !repeated.Observation.StartedAt.Equal(first.Observation.StartedAt) {
		t.Fatalf("repeated observation=%#v runtime=%#v", stored, repeated)
	}
	currentResult = Wait(model.TaskResumeModeRecover, "dependency", "Waiting", nil)
	h.engine.executeClaim(t.Context(), claim())
	stored, ordinary := load()
	if stored.LastError != nil || !ordinary.Observation.StartedAt.Equal(first.Observation.StartedAt) {
		t.Fatal("ordinary wait reset observation")
	}
	currentResult = WaitForObservation(model.TaskResumeModeRecover, "confirmation", "Waiting", nil).
		WithObservationWindow("tx:two", errors.New("transaction remains unconfirmed after five minutes"), model.TaskResumeModeExecute)
	h.engine.executeClaim(t.Context(), claim())
	_, nextStage := load()
	if nextStage.Observation.Key != "tx:two" || !nextStage.Observation.StartedAt.After(first.Observation.StartedAt) {
		t.Fatalf("new stage=%#v", nextStage)
	}
	claimed := claim()
	checkpoint := json.RawMessage(`{"transaction_hash":"one"}`)
	if err := h.repos.Tasks.WriteCheckpoint(t.Context(), row.ID, claimed.ClaimGeneration, checkpoint); err != nil {
		t.Fatal(err)
	}
	setExpired := func() {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"observation": model.TaskObservation{Key: "tx:two", StartedAt: time.Now().Add(-6 * time.Minute)}, "future": map[string]int{"value": 7}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.db.NewUpdate().Model((*model.Task)(nil)).Set("runtime_json = runtime_json || ?::jsonb", json.RawMessage(raw)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	setExpired()
	currentResult = currentResult.WithWorkStartedAt(time.Now().Add(-time.Minute)).WithRetrySettlements(
		func(ctx context.Context, repos *repository.Repositories, _ time.Time) error {
			if _, err := repos.Tasks.ObserveOperation(ctx, row.ID, claimed.ClaimGeneration, "send:one"); err != nil {
				return err
			}
			return repos.Tasks.AppendEvent(ctx, row.ID, "observation_retry", nil)
		},
		func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Tasks.AppendEvent(ctx, row.ID, "observation_exhausted", nil)
		},
	)
	currentResult.settlement = func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Tasks.AppendEvent(ctx, row.ID, "observation_common", nil)
	}
	before, _ := load()
	restore := testutil.FailWrites(t, h.db, "task_events", "INSERT", "NEW.type = 'retry_scheduled'", "injected observation event failure")
	h.engine.settlementRetryDelays = []time.Duration{0, 0}
	if err := h.engine.commitResult(t.Context(), claimed, currentResult); err == nil {
		t.Fatal("event failure committed observation timeout")
	}
	rolledBack, _ := load()
	if rolledBack.RetryCount != 0 || rolledBack.Status != model.TaskStatusRunning || !bytes.Equal(before.Runtime, rolledBack.Runtime) || rolledBack.WorkStartedAt != nil {
		t.Fatalf("partial timeout commit=%#v", rolledBack)
	}
	restore()
	if err := h.engine.commitResult(t.Context(), claimed, currentResult); err != nil {
		t.Fatal(err)
	}
	stored, retried := load()
	if stored.RetryCount != 1 || stored.ResumeMode != model.TaskResumeModeExecute || retried.Observation != nil || retried.OperationKey != "send:one" || stored.WorkStartedAt == nil {
		t.Fatalf("retry settlement=%#v runtime=%#v", stored, retried)
	}
	if !bytes.Contains(stored.Runtime, []byte(`"future"`)) {
		t.Fatal("observation settlement dropped unknown runtime fields")
	}
	currentResult.onRetry = nil
	claimed = claim()
	h.engine.executeClaim(t.Context(), claimed)
	stored, fresh := load()
	if fresh.Observation == nil || stored.RetryCount != 1 || !fresh.Observation.StartedAt.After(first.Observation.StartedAt) {
		t.Fatal("new attempt did not start a fresh observation window")
	}
	claimed = claim()
	setExpired()
	if err := h.engine.commitResult(t.Context(), claimed, currentResult); err != nil {
		t.Fatal(err)
	}
	stored, exhausted := load()
	if stored.Status != model.TaskStatusFailed || stored.RetryCount != 1 || dereference(stored.FailureReason) != "attempts_exhausted" || exhausted.Observation != nil || exhausted.OperationKey != "send:one" {
		t.Fatalf("exhausted=%#v runtime=%#v", stored, exhausted)
	}
	events, err := h.repos.Tasks.ListEvents(t.Context(), row.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["retry_scheduled"] != 1 || counts["observation_retry"] != 1 || counts["observation_exhausted"] != 1 || counts["observation_common"] != 3 {
		t.Fatalf("committed events=%v", counts)
	}
	next, err := h.service.Retry(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if next.RetryCount != 0 || string(next.Runtime) != "{}" || !testutil.JSONEqual(next.Checkpoint, checkpoint) {
		t.Fatalf("manual successor=%#v", next)
	}
}

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
			for _, withEvidence := range []bool{false, true} {
				t.Run(fmt.Sprintf("evidence=%t", withEvidence), func(t *testing.T) {
					limit := 0
					settled := false
					retryHook := false
					h := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: func(_ context.Context, execution Execution) Result {
						result := Retry(cause, "temporary", func(context.Context, *repository.Repositories) error {
							settled = true
							return nil
						}).WithRetrySettlements(func(context.Context, *repository.Repositories, time.Time) error {
							retryHook = true
							return nil
						}, func(context.Context, *repository.Repositories) error {
							retryHook = true
							return nil
						})
						if withEvidence {
							result = result.WithContentionSettlement(func(ctx context.Context, repos *repository.Repositories) error {
								return repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), json.RawMessage(`{"tx_hash":"0xsubmitted"}`))
							})
						}
						return result
					}}, nil)
					row := enqueueTestTask(t, h, "contention")
					before := time.Now()
					h.engine.executeClaim(t.Context(), claimTestTask(t, h))
					stored, err := h.repos.Tasks.GetByID(t.Context(), row.ID)
					if err != nil {
						t.Fatal(err)
					}
					if settled || retryHook || stored.Status != model.TaskStatusPending || stored.ResumeMode != model.TaskResumeModeRecover || stored.RetryCount != 0 || dereference(stored.WaitReason) != "database_contention" {
						t.Fatalf("contention consumed or settled a business opportunity: %#v", stored)
					}
					if withEvidence {
						var checkpoint struct {
							TxHash string `json:"tx_hash"`
						}
						if err := json.Unmarshal(stored.Checkpoint, &checkpoint); err != nil {
							t.Fatal(err)
						}
						if checkpoint.TxHash != "0xsubmitted" {
							t.Fatalf("submitted hash = %q, want 0xsubmitted", checkpoint.TxHash)
						}
					}
					if dereference(stored.LastError) != cause.Error() {
						t.Fatalf("contention diagnostic = %q, want %q", dereference(stored.LastError), cause.Error())
					}
					if stored.AvailableAt.Before(before.Add(h.engine.config.PollInterval)) || stored.AvailableAt.After(time.Now().Add(h.engine.config.PollInterval)) {
						t.Fatalf("contention wait did not use the poll interval: %s", stored.AvailableAt)
					}
				})
			}
		})
	}
}

func TestInvocationTimeoutAllocatesRecoveryButPreservesObservedCompletion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		completed bool
		contended bool
	}{
		{name: "waiting"},
		{name: "completed", completed: true},
		{name: "contended evidence", contended: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)}, nil)
			row := enqueueTestTask(t, old, "timeout")
			definition := testDefinition(nil, true)
			definition.Policy.InvocationTimeout = 10 * time.Millisecond
			h := newTaskHarnessWithDB(t, old.db, scriptedHandler{definition: definition, execute: func(ctx context.Context, execution Execution) Result {
				<-ctx.Done()
				if tc.completed {
					return Complete("Done", nil)
				}
				cause := errors.New("provider response eid=timeout")
				if tc.contended {
					cause = errors.Join(cause, repository.ErrRepositoryContended)
				}
				return Wait(model.TaskResumeModeRecover, "observing", "Waiting", nil).WithDiagnostic(cause).
					WithContentionSettlement(func(ctx context.Context, repos *repository.Repositories) error {
						return repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), json.RawMessage(`{"tx_hash":"0xsubmitted"}`))
					})
			}}, nil)
			h.engine.executeClaim(t.Context(), claimTestTask(t, h))
			stored, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
			if tc.completed {
				if stored.Status != model.TaskStatusCompleted || stored.RetryCount != 0 {
					t.Fatalf("observed completion: %#v", stored)
				}
			} else if tc.contended {
				if stored.Status != model.TaskStatusPending || stored.RetryCount != 0 || dereference(stored.WaitReason) != "database_contention" || !bytes.Contains(stored.Checkpoint, []byte("0xsubmitted")) {
					t.Fatalf("timeout discarded contended evidence: %#v", stored)
				}
			} else if stored.Status != model.TaskStatusPending || stored.RetryCount != 1 || dereference(stored.FailureReason) != "invocation_timeout" {
				t.Fatalf("timeout recovery: %#v", stored)
			}
			if !tc.completed && !strings.Contains(dereference(stored.LastError), "eid=timeout") {
				t.Fatalf("timeout lost provider diagnostic: %#v", stored)
			}
		})
	}
}
