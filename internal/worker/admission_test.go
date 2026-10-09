package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestRegistryValidatesMaxConcurrency(t *testing.T) {
	for _, limit := range []int{-1, 0, 1} {
		definition := testDefinition(nil, false)
		definition.MaxConcurrency = limit
		if err := NewRegistry().Register(scriptedHandler{definition: definition}); (err != nil) != (limit < 0) {
			t.Fatalf("register concurrency %d: %v", limit, err)
		}
	}
}

func startAdmissionEngine(t *testing.T, harness taskHarness) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- harness.engine.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("engine stopped: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("engine did not stop")
		}
	})
	return cancel
}

func enqueueAdmissionTask(t *testing.T, harness taskHarness, taskType model.TaskType, key string) *model.Task {
	t.Helper()
	row, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: taskType, IdempotencyKey: key, Input: testInput{Value: key},
	})
	if err != nil || !created {
		t.Fatalf("enqueue %s: created=%v err=%v", key, created, err)
	}
	return row
}

func receiveAdmission(t *testing.T, entered <-chan Execution) Execution {
	t.Helper()
	select {
	case execution := <-entered:
		return execution
	case <-time.After(3 * time.Second):
		t.Fatal("task did not start")
		return Execution{}
	}
}

func waitAdmissionTask(t *testing.T, harness taskHarness, id int64, predicate func(*model.Task) bool) *model.Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		stored, err := harness.repos.Tasks.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("load task %d: %v", id, err)
		}
		if predicate(stored) {
			return stored
		}
		select {
		case <-ctx.Done():
			t.Fatalf("task %d did not reach expected state: %#v", id, stored)
		case <-ticker.C:
		}
	}
}

func TestEngineTypeConcurrencyPreservesQueueAndFairness(t *testing.T) {
	definition := testDefinition(nil, true)
	definition.MaxConcurrency = 1
	other := testDefinition(nil, true)
	other.Type = "other_operation"
	entered := make(chan Execution, 8)
	advance := make(chan struct{})
	run := func(ctx context.Context, execution Execution) Result {
		entered <- execution
		select {
		case <-advance:
		case <-ctx.Done():
		}
		return Complete("Done", nil)
	}
	h := newTaskHarness(t, scriptedHandler{definition: definition, execute: run, recover: run},
		&EngineConfig{Concurrency: 2, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second},
		scriptedHandler{definition: other})
	holder := enqueueTestTask(t, h, "holder", "holder")
	old := enqueueTestTask(t, h, "old", "old")
	if _, err := h.db.ExecContext(t.Context(), `UPDATE tasks SET resume_mode = 'recover' WHERE id = ?`, holder.ID); err != nil {
		t.Fatal(err)
	}
	startAdmissionEngine(t, h)
	first := receiveAdmission(t, entered)
	if first.ID() != holder.ID || first.Mode() != model.TaskResumeModeRecover {
		t.Fatalf("first invocation = id:%d mode:%s", first.ID(), first.Mode())
	}
	for i := range 4 {
		enqueueTestTask(t, h, fmt.Sprintf("new-%d", i), fmt.Sprintf("new-%d", i))
		plain := enqueueAdmissionTask(t, h, other.Type, fmt.Sprintf("plain-%d", i))
		waitAdmissionTask(t, h, plain.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
		waiting, err := h.repos.Tasks.GetByID(t.Context(), old.ID)
		if err != nil || waiting.Status != model.TaskStatusPending || waiting.ClaimGeneration != 0 ||
			!waiting.AvailableAt.Equal(old.AvailableAt) || waiting.WaitReason != nil || waiting.RetryCount != 0 {
			t.Fatalf("type limit changed the queued task: %#v err=%v", waiting, err)
		}
	}
	advance <- struct{}{}
	next := receiveAdmission(t, entered)
	if next.ID() != old.ID || next.Mode() != model.TaskResumeModeExecute {
		t.Fatalf("new work overtook old task: id:%d mode:%s, want %d", next.ID(), next.Mode(), old.ID)
	}
	advance <- struct{}{}
	waitAdmissionTask(t, h, old.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
}

type observedClaimRepository struct {
	repository.TaskRepository
	filters chan repository.TaskClaimFilter
}

func (r observedClaimRepository) ClaimNext(ctx context.Context, lease time.Duration, filter repository.TaskClaimFilter) (*model.Task, error) {
	select {
	case r.filters <- filter:
	default:
	}
	return r.TaskRepository.ClaimNext(ctx, lease, filter)
}

func TestEngineGlobalConcurrencyIncludesUnrestrictedTypes(t *testing.T) {
	definition := testDefinition(nil, true)
	other := testDefinition(nil, true)
	other.Type = "other_operation"
	entered := make(chan Execution, 3)
	advance := make(chan struct{})
	run := func(ctx context.Context, execution Execution) Result {
		entered <- execution
		select {
		case <-advance:
		case <-ctx.Done():
		}
		return Complete("Done", nil)
	}
	h := newTaskHarness(t, scriptedHandler{definition: definition, execute: run},
		&EngineConfig{Concurrency: 2, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second},
		scriptedHandler{definition: other, execute: run})
	enqueueTestTask(t, h, "first", "first")
	enqueueAdmissionTask(t, h, other.Type, "second")
	third := enqueueAdmissionTask(t, h, other.Type, "third")
	filters := make(chan repository.TaskClaimFilter, 100)
	h.repos.Tasks = observedClaimRepository{TaskRepository: h.repos.Tasks, filters: filters}
	startAdmissionEngine(t, h)
	receiveAdmission(t, entered)
	receiveAdmission(t, entered)
	// A heartbeat while both calls are held also proves idle capacity checks
	// continue without claiming the third task.
	h.engine.lastTick.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !h.engine.Healthy() {
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("full engine stopped updating health")
		}
	}
	stored, err := h.repos.Tasks.GetByID(t.Context(), third.ID)
	if err != nil || stored.Status != model.TaskStatusPending || stored.ClaimGeneration != 0 {
		t.Fatalf("global limit did not leave third task queued: %#v err=%v", stored, err)
	}
	if count := len(filters); count != 2 {
		t.Fatalf("full engine made %d claims, want 2", count)
	}
	advance <- struct{}{}
	if next := receiveAdmission(t, entered); next.ID() != third.ID {
		t.Fatalf("released global slot started task %d, want %d", next.ID(), third.ID)
	}
}

func TestEngineActiveClaimCannotBeReclaimedBeforeInvocationReturns(t *testing.T) {
	definition := testDefinition(nil, true)
	other := testDefinition(nil, true)
	other.Type = "other_operation"
	entered := make(chan Execution, 4)
	release := make(chan struct{})
	var closeOnce sync.Once
	h := newTaskHarness(t, scriptedHandler{
		definition: definition,
		execute: func(_ context.Context, execution Execution) Result {
			entered <- execution
			<-release
			return Complete("Done", nil)
		},
		recover: func(_ context.Context, execution Execution) Result {
			entered <- execution
			return Complete("Recovered", nil)
		},
	}, &EngineConfig{Concurrency: 2, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second},
		scriptedHandler{definition: other})
	row := enqueueTestTask(t, h, "stale", "stale")
	startAdmissionEngine(t, h)
	t.Cleanup(func() { closeOnce.Do(func() { close(release) }) })
	first := receiveAdmission(t, entered)
	if _, err := h.db.ExecContext(t.Context(), `UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), row.ID); err != nil {
		t.Fatal(err)
	}
	plain := enqueueAdmissionTask(t, h, other.Type, "plain")
	waitAdmissionTask(t, h, plain.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
	stored, err := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.ClaimGeneration != first.ClaimGeneration() {
		t.Fatalf("active invocation was reclaimed: %#v err=%v", stored, err)
	}
	closeOnce.Do(func() { close(release) })
	next := receiveAdmission(t, entered)
	if next.ID() != row.ID || next.Mode() != model.TaskResumeModeRecover || next.ClaimGeneration() != first.ClaimGeneration()+1 {
		t.Fatalf("invocation after release = id:%d mode:%s generation:%d", next.ID(), next.Mode(), next.ClaimGeneration())
	}
	waitAdmissionTask(t, h, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
}

func TestEngineConcurrencyHeldThroughSettlement(t *testing.T) {
	definition := testDefinition(nil, true)
	definition.MaxConcurrency = 1
	other := testDefinition(nil, true)
	other.Type = "other_operation"
	entered := make(chan Execution, 2)
	settling := make(chan struct{})
	release := make(chan struct{})
	var closeOnce sync.Once
	h := newTaskHarness(t, scriptedHandler{definition: definition, execute: func(_ context.Context, execution Execution) Result {
		entered <- execution
		return Complete("Done", nil)
	}}, &EngineConfig{
		Concurrency: 2, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second,
		OnTaskSettled: func(row *model.Task, _ repository.TaskTransition) {
			if row.IdempotencyKey == "first" {
				close(settling)
				<-release
			}
		},
	}, scriptedHandler{definition: other})
	enqueueTestTask(t, h, "first", "first")
	second := enqueueTestTask(t, h, "second", "second")
	startAdmissionEngine(t, h)
	t.Cleanup(func() { closeOnce.Do(func() { close(release) }) })
	receiveAdmission(t, entered)
	select {
	case <-settling:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement did not finish")
	}
	plain := enqueueAdmissionTask(t, h, other.Type, "plain")
	waitAdmissionTask(t, h, plain.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
	stored, err := h.repos.Tasks.GetByID(t.Context(), second.ID)
	if err != nil || stored.Status != model.TaskStatusPending || stored.ClaimGeneration != 0 {
		t.Fatalf("type slot released before settlement notification returned: %#v err=%v", stored, err)
	}
	closeOnce.Do(func() { close(release) })
	if next := receiveAdmission(t, entered); next.ID() != second.ID {
		t.Fatalf("next task = %d, want %d", next.ID(), second.ID)
	}
}

func TestEngineConcurrencyReleasedAfterHandlerResults(t *testing.T) {
	for _, outcome := range []string{"wait", "retry", "fail", "cancel", "panic", "settlement failure"} {
		t.Run(outcome, func(t *testing.T) {
			definition := testDefinition(nil, true)
			definition.MaxConcurrency = 1
			entered := make(chan Execution, 2)
			h := newTaskHarness(t, scriptedHandler{definition: definition, execute: func(_ context.Context, execution Execution) Result {
				entered <- execution
				input, _ := DecodeInput[testInput](execution)
				if input.Value == "second" {
					return Complete("Done", nil)
				}
				switch outcome {
				case "wait":
					return Wait(model.TaskResumeModeRecover, time.Hour, "dependency", "Waiting", nil)
				case "retry":
					return Retry(errors.New("temporary"), "temporary", time.Hour, nil)
				case "fail":
					return Fail(errors.New("failed"), "failed", nil)
				case "cancel":
					return Cancel("Cancelled", nil)
				case "settlement failure":
					return Complete("Done", func(context.Context, *repository.Repositories) error { return errors.New("settlement failed") })
				default:
					panic("handler failure")
				}
			}}, &EngineConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second})
			h.engine.settlementRetryDelays = []time.Duration{0}
			enqueueTestTask(t, h, "first", "first")
			second := enqueueTestTask(t, h, "second", "second")
			startAdmissionEngine(t, h)
			receiveAdmission(t, entered)
			if next := receiveAdmission(t, entered); next.ID() != second.ID {
				t.Fatalf("slot was not released to second task: %d", next.ID())
			}
			waitAdmissionTask(t, h, second.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
		})
	}
}

func TestEngineUnknownTypeFailsWithoutBlockingOtherTypes(t *testing.T) {
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)},
		&EngineConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second})
	unknown := enqueueTestTask(t, h, "unknown", "unknown")
	if _, err := h.db.ExecContext(t.Context(), `UPDATE tasks SET type = 'unregistered' WHERE id = ?`, unknown.ID); err != nil {
		t.Fatal(err)
	}
	next := enqueueTestTask(t, h, "next", "next")
	startAdmissionEngine(t, h)
	failed := waitAdmissionTask(t, h, unknown.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusFailed })
	if dereference(failed.FailureReason) != failureHandlerUnavailable {
		t.Fatalf("unknown handler failure = %#v", failed)
	}
	waitAdmissionTask(t, h, next.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
}

func TestEngineShutdownWaitsForInvocations(t *testing.T) {
	definition := testDefinition(nil, true)
	entered := make(chan Execution, 1)
	recovered := make(chan Execution, 1)
	cancelled := make(chan struct{})
	release := make(chan struct{})
	var closeOnce sync.Once
	t.Cleanup(func() { closeOnce.Do(func() { close(release) }) })
	h := newTaskHarness(t, scriptedHandler{
		definition: definition,
		execute: func(ctx context.Context, execution Execution) Result {
			entered <- execution
			<-ctx.Done()
			close(cancelled)
			<-release
			return Complete("Discarded", nil)
		},
		recover: func(_ context.Context, execution Execution) Result {
			recovered <- execution
			return Complete("Recovered", nil)
		},
	}, &EngineConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second})
	shortening := &controlledShortenRepository{TaskRepository: h.repos.Tasks}
	shortening.fail.Store(true)
	h.repos.Tasks = shortening
	row := enqueueTestTask(t, h, "first", "first")
	queued := enqueueTestTask(t, h, "second", "second")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- h.engine.Run(ctx) }()
	first := receiveAdmission(t, entered)
	cancel()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("handler context was not cancelled")
	}
	select {
	case err := <-stopped:
		t.Fatalf("engine stopped before invocation returned: %v", err)
	default:
	}
	closeOnce.Do(func() { close(release) })
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("engine did not finish shutdown")
	}
	stored, err := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ClaimGeneration != first.ClaimGeneration() || stored.FinishedAt != nil {
		t.Fatalf("shutdown invocation was settled: %#v err=%v", stored, err)
	}
	stored, err = h.repos.Tasks.GetByID(t.Context(), queued.ID)
	if err != nil || stored.Status != model.TaskStatusPending || stored.ClaimGeneration != 0 {
		t.Fatalf("shutdown claimed queued task: %#v err=%v", stored, err)
	}
	// Shutdown need not persist lease shortening, but an expired claim must
	// recover even when the last stored mode still permits execution.
	if _, err := h.db.ExecContext(t.Context(), `UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), row.ID); err != nil {
		t.Fatal(err)
	}
	next := claimTestTask(t, h)
	if next.ID != row.ID || next.ResumeMode != model.TaskResumeModeRecover || next.ClaimGeneration != first.ClaimGeneration()+1 {
		t.Fatalf("shutdown recovery claim = %#v", next)
	}
	h.engine.executeClaimSafely(t.Context(), next)
	if invocation := receiveAdmission(t, recovered); invocation.ID() != row.ID || invocation.Mode() != model.TaskResumeModeRecover {
		t.Fatalf("shutdown invoked execution instead of recovery: %#v", invocation)
	}
	stored, err = h.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusCompleted || dereference(stored.StatusMessage) != "Recovered" {
		t.Fatalf("shutdown recovery was not settled: %#v err=%v", stored, err)
	}
}

type failingClaimRepository struct {
	repository.TaskRepository
	failed atomic.Bool
}

func (r *failingClaimRepository) ClaimNext(ctx context.Context, lease time.Duration, filter repository.TaskClaimFilter) (*model.Task, error) {
	if r.failed.CompareAndSwap(false, true) {
		return nil, errors.New("injected claim failure")
	}
	return r.TaskRepository.ClaimNext(ctx, lease, filter)
}

func TestEngineClaimFailureDoesNotConsumeConcurrency(t *testing.T) {
	definition := testDefinition(nil, true)
	definition.MaxConcurrency = 1
	h := newTaskHarness(t, scriptedHandler{definition: definition},
		&EngineConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second})
	row := enqueueTestTask(t, h, "first", "first")
	h.repos.Tasks = &failingClaimRepository{TaskRepository: h.repos.Tasks}
	startAdmissionEngine(t, h)
	waitAdmissionTask(t, h, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
}
