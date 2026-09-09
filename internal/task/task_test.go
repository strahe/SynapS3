package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

const testTaskType model.TaskType = "test_operation"

type testInput struct {
	Value string `json:"value"`
}

type scriptedHandler struct {
	definition Definition
	execute    func(context.Context, Execution) Result
	recover    func(context.Context, Execution) Result
}

type panicError struct{}

func (panicError) Error() string { panic("error formatting failure") }

func (h scriptedHandler) Definition() Definition { return h.definition }

func (h scriptedHandler) Execute(ctx context.Context, execution Execution) Result {
	if h.execute == nil {
		return Complete("executed", nil)
	}
	return h.execute(ctx, execution)
}

func (h scriptedHandler) Recover(ctx context.Context, execution Execution) Result {
	if h.recover == nil {
		return Complete("recovered", nil)
	}
	return h.recover(ctx, execution)
}

func testDefinition(retryLimit *int, allowRetry bool) Definition {
	return Definition{
		Type:         testTaskType,
		InputVersion: 1,
		Codec: StrictJSONCodec(func(input *testInput) error {
			if input.Value == "" {
				return errors.New("value is required")
			}
			return nil
		}),
		RetryLimit: retryLimit,
		AllowRetry: allowRetry,
	}
}

type taskHarness struct {
	db       *bun.DB
	repos    *repository.Repositories
	registry *Registry
	service  *Service
	engine   *Engine
}

func newTaskHarness(t *testing.T, handler Handler, config *EngineConfig) taskHarness {
	t.Helper()
	db := testutil.NewTestFileDB(t)
	repos := repository.NewRepositories(db)
	registry := NewRegistry()
	if err := registry.Register(handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	service, err := NewService(registry, repos, time.Hour)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	engineConfig := EngineConfig{
		Concurrency:                    4,
		PollInterval:                   10 * time.Millisecond,
		LeaseDuration:                  5 * time.Second,
		Retention:                      time.Hour,
		ProviderMutationConcurrency:    4,
		DestructiveMutationConcurrency: 2,
	}
	if config != nil {
		engineConfig = *config
	}
	engine, err := NewEngine(engineConfig, repos, registry, slog.Default())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return taskHarness{db: db, repos: repos, registry: registry, service: service, engine: engine}
}

func enqueueTestTask(t *testing.T, harness taskHarness, key, value string) *model.Task {
	t.Helper()
	taskRow, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type:           testTaskType,
		IdempotencyKey: key,
		Input:          testInput{Value: value},
		SubjectType:    "test_subject",
		SubjectKey:     key,
	})
	if err != nil {
		t.Fatalf("enqueue task: %v", err)
	}
	if !created {
		t.Fatalf("task %s was not created", key)
	}
	return taskRow
}

func claimTestTask(t *testing.T, harness taskHarness) *model.Task {
	t.Helper()
	claimed, err := harness.repos.Tasks.ClaimNext(t.Context(), harness.engine.config.LeaseDuration)
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if claimed == nil {
		t.Fatal("claim returned no task")
	}
	return claimed
}

func TestServiceCanonicalEnqueueAndConflict(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, nil)

	first, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "same", Input: map[string]any{"value": "alpha"},
	})
	if err != nil || !created {
		t.Fatalf("first enqueue = task:%#v created:%v err:%v", first, created, err)
	}
	if string(first.Input) != `{"value":"alpha"}` {
		t.Fatalf("canonical input = %s", first.Input)
	}

	same, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "same", Input: testInput{Value: "alpha"},
	})
	if err != nil || created || same.ID != first.ID {
		t.Fatalf("idempotent enqueue = task:%#v created:%v err:%v", same, created, err)
	}

	_, _, err = harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "same", Input: testInput{Value: "beta"},
	})
	if !errors.Is(err, ErrInputConflict) {
		t.Fatalf("conflicting enqueue error = %v, want %v", err, ErrInputConflict)
	}

	_, _, err = harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "unknown-field", Input: map[string]any{"value": "alpha", "extra": true},
	})
	if err == nil {
		t.Fatal("enqueue with unknown input field succeeded")
	}
}

func TestServiceWakeMakesFuturePendingTaskReady(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, nil)
	row, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "wake", Input: testInput{Value: "wake"},
		AvailableAt: time.Now().Add(time.Hour),
	})
	if err != nil || !created {
		t.Fatalf("enqueue future task = %#v created=%v err=%v", row, created, err)
	}
	if claimed, err := harness.repos.Tasks.ClaimNext(t.Context(), time.Minute); err != nil || claimed != nil {
		t.Fatalf("claim before wake = %#v err=%v", claimed, err)
	}
	if err := harness.repos.WithTx(t.Context(), func(txRepos *repository.Repositories) error {
		woken, wakeErr := harness.service.WakeInTransaction(t.Context(), txRepos, []int64{row.ID})
		if wakeErr != nil {
			return wakeErr
		}
		if woken != 1 {
			return fmt.Errorf("woken tasks = %d, want 1", woken)
		}
		return nil
	}); err != nil {
		t.Fatalf("wake future task: %v", err)
	}
	claimed := claimTestTask(t, harness)
	if claimed.ID != row.ID {
		t.Fatalf("claimed task = %d, want %d", claimed.ID, row.ID)
	}
}

func TestEngineConstructionFreezesRegistry(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, nil)
	err := harness.registry.Register(scriptedHandler{definition: Definition{
		Type: "late_operation", InputVersion: 1,
		Codec: StrictJSONCodec(func(input *testInput) error { return nil }),
	}})
	if !errors.Is(err, ErrRegistryFrozen) {
		t.Fatalf("late registry mutation error = %v, want frozen registry", err)
	}
}

func TestRegistryFreezesDefinitionAtRegistration(t *testing.T) {
	limit := 5
	handler := &scriptedHandler{definition: testDefinition(&limit, true)}
	registry := NewRegistry()
	if err := registry.Register(handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	handler.definition.InputVersion = 2
	handler.definition.AllowRetry = false
	limit = 9

	definition, ok := registry.Definition(testTaskType)
	if !ok || definition.InputVersion != 1 || !definition.AllowRetry || definition.RetryLimit == nil || *definition.RetryLimit != 5 {
		t.Fatalf("registered definition changed = %#v", definition)
	}
	*definition.RetryLimit = 11
	again, ok := registry.Definition(testTaskType)
	if !ok || again.RetryLimit == nil || *again.RetryLimit != 5 {
		t.Fatalf("returned definition mutated registry = %#v", again)
	}
}

func TestEngineSettlesAllFiveStates(t *testing.T) {
	limit := 0
	tests := []struct {
		name          string
		result        Result
		wantStatus    model.TaskStatus
		wantMode      model.TaskResumeMode
		wantRetry     int
		wantRetention bool
	}{
		{name: "completed", result: Complete("done", nil), wantStatus: model.TaskStatusCompleted, wantMode: model.TaskResumeModeRecover, wantRetention: true},
		{name: "pending", result: Suspend(model.TaskResumeModeExecute, time.Hour, "scheduled", "later", nil), wantStatus: model.TaskStatusPending, wantMode: model.TaskResumeModeExecute},
		{name: "failed", result: Fail(errors.New("permanent"), "permanent_failure", nil), wantStatus: model.TaskStatusFailed, wantMode: model.TaskResumeModeRecover},
		{name: "cancelled", result: Cancel("superseded", nil), wantStatus: model.TaskStatusCancelled, wantMode: model.TaskResumeModeRecover, wantRetention: true},
		{name: "retry limit reached", result: Retry(errors.New("temporary"), "temporary_failure", 0, nil), wantStatus: model.TaskStatusFailed, wantMode: model.TaskResumeModeRecover},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newTaskHarness(t, scriptedHandler{
				definition: testDefinition(&limit, true),
				execute:    func(context.Context, Execution) Result { return tt.result },
			}, nil)
			row := enqueueTestTask(t, harness, tt.name, tt.name)
			claimed := claimTestTask(t, harness)
			harness.engine.executeClaim(t.Context(), claimed)

			stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil {
				t.Fatalf("get settled task: %v", err)
			}
			if stored.Status != tt.wantStatus || stored.ResumeMode != tt.wantMode || stored.RetryCount != tt.wantRetry {
				t.Fatalf("settled task = status:%s mode:%s retries:%d", stored.Status, stored.ResumeMode, stored.RetryCount)
			}
			if (stored.RetentionUntil != nil) != tt.wantRetention {
				t.Fatalf("retention = %v, want present %v", stored.RetentionUntil, tt.wantRetention)
			}
			if stored.Status != model.TaskStatusPending && stored.FinishedAt == nil {
				t.Fatal("terminal task has no finished time")
			}
			if stored.Status == model.TaskStatusCompleted || stored.Status == model.TaskStatusCancelled {
				again, claimErr := harness.repos.Tasks.ClaimNext(t.Context(), time.Minute)
				if claimErr != nil || again != nil {
					t.Fatalf("terminal task revived = %#v, err=%v", again, claimErr)
				}
			}
		})
	}
}

func TestEngineNotifiesAfterSettlementCommits(t *testing.T) {
	limit := 1
	var notified atomic.Int64
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second, Retention: time.Hour,
		ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
		OnTaskSettled: func(taskRow *model.Task, transition repository.TaskTransition) {
			if taskRow.IdempotencyKey != "notify" || transition.Status != model.TaskStatusCompleted {
				t.Errorf("settlement notification = task:%#v transition:%#v", taskRow, transition)
			}
			notified.Add(1)
		},
	}
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, &config)
	enqueueTestTask(t, harness, "notify", "notify")
	claimed := claimTestTask(t, harness)
	harness.engine.executeClaim(t.Context(), claimed)
	if notified.Load() != 1 {
		t.Fatalf("settlement notifications = %d, want 1", notified.Load())
	}
}

func TestRetryBackoffUsesPersistedRetryCount(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, nil)
	harness.engine.retryDelay = func(retryCount int) time.Duration {
		if retryCount != 3 {
			t.Fatalf("retry count = %d, want 3", retryCount)
		}
		return 37 * time.Second
	}
	now := time.Now()
	transition := harness.engine.transitionFor(&model.Task{RetryCount: 3, RetryLimit: &limit}, RetryBackoff(errors.New("temporary"), "temporary_failure", nil))
	if transition.Status != model.TaskStatusPending || !transition.IncrementRetry {
		t.Fatalf("transition = %#v, want pending retry", transition)
	}
	if transition.AvailableAt.Before(now.Add(37*time.Second)) || transition.AvailableAt.After(time.Now().Add(37*time.Second)) {
		t.Fatalf("available at = %s, want 37 second delay", transition.AvailableAt)
	}
}

func TestRetryDelayIsExponentialJitteredAndCapped(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		jitter     float64
		want       time.Duration
	}{
		{name: "first low jitter", retryCount: 0, jitter: 0, want: 8 * time.Second},
		{name: "first midpoint", retryCount: 0, jitter: 0.5, want: 10 * time.Second},
		{name: "third midpoint", retryCount: 2, jitter: 0.5, want: 40 * time.Second},
		{name: "negative count", retryCount: -1, jitter: 0.5, want: 10 * time.Second},
		{name: "hard cap", retryCount: 20, jitter: 1, want: 5 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryDelayWithJitter(tt.retryCount, tt.jitter); got != tt.want {
				t.Fatalf("delay = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestServiceRetryForcesRecoverAndAcknowledgeStartsRetention(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			return Fail(errors.New("failed"), "test_failure", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "retry", "retry")
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))

	if err := harness.service.Retry(t.Context(), row.ID); err != nil {
		t.Fatalf("retry failed task: %v", err)
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusPending || stored.ResumeMode != model.TaskResumeModeRecover || stored.RetryCount != 0 {
		t.Fatalf("retried task = %#v, err=%v", stored, err)
	}

	claimed := claimTestTask(t, harness)
	if claimed.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("manual retry claim mode = %s", claimed.ResumeMode)
	}
	if err := harness.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
	}); err != nil {
		t.Fatalf("fail retried task: %v", err)
	}
	if err := harness.service.Acknowledge(t.Context(), row.ID); err != nil {
		t.Fatalf("acknowledge failed task: %v", err)
	}
	stored, err = harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.AcknowledgedAt == nil || stored.RetentionUntil == nil {
		t.Fatalf("acknowledged task = %#v, err=%v", stored, err)
	}
}

func TestServiceManualRetryPredicateUsesFailureEvidence(t *testing.T) {
	limit := 5
	definition := testDefinition(&limit, true)
	definition.CanManualRetry = func(task *model.Task) bool {
		return task != nil && task.FailureReason != nil && *task.FailureReason != "unsafe_outcome"
	}
	harness := newTaskHarness(t, scriptedHandler{
		definition: definition,
		execute: func(context.Context, Execution) Result {
			return Fail(errors.New("outcome is unknown"), "unsafe_outcome", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "unsafe-retry", "unsafe-retry")
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))

	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored == nil || stored.Status != model.TaskStatusFailed {
		t.Fatalf("failed task = %#v, err=%v", stored, err)
	}
	if harness.service.Retryable(stored) {
		t.Fatal("unsafe failed task is retryable")
	}
	if err := harness.service.Retry(t.Context(), row.ID); !errors.Is(err, ErrRetryUnsupported) {
		t.Fatalf("Retry = %v, want ErrRetryUnsupported", err)
	}
}

func TestCancellationWakesPendingTaskAndForcesRecovery(t *testing.T) {
	limit := 5
	var executed atomic.Bool
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			executed.Store(true)
			return Complete("must not execute", nil)
		},
		recover: func(_ context.Context, execution Execution) Result {
			if !execution.CancellationRequested() || execution.CancellationReason() != "owner stopped" {
				return Fail(errors.New("missing cancellation request"), "test_failure", nil)
			}
			return Cancel("cancelled", nil)
		},
	}, nil)
	row, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "cancel-pending", Input: testInput{Value: "cancel-pending"},
		AvailableAt: time.Now().Add(24 * time.Hour),
	})
	if err != nil || !created {
		t.Fatalf("enqueue delayed task = %#v created=%v err=%v", row, created, err)
	}
	if err := harness.repos.Tasks.RequestCancellation(t.Context(), row.ID, "owner stopped"); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	claimed := claimTestTask(t, harness)
	if claimed.ResumeMode != model.TaskResumeModeRecover || claimed.CancellationRequestedAt == nil ||
		claimed.AvailableAt.Before(*claimed.CancellationRequestedAt) {
		t.Fatalf("cancelled claim = %#v", claimed)
	}
	harness.engine.executeClaim(t.Context(), claimed)
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusCancelled || executed.Load() {
		t.Fatalf("cancelled task = %#v executed=%v err=%v", stored, executed.Load(), err)
	}
}

func TestCancellationOverridesPendingSettlementScheduleAndMode(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, nil)
	row := enqueueTestTask(t, harness, "cancel-running", "cancel-running")
	claimed := claimTestTask(t, harness)
	if err := harness.repos.Tasks.RequestCancellation(t.Context(), row.ID, "owner stopped"); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	if err := harness.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
		AvailableAt: time.Now().Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("suspend cancellation-requested task: %v", err)
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusPending || stored.ResumeMode != model.TaskResumeModeRecover ||
		stored.CancellationRequestedAt == nil || stored.AvailableAt.Before(*stored.CancellationRequestedAt) ||
		stored.AvailableAt.After(time.Now().Add(time.Second)) {
		t.Fatalf("suspended cancellation-requested task = %#v, err=%v", stored, err)
	}
}

func TestExpiredClaimIsRecoveredAndStaleGenerationIsFenced(t *testing.T) {
	limit := 5
	var executes, recovers atomic.Int64
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			executes.Add(1)
			return Complete("executed", nil)
		},
		recover: func(context.Context, Execution) Result {
			recovers.Add(1)
			return Complete("recovered", nil)
		},
	}, nil)
	enqueueTestTask(t, harness, "takeover", "takeover")
	stale := claimTestTask(t, harness)
	if _, err := harness.db.NewRaw(`UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), stale.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire first claim: %v", err)
	}
	fresh := claimTestTask(t, harness)
	if fresh.ClaimGeneration != stale.ClaimGeneration+1 || fresh.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("takeover claim = generation:%d mode:%s", fresh.ClaimGeneration, fresh.ResumeMode)
	}
	if err := harness.repos.Tasks.WriteCheckpoint(t.Context(), stale.ID, stale.ClaimGeneration, []byte(`{"stale":true}`)); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("stale checkpoint error = %v", err)
	}
	if err := harness.repos.Tasks.Settle(t.Context(), stale.ID, stale.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover, RetentionUntil: new(time.Now().Add(time.Hour)),
	}); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("stale settlement error = %v", err)
	}

	harness.engine.executeClaim(t.Context(), fresh)
	if executes.Load() != 0 || recovers.Load() != 1 {
		t.Fatalf("handler calls = execute:%d recover:%d", executes.Load(), recovers.Load())
	}
}

func TestEngineRejectsCorruptInputBeforeHandler(t *testing.T) {
	limit := 5
	var called atomic.Bool
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			called.Store(true)
			return Complete("unexpected", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "corrupt", "corrupt")
	if _, err := harness.db.NewRaw(`UPDATE tasks SET input_hash = ? WHERE id = ?`, "00", row.ID).Exec(t.Context()); err != nil {
		t.Fatalf("corrupt task hash: %v", err)
	}
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusFailed || stored.FailureReason == nil || *stored.FailureReason != "invalid_input_hash" {
		t.Fatalf("corrupt task result = %#v, err=%v", stored, err)
	}
	if called.Load() {
		t.Fatal("handler ran with corrupt input")
	}
}

func TestEngineAcceptsDatabaseJSONNormalizationAndUsesCanonicalInput(t *testing.T) {
	limit := 5
	var gotInput string
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(_ context.Context, execution Execution) Result {
			gotInput = string(execution.Input())
			return Complete("normalized", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "normalized", "normalized")
	if _, err := harness.db.NewRaw(`UPDATE task_payloads SET input_json = ? WHERE task_id = ?`, `{ "value" : "normalized" }`, row.ID).Exec(t.Context()); err != nil {
		t.Fatalf("normalize stored task JSON: %v", err)
	}
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusCompleted {
		t.Fatalf("normalized task result = %#v, err=%v", stored, err)
	}
	if gotInput != `{"value":"normalized"}` {
		t.Fatalf("handler input = %q, want canonical JSON", gotInput)
	}
}

func TestCodecPanicFailsClosedDuringEnqueueAndExecution(t *testing.T) {
	limit := 5
	var panicCodec atomic.Bool
	var handlerCalled atomic.Bool
	strict := StrictJSONCodec(func(input *testInput) error {
		if input.Value == "" {
			return errors.New("value is required")
		}
		return nil
	})
	definition := testDefinition(&limit, true)
	definition.Codec = CodecFunc(func(input json.RawMessage) (json.RawMessage, error) {
		if panicCodec.Load() {
			panic("codec failure")
		}
		return strict.Canonicalize(input)
	})
	harness := newTaskHarness(t, scriptedHandler{
		definition: definition,
		execute: func(context.Context, Execution) Result {
			handlerCalled.Store(true)
			return Complete("unexpected", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "codec-panic", "codec-panic")
	panicCodec.Store(true)
	if _, _, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "codec-panic-enqueue", Input: testInput{Value: "value"},
	}); !errors.Is(err, ErrCodecPanic) {
		t.Fatalf("enqueue codec panic error = %v, want ErrCodecPanic", err)
	}

	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusFailed || stored.FailureReason == nil || *stored.FailureReason != "input_codec_panic" {
		t.Fatalf("codec panic task result = %#v, err=%v", stored, err)
	}
	if handlerCalled.Load() {
		t.Fatal("handler ran after its input codec panicked")
	}
}

func TestCodecInvalidCanonicalJSONFailsClosed(t *testing.T) {
	limit := 5
	definition := testDefinition(&limit, true)
	definition.Codec = CodecFunc(func(json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`not-json`), nil
	})
	harness := newTaskHarness(t, scriptedHandler{definition: definition}, nil)
	if _, _, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, IdempotencyKey: "invalid-canonical", Input: testInput{Value: "value"},
	}); !errors.Is(err, ErrInvalidCanonical) {
		t.Fatalf("enqueue invalid canonical error = %v, want ErrInvalidCanonical", err)
	}
}

func TestUnexpectedClaimPanicForcesRecoveryWithoutKillingWorker(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			return Retry(panicError{}, "formatting_failed", 0, nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "claim-panic", "claim-panic")
	harness.engine.executeClaimSafely(t.Context(), claimTestTask(t, harness))
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ResumeMode != model.TaskResumeModeRecover || stored.LeaseUntil == nil {
		t.Fatalf("task after unexpected panic = %#v, err=%v", stored, err)
	}
	if stored.LeaseUntil.After(time.Now().Add(2 * harness.engine.config.PollInterval)) {
		t.Fatalf("unexpected panic left a long lease: %s", stored.LeaseUntil)
	}
}

func TestEnginePanicAndInvalidResultFailClosed(t *testing.T) {
	limit := 5
	tests := []struct {
		name       string
		execute    func(context.Context, Execution) Result
		wantReason string
	}{
		{name: "panic", execute: func(context.Context, Execution) Result { panic("boom") }, wantReason: "handler_panic"},
		{name: "invalid", execute: func(context.Context, Execution) Result { return Result{} }, wantReason: "invalid_result"},
		{name: "failure without evidence", execute: func(context.Context, Execution) Result {
			return Fail(nil, "", nil)
		}, wantReason: "invalid_result"},
		{name: "retry without reason", execute: func(context.Context, Execution) Result {
			return Retry(errors.New("temporary"), "", 0, nil)
		}, wantReason: "invalid_result"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: tt.execute}, nil)
			row := enqueueTestTask(t, harness, tt.name, tt.name)
			harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
			stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil || stored.Status != model.TaskStatusFailed || stored.FailureReason == nil || *stored.FailureReason != tt.wantReason {
				t.Fatalf("failed-closed task = %#v, err=%v", stored, err)
			}
		})
	}
}

func TestEngineShutdownDiscardsHandlerResultAndForcesRecovery(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(ctx context.Context, _ Execution) Result {
			<-ctx.Done()
			return Complete("must be discarded", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "shutdown", "shutdown")
	claimed := claimTestTask(t, harness)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	harness.engine.executeClaim(ctx, claimed)
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("shutdown task = %#v, err=%v", stored, err)
	}
}

func TestRecoverCannotStartExternalEffect(t *testing.T) {
	limit := 5
	var called atomic.Bool
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		recover: func(ctx context.Context, execution Execution) Result {
			err := execution.WithResource(ctx, ResourceProviderMutation, func(context.Context) error {
				called.Store(true)
				return nil
			})
			if !errors.Is(err, ErrEffectForbidden) {
				return Fail(fmt.Errorf("resource error = %v", err), "unexpected_resource_error", nil)
			}
			return Complete("recovery remained read-only", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "recover-effect", "recover-effect")
	if _, err := harness.db.NewRaw(`UPDATE tasks SET resume_mode = ? WHERE id = ?`, model.TaskResumeModeRecover, row.ID).Exec(t.Context()); err != nil {
		t.Fatalf("set recovery mode: %v", err)
	}
	claimed := claimTestTask(t, harness)
	harness.engine.executeClaim(t.Context(), claimed)
	if called.Load() {
		t.Fatal("recovery handler started an external effect")
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusCompleted {
		t.Fatalf("recovery task = %#v, err=%v", stored, err)
	}
}

func TestWithCheckpointedEffectSeparatesAdmissionFromAttempt(t *testing.T) {
	injected := errors.New("injected failure")
	tests := []struct {
		name            string
		mode            model.TaskResumeMode
		resource        resourceRunner
		checkpoint      checkpointWriter
		wantAttempted   bool
		wantEffectCalls int
		wantError       error
	}{
		{
			name: "resource admission fails", mode: model.TaskResumeModeExecute,
			resource: func(context.Context, Resource, func(context.Context) error) error { return context.Canceled },
			checkpoint: func(context.Context, any, Settlement) error {
				t.Fatal("checkpoint ran before resource admission")
				return nil
			},
			wantError: context.Canceled,
		},
		{
			name: "checkpoint fails", mode: model.TaskResumeModeExecute,
			resource: func(ctx context.Context, _ Resource, fn func(context.Context) error) error { return fn(ctx) },
			checkpoint: func(context.Context, any, Settlement) error {
				return injected
			},
			wantError: injected,
		},
		{
			name: "effect fails", mode: model.TaskResumeModeExecute,
			resource: func(ctx context.Context, _ Resource, fn func(context.Context) error) error { return fn(ctx) },
			checkpoint: func(ctx context.Context, _ any, settlement Settlement) error {
				return settlement(ctx, nil)
			},
			wantAttempted: true, wantEffectCalls: 1, wantError: injected,
		},
		{
			name: "recovery is forbidden", mode: model.TaskResumeModeRecover,
			resource: func(context.Context, Resource, func(context.Context) error) error {
				t.Fatal("recovery reached resource runner")
				return nil
			},
			checkpoint: func(context.Context, any, Settlement) error {
				t.Fatal("recovery wrote checkpoint")
				return nil
			},
			wantError: ErrEffectForbidden,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effectCalls := 0
			execution := Execution{
				task: model.Task{ResumeMode: tt.mode}, checkpoint: tt.checkpoint, resource: tt.resource,
			}
			attempted, err := execution.WithCheckpointedEffect(t.Context(), ResourceProviderMutation, map[string]bool{"attempted": true}, func(context.Context, *repository.Repositories) error {
				return nil
			}, func(context.Context) error {
				effectCalls++
				return injected
			})
			if attempted != tt.wantAttempted || effectCalls != tt.wantEffectCalls || !errors.Is(err, tt.wantError) {
				t.Fatalf("result = attempted:%v calls:%d err:%v, want attempted:%v calls:%d err:%v", attempted, effectCalls, err, tt.wantAttempted, tt.wantEffectCalls, tt.wantError)
			}
		})
	}
}

func TestCheckpointedEffectRollsBackEvidenceBeforeEffect(t *testing.T) {
	limit := 5
	var effectCalled atomic.Bool
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(ctx context.Context, execution Execution) Result {
			attempted, err := execution.WithCheckpointedEffect(ctx, ResourceProviderMutation, map[string]bool{"attempted": true}, func(ctx context.Context, repos *repository.Repositories) error {
				if err := repos.Tasks.RequestCancellation(ctx, execution.ID(), "must roll back"); err != nil {
					return err
				}
				return errors.New("reject checkpoint evidence")
			}, func(context.Context) error {
				effectCalled.Store(true)
				return nil
			})
			if attempted || err == nil {
				return Fail(fmt.Errorf("checkpoint result = attempted:%v err:%v", attempted, err), "unexpected_checkpoint_result", nil)
			}
			return Fail(err, "effect_not_started", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "checkpoint-rollback", "checkpoint-rollback")
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusFailed || stored.FailureReason == nil || *stored.FailureReason != "effect_not_started" || stored.CancellationRequestedAt != nil || len(stored.Checkpoint) != 0 {
		t.Fatalf("rolled-back task = %#v, err=%v", stored, err)
	}
	if effectCalled.Load() {
		t.Fatal("effect ran after checkpoint settlement failed")
	}
}

func TestExternalEffectRevalidatesClaimAfterResourceAdmission(t *testing.T) {
	limit := 5
	var called atomic.Bool
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(ctx context.Context, execution Execution) Result {
			err := execution.WithResource(ctx, ResourceProviderMutation, func(context.Context) error {
				called.Store(true)
				return nil
			})
			if err != nil {
				return Fail(err, "resource_claim_lost", nil)
			}
			return Complete("effect completed", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "stale-effect", "stale-effect")
	stale := claimTestTask(t, harness)
	if _, err := harness.db.NewRaw(`UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), row.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire stale claim: %v", err)
	}
	fresh := claimTestTask(t, harness)

	harness.engine.executeClaim(t.Context(), stale)
	if called.Load() {
		t.Fatal("stale claim started an external effect")
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ClaimGeneration != fresh.ClaimGeneration {
		t.Fatalf("task after stale effect attempt = %#v, err=%v", stored, err)
	}
}

type renewFailureRepository struct {
	repository.TaskRepository
}

func (r renewFailureRepository) RenewLease(context.Context, int64, int64, time.Duration) (time.Time, error) {
	return time.Time{}, errors.New("injected renewal failure")
}

type countingRenewRepository struct {
	repository.TaskRepository
	renewals atomic.Int64
}

func (r *countingRenewRepository) RenewLease(ctx context.Context, id, generation int64, duration time.Duration) (time.Time, error) {
	r.renewals.Add(1)
	return r.TaskRepository.RenewLease(ctx, id, generation, duration)
}

type controlledShortenRepository struct {
	repository.TaskRepository
	fail atomic.Bool
}

func (r *controlledShortenRepository) ShortenLease(ctx context.Context, id, generation int64, duration time.Duration) error {
	if r.fail.Load() {
		return errors.New("injected lease shortening failure")
	}
	return r.TaskRepository.ShortenLease(ctx, id, generation, duration)
}

type delayedShortenRepository struct {
	repository.TaskRepository
	delay              time.Duration
	shortened          atomic.Bool
	renewalsAfterShort atomic.Int64
}

func (r *delayedShortenRepository) RenewLease(ctx context.Context, id, generation int64, duration time.Duration) (time.Time, error) {
	if r.shortened.Load() {
		r.renewalsAfterShort.Add(1)
	}
	return r.TaskRepository.RenewLease(ctx, id, generation, duration)
}

func (r *delayedShortenRepository) ShortenLease(ctx context.Context, id, generation int64, duration time.Duration) error {
	err := r.TaskRepository.ShortenLease(ctx, id, generation, duration)
	if err == nil {
		r.shortened.Store(true)
		time.Sleep(r.delay)
	}
	return err
}

func TestEngineRenewalFailureCancelsBeforeSafetyBoundary(t *testing.T) {
	limit := 5
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second, Retention: time.Hour,
		ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
	}
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(ctx context.Context, _ Execution) Result {
			<-ctx.Done()
			return Complete("must be discarded", nil)
		},
	}, &config)
	row := enqueueTestTask(t, harness, "renewal", "renewal")
	claimed := claimTestTask(t, harness)
	harness.repos.Tasks = renewFailureRepository{TaskRepository: harness.repos.Tasks}
	harness.engine.renewalRetryDelays = []time.Duration{0, time.Second}
	started := time.Now()
	harness.engine.executeClaim(t.Context(), claimed)
	if elapsed := time.Since(started); elapsed >= config.LeaseDuration-config.LeaseDuration/3+30*time.Millisecond {
		t.Fatalf("renewal uncertainty cancelled after %s", elapsed)
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("renewal failure task = %#v, err=%v", stored, err)
	}
}

func TestEngineSuccessfulRenewalKeepsHealthCurrent(t *testing.T) {
	limit := 5
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second, Retention: time.Hour,
		ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
	}
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, &config)
	enqueueTestTask(t, harness, "renewal-health", "renewal-health")
	claimed := claimTestTask(t, harness)
	harness.engine.lastTick.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	var leaseSafe atomic.Bool
	leaseSafe.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go harness.engine.renewLease(ctx, claimed, &leaseSafe, cancel, stop, stopped)

	deadline := time.Now().Add(5 * time.Second)
	for !harness.engine.Healthy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !harness.engine.Healthy() {
		t.Fatal("successful lease renewal did not refresh engine health")
	}
	close(stop)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("lease renewal did not stop")
	}
	if !leaseSafe.Load() {
		t.Fatal("successful lease renewal marked the claim unsafe")
	}
}

func TestEngineSettlementFailureShortensLeaseAndRecovers(t *testing.T) {
	limit := 5
	var recovers atomic.Int64
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second, Retention: time.Hour,
		ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
	}
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute:    func(context.Context, Execution) Result { return Complete("first", nil) },
		recover: func(context.Context, Execution) Result {
			recovers.Add(1)
			return Complete("recovered", nil)
		},
	}, &config)
	row := enqueueTestTask(t, harness, "settlement", "settlement")
	claimed := claimTestTask(t, harness)
	ordered := &delayedShortenRepository{
		TaskRepository: harness.repos.Tasks,
		delay:          2 * config.LeaseDuration / 3,
	}
	harness.repos.Tasks = ordered
	if _, err := harness.db.ExecContext(t.Context(), `CREATE TRIGGER fail_task_settlement
		BEFORE UPDATE OF status ON tasks
		WHEN OLD.status = 'running' AND NEW.status <> 'running'
		BEGIN SELECT RAISE(FAIL, 'injected settlement failure'); END`); err != nil {
		t.Fatalf("create settlement fault: %v", err)
	}
	harness.engine.settlementRetryDelays = []time.Duration{0, 0, 0, 0}
	harness.engine.executeClaim(t.Context(), claimed)
	if got := ordered.renewalsAfterShort.Load(); got != 0 {
		t.Fatalf("lease renewed %d times after it was shortened", got)
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ResumeMode != model.TaskResumeModeRecover || stored.LeaseUntil == nil {
		t.Fatalf("uncertain settlement task = %#v, err=%v", stored, err)
	}
	if stored.LeaseUntil.After(time.Now().Add(2 * harness.engine.config.PollInterval)) {
		t.Fatalf("settlement failure lease was not shortened: %s", stored.LeaseUntil)
	}
	if _, err := harness.db.ExecContext(t.Context(), `DROP TRIGGER fail_task_settlement`); err != nil {
		t.Fatalf("drop settlement fault: %v", err)
	}
	if _, err := harness.db.NewRaw(`UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Millisecond), row.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire shortened lease: %v", err)
	}
	fresh := claimTestTask(t, harness)
	if fresh.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("settlement recovery mode = %s", fresh.ResumeMode)
	}
	harness.engine.executeClaim(t.Context(), fresh)
	stored, err = harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusCompleted || recovers.Load() != 1 {
		t.Fatalf("recovered settlement task = %#v recover calls=%d err=%v", stored, recovers.Load(), err)
	}
}

func TestEngineSettlementPanicRollsBackAndShortensLease(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			return Complete("must not settle", func(context.Context, *repository.Repositories) error {
				panic("settlement boom")
			})
		},
	}, nil)
	row := enqueueTestTask(t, harness, "settlement-panic", "settlement-panic")
	claimed := claimTestTask(t, harness)
	harness.engine.settlementRetryDelays = []time.Duration{0, 0, 0, 0}

	harness.engine.executeClaim(t.Context(), claimed)

	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ResumeMode != model.TaskResumeModeRecover || stored.LeaseUntil == nil {
		t.Fatalf("task after settlement panic = %#v, err=%v", stored, err)
	}
	if stored.LeaseUntil.After(time.Now().Add(2 * harness.engine.config.PollInterval)) {
		t.Fatalf("settlement panic lease was not shortened: %s", stored.LeaseUntil)
	}
}

func TestEngineRecoveryQueueDoesNotDropLeaseShorteningWork(t *testing.T) {
	limit := 5
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: time.Minute, Retention: time.Hour,
		ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
	}
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, &config)
	controlled := &controlledShortenRepository{TaskRepository: harness.repos.Tasks}
	controlled.fail.Store(true)
	harness.repos.Tasks = controlled

	claimed := make([]*model.Task, 0, 3)
	for i := range 3 {
		enqueueTestTask(t, harness, fmt.Sprintf("recovery-queue-%d", i), fmt.Sprintf("value-%d", i))
		claimed = append(claimed, claimTestTask(t, harness))
	}
	for _, row := range claimed {
		harness.engine.abandonClaim(row)
	}
	controlled.fail.Store(false)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		harness.engine.runRecoveryQueue(ctx)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for _, row := range claimed {
		for {
			stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil {
				t.Fatalf("load recovered task %d: %v", row.ID, err)
			}
			if stored.LeaseUntil != nil && stored.ResumeMode == model.TaskResumeModeRecover &&
				stored.LeaseUntil.Before(time.Now().Add(2*config.PollInterval)) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("task %d lease was not shortened: %#v", row.ID, stored)
			}
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery queue did not stop")
	}
}

func TestEngineRenewsLeaseWhileSettlementIsRetried(t *testing.T) {
	limit := 5
	var settlements atomic.Int64
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second, Retention: time.Hour,
		ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
	}
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			return Complete("settled", func(context.Context, *repository.Repositories) error {
				if settlements.Add(1) < 4 {
					return errors.New("injected transient settlement failure")
				}
				return nil
			})
		},
	}, &config)
	row := enqueueTestTask(t, harness, "settlement-renewal", "settlement-renewal")
	claimed := claimTestTask(t, harness)
	counting := &countingRenewRepository{TaskRepository: harness.repos.Tasks}
	harness.repos.Tasks = counting
	harness.engine.settlementRetryDelays = []time.Duration{0, 1200 * time.Millisecond, 1200 * time.Millisecond, 1200 * time.Millisecond}

	harness.engine.executeClaim(t.Context(), claimed)

	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusCompleted {
		t.Fatalf("task after settlement retries = %#v, err=%v", stored, err)
	}
	if got := settlements.Load(); got != 4 {
		t.Fatalf("settlement calls = %d, want 4", got)
	}
	if got := counting.renewals.Load(); got != 10 {
		t.Fatalf("lease renewals = %d, want retries renewed before and during waits", got)
	}
}

func TestEngineConfirmsLeaseBeforeSettlement(t *testing.T) {
	limit := 5
	var settlements atomic.Int64
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			return Complete("must not settle", func(context.Context, *repository.Repositories) error {
				settlements.Add(1)
				return nil
			})
		},
	}, nil)
	row := enqueueTestTask(t, harness, "settlement-lease", "settlement-lease")
	claimed := claimTestTask(t, harness)
	harness.repos.Tasks = renewFailureRepository{TaskRepository: harness.repos.Tasks}

	harness.engine.executeClaim(t.Context(), claimed)

	if got := settlements.Load(); got != 0 {
		t.Fatalf("settlement ran %d times without a confirmed lease", got)
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ResumeMode != model.TaskResumeModeRecover || stored.LeaseUntil == nil {
		t.Fatalf("task after unconfirmed settlement lease = %#v, err=%v", stored, err)
	}
	if stored.LeaseUntil.After(time.Now().Add(2 * harness.engine.config.PollInterval)) {
		t.Fatalf("unconfirmed settlement lease was not shortened: %s", stored.LeaseUntil)
	}
}

func TestClaimNextConcurrentClaimsAreUnique(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, nil)
	harness.db.SetMaxOpenConns(8)
	const total = 24
	for i := range total {
		enqueueTestTask(t, harness, fmt.Sprintf("claim-%02d", i), fmt.Sprintf("value-%02d", i))
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	claimedIDs := make(chan int64, total)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for ctx.Err() == nil {
				claimed, err := harness.repos.Tasks.ClaimNext(ctx, time.Minute)
				if err != nil {
					continue
				}
				if claimed == nil {
					return
				}
				claimedIDs <- claimed.ID
			}
		})
	}
	workers.Wait()
	close(claimedIDs)
	seen := make(map[int64]struct{}, total)
	for id := range claimedIDs {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("task %d was claimed twice", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("unique claims = %d, want %d", len(seen), total)
	}
}

func TestProviderResourceGateReachesAndEnforcesConfiguredLimit(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, nil)
	const calls = 12
	var running, maximum atomic.Int64
	entered := make(chan struct{}, calls)
	release := make(chan struct{})
	errorsCh := make(chan error, calls)
	var workers sync.WaitGroup
	for range calls {
		workers.Go(func() {
			err := harness.engine.withResource(t.Context(), ResourceProviderMutation, func(context.Context) error {
				current := running.Add(1)
				for {
					observed := maximum.Load()
					if current <= observed || maximum.CompareAndSwap(observed, current) {
						break
					}
				}
				entered <- struct{}{}
				<-release
				running.Add(-1)
				return nil
			})
			errorsCh <- err
		})
	}
	for range harness.engine.config.ProviderMutationConcurrency {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("provider mutation gate did not reach configured concurrency")
		}
	}
	if maximum.Load() != int64(harness.engine.config.ProviderMutationConcurrency) {
		t.Fatalf("provider mutation concurrency = %d", maximum.Load())
	}
	select {
	case <-entered:
		t.Fatal("provider mutation gate exceeded configured concurrency")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	workers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("provider resource call: %v", err)
		}
	}
	if maximum.Load() != 4 {
		t.Fatalf("provider mutation maximum = %d, want 4", maximum.Load())
	}
}
