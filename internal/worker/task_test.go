package worker

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
		WorkStart:    WorkStartOnEffect,
		InputVersion: 1,
		Codec: StrictJSONCodec(func(input *testInput) error {
			if input.Value == "" {
				return errors.New("value is required")
			}
			return nil
		}),
		Identity:   testIdentity,
		Policy:     ExecutionPolicy{MaxAttempts: testAttempts(retryLimit), Backoff: BackoffPolicy{InitialDelay: time.Nanosecond, Multiplier: 1, MaximumDelay: time.Nanosecond}},
		AllowRetry: allowRetry,
	}
}

// testIdentity names each test round by its input value.
func testIdentity(canonical json.RawMessage) (Identity, error) {
	var input testInput
	if err := json.Unmarshal(canonical, &input); err != nil {
		return Identity{}, err
	}
	return Identity{SubjectType: "test_subject", SubjectKey: input.Value, Generation: new(int64(0))}, nil
}

type taskHarness struct {
	db       *bun.DB
	repos    *repository.Repositories
	registry *Registry
	service  *Service
	engine   *Engine
}

func newTaskHarness(t *testing.T, handler Handler, config *EngineConfig, others ...Handler) taskHarness {
	t.Helper()
	return newTaskHarnessWithDB(t, testutil.NewTestDB(t), handler, config, others...)
}

func newTaskHarnessWithDB(t *testing.T, db *bun.DB, handler Handler, config *EngineConfig, others ...Handler) taskHarness {
	t.Helper()
	repos := repository.NewRepositories(db)
	registry := NewRegistry()
	if err := registry.Register(handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	for _, other := range others {
		if err := registry.Register(other); err != nil {
			t.Fatalf("register handler: %v", err)
		}
	}
	if handler.Definition().Type.IsRecurringSystem() {
		if err := registry.RegisterSchedule(ScheduleDefinition{Key: "test-cycle", Type: handler.Definition().Type, Interval: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	service, err := NewService(registry, repos)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	engineConfig := EngineConfig{
		Concurrency:   4,
		PollInterval:  10 * time.Millisecond,
		LeaseDuration: 5 * time.Second,
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

func enqueueTestTask(t *testing.T, harness taskHarness, value string) *model.Task {
	t.Helper()
	taskRow, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{Type: testTaskType, Input: testInput{Value: value}})
	if err != nil {
		t.Fatalf("enqueue task: %v", err)
	}
	if !created {
		t.Fatalf("task %s was not created", value)
	}
	return taskRow
}

func claimTestTask(t *testing.T, harness taskHarness) *model.Task {
	t.Helper()
	claimed, err := harness.repos.Tasks.ClaimNext(t.Context(), harness.engine.config.LeaseDuration, repository.TaskClaimFilter{})
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if claimed == nil {
		t.Fatal("claim returned no task")
	}
	return claimed
}

type rowInput struct {
	RowID int64  `json:"row_id"`
	Note  string `json:"note,omitempty"`
}

func rowDefinition() Definition {
	definition := testDefinition(nil, true)
	definition.Codec = StrictJSONCodec[rowInput](nil)
	definition.Identity = IdentityFromInput("test_row", func(input rowInput) (int64, int64) { return input.RowID, 0 })
	return definition
}

func TestServiceCanonicalEnqueueAndConflict(t *testing.T) {
	harness := newTaskHarness(t, scriptedHandler{definition: rowDefinition()}, nil)

	first, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, Input: map[string]any{"note": "alpha", "row_id": 1},
	})
	if err != nil || !created {
		t.Fatalf("first enqueue = task:%#v created:%v err:%v", first, created, err)
	}
	if !testutil.JSONEqual(first.Input, []byte(`{"row_id":1,"note":"alpha"}`)) {
		t.Fatalf("canonical input = %s", first.Input)
	}

	same, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, Input: rowInput{RowID: 1, Note: "alpha"},
	})
	if err != nil || created || same.ID != first.ID {
		t.Fatalf("idempotent enqueue = task:%#v created:%v err:%v", same, created, err)
	}

	_, _, err = harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, Input: rowInput{RowID: 1, Note: "beta"},
	})
	if !errors.Is(err, ErrInputConflict) {
		t.Fatalf("conflicting enqueue error = %v, want %v", err, ErrInputConflict)
	}

	_, _, err = harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, Input: map[string]any{"row_id": 2, "extra": true},
	})
	if err == nil {
		t.Fatal("enqueue with unknown input field succeeded")
	}
}

func TestServiceDerivesIdentityFromInput(t *testing.T) {
	harness := newTaskHarness(t, scriptedHandler{definition: rowDefinition()}, nil)

	derived, _, err := harness.service.Enqueue(t.Context(), EnqueueRequest{Type: testTaskType, Input: rowInput{RowID: 7}})
	if err != nil || derived.SubjectType != "test_row" || derived.SubjectKey != "7" || derived.Generation == nil || *derived.Generation != 0 {
		t.Fatalf("derived identity task = %#v, err=%v, want test_row/7 generation 0", derived, err)
	}
	if _, _, err := harness.service.Enqueue(t.Context(), EnqueueRequest{Type: testTaskType, Input: rowInput{RowID: 0}}); err == nil {
		t.Fatal("enqueue without a subject row succeeded")
	}
}

func TestServiceWakeMakesFuturePendingTaskReady(t *testing.T) {
	limit := 5
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: func(context.Context, Execution) Result {
		return WaitUntil(model.TaskResumeModeExecute, time.Now().Add(time.Hour), "dependency", "Waiting", nil)
	}}, nil)
	row, created, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, Input: testInput{Value: "wake"},
	})
	if err != nil || !created {
		t.Fatalf("enqueue future task = %#v created=%v err=%v", row, created, err)
	}
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	if claimed, err := harness.repos.Tasks.ClaimNext(t.Context(), time.Minute, repository.TaskClaimFilter{}); err != nil || claimed != nil {
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
		Type: "late_operation", InputVersion: 1, WorkStart: WorkStartOnEffect, Policy: ExecutionPolicy{MaxAttempts: 6, Backoff: DefaultBackoffPolicy()},
		Codec: StrictJSONCodec(func(input *testInput) error { return nil }), Identity: testIdentity,
	}})
	if !errors.Is(err, ErrRegistryFrozen) {
		t.Fatalf("late registry mutation error = %v, want frozen registry", err)
	}
}

func TestRegistryFreezesDefinitionAtRegistration(t *testing.T) {
	limit := 5
	handler := &scriptedHandler{definition: testDefinition(&limit, true)}
	handler.definition.Policy.ResendBackoff = &BackoffPolicy{InitialDelay: time.Minute, Multiplier: 2, MaximumDelay: 30 * time.Minute}
	registry := NewRegistry()
	if err := registry.Register(handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	handler.definition.InputVersion = 2
	handler.definition.AllowRetry = false
	handler.definition.Policy.ResendBackoff.InitialDelay = time.Hour
	limit = 9

	definition, ok := registry.Definition(testTaskType)
	if !ok || definition.InputVersion != 1 || !definition.AllowRetry || definition.Policy.MaxAttempts != 6 || definition.Policy.ResendBackoff.InitialDelay != time.Minute {
		t.Fatalf("registered definition changed = %#v", definition)
	}
	definition.Policy.MaxAttempts = 11
	definition.Policy.ResendBackoff.InitialDelay = time.Hour
	again, ok := registry.Definition(testTaskType)
	if !ok || again.Policy.MaxAttempts != 6 || again.Policy.ResendBackoff.InitialDelay != time.Minute {
		t.Fatalf("returned definition mutated registry = %#v", again)
	}
	execution := Execution{policy: again.Policy}
	execution.Policy().ResendBackoff.InitialDelay = time.Hour
	if execution.Policy().ResendBackoff.InitialDelay != time.Minute {
		t.Fatal("returned execution policy mutated the claim")
	}
}

func TestEngineSettlesAllFiveStates(t *testing.T) {
	limit := 0
	tests := []struct {
		name       string
		result     Result
		wantStatus model.TaskStatus
		wantMode   model.TaskResumeMode
		wantRetry  int
	}{
		{name: "completed", result: Complete("done", nil), wantStatus: model.TaskStatusCompleted, wantMode: model.TaskResumeModeRecover},
		{name: "pending", result: Wait(model.TaskResumeModeExecute, "scheduled", "later", nil), wantStatus: model.TaskStatusPending, wantMode: model.TaskResumeModeExecute},
		{name: "failed", result: Fail(errors.New("permanent"), "permanent_failure", nil), wantStatus: model.TaskStatusFailed, wantMode: model.TaskResumeModeRecover},
		{name: "cancelled", result: Cancel("superseded", nil), wantStatus: model.TaskStatusCancelled, wantMode: model.TaskResumeModeRecover},
		{name: "retry limit reached", result: Retry(errors.New("temporary"), "temporary_failure", nil), wantStatus: model.TaskStatusFailed, wantMode: model.TaskResumeModeRecover},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newTaskHarness(t, scriptedHandler{
				definition: testDefinition(&limit, true),
				execute:    func(context.Context, Execution) Result { return tt.result },
			}, nil)
			row := enqueueTestTask(t, harness, tt.name)
			claimed := claimTestTask(t, harness)
			harness.engine.executeClaim(t.Context(), claimed)

			stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil {
				t.Fatalf("get settled task: %v", err)
			}
			if stored.Status != tt.wantStatus || stored.ResumeMode != tt.wantMode || stored.RetryCount != tt.wantRetry {
				t.Fatalf("settled task = status:%s mode:%s retries:%d", stored.Status, stored.ResumeMode, stored.RetryCount)
			}
			if stored.Status != model.TaskStatusPending && stored.FinishedAt == nil {
				t.Fatal("terminal task has no finished time")
			}
			if stored.Status == model.TaskStatusCompleted || stored.Status == model.TaskStatusCancelled {
				again, claimErr := harness.repos.Tasks.ClaimNext(t.Context(), time.Minute, repository.TaskClaimFilter{})
				if claimErr != nil || again != nil {
					t.Fatalf("terminal task revived = %#v, err=%v", again, claimErr)
				}
			}
		})
	}
}

func TestEngineRejectsWaitWithError(t *testing.T) {
	result := Wait(model.TaskResumeModeRecover, "provider_confirmation", "Checking request", nil)
	result.err = errors.New("query failed")
	if !errors.Is(validateResult(result, nil), ErrInvalidResult) {
		t.Fatal("Wait accepted an error")
	}
}

func TestEngineNotifiesAfterSettlementCommits(t *testing.T) {
	limit := 1
	var notified atomic.Int64
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 5 * time.Second,
		OnTaskSettled: func(taskRow *model.Task, transition repository.TaskTransition) {
			if taskRow.SubjectKey != "notify" || transition.Status != model.TaskStatusCompleted {
				t.Errorf("settlement notification = task:%#v transition:%#v", taskRow, transition)
			}
			notified.Add(1)
		},
	}
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, &config)
	enqueueTestTask(t, harness, "notify")
	claimed := claimTestTask(t, harness)
	harness.engine.executeClaim(t.Context(), claimed)
	if notified.Load() != 1 {
		t.Fatalf("settlement notifications = %d, want 1", notified.Load())
	}
}

func TestRetryBackoffUsesPersistedRetryCount(t *testing.T) {
	limit := 5
	definition := testDefinition(&limit, true)
	definition.Policy.Backoff = BackoffPolicy{InitialDelay: time.Second, Multiplier: 3, MaximumDelay: time.Minute}
	harness := newTaskHarness(t, scriptedHandler{definition: definition}, nil)
	now := time.Now()
	row := enqueueTestTask(t, harness, "backoff")
	row.RetryCount = 3
	transition := harness.engine.transitionFor(row, Retry(errors.New("temporary"), "temporary_failure", nil))
	if transition.Status != model.TaskStatusPending || !transition.IncrementRetry {
		t.Fatalf("transition = %#v, want pending retry", transition)
	}
	if transition.AvailableAt.Before(now.Add(27*time.Second)) || transition.AvailableAt.After(time.Now().Add(27*time.Second)) {
		t.Fatalf("available at = %s, want 27 second delay", transition.AvailableAt)
	}
}

func TestDependencyRetryAtHonorsBudget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   int
		retries int
		allowed bool
	}{
		{name: "negative count", limit: 2, retries: -1},
		{name: "disabled", retries: 0},
		{name: "last allowed retry", limit: 2, retries: 1, allowed: true},
		{name: "exhausted", limit: 2, retries: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execution := Execution{policy: ExecutionPolicy{
				MaxAttempts:       1,
				DependencyRetries: tc.limit,
				Backoff:           BackoffPolicy{InitialDelay: time.Second, Multiplier: 2, MaximumDelay: 4 * time.Second},
			}}
			before := time.Now().Add(2 * time.Second)
			at, allowed := execution.DependencyRetryAt(tc.retries)
			after := time.Now().Add(2 * time.Second)
			if allowed != tc.allowed {
				t.Fatalf("allowed = %t, want %t", allowed, tc.allowed)
			}
			if !allowed {
				if !at.IsZero() {
					t.Fatalf("rejected retry time = %s", at)
				}
				return
			}
			if at.Before(before) || at.After(after) {
				t.Fatalf("retry time = %s, want between %s and %s", at, before, after)
			}
		})
	}
}

func TestResendAtRequiresDeclaredDeterministicPolicy(t *testing.T) {
	policy := ExecutionPolicy{MaxAttempts: 6, Backoff: DefaultBackoffPolicy(), ResendBackoff: &BackoffPolicy{InitialDelay: time.Minute, Multiplier: 2, MaximumDelay: 4 * time.Minute}}
	execution := Execution{policy: policy}
	sentAt := time.Date(2026, time.October, 12, 1, 2, 3, 0, time.UTC)
	for _, test := range []struct {
		sends int
		delay time.Duration
	}{{1, time.Minute}, {2, 2 * time.Minute}, {20, 4 * time.Minute}} {
		first, err := execution.ResendAt(sentAt, test.sends)
		second, repeatedErr := execution.ResendAt(sentAt, test.sends)
		if err != nil || repeatedErr != nil || !first.Equal(sentAt.Add(test.delay)) || !first.Equal(second) {
			t.Fatalf("resend %d = %s, repeated=%s errors=%v,%v", test.sends, first, second, err, repeatedErr)
		}
	}
	if _, err := execution.ResendAt(time.Time{}, 1); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("zero send time error = %v", err)
	}
	if _, err := execution.ResendAt(sentAt, 0); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("zero send count error = %v", err)
	}
	policy.ResendBackoff = nil
	if err := policy.validate(); err != nil {
		t.Fatalf("policy without resend work = %v", err)
	}
	if _, err := (Execution{policy: policy}).ResendAt(sentAt, 1); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("missing resend policy error = %v", err)
	}
	policy.ResendBackoff = &BackoffPolicy{InitialDelay: time.Minute, Multiplier: 2, MaximumDelay: time.Minute, Jitter: .1}
	if err := policy.validate(); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("jittered resend policy error = %v", err)
	}
	policy.ResendBackoff = &BackoffPolicy{InitialDelay: time.Minute, Multiplier: 0, MaximumDelay: time.Minute}
	if err := policy.validate(); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("invalid resend curve error = %v", err)
	}
}

func TestWaitUsesPolicyIntervalsAndAbsoluteDeadline(t *testing.T) {
	row := &model.Task{Type: testTaskType}
	deadline := time.Now().Add(time.Hour)
	for _, test := range []struct {
		name         string
		result       Result
		waitInterval time.Duration
		deadline     time.Time
		delay        time.Duration
	}{
		{name: "poll", result: Wait(model.TaskResumeModeRecover, "dependency", "Waiting", nil), delay: 17 * time.Second},
		{name: "type interval", result: Wait(model.TaskResumeModeExecute, "dependency", "Waiting", nil), waitInterval: 41 * time.Second, delay: 41 * time.Second},
		{name: "observation", result: WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Waiting", nil), waitInterval: 41 * time.Second, delay: 30 * time.Second},
		{name: "deadline", result: WaitUntil(model.TaskResumeModeRecover, deadline, "protocol", "Waiting", nil), waitInterval: 41 * time.Second, deadline: deadline},
		{name: "past deadline", result: WaitUntil(model.TaskResumeModeRecover, time.Now().Add(-time.Hour), "protocol", "Waiting", nil), waitInterval: 41 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			definition := testDefinition(nil, true)
			definition.Policy.WaitInterval = test.waitInterval
			registry := NewRegistry()
			if err := registry.Register(scriptedHandler{definition: definition}); err != nil {
				t.Fatal(err)
			}
			engine := &Engine{config: EngineConfig{PollInterval: 17 * time.Second}, registry: registry}
			now := time.Now()
			transition := engine.transitionFor(row, test.result)
			if transition.IncrementRetry || transition.Status != model.TaskStatusPending || transition.WaitReason == nil {
				t.Fatalf("wait transition = %#v", transition)
			}
			if !test.deadline.IsZero() {
				if !transition.AvailableAt.Equal(test.deadline) {
					t.Fatalf("wait deadline = %s, want %s", transition.AvailableAt, test.deadline)
				}
			} else if transition.AvailableAt.Before(now.Add(test.delay)) || transition.AvailableAt.After(time.Now().Add(test.delay)) {
				t.Fatalf("%s deadline = %s", test.name, transition.AvailableAt)
			}
		})
	}
	if err := validateResult(WaitUntil(model.TaskResumeModeRecover, time.Time{}, "protocol", "Waiting", nil), nil); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("zero wait deadline error = %v", err)
	}
}

func TestRegistryValidatesWaitAndDependencyPolicy(t *testing.T) {
	for _, tc := range []struct {
		name              string
		waitInterval      time.Duration
		interval          time.Duration
		dependencyRetries int
		want              time.Duration
		invalid           bool
	}{
		{name: "default", want: 30 * time.Second},
		{name: "observation", interval: 11 * time.Second, want: 11 * time.Second},
		{name: "ordinary", waitInterval: 41 * time.Second, want: 30 * time.Second},
		{name: "negative observation", interval: -time.Second, invalid: true},
		{name: "negative ordinary", waitInterval: -time.Second, invalid: true},
		{name: "negative dependency retries", dependencyRetries: -1, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition := testDefinition(nil, true)
			definition.Policy.WaitInterval = tc.waitInterval
			definition.Policy.ObservationInterval = tc.interval
			definition.Policy.DependencyRetries = tc.dependencyRetries
			registry := NewRegistry()
			err := registry.Register(scriptedHandler{definition: definition})
			if tc.invalid {
				if !errors.Is(err, ErrInvalidPolicy) {
					t.Fatalf("invalid policy: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			registered, ok := registry.Definition(testTaskType)
			if !ok || registered.Policy.ObservationInterval != tc.want || registered.Policy.WaitInterval != tc.waitInterval || definition.Policy.ObservationInterval != tc.interval || definition.Policy.WaitInterval != tc.waitInterval {
				t.Fatalf("registered policy=%#v input=%#v", registered.Policy, definition.Policy)
			}
			registered.Policy.ObservationInterval = time.Hour
			registered.Policy.WaitInterval = time.Hour
			again, _ := registry.Definition(testTaskType)
			if again.Policy.ObservationInterval != tc.want || again.Policy.WaitInterval != tc.waitInterval {
				t.Fatal("returned policy changed registry")
			}
		})
	}
}

func TestBackoffPolicyDelayIsExponentialJitteredAndCapped(t *testing.T) {
	tests := []struct {
		name             string
		attempt          int
		jitter           float64
		minimum, maximum time.Duration
	}{
		{name: "first retry", attempt: 0, minimum: 10 * time.Second, maximum: 10 * time.Second},
		{name: "third retry", attempt: 2, minimum: 40 * time.Second, maximum: 40 * time.Second},
		{name: "capped retry", attempt: 20, minimum: 5 * time.Minute, maximum: 5 * time.Minute},
		{name: "first retry with jitter", attempt: 0, jitter: .2, minimum: 8 * time.Second, maximum: 12 * time.Second},
		{name: "capped retry with jitter", attempt: 20, jitter: .2, minimum: 4 * time.Minute, maximum: 5 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := DefaultBackoffPolicy()
			policy.Jitter = tt.jitter
			if got := policy.delay(tt.attempt); got < tt.minimum || got > tt.maximum {
				t.Fatalf("delay = %s, want between %s and %s", got, tt.minimum, tt.maximum)
			}
		})
	}
}

func TestServiceRetryCreatesIndependentRoundAndAcknowledgeKeepsFailed(t *testing.T) {
	limit := 5
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: func(context.Context, Execution) Result { return Fail(errors.New("failed"), "test_failure", nil) }}, nil)
	row := enqueueTestTask(t, h, "retry")
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	original, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	next, err := h.service.Retry(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID <= row.ID || next.RetryOfTaskID == nil || *next.RetryOfTaskID != row.ID || next.RetryCount != 0 || next.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("successor: %#v", next)
	}
	again, err := h.service.Retry(t.Context(), row.ID)
	if err != nil || again.ID != next.ID {
		t.Fatalf("replay: %#v %v", again, err)
	}
	stored, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if stored.Status != model.TaskStatusFailed || stored.SupersededAt == nil || stored.RetryCount != original.RetryCount || dereference(stored.LastError) != dereference(original.LastError) || !stored.FinishedAt.Equal(*original.FinishedAt) {
		t.Fatalf("source changed: %#v", stored)
	}
	if err := h.service.Acknowledge(t.Context(), row.ID); err != nil {
		t.Fatal(err)
	}
	stored, _ = h.repos.Tasks.GetByID(t.Context(), row.ID)
	if stored.Status != model.TaskStatusFailed || stored.AcknowledgedAt == nil {
		t.Fatalf("ack: %#v", stored)
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
	row := enqueueTestTask(t, harness, "unsafe-retry")
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))

	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored == nil || stored.Status != model.TaskStatusFailed {
		t.Fatalf("failed task = %#v, err=%v", stored, err)
	}
	if harness.service.Retryable(stored) {
		t.Fatal("unsafe failed task is retryable")
	}
	if _, err := harness.service.Retry(t.Context(), row.ID); !errors.Is(err, ErrRetryUnsupported) {
		t.Fatalf("Retry = %v, want ErrRetryUnsupported", err)
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
	enqueueTestTask(t, harness, "takeover")
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
	if err := harness.repos.Tasks.MarkWorkStarted(t.Context(), stale.ID, stale.ClaimGeneration, time.Now()); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("stale work start error = %v", err)
	}
	if err := harness.repos.Tasks.Settle(t.Context(), stale.ID, stale.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover,
	}); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("stale settlement error = %v", err)
	}

	harness.engine.executeClaim(t.Context(), fresh)
	if executes.Load() != 0 || recovers.Load() != 1 {
		t.Fatalf("handler calls = execute:%d recover:%d", executes.Load(), recovers.Load())
	}
}

func TestCompatibleInputChangesPreserveEnqueueExecutionAndRetry(t *testing.T) {
	type reorderedInput struct {
		Note  string `json:"note,omitempty"`
		RowID int64  `json:"row_id"`
	}
	type extendedInput struct {
		RowID    int64  `json:"row_id"`
		Note     string `json:"note,omitempty"`
		Priority int    `json:"priority"`
	}
	for _, tc := range []struct {
		name      string
		codec     Codec
		version   int
		canonical string
	}{
		{"jsonb normalization", StrictJSONCodec[rowInput](nil), 1, `{"row_id":1,"note":"alpha"}`},
		{"field order", StrictJSONCodec[reorderedInput](nil), 1, `{"note":"alpha","row_id":1}`},
		{"optional default", StrictJSONCodec[extendedInput](nil), 1, `{"row_id":1,"note":"alpha","priority":0}`},
		{"unsupported version", StrictJSONCodec[rowInput](nil), 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := newTaskHarness(t, scriptedHandler{definition: rowDefinition()}, nil)
			row, _, err := old.service.Enqueue(t.Context(), EnqueueRequest{Type: testTaskType, Input: rowInput{RowID: 1, Note: "alpha"}})
			if err != nil {
				t.Fatal(err)
			}
			definition := rowDefinition()
			definition.Codec, definition.InputVersion = tc.codec, tc.version
			calls := 0
			checkInput := func(execution Execution) {
				calls++
				if string(execution.Input()) != tc.canonical {
					t.Fatalf("handler input = %s, want %s", execution.Input(), tc.canonical)
				}
			}
			current := newTaskHarnessWithDB(t, old.db, scriptedHandler{
				definition: definition,
				execute: func(_ context.Context, execution Execution) Result {
					checkInput(execution)
					return Fail(errors.New("stopped"), "stopped", nil)
				},
				recover: func(_ context.Context, execution Execution) Result {
					checkInput(execution)
					return Complete("Recovered", nil)
				},
			}, nil)
			duplicate, created, err := current.service.Enqueue(t.Context(), EnqueueRequest{Type: testTaskType, Input: rowInput{RowID: 1, Note: "alpha"}})
			if tc.version != 1 {
				if !errors.Is(err, ErrInputConflict) {
					t.Fatalf("version conflict = %v", err)
				}
				current.engine.executeClaim(t.Context(), claimTestTask(t, current))
				failed, err := current.repos.Tasks.GetByID(t.Context(), row.ID)
				if err != nil || failed.Status != model.TaskStatusFailed || dereference(failed.FailureReason) != "input_version_unsupported" || calls != 0 {
					t.Fatalf("unsupported input result = %#v, calls=%d, err=%v", failed, calls, err)
				}
				if _, err := current.service.Retry(t.Context(), row.ID); !errors.Is(err, repository.ErrConflict) {
					t.Fatalf("unsupported input retry = %v", err)
				}
				return
			}
			if err != nil || created || duplicate.ID != row.ID {
				t.Fatalf("compatible enqueue = %#v, created=%v, err=%v", duplicate, created, err)
			}
			if tc.name == "optional default" {
				_, _, err := current.service.Enqueue(t.Context(), EnqueueRequest{Type: testTaskType, Input: extendedInput{RowID: 1, Note: "alpha", Priority: 1}})
				if !errors.Is(err, ErrInputConflict) {
					t.Fatalf("nondefault input conflict = %v", err)
				}
			}
			current.engine.executeClaim(t.Context(), claimTestTask(t, current))
			next, err := current.service.Retry(t.Context(), row.ID)
			if err != nil {
				t.Fatal(err)
			}
			current.engine.executeClaim(t.Context(), claimTestTask(t, current))
			completed, err := current.repos.Tasks.GetByID(t.Context(), next.ID)
			if err != nil || completed.Status != model.TaskStatusCompleted || calls != 2 {
				t.Fatalf("compatible retry = %#v, calls=%d, err=%v", completed, calls, err)
			}
		})
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
	row := enqueueTestTask(t, harness, "codec-panic")
	panicCodec.Store(true)
	if _, _, err := harness.service.Enqueue(t.Context(), EnqueueRequest{
		Type: testTaskType, Input: testInput{Value: "value"},
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
		Type: testTaskType, Input: testInput{Value: "value"},
	}); !errors.Is(err, ErrInvalidCanonical) {
		t.Fatalf("enqueue invalid canonical error = %v, want ErrInvalidCanonical", err)
	}
}

func TestUnexpectedClaimPanicForcesRecoveryWithoutKillingWorker(t *testing.T) {
	limit := 5
	config := EngineConfig{Concurrency: 1, PollInterval: 100 * time.Millisecond, LeaseDuration: 5 * time.Second}
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(context.Context, Execution) Result {
			return Retry(panicError{}, "formatting_failed", nil)
		},
	}, &config)
	shortener := &delayedShortenRepository{TaskRepository: harness.repos.Tasks}
	controlled := &controlledShortenRepository{TaskRepository: shortener}
	controlled.fail.Store(true)
	harness.repos.Tasks = controlled
	row := enqueueTestTask(t, harness, "claim-panic")
	harness.engine.executeClaimSafely(t.Context(), claimTestTask(t, harness))
	controlled.fail.Store(false)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	done := make(chan struct{})
	go func() {
		harness.engine.runRecoveryQueue(ctx)
		close(done)
	}()
	stopRecovery := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("panic recovery queue did not stop")
		}
	}
	t.Cleanup(stopRecovery)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !shortener.shortened.Load() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("panic recovery did not shorten the task lease")
		}
	}
	stopRecovery()

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
			return Retry(errors.New("temporary"), "", nil)
		}, wantReason: "invalid_result"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true), execute: tt.execute}, nil)
			row := enqueueTestTask(t, harness, tt.name)
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
	row := enqueueTestTask(t, harness, "shutdown")
	claimed := claimTestTask(t, harness)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	harness.engine.executeClaim(ctx, claimed)
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning || stored.ClaimGeneration != claimed.ClaimGeneration || stored.FinishedAt != nil {
		t.Fatalf("shutdown task = %#v, err=%v", stored, err)
	}
	if _, err := harness.db.ExecContext(t.Context(), `UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), row.ID); err != nil {
		t.Fatal(err)
	}
	recovered := claimTestTask(t, harness)
	if recovered.ID != row.ID || recovered.ResumeMode != model.TaskResumeModeRecover || recovered.ClaimGeneration != claimed.ClaimGeneration+1 {
		t.Fatalf("shutdown recovery claim = %#v", recovered)
	}
}

func TestRecoverCannotStartExternalEffect(t *testing.T) {
	limit := 5
	var called atomic.Bool
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		recover: func(ctx context.Context, execution Execution) Result {
			_, err := execution.WithCheckpointedEffect(ctx, "test-effect", map[string]bool{"attempted": true}, nil, func(context.Context) error {
				called.Store(true)
				return nil
			})
			if !errors.Is(err, ErrEffectForbidden) {
				return Fail(fmt.Errorf("effect error = %v", err), "unexpected_effect_error", nil)
			}
			return Complete("recovery remained read-only", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "recover-effect")
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
		cancelled       bool
		admit           func(context.Context, string, any, Settlement) error
		wantAttempted   bool
		wantEffectCalls int
		wantError       error
	}{
		{name: "cancelled context", mode: model.TaskResumeModeExecute, cancelled: true, wantError: context.Canceled},
		{
			name: "checkpoint fails", mode: model.TaskResumeModeExecute,
			admit: func(context.Context, string, any, Settlement) error { return injected }, wantError: injected,
		},
		{
			name: "effect fails", mode: model.TaskResumeModeExecute,
			admit:         func(context.Context, string, any, Settlement) error { return nil },
			wantAttempted: true, wantEffectCalls: 1, wantError: injected,
		},
		{name: "recovery is forbidden", mode: model.TaskResumeModeRecover, wantError: ErrEffectForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effectCalls := 0
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.cancelled {
				cancel()
			}
			execution := Execution{task: model.Task{ResumeMode: tt.mode}, admit: tt.admit}
			if execution.admit == nil {
				execution.admit = func(context.Context, string, any, Settlement) error {
					t.Fatal("forbidden effect reached admission")
					return nil
				}
			}
			attempted, err := execution.WithCheckpointedEffect(ctx, "test-effect", map[string]bool{"attempted": true}, nil,
				func(context.Context) error { effectCalls++; return injected })
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
			attempted, err := execution.WithCheckpointedEffect(ctx, "test-effect", map[string]bool{"attempted": true}, func(ctx context.Context, repos *repository.Repositories) error {
				if err := repos.Tasks.AppendEvent(ctx, execution.ID(), "rejected_evidence", json.RawMessage(`{}`)); err != nil {
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
	row := enqueueTestTask(t, harness, "checkpoint-rollback")
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusFailed || stored.FailureReason == nil || *stored.FailureReason != "effect_not_started" || len(stored.Checkpoint) != 0 || stored.WorkStartedAt != nil {
		t.Fatalf("rolled-back task = %#v, err=%v", stored, err)
	}
	if effectCalled.Load() {
		t.Fatal("effect ran after checkpoint settlement failed")
	}
	events, err := harness.repos.Tasks.ListEvents(t.Context(), row.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == "rejected_evidence" {
			t.Fatal("checkpoint evidence committed after settlement failed")
		}
	}
}

func TestCheckpointedEffectCommitsCheckpointBeforeEffect(t *testing.T) {
	assertCheckpointedEffectCommitsCheckpointBeforeEffect(t, testutil.NewTestDB(t))
}

func assertCheckpointedEffectCommitsCheckpointBeforeEffect(t *testing.T, db *bun.DB) {
	t.Helper()
	limit := 5
	var repos *repository.Repositories
	var duringEffect json.RawMessage
	harness := newTaskHarnessWithDB(t, db, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(ctx context.Context, execution Execution) Result {
			attempted, err := execution.WithCheckpointedEffect(ctx, "test-effect", map[string]string{"attempt": "one"}, nil, func(ctx context.Context) error {
				stored, err := repos.Tasks.GetByID(ctx, execution.ID())
				if err != nil {
					return err
				}
				duringEffect = stored.Checkpoint
				if stored.WorkStartedAt == nil {
					return errors.New("work start was not committed before the effect")
				}
				return nil
			})
			if !attempted || err != nil {
				return Fail(fmt.Errorf("checkpointed effect = attempted:%v err:%v", attempted, err), "unexpected_effect_result", nil)
			}
			return Complete("effect finished", nil)
		},
	}, nil)
	repos = harness.repos
	row := enqueueTestTask(t, harness, "checkpoint-commit")
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))

	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil {
		t.Fatalf("load task: %v", err)
	}
	if stored.Status != model.TaskStatusCompleted {
		t.Fatalf("task after effect = %#v", stored)
	}
	for label, raw := range map[string]json.RawMessage{"during effect": duringEffect, "after settlement": stored.Checkpoint} {
		var checkpoint map[string]string
		if err := json.Unmarshal(raw, &checkpoint); err != nil || checkpoint["attempt"] != "one" {
			t.Fatalf("checkpoint %s = %s, err=%v", label, raw, err)
		}
	}
}

func TestExternalEffectRejectsStaleClaim(t *testing.T) {
	limit := 5
	var called atomic.Bool
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(ctx context.Context, execution Execution) Result {
			_, err := execution.WithCheckpointedEffect(ctx, "test-effect", map[string]bool{"attempted": true}, nil, func(context.Context) error {
				called.Store(true)
				return nil
			})
			if err != nil {
				return Fail(err, "effect_claim_lost", nil)
			}
			return Complete("effect completed", nil)
		},
	}, nil)
	row := enqueueTestTask(t, harness, "stale-effect")
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
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second,
	}
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute: func(ctx context.Context, _ Execution) Result {
			<-ctx.Done()
			return Complete("must be discarded", nil)
		},
	}, &config)
	row := enqueueTestTask(t, harness, "renewal")
	claimed := claimTestTask(t, harness)
	controlled := &controlledShortenRepository{TaskRepository: renewFailureRepository{TaskRepository: harness.repos.Tasks}}
	controlled.fail.Store(true)
	harness.repos.Tasks = controlled
	harness.engine.renewalRetryDelays = []time.Duration{0, time.Second}
	started := time.Now()
	harness.engine.executeClaim(t.Context(), claimed)
	if elapsed := time.Since(started); elapsed >= config.LeaseDuration-config.LeaseDuration/3+30*time.Millisecond {
		t.Fatalf("renewal uncertainty cancelled after %s", elapsed)
	}
	stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != model.TaskStatusRunning {
		t.Fatalf("renewal failure task = %#v, err=%v", stored, err)
	}
	if stored.ResumeMode != model.TaskResumeModeRecover {
		request, queued := harness.engine.nextRecovery()
		if !queued || request.id != row.ID || request.generation != claimed.ClaimGeneration {
			t.Fatalf("renewal failure recovery = %#v, queued=%t; task=%#v", request, queued, stored)
		}
	}
}

func TestEngineSuccessfulRenewalKeepsHealthCurrent(t *testing.T) {
	limit := 5
	config := EngineConfig{
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second,
	}
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, &config)
	enqueueTestTask(t, harness, "renewal-health")
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
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second,
	}
	harness := newTaskHarness(t, scriptedHandler{
		definition: testDefinition(&limit, true),
		execute:    func(context.Context, Execution) Result { return Complete("first", nil) },
		recover: func(context.Context, Execution) Result {
			recovers.Add(1)
			return Complete("recovered", nil)
		},
	}, &config)
	row := enqueueTestTask(t, harness, "settlement")
	claimed := claimTestTask(t, harness)
	ordered := &delayedShortenRepository{
		TaskRepository: harness.repos.Tasks,
		delay:          2 * config.LeaseDuration / 3,
	}
	harness.repos.Tasks = ordered
	restoreFailTaskSettlement := testutil.FailWrites(t, harness.db, "tasks", "UPDATE", "NEW.status = 'completed'", "injected settlement failure")
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
	restoreFailTaskSettlement()
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
	row := enqueueTestTask(t, harness, "settlement-panic")
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
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: time.Minute,
	}
	harness := newTaskHarness(t, scriptedHandler{definition: testDefinition(&limit, true)}, &config)
	controlled := &controlledShortenRepository{TaskRepository: harness.repos.Tasks}
	controlled.fail.Store(true)
	harness.repos.Tasks = controlled

	claimed := make([]*model.Task, 0, 3)
	for i := range 3 {
		enqueueTestTask(t, harness, fmt.Sprintf("value-%d", i))
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
		Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseDuration: 3 * time.Second,
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
	row := enqueueTestTask(t, harness, "settlement-renewal")
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
	row := enqueueTestTask(t, harness, "settlement-lease")
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
		enqueueTestTask(t, harness, fmt.Sprintf("value-%02d", i))
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	claimedIDs := make(chan int64, total)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for ctx.Err() == nil {
				claimed, err := harness.repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{})
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

func testAttempts(limit *int) int {
	if limit == nil {
		return 6
	}
	return *limit + 1
}
