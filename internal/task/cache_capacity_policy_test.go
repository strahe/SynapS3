package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/systemtask"
	"github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/uptrace/bun"
)

func TestCacheCapacityIdleChecksKeepOneRound(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyLRU, maxBytes: 100, highPercent: 90, lowPercent: 60})
	planner, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	var generation int64
	for range 3 {
		current := waitForTask(t, runtime.repos, planner.ID, func(row *model.Task) bool {
			return row.Status == model.TaskStatusPending && row.WaitReason != nil && *row.WaitReason == "scheduled" && row.ClaimGeneration > generation
		})
		if current.FinishedAt != nil || current.RetryCount != 0 || current.ResumeMode != model.TaskResumeModeExecute || !current.AvailableAt.After(time.Now().Add(4*time.Second)) {
			t.Fatalf("idle capacity round = %#v", current)
		}
		generation = current.ClaimGeneration
		wakeTask(t, runtime, planner.ID)
	}
	history, err := runtime.repos.Tasks.List(t.Context(), repository.TaskListFilter{Scope: repository.TaskScopeHistory, Type: model.TaskTypeCacheCapacityReconcile})
	if err != nil || len(history.Tasks) != 0 {
		t.Fatalf("idle capacity history = %#v, err=%v", history, err)
	}
}

type capacityArchiveObserver struct {
	failed   chan struct{}
	reported atomic.Bool
}

func (*capacityArchiveObserver) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *capacityArchiveObserver) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if event.Err != nil && strings.Contains(event.Query, "task_history") && !h.reported.Swap(true) {
		close(h.failed)
	}
}

func TestCacheCapacityCompletionPreservesDemandUntilArchiveCommits(t *testing.T) {
	for _, policy := range []cache.EvictionPolicy{cache.EvictionPolicyLRU, cache.EvictionPolicyAfterUpload} {
		t.Run(string(policy), func(t *testing.T) {
			checkpoint := json.RawMessage(`{"cycle_active":true,"refused_write_bytes":20}`)
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: policy, maxBytes: 100, highPercent: 90, lowPercent: 60, commitSealOnCachePressure: policy == cache.EvictionPolicyAfterUpload})
			planner, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", checkpoint).Where("id = ?", planner.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			observer := &capacityArchiveObserver{failed: make(chan struct{})}
			runtime.db.AddQueryHook(observer)
			if _, err := runtime.db.ExecContext(t.Context(), `CREATE TRIGGER reject_capacity_archive BEFORE INSERT ON task_history BEGIN SELECT RAISE(ABORT, 'injected capacity archive failure'); END`); err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			select {
			case <-observer.failed:
			case <-time.After(3 * time.Second):
				t.Fatal("capacity completion did not reach the archive fault")
			}
			current, err := runtime.repos.Tasks.GetByID(t.Context(), planner.ID)
			if err != nil || current.Status != model.TaskStatusRunning || string(current.Checkpoint) != string(checkpoint) {
				t.Fatalf("uncommitted completion lost demand: %#v, err=%v", current, err)
			}
			if _, err := runtime.db.ExecContext(t.Context(), `DROP TRIGGER reject_capacity_archive`); err != nil {
				t.Fatal(err)
			}
			completed := waitForTask(t, runtime.repos, planner.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
			var cp struct {
				CycleActive       bool  `json:"cycle_active"`
				RefusedWriteBytes int64 `json:"refused_write_bytes"`
			}
			if err := json.Unmarshal(completed.Checkpoint, &cp); err != nil || cp.RefusedWriteBytes != 0 || policy == cache.EvictionPolicyLRU && cp.CycleActive {
				t.Fatalf("completed capacity checkpoint = %s, err=%v", completed.Checkpoint, err)
			}
		})
	}
}

func TestCacheCapacityHealthyCycleClearsPreviousFailureOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy cache.EvictionPolicy
		legacy bool
	}{
		{"lru/direct", cache.EvictionPolicyLRU, false},
		{"disabled/direct", cache.EvictionPolicyNone, false},
		{"lru/legacy", cache.EvictionPolicyLRU, true},
		{"disabled/legacy", cache.EvictionPolicyNone, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime := newScheduledCapacityRuntime(t, handlerRuntimeOptions{policy: tc.policy, maxBytes: 100, highPercent: 90, lowPercent: 60})
			const key = "capacity-policy"
			source, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: key + ":1", Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity"})
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.TaskSchedules.Ensure(t.Context(), key, time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.TaskSchedules.SetHead(t.Context(), key, 0, 1, nil, &source.ID, time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("failure_reason = ?", "test_capacity_failed").Set("finished_at = ?", time.Now()).Where("id = ?", source.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			healthyKey, nextKey, historyCount := key+":2", key+":3", 2
			if tc.legacy {
				legacy, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: source.Type, IdempotencyKey: key + ":2", Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity"})
				if err != nil {
					t.Fatal(err)
				}
				policy := map[string]any{}
				if err := json.Unmarshal(legacy.Policy, &policy); err != nil {
					t.Fatal(err)
				}
				policy["version"], policy["legacy"] = 0, true
				encoded, err := json.Marshal(policy)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("policy_json = ?", encoded).Set("wait_reason = ?", "scheduled").Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", legacy.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := runtime.repos.TaskSchedules.SetHead(t.Context(), key, 1, 2, &source.ID, &legacy.ID, time.Now().Add(-time.Second)); err != nil {
					t.Fatal(err)
				}
				healthyKey, nextKey, historyCount = key+":3", key+":4", 3
			}
			observer := &capacityArchiveObserver{failed: make(chan struct{})}
			runtime.db.AddQueryHook(observer)
			if _, err := runtime.db.ExecContext(t.Context(), `CREATE TRIGGER reject_healthy_capacity_archive BEFORE INSERT ON task_history WHEN NEW.status = 'completed' BEGIN SELECT RAISE(ABORT, 'injected healthy capacity archive failure'); END`); err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			select {
			case <-observer.failed:
			case <-time.After(3 * time.Second):
				t.Fatal("healthy capacity completion did not reach the archive fault")
			}
			unsettled, err := runtime.repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeCacheCapacityReconcile, healthyKey)
			if err != nil || unsettled == nil || unsettled.Status != model.TaskStatusRunning || !strings.Contains(string(unsettled.Checkpoint), `"previous_cycle_failed":true`) {
				t.Fatalf("uncommitted healthy completion lost failure evidence: %#v, err=%v", unsettled, err)
			}
			previous, err := runtime.repos.Tasks.GetByID(t.Context(), source.ID)
			if err != nil || previous == nil || previous.Status != model.TaskStatusFailed || previous.SupersededAt != nil {
				t.Fatalf("uncommitted healthy completion hid the previous failure: %#v, err=%v", previous, err)
			}
			if _, err := runtime.db.ExecContext(t.Context(), `DROP TRIGGER reject_healthy_capacity_archive`); err != nil {
				t.Fatal(err)
			}
			waitForTask(t, runtime.repos, source.ID, func(row *model.Task) bool { return row.SupersededAt != nil })
			completed, err := runtime.repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeCacheCapacityReconcile, healthyKey)
			if err != nil || completed == nil || completed.Status != model.TaskStatusCompleted || strings.Contains(string(completed.Checkpoint), "previous_cycle_failed") {
				t.Fatalf("first healthy cycle = %#v, err=%v", completed, err)
			}
			if err := runtime.repos.TaskSchedules.ScheduleNext(t.Context(), key, completed.ID, time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			var next *model.Task
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				next, err = runtime.repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeCacheCapacityReconcile, nextKey)
				if err != nil {
					t.Fatal(err)
				}
				if next != nil {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if next == nil {
				t.Fatal("next capacity cycle was not dispatched")
			}
			idle := waitForTask(t, runtime.repos, next.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusPending && row.WaitReason != nil && *row.WaitReason == "scheduled"
			})
			generation := idle.ClaimGeneration
			wakeTask(t, runtime, idle.ID)
			waitForTask(t, runtime.repos, idle.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusPending && row.ClaimGeneration > generation
			})
			history, err := runtime.repos.Tasks.List(t.Context(), repository.TaskListFilter{Scope: repository.TaskScopeHistory, Type: model.TaskTypeCacheCapacityReconcile})
			if err != nil || len(history.Tasks) != historyCount {
				t.Fatalf("healthy idle cycle created extra history: %#v, err=%v", history, err)
			}
		})
	}
}

func TestCacheCapacityMergesRefusalBeforeCompletingAfterUpload(t *testing.T) {
	for _, failCheckpoint := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpoint_failure_%t", failCheckpoint), func(t *testing.T) {
			var consumed atomic.Bool
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyAfterUpload, maxBytes: 100, commitSealOnCachePressure: true, cache: &testutil.MockCache{
				CapacitySnapshotFunc: func() cache.CapacitySnapshot { return cache.CapacitySnapshot{UsedBytes: 70, MaxBytes: 100} },
				ConsumeRefusedWriteBytesFunc: func() int64 {
					if !consumed.Swap(true) {
						return 40
					}
					return 0
				},
			}})
			planner, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(`{"cycle_active":false,"refused_write_bytes":20}`)).Where("id = ?", planner.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if failCheckpoint {
				if _, err := runtime.db.ExecContext(t.Context(), `CREATE TRIGGER reject_capacity_demand BEFORE UPDATE OF checkpoint_json ON tasks BEGIN SELECT RAISE(ABORT, 'injected capacity demand failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			if failCheckpoint {
				waitForTask(t, runtime.repos, planner.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusPending && row.RetryCount > 0 })
				if _, err := runtime.db.ExecContext(t.Context(), `DROP TRIGGER reject_capacity_demand`); err != nil {
					t.Fatal(err)
				}
				wakeTask(t, runtime, planner.ID)
			}
			current := waitForTask(t, runtime.repos, planner.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusPending && row.WaitReason != nil && *row.WaitReason == "cache_cleanup"
			})
			var cp struct {
				RefusedWriteBytes int64 `json:"refused_write_bytes"`
			}
			if err := json.Unmarshal(current.Checkpoint, &cp); err != nil || cp.RefusedWriteBytes != 40 || current.FinishedAt != nil {
				t.Fatalf("new refusal was dropped after meeting the old target: %#v, err=%v", current, err)
			}
		})
	}
}

func TestCacheCapacityNextCyclePreservesUnparseableDemand(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyNone})
	definition, ok := runtime.registry.Definition(model.TaskTypeCacheCapacityReconcile)
	if !ok {
		t.Fatal("capacity definition is unavailable")
	}
	checkpoint := json.RawMessage(`{"cycle_active":true,"refused_write_bytes":"unknown"}`)
	inherited, err := definition.NextCycleCheckpoint(t.Context(), runtime.repos, &model.Task{Status: model.TaskStatusFailed, Checkpoint: checkpoint})
	if err != nil {
		t.Fatal(err)
	}
	planner, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", inherited).Where("id = ?", planner.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, planner.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "invalid_checkpoint" || string(failed.Checkpoint) != string(checkpoint) {
		t.Fatalf("unparseable demand was discarded or reported as healthy: %#v", failed)
	}
}

func newScheduledCapacityRuntime(t *testing.T, options handlerRuntimeOptions) handlerTestRuntime {
	t.Helper()
	runtime := newHandlerTestRuntime(t, options)
	registry := taskengine.NewRegistry()
	service, err := taskengine.NewService(registry, runtime.repos)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.Register(registry, service, task.Dependencies{Repositories: runtime.repos, Cache: runtime.cache, CacheGate: runtime.gate, CacheTracker: runtime.tracker, Storage: runtime.storage, EvictionPolicy: options.policy, MaxCacheBytes: options.maxBytes, LRUHighPercent: options.highPercent, LRULowPercent: options.lowPercent, DefaultCopies: 1, Logger: slog.Default()}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterSchedule(taskengine.ScheduleDefinition{Key: "capacity-policy", Type: model.TaskTypeCacheCapacityReconcile, Subject: taskengine.Subject{Type: "system", Key: "cache-capacity"}, Input: systemtask.Input{}, Interval: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration}, runtime.repos, registry, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	runtime.registry, runtime.service, runtime.engine = registry, service, engine
	return runtime
}

func TestCacheCapacityLRUPlansEachFailedCandidateOnceAcrossBatches(t *testing.T) {
	// Retrying a full batch atomically uses the production lease.
	cfg, err := config.DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	leaseDuration := cfg.Worker.Tasks.LeaseDuration
	const plannerWatchdog = 30 * time.Second
	for _, failedCount := range []int{50, 100} {
		t.Run(fmt.Sprintf("%d_failed", failedCount), func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyLRU, maxBytes: 200, highPercent: 50, lowPercent: 10, cache: &testutil.MockCache{
				CapacitySnapshotFunc: func() cache.CapacitySnapshot { return cache.CapacitySnapshot{UsedBytes: 150, MaxBytes: 200} },
				DeleteFunc:           func(ctx context.Context, _, _ string) error { <-ctx.Done(); return ctx.Err() },
			}})
			base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
			var failures []int64
			for i := range 150 {
				version := seedStoredCacheObject(t, runtime, 1, base.Add(time.Duration(i)*time.Second))
				if i >= failedCount {
					continue
				}
				reservation, err := runtime.repos.CacheEvictions.PrepareEviction(t.Context(), *version.ContentID)
				if err != nil {
					t.Fatal(err)
				}
				accessedAt := base.Add(time.Duration(i) * time.Second)
				source, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(*version.ContentID, reservation.Generation), Input: cacheeviction.EvictInput{ContentID: *version.ContentID, Generation: reservation.Generation, AccessedAt: &accessedAt}})
				if err != nil {
					t.Fatal(err)
				}
				if err := runtime.repos.CacheEvictions.BindEvictionTask(t.Context(), *version.ContentID, reservation.Generation, source.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("failure_reason = ?", "test_eviction_failed").Set("finished_at = ?", time.Now()).Where("id = ?", source.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				failures = append(failures, source.ID)
			}
			planner, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}})
			if err != nil {
				t.Fatal(err)
			}
			prepareHandlerTaskFixtures(t, runtime)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			settled := make(chan repository.TaskTransition, 1)
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{
				Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: leaseDuration,

				OnTaskSettled: func(claimed *model.Task, transition repository.TaskTransition) {
					if claimed.ID == planner.ID {
						settled <- transition
						cancel()
					}
				},
			}, runtime.repos, runtime.registry, nil)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { defer close(done); _ = engine.Run(ctx) }()
			defer stopHandlerEngine(t, cancel, done)
			select {
			case transition := <-settled:
				if transition.Status != model.TaskStatusPending {
					t.Fatalf("capacity planner settlement = %#v", transition)
				}
			case <-time.After(plannerWatchdog):
				t.Fatal("capacity planner did not settle")
			}
			stopHandlerEngine(t, cancel, done)
			current, err := runtime.repos.Tasks.GetByID(t.Context(), planner.ID)
			if err != nil || current == nil || current.Status != model.TaskStatusPending || current.RetryCount != 1 {
				t.Fatalf("capacity planner = %#v, err=%v", current, err)
			}
			for _, id := range failures {
				if next, err := runtime.repos.Tasks.GetDirectSuccessor(t.Context(), id); err != nil || next == nil {
					t.Fatalf("failed candidate %d successor = %#v, err=%v", id, next, err)
				}
			}
			var owners int
			if err := runtime.db.NewSelect().Model((*model.ObjectCache)(nil)).Where("cache_active_task_id IS NOT NULL").ColumnExpr("COUNT(*)").Scan(t.Context(), &owners); err != nil || owners != 130 {
				t.Fatalf("planned unique capacity = %d bytes, err=%v, want 130", owners, err)
			}
		})
	}
}
