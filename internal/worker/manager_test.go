package worker_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

func TestManager_RecoverOnStartup_ReleasesExpiredLeases(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	// Create a bucket and object for the task
	bucket := testutil.SeedBucket(t, db, "mgr-lease-bucket")
	obj := &model.Object{
		BucketID:   bucket.ID,
		Key:        "mgr-lease-key",
		Size:       100,
		ETag:       "etag1",
		State:      model.ObjectStateCached,
		Generation: 1,
	}
	if _, err := db.NewInsert().Model(obj).Exec(ctx); err != nil {
		t.Fatalf("inserting object: %v", err)
	}

	// Create a task and claim it, then manually set its lease_until to the past
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          obj.ID,
		RefGeneration:  1,
		IdempotencyKey: "upload:lease:1",
		Status:         model.TaskStatusRunning,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("creating task: %v", err)
	}

	// Set lease_until and claimed_at to past (expired lease)
	pastLease := time.Now().Add(-1 * time.Hour)
	_, err := db.NewUpdate().Model((*model.Task)(nil)).
		Set("claimed_at = ?", pastLease).
		Set("lease_until = ?", pastLease).
		Set("started_at = ?", pastLease).
		Where("id = ?", task.ID).
		Exec(ctx)
	if err != nil {
		t.Fatalf("updating lease: %v", err)
	}

	// Start manager with no workers — returns immediately after recovery
	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	// Task should be back to pending
	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("getting task: %v", err)
	}
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task status pending after lease release, got %s", got.Status)
	}
}

func TestManager_RecoverOnStartup_ResetsStaleStates(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-stale-bucket")

	// Create an object in "uploading" state
	obj := &model.Object{
		BucketID:   bucket.ID,
		Key:        "mgr-stale-key",
		Size:       100,
		ETag:       "etag2",
		State:      model.ObjectStateCached,
		Generation: 1,
	}
	if _, err := db.NewInsert().Model(obj).Exec(ctx); err != nil {
		t.Fatalf("inserting object: %v", err)
	}

	// Transition to uploading
	if err := repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition: %v", err)
	}

	// Manually set updated_at to more than 10 minutes ago (stale threshold)
	staleTime := time.Now().Add(-15 * time.Minute)
	_, err := db.NewUpdate().Model((*model.Object)(nil)).
		Set("updated_at = ?", staleTime).
		Where("id = ?", obj.ID).
		Exec(ctx)
	if err != nil {
		t.Fatalf("setting stale timestamp: %v", err)
	}

	// Start manager — recovery should reset uploading → cached
	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	got, err := repos.Objects.GetByID(ctx, obj.ID)
	if err != nil {
		t.Fatalf("getting object: %v", err)
	}
	if got.State != model.ObjectStateCached {
		t.Errorf("expected object reset to cached, got %s", got.State)
	}
}

func TestManager_ReconcileTasks_CreatesMissingTasks(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-reconcile-bucket")

	// Create an object in "cached" state (should have an upload task)
	obj := &model.Object{
		BucketID:   bucket.ID,
		Key:        "mgr-reconcile-key",
		Size:       100,
		ETag:       "etag3",
		State:      model.ObjectStateCached,
		Generation: 1,
	}
	if _, err := db.NewInsert().Model(obj).Exec(ctx); err != nil {
		t.Fatalf("inserting object: %v", err)
	}

	// No task exists yet — manager should create one during reconciliation
	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	// Claim the task created by reconciliation
	task, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("claiming task: %v", err)
	}
	if task == nil {
		t.Fatal("expected reconciliation to create missing upload task")
	}
	if task.RefID != obj.ID || task.RefGeneration != 1 {
		t.Errorf("task refs mismatch: got refID=%d gen=%d, want %d/1", task.RefID, task.RefGeneration, obj.ID)
	}
}

func TestManager_WorkerHealth(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	w1 := &stubWorker{name: "alpha", isHealthy: true}
	w2 := &stubWorker{name: "beta", isHealthy: false}
	w3 := &stubWorker{name: "gamma", isHealthy: true}

	mgr := worker.NewManager(repos, slog.Default(), false, w1, w2, w3)
	health := mgr.WorkerHealth()

	if len(health) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(health))
	}
	if !health["alpha"] {
		t.Error("expected alpha healthy")
	}
	if health["beta"] {
		t.Error("expected beta unhealthy")
	}
	if !health["gamma"] {
		t.Error("expected gamma healthy")
	}
}

func TestManager_WorkerHealth_Empty(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	mgr := worker.NewManager(repos, slog.Default(), false)
	health := mgr.WorkerHealth()
	if len(health) != 0 {
		t.Errorf("expected empty map, got %d entries", len(health))
	}
}

func TestManager_WorkerHealth_RealWorkers(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	mc := &testutil.MockCache{}
	sm := state.NewObjectStateMachine()
	logger := slog.Default()
	poll := 50 * time.Millisecond

	up := worker.NewUploader(repos, mc, nil, nil, sm, true, 1, poll, logger)
	ev := worker.NewEvictor(repos, mc, sm, 1, poll, logger)

	mgr := worker.NewManager(repos, logger, true, up, ev)
	health := mgr.WorkerHealth()

	expected := map[string]bool{
		"uploader": true,
		"evictor":  true,
	}
	for name, wantHealthy := range expected {
		got, ok := health[name]
		if !ok {
			t.Errorf("missing worker %q in health map", name)
			continue
		}
		if got != wantHealthy {
			t.Errorf("worker %q: expected healthy=%v, got %v", name, wantHealthy, got)
		}
	}
}

func TestManager_ReconcileTasks_IdempotencyDedup(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-dedup-bucket")

	obj := &model.Object{
		BucketID:   bucket.ID,
		Key:        "mgr-dedup-key",
		Size:       100,
		ETag:       "etag4",
		State:      model.ObjectStateCached,
		Generation: 1,
	}
	if _, err := db.NewInsert().Model(obj).Exec(ctx); err != nil {
		t.Fatalf("inserting object: %v", err)
	}

	// Pre-create the task with the same idempotency key the manager would use
	existingTask := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          obj.ID,
		RefGeneration:  1,
		IdempotencyKey: fmt.Sprintf("upload:%d:%d", obj.ID, int64(1)),
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, existingTask); err != nil {
		t.Fatalf("creating existing task: %v", err)
	}

	// Start manager — reconciliation should skip (idempotency dedup)
	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	// Claim the task — should be exactly one (the pre-existing one)
	task, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("claiming task: %v", err)
	}
	if task == nil {
		t.Fatal("expected existing task to still be claimable")
	}
	if task.ID != existingTask.ID {
		t.Errorf("expected existing task ID %d, got %d", existingTask.ID, task.ID)
	}

	// No second task should be claimable
	dup, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
	if dup != nil {
		t.Error("expected no duplicate task after idempotency dedup")
	}
}

func TestManager_ReconcileTasks_AutoEvictGuard(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-autoevict-off")
	obj := &model.Object{
		BucketID:   bucket.ID,
		Key:        "stored-no-evict",
		Size:       100,
		ETag:       "etag5",
		State:      model.ObjectStateStored,
		Generation: 1,
	}
	if _, err := db.NewInsert().Model(obj).Exec(ctx); err != nil {
		t.Fatalf("inserting object: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	task, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict task: %v", err)
	}
	if task != nil {
		t.Fatal("expected no evict_cache task when autoEvict is disabled")
	}
}

func TestManager_ReconcileTasks_AutoEvictEnabled(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-autoevict-on")
	obj := &model.Object{
		BucketID:   bucket.ID,
		Key:        "stored-with-evict",
		Size:       100,
		ETag:       "etag6",
		State:      model.ObjectStateStored,
		Generation: 1,
	}
	if _, err := db.NewInsert().Model(obj).Exec(ctx); err != nil {
		t.Fatalf("inserting object: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), true)
	mgr.Start(ctx)

	task, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict task: %v", err)
	}
	if task == nil {
		t.Fatal("expected evict_cache task when autoEvict is enabled")
	}
	if task.RefID != obj.ID || task.RefGeneration != obj.Generation {
		t.Fatalf("evict task refs = (%d,%d), want (%d,%d)", task.RefID, task.RefGeneration, obj.ID, obj.Generation)
	}
}
