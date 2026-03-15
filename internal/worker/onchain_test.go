package worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/data-preservation-programs/go-synapse/pdp"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/worker"
)

// seedUploadedObject creates a bucket+object in "uploaded" state with PieceCID set.
func seedUploadedObject(t *testing.T, env *testWorkerEnv) (*model.Bucket, int64, int64) {
	t.Helper()
	ctx := context.Background()

	bucket, objID, gen := seedObjectInDB(t, env, model.BucketStatusActive)

	// Set a ProofSetID on the bucket
	proofSetID := "42"
	if err := env.repos.Buckets.SetProofSetID(ctx, bucket.ID, proofSetID); err != nil {
		t.Fatalf("setting proof set ID: %v", err)
	}

	// Transition cached → uploading → uploaded with PieceCID
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition to uploading: %v", err)
	}

	pieceCID := testCID(t).String()
	if err := env.repos.Objects.SetPieceCIDAndTransition(ctx, objID, gen, pieceCID, model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("SetPieceCIDAndTransition: %v", err)
	}
	return bucket, objID, gen
}

func TestOnChain_HappyPath(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedUploadedObject(t, env)

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	env.proof.AddRootsFunc = func(_ context.Context, _ *big.Int, _ []pdp.Root) (*pdp.AddRootsResult, error) {
		return &pdp.AddRootsResult{RootsAdded: 1}, nil
	}

	oc := worker.NewOnChain(env.repos, env.proof, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	ctx := context.Background()

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetByID(ctx, objID)
	if obj.State != model.ObjectStateOnChained {
		t.Errorf("expected object state onchained, got %s", obj.State)
	}

	// With evictAfterOnChain=true, an evict_cache task should exist
	evictTask, err := env.repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict task: %v", err)
	}
	if evictTask == nil {
		t.Fatal("expected evict_cache chain task to exist")
	}
	if evictTask.RefID != objID || evictTask.RefGeneration != gen {
		t.Errorf("evict task refs mismatch: got refID=%d gen=%d, want %d/%d",
			evictTask.RefID, evictTask.RefGeneration, objID, gen)
	}
}

func TestOnChain_NoEvictAfterOnChain(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedUploadedObject(t, env)

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	env.proof.AddRootsFunc = func(_ context.Context, _ *big.Int, _ []pdp.Root) (*pdp.AddRootsResult, error) {
		return &pdp.AddRootsResult{RootsAdded: 1}, nil
	}

	// evictAfterOnChain = false
	oc := worker.NewOnChain(env.repos, env.proof, env.sm, false, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	ctx := context.Background()

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	// No evict_cache task should exist
	evictTask, _ := env.repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if evictTask != nil {
		t.Error("expected no evict_cache task when evictAfterOnChain=false")
	}
}

func TestOnChain_StaleGeneration(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, _ := seedUploadedObject(t, env)

	// Task with stale generation
	task := &model.Task{
		Type:           model.TaskTypeAddRoots,
		RefType:        "object",
		RefID:          objID,
		RefGeneration:  0, // stale
		IdempotencyKey: "add_roots:stale:0",
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("creating task: %v", err)
	}

	oc := worker.NewOnChain(env.repos, env.proof, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "stale generation") {
		t.Errorf("expected stale generation error, got %v", got.LastError)
	}
}

func TestOnChain_NilProofSetClient(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedUploadedObject(t, env)

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	oc := worker.NewOnChain(env.repos, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "proof set client not configured") {
		t.Errorf("expected proof set client error, got %v", got.LastError)
	}
}

func TestOnChain_NoPieceCID(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	// Create object in uploaded state but WITHOUT PieceCID
	bucket, objID, gen := seedObjectInDB(t, env, model.BucketStatusActive)
	proofSetID := "42"
	if err := env.repos.Buckets.SetProofSetID(ctx, bucket.ID, proofSetID); err != nil {
		t.Fatalf("setting proof set ID: %v", err)
	}

	// Manually transition to uploaded without setting PieceCID
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition to uploading: %v", err)
	}
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("transition to uploaded: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	oc := worker.NewOnChain(env.repos, env.proof, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "no PieceCID") {
		t.Errorf("expected no PieceCID error, got %v", got.LastError)
	}
}

func TestOnChain_NoProofSetID(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	// Create uploaded object but bucket has no ProofSetID
	bucket, objID, gen := seedObjectInDB(t, env, model.BucketStatusActive)
	_ = bucket // bucket has no ProofSetID

	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition to uploading: %v", err)
	}
	pieceCID := testCID(t).String()
	if err := env.repos.Objects.SetPieceCIDAndTransition(ctx, objID, gen, pieceCID, model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("SetPieceCIDAndTransition: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	oc := worker.NewOnChain(env.repos, env.proof, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "no ProofSetID") {
		t.Errorf("expected no ProofSetID error, got %v", got.LastError)
	}
}

func TestOnChain_AddRootsFailure_Retry(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedUploadedObject(t, env)

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	env.proof.AddRootsFunc = func(_ context.Context, _ *big.Int, _ []pdp.Root) (*pdp.AddRootsResult, error) {
		return nil, errors.New("chain congested")
	}

	oc := worker.NewOnChain(env.repos, env.proof, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	// After Fail+Requeue task goes back to pending with future ScheduledAt
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = oc.Run(ctx)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
}

func TestOnChain_EvictChainTaskFailure(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedUploadedObject(t, env)

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	env.proof.AddRootsFunc = func(_ context.Context, _ *big.Int, _ []pdp.Root) (*pdp.AddRootsResult, error) {
		return &pdp.AddRootsResult{RootsAdded: 1}, nil
	}

	// Pre-create conflicting evict_cache task
	conflict := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefGeneration:  gen,
		IdempotencyKey: "evict_cache:" + strings.Join([]string{big.NewInt(objID).String(), big.NewInt(gen).String()}, ":"),
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), conflict); err != nil {
		t.Fatalf("creating conflict task: %v", err)
	}

	oc := worker.NewOnChain(env.repos, env.proof, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	// ErrAlreadyExists is treated as idempotent success — task completes
	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (evict task already exists = idempotent), got %s", got.Status)
	}
}

func TestOnChain_OnChainedStateRecovery(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedOnChainedObject(t, env)

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	// AddRoots should NOT be called — recovery skips on-chain work
	env.proof.AddRootsFunc = func(_ context.Context, _ *big.Int, _ []pdp.Root) (*pdp.AddRootsResult, error) {
		t.Fatal("AddRoots should not be called during onchained-state recovery")
		return nil, nil
	}

	oc := worker.NewOnChain(env.repos, env.proof, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	// Task should complete successfully
	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed via onchained recovery, got %s", got.Status)
	}

	// Verify evict task was created by trying to create a duplicate
	evictTask := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefGeneration:  gen,
		IdempotencyKey: fmt.Sprintf("evict_cache:%d:%d", objID, gen),
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	err := env.repos.Tasks.Create(context.Background(), evictTask)
	if !errors.Is(err, repository.ErrAlreadyExists) {
		t.Errorf("expected evict task to already exist (ErrAlreadyExists), got %v", err)
	}
}

func TestOnChain_OnChainedStateRecovery_NoEvict(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedOnChainedObject(t, env)

	task := seedTask(t, env, model.TaskTypeAddRoots, objID, gen, 5, 0)

	// evictAfterOnChain = false
	oc := worker.NewOnChain(env.repos, env.proof, env.sm, false, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, oc, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed via recovery (no evict), got %s", got.Status)
	}

	// No evict task should exist — creating one should succeed (not ErrAlreadyExists)
	evictTask := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefGeneration:  gen,
		IdempotencyKey: fmt.Sprintf("evict_cache:%d:%d", objID, gen),
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	err := env.repos.Tasks.Create(context.Background(), evictTask)
	if err != nil {
		t.Errorf("expected no existing evict task when evictAfterOnChain=false, got %v", err)
	}
}
