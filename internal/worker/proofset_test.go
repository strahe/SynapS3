package worker_test

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/data-preservation-programs/go-synapse/pdp"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/worker"
)

func TestProofSet_CreateHappyPath(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "ps-create-bucket", Status: model.BucketStatusCreating}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	task := seedBucketTask(t, env, model.TaskTypeCreateProofSet, bucket.ID, 5, 0)

	env.proof.CreateProofSetFunc = func(_ context.Context, _ pdp.CreateProofSetOptions) (*pdp.ProofSetResult, error) {
		return &pdp.ProofSetResult{ProofSetID: big.NewInt(42)}, nil
	}

	pw := worker.NewProofSetWorker(env.repos, env.proof, env.cache, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, pw, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	b, _ := env.repos.Buckets.GetByID(ctx, bucket.ID)
	if b.Status != model.BucketStatusActive {
		t.Errorf("expected bucket active, got %s", b.Status)
	}
	if b.ProofSetID == nil || *b.ProofSetID != "42" {
		t.Errorf("expected ProofSetID '42', got %v", b.ProofSetID)
	}
}

func TestProofSet_CreateFailure_Retry(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "ps-retry-bucket", Status: model.BucketStatusCreating}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	task := seedBucketTask(t, env, model.TaskTypeCreateProofSet, bucket.ID, 5, 0)

	env.proof.CreateProofSetFunc = func(_ context.Context, _ pdp.CreateProofSetOptions) (*pdp.ProofSetResult, error) {
		return nil, errors.New("chain congested")
	}

	pw := worker.NewProofSetWorker(env.repos, env.proof, env.cache, 1, 50*time.Millisecond, slog.Default())
	// After Fail+Requeue task goes back to pending with future ScheduledAt
	runCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_ = pw.Run(runCtx)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}

	// Bucket should still be in creating status
	b, _ := env.repos.Buckets.GetByID(context.Background(), bucket.ID)
	if b.Status != model.BucketStatusCreating {
		t.Errorf("expected bucket still creating, got %s", b.Status)
	}
}

func TestProofSet_CreateFailure_MaxRetries(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "ps-maxretry-bucket", Status: model.BucketStatusCreating}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	task := seedBucketTask(t, env, model.TaskTypeCreateProofSet, bucket.ID, 5, 4)

	env.proof.CreateProofSetFunc = func(_ context.Context, _ pdp.CreateProofSetOptions) (*pdp.ProofSetResult, error) {
		return nil, errors.New("permanent failure")
	}

	pw := worker.NewProofSetWorker(env.repos, env.proof, env.cache, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, pw, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}

	b, _ := env.repos.Buckets.GetByID(ctx, bucket.ID)
	if b.Status != model.BucketStatusCreateFailed {
		t.Errorf("expected bucket create_failed, got %s", b.Status)
	}
}

func TestProofSet_DeleteHappyPath(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	proofSetID := "99"
	bucket := &model.Bucket{Name: "ps-delete-bucket", Status: model.BucketStatusDeleting, ProofSetID: &proofSetID}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	task := seedBucketTask(t, env, model.TaskTypeDeleteProofSet, bucket.ID, 5, 0)

	env.proof.DeleteProofSetFunc = func(_ context.Context, psID *big.Int, _ []byte) error {
		if psID.Int64() != 99 {
			t.Errorf("expected ProofSetID 99, got %d", psID.Int64())
		}
		return nil
	}

	pw := worker.NewProofSetWorker(env.repos, env.proof, env.cache, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, pw, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	// Bucket should be hard-deleted
	b, _ := env.repos.Buckets.GetByID(ctx, bucket.ID)
	if b != nil {
		t.Errorf("expected bucket to be hard-deleted, but found status=%s", b.Status)
	}
}

func TestProofSet_DeleteNoProofSet(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	// Bucket in deleting status without ProofSetID
	bucket := &model.Bucket{Name: "ps-delete-nops-bucket", Status: model.BucketStatusDeleting}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	task := seedBucketTask(t, env, model.TaskTypeDeleteProofSet, bucket.ID, 5, 0)

	// DeleteProofSet should NOT be called
	env.proof.DeleteProofSetFunc = func(_ context.Context, _ *big.Int, _ []byte) error {
		t.Error("DeleteProofSet should not be called when bucket has no ProofSetID")
		return nil
	}

	pw := worker.NewProofSetWorker(env.repos, env.proof, env.cache, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, pw, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	// Bucket should be hard-deleted directly
	b, _ := env.repos.Buckets.GetByID(ctx, bucket.ID)
	if b != nil {
		t.Errorf("expected bucket to be hard-deleted, but found status=%s", b.Status)
	}
}

func TestProofSet_NilClient(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "ps-nil-bucket", Status: model.BucketStatusCreating}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	task := seedBucketTask(t, env, model.TaskTypeCreateProofSet, bucket.ID, 5, 0)

	pw := worker.NewProofSetWorker(env.repos, nil, env.cache, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, pw, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "proof set client not configured") {
		t.Errorf("expected proof set client error, got %v", got.LastError)
	}
}
