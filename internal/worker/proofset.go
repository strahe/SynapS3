package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/data-preservation-programs/go-synapse/pdp"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"golang.org/x/sync/errgroup"
)

// ProofSetWorker handles create_proof_set and delete_proof_set tasks for bucket lifecycle.
type ProofSetWorker struct {
	repos        *repository.Repositories
	proofSet     synapse.ProofSetClient
	cache        cache.Cache
	concurrency  int
	pollInterval time.Duration
	leaseTTL     time.Duration
	logger       *slog.Logger
	*livenessTracker
}

// NewProofSetWorker creates a new proof-set lifecycle worker.
func NewProofSetWorker(repos *repository.Repositories, pc synapse.ProofSetClient, c cache.Cache, concurrency int, pollInterval time.Duration, logger *slog.Logger) *ProofSetWorker {
	return &ProofSetWorker{
		repos:           repos,
		proofSet:        pc,
		cache:           c,
		concurrency:     concurrency,
		pollInterval:    pollInterval,
		leaseTTL:        15 * time.Minute,
		logger:          logger,
		livenessTracker: newLivenessTracker(pollInterval),
	}
}

func (p *ProofSetWorker) Name() string { return "proofset" }

func (p *ProofSetWorker) Run(ctx context.Context) error {
	return pollLoop(ctx, p.pollInterval, p.tick)
}

func (p *ProofSetWorker) tick(ctx context.Context) error {
	p.recordTick()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(p.concurrency)

	for _, taskType := range []model.TaskType{model.TaskTypeCreateProofSet, model.TaskTypeDeleteProofSet} {
		for range p.concurrency {
			task, err := p.repos.Tasks.ClaimPending(gctx, taskType, p.leaseTTL)
			if err != nil {
				p.logger.Error("claiming proofset task", "type", taskType, "error", err)
				break
			}
			if task == nil {
				break
			}

			g.Go(func() error {
				p.processTask(gctx, task)
				return nil
			})
		}
	}

	err := g.Wait()
	return err
}

// Healthy returns true if the worker has ticked recently.
func (p *ProofSetWorker) Healthy() bool { return p.healthy() }

func (p *ProofSetWorker) processTask(ctx context.Context, task *model.Task) {
	start := time.Now()
	defer func() {
		admin.WorkerTaskDuration.WithLabelValues("proofset").Observe(time.Since(start).Seconds())
	}()

	if p.proofSet == nil {
		p.logger.Warn("proof set client not configured, failing task", "taskID", task.ID)
		_ = p.repos.Tasks.Fail(ctx, task.ID, "proof set client not configured")
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	switch task.Type {
	case model.TaskTypeCreateProofSet:
		p.processCreate(ctx, task)
	case model.TaskTypeDeleteProofSet:
		p.processDelete(ctx, task)
	default:
		p.logger.Error("unknown task type", "type", task.Type)
		_ = p.repos.Tasks.Fail(ctx, task.ID, "unknown task type")
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
	}
}

func (p *ProofSetWorker) processCreate(ctx context.Context, task *model.Task) {
	logger := p.logger.With("taskID", task.ID, "bucketID", task.RefID)

	bucket, err := p.repos.Buckets.GetByID(ctx, task.RefID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "error", err)
		_ = p.repos.Tasks.Fail(ctx, task.ID, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	if bucket.Status != model.BucketStatusCreating {
		logger.Warn("bucket not in creating status", "status", bucket.Status)
		_ = p.repos.Tasks.Fail(ctx, task.ID, "bucket not in creating status")
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	result, err := p.proofSet.CreateProofSet(ctx, pdp.CreateProofSetOptions{})
	if err != nil {
		logger.Error("CreateProofSet failed", "error", err)
		if task.RetryCount+1 >= task.MaxRetries {
			if err := p.repos.Buckets.UpdateStatus(ctx, bucket.ID, model.BucketStatusCreating, model.BucketStatusCreateFailed); err != nil {
				logger.Error("failed to update bucket status to create_failed", "error", err)
			}
			if ftErr := p.repos.Tasks.FailTerminal(ctx, task.ID, fmt.Sprintf("CreateProofSet: %v (max retries reached)", err)); ftErr != nil {
				logger.Error("failed to mark task as dead-letter", "error", ftErr)
			} else {
				admin.DeadLetterTotal.WithLabelValues("proofset", string(model.TaskTypeCreateProofSet)).Inc()
			}
		} else {
			_ = p.repos.Tasks.Fail(ctx, task.ID, err.Error())
			_ = p.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
		}
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	proofSetID := result.ProofSetID.String()
	if err := p.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.Buckets.SetProofSetID(ctx, bucket.ID, proofSetID); err != nil {
			return fmt.Errorf("SetProofSetID: %w", err)
		}
		return txRepos.Buckets.UpdateStatus(ctx, bucket.ID, model.BucketStatusCreating, model.BucketStatusActive)
	}); err != nil {
		logger.Error("atomic SetProofSetID+UpdateStatus failed", "error", err)
		_ = p.repos.Tasks.Fail(ctx, task.ID, err.Error())
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	_ = p.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("proofset", "success").Inc()
	logger.Info("proof set created", "proofSetID", proofSetID)
}

func (p *ProofSetWorker) processDelete(ctx context.Context, task *model.Task) {
	logger := p.logger.With("taskID", task.ID, "bucketID", task.RefID)

	bucket, err := p.repos.Buckets.GetByID(ctx, task.RefID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "error", err)
		_ = p.repos.Tasks.Fail(ctx, task.ID, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	if bucket.Status != model.BucketStatusDeleting {
		logger.Warn("bucket not in deleting status", "status", bucket.Status)
		_ = p.repos.Tasks.Fail(ctx, task.ID, "bucket not in deleting status")
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	if bucket.ProofSetID == nil || *bucket.ProofSetID == "" {
		// No proof set to delete — just hard-delete the bucket row and clean cache
		if err := p.repos.Buckets.HardDelete(ctx, bucket.ID); err != nil {
			logger.Error("HardDelete failed", "error", err)
			_ = p.repos.Tasks.Fail(ctx, task.ID, err.Error())
			admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
			return
		}
		_ = p.cache.DeleteBucketDir(ctx, bucket.Name)
		_ = p.repos.Tasks.Complete(ctx, task.ID)
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "success").Inc()
		logger.Info("bucket hard-deleted (no proof set)")
		return
	}

	proofSetID := new(big.Int)
	if _, ok := proofSetID.SetString(*bucket.ProofSetID, 10); !ok {
		logger.Error("invalid ProofSetID", "proofSetID", *bucket.ProofSetID)
		_ = p.repos.Tasks.Fail(ctx, task.ID, "invalid ProofSetID")
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	if err := p.proofSet.DeleteProofSet(ctx, proofSetID, nil); err != nil {
		logger.Error("DeleteProofSet failed", "error", err)
		if task.RetryCount+1 >= task.MaxRetries {
			if err := p.repos.Buckets.UpdateStatus(ctx, bucket.ID, model.BucketStatusDeleting, model.BucketStatusDeleteFailed); err != nil {
				logger.Error("failed to update bucket status to delete_failed", "error", err)
			}
			if ftErr := p.repos.Tasks.FailTerminal(ctx, task.ID, fmt.Sprintf("DeleteProofSet: %v (max retries reached)", err)); ftErr != nil {
				logger.Error("failed to mark task as dead-letter", "error", ftErr)
			} else {
				admin.DeadLetterTotal.WithLabelValues("proofset", string(model.TaskTypeDeleteProofSet)).Inc()
			}
		} else {
			_ = p.repos.Tasks.Fail(ctx, task.ID, err.Error())
			_ = p.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
		}
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}

	// ProofSet deleted successfully — hard-delete the bucket row and clean cache
	if err := p.repos.Buckets.HardDelete(ctx, bucket.ID); err != nil {
		logger.Error("HardDelete failed after proof set deletion", "error", err)
		_ = p.repos.Tasks.Fail(ctx, task.ID, err.Error())
		admin.WorkerTasksProcessed.WithLabelValues("proofset", "failure").Inc()
		return
	}
	_ = p.cache.DeleteBucketDir(ctx, bucket.Name)

	_ = p.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("proofset", "success").Inc()
	logger.Info("proof set deleted and bucket removed")
}
