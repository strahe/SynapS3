package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"golang.org/x/sync/errgroup"

	"github.com/data-preservation-programs/go-synapse/pdp"
)

// OnChain claims add_roots tasks and submits object roots to the bucket's ProofSet.
type OnChain struct {
	repos             *repository.Repositories
	proofSet          synapse.ProofSetClient
	stateMachine      *state.Machine
	evictAfterOnChain bool
	concurrency       int
	pollInterval      time.Duration
	leaseTTL          time.Duration
	logger            *slog.Logger
}

// NewOnChain creates a new on-chain worker.
func NewOnChain(repos *repository.Repositories, pc synapse.ProofSetClient, sm *state.Machine, evictAfterOnChain bool, concurrency int, pollInterval time.Duration, logger *slog.Logger) *OnChain {
	return &OnChain{
		repos:             repos,
		proofSet:          pc,
		stateMachine:      sm,
		evictAfterOnChain: evictAfterOnChain,
		concurrency:       concurrency,
		pollInterval:      pollInterval,
		leaseTTL:          15 * time.Minute,
		logger:            logger,
	}
}

func (o *OnChain) Name() string { return "onchain" }

func (o *OnChain) Run(ctx context.Context) error {
	return pollLoop(ctx, o.pollInterval, o.tick)
}

func (o *OnChain) tick(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(o.concurrency)

	for range o.concurrency {
		task, err := o.repos.Tasks.ClaimPending(gctx, model.TaskTypeAddRoots, o.leaseTTL)
		if err != nil {
			o.logger.Error("claiming add_roots task", "error", err)
			break
		}
		if task == nil {
			break
		}

		g.Go(func() error {
			o.processTask(gctx, task)
			return nil
		})
	}

	return g.Wait()
}

func (o *OnChain) processTask(ctx context.Context, task *model.Task) {
	logger := o.logger.With("taskID", task.ID, "objectID", task.RefID, "gen", task.RefGeneration)

	if o.proofSet == nil {
		logger.Warn("proof set client not configured, failing task")
		_ = o.repos.Tasks.Fail(ctx, task.ID, "proof set client not configured")
		return
	}

	obj, err := o.repos.Objects.GetByID(ctx, task.RefID)
	if err != nil || obj == nil {
		logger.Warn("object not found for add_roots task", "error", err)
		_ = o.repos.Tasks.Fail(ctx, task.ID, "object not found")
		return
	}

	if obj.Generation != task.RefGeneration {
		logger.Warn("stale generation, skipping")
		_ = o.repos.Tasks.Fail(ctx, task.ID, "stale generation")
		return
	}

	if obj.PieceCID == nil || *obj.PieceCID == "" {
		logger.Error("object has no PieceCID")
		_ = o.repos.Tasks.Fail(ctx, task.ID, "no PieceCID")
		return
	}

	// Get bucket's ProofSetID
	bucket, err := o.repos.Buckets.GetByID(ctx, obj.BucketID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "bucketID", obj.BucketID, "error", err)
		_ = o.repos.Tasks.Fail(ctx, task.ID, "bucket not found")
		return
	}
	if bucket.ProofSetID == nil || *bucket.ProofSetID == "" {
		logger.Warn("bucket has no ProofSetID yet")
		_ = o.repos.Tasks.Fail(ctx, task.ID, "bucket has no ProofSetID")
		return
	}

	proofSetID := new(big.Int)
	if _, ok := proofSetID.SetString(*bucket.ProofSetID, 10); !ok {
		logger.Error("invalid ProofSetID", "proofSetID", *bucket.ProofSetID)
		_ = o.repos.Tasks.Fail(ctx, task.ID, "invalid ProofSetID")
		return
	}

	// Transition uploaded → onchaining (on retry, object may already be in onchaining state)
	if obj.State != model.ObjectStateOnChaining {
		if err := state.TransitionState(ctx, o.stateMachine, o.repos.Objects, task.RefID, task.RefGeneration,
			model.ObjectStateUploaded, model.ObjectStateOnChaining); err != nil {
			logger.Warn("state transition uploaded→onchaining failed", "error", err)
			_ = o.repos.Tasks.Fail(ctx, task.ID, err.Error())
			return
		}
	}

	// Parse PieceCID and add root
	pieceCID, err := cid.Decode(*obj.PieceCID)
	if err != nil {
		logger.Error("invalid PieceCID format", "error", err)
		_ = state.TransitionToFailed(ctx, o.stateMachine, o.repos.Objects, task.RefID, task.RefGeneration,
			model.ObjectStateOnChaining, fmt.Sprintf("invalid PieceCID: %v", err))
		_ = o.repos.Tasks.Fail(ctx, task.ID, err.Error())
		return
	}

	roots := []pdp.Root{{
		PieceCID: pieceCID,
	}}

	if _, err := o.proofSet.AddRoots(ctx, proofSetID, roots); err != nil {
		logger.Error("AddRoots failed", "error", err)
		if task.RetryCount+1 >= task.MaxRetries {
			_ = state.TransitionToFailed(ctx, o.stateMachine, o.repos.Objects, task.RefID, task.RefGeneration,
				model.ObjectStateOnChaining, fmt.Sprintf("AddRoots: %v (max retries reached)", err))
			_ = o.repos.Tasks.Fail(ctx, task.ID, err.Error())
		} else {
			_ = o.repos.Tasks.Fail(ctx, task.ID, err.Error())
			_ = o.repos.Tasks.Requeue(ctx, task.ID, time.Duration(task.RetryCount+1)*30*time.Second)
		}
		return
	}

	// Transition onchaining → onchained
	if err := state.TransitionState(ctx, o.stateMachine, o.repos.Objects, task.RefID, task.RefGeneration,
		model.ObjectStateOnChaining, model.ObjectStateOnChained); err != nil {
		logger.Error("state transition onchaining→onchained failed", "error", err)
		_ = o.repos.Tasks.Fail(ctx, task.ID, err.Error())
		return
	}

	// Chain: enqueue evict_cache task if configured
	if o.evictAfterOnChain {
		chainTask := &model.Task{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          task.RefID,
			RefGeneration:  task.RefGeneration,
			IdempotencyKey: fmt.Sprintf("evict_cache:%d:%d", task.RefID, task.RefGeneration),
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		if err := o.repos.Tasks.Create(ctx, chainTask); err != nil {
			logger.Error("enqueuing evict_cache task", "error", err)
			_ = o.repos.Tasks.Fail(ctx, task.ID, fmt.Sprintf("chain task creation: %v", err))
			return
		}
	}

	_ = o.repos.Tasks.Complete(ctx, task.ID)
	logger.Info("on-chain submission completed")
}
