package cache

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type DurabilityDependencies struct {
	Repositories   *repository.Repositories
	Scheduler      *taskengine.Scheduler
	EvictionPolicy cache.EvictionPolicy
}
type DurabilityHandler struct {
	*taskengine.FuncHandler
	deps DurabilityDependencies
}

func NewDurabilityHandler(deps DurabilityDependencies) (*DurabilityHandler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("cache durability handler requires repositories")
	}
	h := &DurabilityHandler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}

func (h *DurabilityHandler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeCacheReconcileDurability, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec:   taskengine.StrictJSONCodec(cacheeviction.ValidateDurabilityInput),
		Subject: taskengine.SubjectFromInput("bucket", func(input cacheeviction.DurabilityInput) int64 { return input.BucketID }),
		Policy:  taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
	}
	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
		var input cacheeviction.DurabilityInput
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return err
		}
		bucket, err := repos.Buckets.GetByID(ctx, input.BucketID)
		if err != nil {
			return err
		}
		if bucket == nil || bucket.DurabilityTaskID == nil || *bucket.DurabilityTaskID != source.ID || bucket.DurabilityGeneration != input.Generation {
			return repository.ErrConflict
		}
		return nil
	}
	definition.LegacyHandoff = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
		var input cacheeviction.DurabilityInput
		if err := json.Unmarshal(old.Input, &input); err != nil {
			return err
		}
		return repos.CacheEvictions.TransferDurabilityTaskOwner(ctx, input.BucketID, input.Generation, old.ID, next.ID)
	}
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, err := taskengine.DecodeInput[cacheeviction.DurabilityInput](execution)
		if err != nil {
			return decodeFailure(string(definition.Type), err)
		}
		candidate, err := h.deps.Repositories.CacheEvictions.NextBucketDurabilityCandidate(ctx, input.BucketID, input.Generation, execution.ID())
		if err != nil {
			if errors.Is(err, repository.ErrConflict) {
				return taskengine.Cancel("A newer storage policy update replaced this operation", nil)
			}
			return retryTask(err, "durability_scan_failed")
		}
		if candidate == nil {
			return taskengine.Complete("Bucket storage policy applied", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.CacheEvictions.CompleteBucketDurability(ctx, input.BucketID, input.Generation, execution.ID())
			})
		}
		return taskengine.Wait(model.TaskResumeModeExecute, 0, "more_work", "Applying bucket storage policy", func(ctx context.Context, repos *repository.Repositories) error {
			if h.deps.EvictionPolicy != cache.EvictionPolicyAfterUpload {
				return nil
			}
			if h.deps.Scheduler == nil {
				return errors.New("task service is unavailable")
			}
			_, err := enqueueEviction(ctx, repos, h.deps.Scheduler, candidate.ID, nil)
			return err
		})
	}
	return taskengine.NewFuncHandler(definition, run, run)
}
