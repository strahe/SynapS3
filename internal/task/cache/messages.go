package cache

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type EvictionDependencies struct {
	Scheduler      *taskengine.Scheduler
	EvictionPolicy cache.EvictionPolicy
}

func NewEvictCacheContentReceiver(deps EvictionDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("cache eviction receiver requires a scheduler")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		input, ok := message.(storagepipeline.EvictCacheContent)
		if !ok || input.ContentID <= 0 {
			return errors.New("invalid cache eviction message")
		}
		_, err := enqueueEviction(ctx, tx, deps.Scheduler, input.ContentID, nil)
		return err
	}, nil
}

func NewContentDurabilityReachedSubscriber(deps EvictionDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("cache durability subscriber requires a scheduler")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		input, ok := message.(storagepipeline.ContentDurabilityReached)
		if !ok {
			return errors.New("invalid content durability message")
		}
		for _, id := range input.ContentIDs {
			if id <= 0 {
				return errors.New("invalid content durability message")
			}
		}
		if deps.EvictionPolicy != cache.EvictionPolicyAfterUpload {
			return nil
		}
		seen := make(map[int64]struct{}, len(input.ContentIDs))
		for _, id := range input.ContentIDs {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			if _, err := enqueueEviction(ctx, tx, deps.Scheduler, id, nil); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func enqueueEviction(ctx context.Context, tx *repository.Repositories, scheduler *taskengine.Scheduler, contentID int64, accessedAt *time.Time) (bool, error) {
	if scheduler == nil {
		return false, errors.New("cache eviction scheduler is unavailable")
	}
	reservation, err := tx.CacheEvictions.PrepareEviction(ctx, contentID)
	if err != nil {
		return false, err
	}
	if reservation.ActiveTaskID != nil {
		return false, nil
	}
	generation := reservation.Generation
	taskRow, _, err := scheduler.EnqueueInTransaction(ctx, tx, taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(contentID, generation),
		Input:       cacheeviction.EvictInput{ContentID: contentID, Generation: generation, AccessedAt: accessedAt},
		SubjectType: model.TaskSubjectStorageContent, SubjectKey: strconv.FormatInt(contentID, 10),
	})
	if err != nil {
		return false, err
	}
	if err := tx.CacheEvictions.BindEvictionTask(ctx, contentID, generation, taskRow.ID); err != nil {
		return false, err
	}
	return true, nil
}
