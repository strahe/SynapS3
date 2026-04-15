package bucketlifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

// Service coordinates bucket lifecycle operations shared by multiple entrypoints.
type Service struct {
	repos  *repository.Repositories
	cache  cache.Cache
	logger *slog.Logger
}

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrBucketDeleteBlocked = errors.New("bucket delete blocked by in-flight work")
	ErrBucketNotDeletable  = errors.New("bucket not deletable")
	ErrBucketTooLarge      = errors.New("bucket has too many objects for single-pass recursive delete")
)

type DeleteOptions struct {
	Recursive bool
}

// maxRecursiveDeleteObjects is the maximum number of objects that can be deleted
// in a single recursive bucket delete operation. Buckets with more objects than
// this should be emptied in batches before deletion.
const maxRecursiveDeleteObjects = 10000

func New(repos *repository.Repositories, c cache.Cache, logger *slog.Logger) *Service {
	return &Service{
		repos:  repos,
		cache:  c,
		logger: logger,
	}
}

func (s *Service) Create(ctx context.Context, name string) (*model.Bucket, error) {
	bucket := &model.Bucket{
		Name:   name,
		Status: model.BucketStatusCreating,
	}

	if err := s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.Buckets.Create(ctx, bucket); err != nil {
			return fmt.Errorf("creating bucket %q: %w", name, err)
		}

		task := &model.Task{
			Type:           model.TaskTypeCreateProofSet,
			RefType:        "bucket",
			RefID:          bucket.ID,
			IdempotencyKey: fmt.Sprintf("create_proof_set:%d", bucket.ID),
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		return nil, err
	}

	if err := s.cache.CreateBucketDir(ctx, name); err != nil && s.logger != nil {
		s.logger.Warn("pre-creating cache dir failed (non-fatal)", "bucket", name, "error", err)
	}

	if s.logger != nil {
		s.logger.Info("bucket created (pending proof set)", "bucket", name, "id", bucket.ID)
	}
	return bucket, nil
}

func (s *Service) Delete(ctx context.Context, name string, opts DeleteOptions) (*model.Bucket, error) {
	bucket, err := s.repos.Buckets.GetByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || !bucket.Status.IsVisible() {
		return nil, ErrBucketNotFound
	}
	if bucket.Status != model.BucketStatusActive {
		return nil, ErrBucketNotDeletable
	}

	var objectsToDelete []model.Object
	if err := s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		// Check for active bucket-scoped lifecycle tasks (e.g. create_proof_set, delete_proof_set).
		activeBucketTasks, err := txRepos.Tasks.CountActiveBucketTasksByBucketID(ctx, bucket.ID)
		if err != nil {
			return fmt.Errorf("counting active bucket tasks: %w", err)
		}
		if activeBucketTasks > 0 {
			return ErrBucketDeleteBlocked
		}
		activeMultiparts, err := txRepos.Multiparts.CountActiveByBucket(ctx, bucket.ID)
		if err != nil {
			return fmt.Errorf("counting active multipart uploads: %w", err)
		}
		if activeMultiparts > 0 {
			return ErrBucketDeleteBlocked
		}

		listLimit := 1
		if opts.Recursive {
			listLimit = maxRecursiveDeleteObjects + 1
		}
		objects, err := txRepos.Objects.ListByBucket(ctx, bucket.ID, "", "", listLimit)
		if err != nil {
			return fmt.Errorf("checking bucket contents: %w", err)
		}
		if len(objects) > 0 && !opts.Recursive {
			return ErrBucketNotEmpty
		}
		if opts.Recursive {
			if len(objects) > maxRecursiveDeleteObjects {
				return ErrBucketTooLarge
			}
			activeTasks, err := txRepos.Tasks.CountActiveObjectTasksByBucket(ctx, bucket.ID)
			if err != nil {
				return fmt.Errorf("counting active bucket object tasks: %w", err)
			}
			if activeTasks > 0 {
				return ErrBucketDeleteBlocked
			}
			for _, object := range objects {
				if !recursiveDeleteSafeState(object.State) {
					return ErrBucketDeleteBlocked
				}
			}
			objectsToDelete = append(objectsToDelete, objects...)
			for _, object := range objects {
				if err := txRepos.Objects.SoftDelete(ctx, object.ID); err != nil {
					return fmt.Errorf("soft-deleting object %d: %w", object.ID, err)
				}
			}
		}

		if err := txRepos.Buckets.UpdateStatus(ctx, bucket.ID, bucket.Status, model.BucketStatusDeleting); err != nil {
			return fmt.Errorf("transitioning bucket to deleting (%v): %w", err, ErrBucketNotDeletable)
		}

		task := &model.Task{
			Type:           model.TaskTypeDeleteProofSet,
			RefType:        "bucket",
			RefID:          bucket.ID,
			IdempotencyKey: fmt.Sprintf("delete_proof_set:%d", bucket.ID),
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		return nil, err
	}

	if opts.Recursive {
		for _, object := range objectsToDelete {
			if err := s.cache.Delete(ctx, name, object.Key); err != nil && s.logger != nil {
				s.logger.Warn("failed to delete object from cache during recursive bucket delete", "bucket", name, "key", object.Key, "error", err)
			}
		}
	}

	bucket.Status = model.BucketStatusDeleting
	if s.logger != nil {
		s.logger.Info("bucket deletion initiated", "bucket", name, "recursive", opts.Recursive)
	}
	return bucket, nil
}

func recursiveDeleteSafeState(state model.ObjectState) bool {
	switch state {
	case model.ObjectStateCached, model.ObjectStateFailed, model.ObjectStateOnChained, model.ObjectStateCacheEvicted:
		return true
	default:
		return false
	}
}
