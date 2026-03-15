package backend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
	if input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	name := *input.Bucket

	// Atomic: create bucket (status=creating) + enqueue create_proof_set task.
	var bucketID int64
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		bucket := &model.Bucket{
			Name:   name,
			Status: model.BucketStatusCreating,
		}
		if err := txRepos.Buckets.Create(ctx, bucket); err != nil {
			if errors.Is(err, repository.ErrAlreadyExists) {
				return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
			}
			return fmt.Errorf("creating bucket %q: %w", name, err)
		}
		bucketID = bucket.ID

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
		return err
	}

	if err := b.cache.CreateBucketDir(ctx, name); err != nil {
		// Bucket is already committed in 'creating' status. Cache dir will be
		// created implicitly by cache.Put (MkdirAll), so this is non-fatal.
		b.logger.Warn("pre-creating cache dir failed (non-fatal)", "bucket", name, "error", err)
	}

	b.logger.Info("bucket created (pending proof set)", "bucket", name, "id", bucketID)
	return nil
}

func (b *SynapseBackend) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bucket, err := b.repos.Buckets.GetByName(ctx, *input.Bucket)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || !bucket.Status.IsVisible() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	return &s3.HeadBucketOutput{}, nil
}

func (b *SynapseBackend) DeleteBucket(ctx context.Context, bucket string) error {
	bkt, err := b.getBucket(ctx, bucket)
	if err != nil {
		return err
	}

	// Only active buckets can be deleted. Reject creating/deleting/failed states.
	if bkt.Status != model.BucketStatusActive {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	// Atomic: empty check + status CAS (active→deleting) + enqueue delete_proof_set task.
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		objects, err := txRepos.Objects.ListByBucket(ctx, bkt.ID, "", "", 1)
		if err != nil {
			return fmt.Errorf("checking bucket contents: %w", err)
		}
		if len(objects) > 0 {
			return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
		}

		if err := txRepos.Buckets.UpdateStatus(ctx, bkt.ID, model.BucketStatusActive, model.BucketStatusDeleting); err != nil {
			return fmt.Errorf("transitioning bucket to deleting: %w", err)
		}

		task := &model.Task{
			Type:           model.TaskTypeDeleteProofSet,
			RefType:        "bucket",
			RefID:          bkt.ID,
			IdempotencyKey: fmt.Sprintf("delete_proof_set:%d", bkt.ID),
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		return err
	}

	b.logger.Info("bucket deletion initiated", "bucket", bucket)
	return nil
}

func (b *SynapseBackend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	buckets, err := b.repos.Buckets.ListActive(ctx)
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, fmt.Errorf("listing buckets: %w", err)
	}

	result := s3response.ListAllMyBucketsResult{
		Owner: s3response.CanonicalUser{
			ID:          "synaps3",
			DisplayName: "SynapS3",
		},
	}
	for _, bkt := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, s3response.ListAllMyBucketsEntry{
			Name:         bkt.Name,
			CreationDate: bkt.CreatedAt,
		})
	}

	return result, nil
}
