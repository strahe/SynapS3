package backend

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
	if input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	name := *input.Bucket

	bucket := &model.Bucket{
		Name:   name,
		Status: model.BucketStatusActive,
	}

	if err := b.repos.Buckets.Create(ctx, bucket); err != nil {
		// TODO: distinguish duplicate-key error → BucketAlreadyExists
		return fmt.Errorf("creating bucket %q: %w", name, err)
	}

	if err := b.cache.CreateBucketDir(ctx, name); err != nil {
		return fmt.Errorf("creating cache dir for bucket %q: %w", name, err)
	}

	b.logger.Info("bucket created", "bucket", name, "id", bucket.ID)
	// TODO: synchronously create ProofSet via go-synapse and store proof_set_id.
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
	if bucket == nil || bucket.Status == model.BucketStatusDeleted {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	return &s3.HeadBucketOutput{}, nil
}

func (b *SynapseBackend) DeleteBucket(ctx context.Context, bucket string) error {
	bkt, err := b.getBucket(ctx, bucket)
	if err != nil {
		return err
	}

	// Ensure bucket is empty (soft-deleted objects are automatically filtered).
	objects, err := b.repos.Objects.ListByBucket(ctx, bkt.ID, "", 1)
	if err != nil {
		return fmt.Errorf("checking bucket contents: %w", err)
	}
	if len(objects) > 0 {
		return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
	}

	if err := b.repos.Buckets.SoftDelete(ctx, bkt.ID); err != nil {
		return fmt.Errorf("deleting bucket: %w", err)
	}

	_ = b.cache.DeleteBucketDir(ctx, bucket)

	b.logger.Info("bucket deleted", "bucket", bucket)
	// TODO: retire/delete ProofSet via go-synapse.
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
