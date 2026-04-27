package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
	if input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	name := *input.Bucket

	_, err := b.bucketLifecycle.CreateWithACL(ctx, name, defaultACL)
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
		}
		return err
	}
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
		if !input.IsAdmin {
			if input.Owner == "" || len(bkt.ACL) == 0 {
				continue
			}
			acl, err := auth.ParseACL(bkt.ACL)
			if err != nil {
				if b.logger != nil {
					b.logger.Warn("skipping bucket with malformed ACL", "bucket", bkt.Name, "error", err)
				}
				continue
			}
			if acl.Owner == "" || acl.Owner != input.Owner {
				continue
			}
		}
		result.Buckets.Bucket = append(result.Buckets.Bucket, s3response.ListAllMyBucketsEntry{
			Name:         bkt.Name,
			CreationDate: bkt.CreatedAt,
		})
	}

	return result, nil
}
