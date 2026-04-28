package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
	if input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	name := *input.Bucket

	owner, err := ownerFromACL(defaultACL)
	if err != nil {
		return s3err.GetAPIError(s3err.ErrInvalidArgument)
	}
	if owner != "" {
		err = b.createBucketWithOwner(ctx, name, owner, defaultACL)
	} else {
		err = b.createBucketWithOwner(ctx, name, "", defaultACL)
	}
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
		}
		if errors.Is(err, auth.ErrNoSuchUser) {
			return s3err.GetAPIError(s3err.ErrAccessDenied)
		}
		return err
	}
	return nil
}

func (b *SynapseBackend) createBucketWithOwner(ctx context.Context, name, owner string, acl []byte) error {
	var ownerPtr *string
	if owner != "" {
		ownerPtr = &owner
	}
	var bucket *model.Bucket
	err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if owner != "" {
			account, err := txRepos.S3Accounts.LockByAccessKey(ctx, owner)
			if err != nil {
				return err
			}
			if account == nil {
				return auth.ErrNoSuchUser
			}
		}
		bucket = &model.Bucket{
			Name:           name,
			ACL:            acl,
			OwnerAccessKey: ownerPtr,
			Status:         model.BucketStatusActive,
		}
		return txRepos.Buckets.Create(ctx, bucket)
	})
	if err != nil {
		return err
	}
	b.bucketLifecycle.EnsureCacheBucketDir(ctx, bucket.Name)
	return nil
}

func ownerFromACL(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	acl, err := auth.ParseACL(data)
	if err != nil {
		return "", err
	}
	return acl.Owner, nil
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
			if input.Owner == "" || bkt.OwnerAccessKey == nil {
				continue
			}
			if *bkt.OwnerAccessKey != input.Owner {
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

func (b *SynapseBackend) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	buckets, err := b.repos.Buckets.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing buckets: %w", err)
	}

	result := make([]s3response.Bucket, 0, len(buckets))
	for _, bkt := range buckets {
		owner := ""
		if bkt.OwnerAccessKey != nil {
			owner = *bkt.OwnerAccessKey
		}
		result = append(result, s3response.Bucket{
			Name:  bkt.Name,
			Owner: owner,
		})
	}

	return result, nil
}
