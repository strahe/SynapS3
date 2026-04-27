package backend

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// GetBucketAcl returns the persisted ACL for the specified bucket.
func (b *SynapseBackend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bkt, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return nil, err
	}

	return bkt.ACL, nil
}

// PutBucketAcl persists bucket ACL updates used by VersityGW access control.
func (b *SynapseBackend) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	bkt, err := b.getBucket(ctx, bucket)
	if err != nil {
		return err
	}
	if !bkt.Status.IsWritable() {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	return b.repos.Buckets.SetACL(ctx, bucket, data)
}

func (b *SynapseBackend) ChangeBucketOwner(ctx context.Context, bucket, owner string) error {
	return auth.UpdateBucketACLOwner(ctx, b, bucket, owner)
}

func (b *SynapseBackend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return nil, err
	}
	return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
}

func (b *SynapseBackend) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return err
	}
	return nil
}

// GetBucketVersioning reports that versioning is not configured.
func (b *SynapseBackend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return s3response.GetBucketVersioningOutput{}, err
	}

	return s3response.GetBucketVersioningOutput{}, nil
}

// GetObjectLockConfiguration reports that object lock is not configured.
func (b *SynapseBackend) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return nil, err
	}

	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
}

// GetBucketOwnershipControls returns an ACL-compatible ownership mode.
func (b *SynapseBackend) GetBucketOwnershipControls(ctx context.Context, bucket string) (types.ObjectOwnership, error) {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return "", err
	}

	return types.ObjectOwnershipBucketOwnerPreferred, nil
}

// GetObjectAcl returns an empty ACL for the specified object.
// TODO: verify object exists and return NoSuchKey for missing keys.
func (b *SynapseBackend) GetObjectAcl(ctx context.Context, input *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	if _, err := b.getBucket(ctx, *input.Bucket); err != nil {
		return nil, err
	}

	return &s3.GetObjectAclOutput{}, nil
}

// PutObjectAcl accepts and discards object-level ACL updates.
// TODO: verify object exists and return NoSuchKey for missing keys.
func (b *SynapseBackend) PutObjectAcl(ctx context.Context, input *s3.PutObjectAclInput) error {
	if input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bkt, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return err
	}
	if !bkt.Status.IsWritable() {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	return nil
}

// GetBucketTagging returns no tags for the specified bucket.
func (b *SynapseBackend) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return nil, err
	}

	return nil, s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)
}
