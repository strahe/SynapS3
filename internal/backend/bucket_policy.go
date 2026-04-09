package backend

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// GetBucketAcl returns the ACL for the specified bucket.
// Currently returns an empty ACL; the middleware falls back to root-user ownership.
func (b *SynapseBackend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	if _, err := b.getBucket(ctx, *input.Bucket); err != nil {
		return nil, err
	}

	return nil, nil
}

// PutBucketAcl accepts and discards ACL updates.
// ACL enforcement is not yet implemented.
func (b *SynapseBackend) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	bkt, err := b.getBucket(ctx, bucket)
	if err != nil {
		return err
	}
	if !bkt.Status.IsWritable() {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
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

// GetBucketOwnershipControls returns the default BucketOwnerEnforced ownership.
func (b *SynapseBackend) GetBucketOwnershipControls(ctx context.Context, bucket string) (types.ObjectOwnership, error) {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return "", err
	}

	return types.ObjectOwnershipBucketOwnerEnforced, nil
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
