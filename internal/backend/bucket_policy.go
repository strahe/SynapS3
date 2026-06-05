package backend

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// GetBucketAcl returns the persisted ACL for the specified bucket.
func (b *SynapseBackend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	if input == nil || input.Bucket == nil {
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

	if owner, err := ownerFromACL(data); err != nil {
		return invalidArgument("ACL")
	} else if owner != "" {
		if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
			account, err := txRepos.S3Accounts.LockByAccessKey(ctx, owner)
			if err != nil {
				return err
			}
			if account == nil {
				return auth.ErrNoSuchUser
			}
			return txRepos.Buckets.SetOwnerAndACL(ctx, bucket, &owner, data)
		}); err != nil {
			if errors.Is(err, auth.ErrNoSuchUser) {
				return s3err.GetAPIError(s3err.ErrAccessDenied)
			}
			return err
		}
		return nil
	}

	if bkt.OwnerAccessKey != nil && *bkt.OwnerAccessKey != "" {
		acl, err := aclPreservingOwner(data, *bkt.OwnerAccessKey)
		if err != nil {
			return invalidArgument("ACL")
		}
		data = acl
	}
	return b.repos.Buckets.SetACL(ctx, bucket, data)
}

func aclPreservingOwner(data []byte, owner string) ([]byte, error) {
	acl := auth.ACL{}
	if len(data) > 0 {
		parsed, err := auth.ParseACL(data)
		if err != nil {
			return nil, err
		}
		acl = parsed
	}
	acl.Owner = owner
	if len(acl.Grantees) == 0 {
		// Store owner-only ACLs with an explicit FULL_CONTROL grant so VersityGW's
		// ACL verifier has a concrete owner grant to match on subsequent requests.
		acl.Grantees = []auth.Grantee{{
			Permission: auth.PermissionFullControl,
			Access:     owner,
			Type:       types.TypeCanonicalUser,
		}}
	}
	return json.Marshal(acl)
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

// PutBucketVersioning accepts Enabled because SynapS3 enforces versioning for every bucket.
func (b *SynapseBackend) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return err
	}
	switch status {
	case types.BucketVersioningStatusEnabled:
		return nil
	case types.BucketVersioningStatusSuspended:
		return s3err.APIError{
			Code:           "InvalidBucketState",
			Description:    "Bucket versioning is always enabled in SynapS3.",
			HTTPStatusCode: http.StatusBadRequest,
		}
	default:
		return invalidArgument("Status")
	}
}

// GetBucketVersioning reports that SynapS3 buckets are always versioning-enabled.
func (b *SynapseBackend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return s3response.GetBucketVersioningOutput{}, err
	}

	status := types.BucketVersioningStatusEnabled
	return s3response.GetBucketVersioningOutput{Status: &status}, nil
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

func (b *SynapseBackend) PutBucketOwnershipControls(ctx context.Context, bucket string, ownership types.ObjectOwnership) error {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return err
	}
	switch ownership {
	case types.ObjectOwnershipBucketOwnerPreferred:
		return nil
	default:
		return s3err.GetInvalidArgObjectOwnership(string(ownership))
	}
}

func (b *SynapseBackend) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	if _, err := b.getBucket(ctx, bucket); err != nil {
		return err
	}
	return nil
}

// GetObjectAcl returns an empty ACL for the specified object.
// TODO: verify object exists and return NoSuchKey for missing keys.
func (b *SynapseBackend) GetObjectAcl(ctx context.Context, input *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	if input == nil || input.Bucket == nil {
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
	if input == nil || input.Bucket == nil {
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
