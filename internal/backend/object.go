package backend

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	bucketName := derefStr(input.Bucket)
	keyName := derefStr(input.Key)

	bucket, err := b.getBucket(ctx, bucketName)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	// Write to local cache with fsync.
	cacheInfo, err := b.cache.Put(ctx, bucketName, keyName, input.Body)
	if err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("caching object: %w", err)
	}

	// Build metadata map from input.
	meta := make(map[string]string)
	if input.Metadata != nil {
		meta = input.Metadata
	}

	var objectID int64
	var newGen int64

	// Atomic transaction: upsert object + enqueue task.
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		obj := &model.Object{
			BucketID:    bucket.ID,
			Key:         keyName,
			Size:        cacheInfo.Size,
			ETag:        cacheInfo.ETag,
			Checksum:    cacheInfo.Checksum,
			ContentType: stringOrDefault(input.ContentType, "application/octet-stream"),
			Metadata:    meta,
			CachePath:   cacheInfo.Path,
			State:       model.ObjectStateCached,
			MaxRetries:  5,
		}

		id, gen, err := txRepos.Objects.UpsertAndBumpGeneration(ctx, obj)
		if err != nil {
			return fmt.Errorf("upserting object: %w", err)
		}
		objectID = id
		newGen = gen

		// Enqueue upload task with correct idempotency key: upload:objectID:generation.
		task := &model.Task{
			Type:           model.TaskTypeUploadToSP,
			RefType:        "object",
			RefID:          objectID,
			RefGeneration:  newGen,
			IdempotencyKey: fmt.Sprintf("upload:%d:%d", objectID, newGen),
			Status:         model.TaskStatusPending,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		// Only clean up cache for truly new objects (gen 1).
		// For overwrites (gen > 1), the rename already destroyed the old file;
		// deleting the new file would cause data loss.
		// When newGen == 0 (upsert itself failed), we can't distinguish new vs overwrite,
		// so we leave the file. TODO: implement orphan reconciliation (compare cache files against DB).
		if newGen == 1 {
			_ = b.cache.Delete(ctx, bucketName, keyName)
		}
		return s3response.PutObjectOutput{}, err
	}

	b.logger.Info("object stored", "bucket", bucketName, "key", keyName, "size", cacheInfo.Size, "gen", newGen)

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	return s3response.PutObjectOutput{
		ETag: etag,
	}, nil
}

func (b *SynapseBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return nil, err
	}

	obj, err := b.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, *input.Key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if obj == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	// Try local cache first (no TOCTOU — call Get directly, handle ErrNotExist).
	rc, _, cacheErr := b.cache.Get(ctx, *input.Bucket, *input.Key)
	if cacheErr == nil {
		etag := fmt.Sprintf(`"%s"`, obj.ETag)
		contentType := obj.ContentType

		return &s3.GetObjectOutput{
			Body:          rc,
			ContentLength: &obj.Size,
			ETag:          &etag,
			ContentType:   &contentType,
		}, nil
	}

	if !os.IsNotExist(cacheErr) {
		return nil, fmt.Errorf("reading from cache: %w", cacheErr)
	}

	// Cache miss — object has been evicted, download from SP.
	// TODO: implement download from Storage Provider via go-synapse.
	return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
}

func (b *SynapseBackend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return nil, err
	}

	obj, err := b.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, *input.Key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if obj == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	etag := fmt.Sprintf(`"%s"`, obj.ETag)
	contentType := obj.ContentType
	lastModified := obj.UpdatedAt

	return &s3.HeadObjectOutput{
		ContentLength: &obj.Size,
		ETag:          &etag,
		ContentType:   &contentType,
		LastModified:  &lastModified,
	}, nil
}

func (b *SynapseBackend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return nil, err
	}

	// Soft-delete object (Bun soft_delete tag handles setting deleted_at).
	obj, err := b.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, *input.Key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if obj != nil {
		if err := b.repos.Objects.SoftDelete(ctx, obj.ID); err != nil {
			return nil, fmt.Errorf("deleting object: %w", err)
		}
		_ = b.cache.Delete(ctx, *input.Bucket, *input.Key)
	}

	return &s3.DeleteObjectOutput{}, nil
}

func (b *SynapseBackend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	var result s3response.DeleteResult

	for _, obj := range input.Delete.Objects {
		key := *obj.Key
		_, err := b.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: input.Bucket,
			Key:    &key,
		})
		if err != nil {
			code := "InternalError"
			msg := err.Error()
			result.Error = append(result.Error, types.Error{
				Key:     &key,
				Code:    &code,
				Message: &msg,
			})
		} else {
			result.Deleted = append(result.Deleted, types.DeletedObject{
				Key: &key,
			})
		}
	}
	return result, nil
}

func (b *SynapseBackend) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if input.Bucket == nil {
		return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	maxKeys := int32(1000)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}

	prefix := derefStr(input.Prefix)
	objects, err := b.repos.Objects.ListByBucket(ctx, bucket.ID, prefix, int(maxKeys)+1)
	if err != nil {
		return s3response.ListObjectsResult{}, fmt.Errorf("listing objects: %w", err)
	}

	// Apply marker filter (ListByBucket doesn't handle marker).
	if input.Marker != nil && *input.Marker != "" {
		filtered := objects[:0]
		for _, obj := range objects {
			if obj.Key > *input.Marker {
				filtered = append(filtered, obj)
			}
		}
		objects = filtered
	}

	isTruncated := false
	result := s3response.ListObjectsResult{
		Name:        input.Bucket,
		Prefix:      input.Prefix,
		Marker:      input.Marker,
		IsTruncated: &isTruncated,
	}

	truncated := len(objects) > int(maxKeys)
	if truncated {
		objects = objects[:maxKeys]
		*result.IsTruncated = true
		nextMarker := objects[len(objects)-1].Key
		result.NextMarker = &nextMarker
	}

	for _, obj := range objects {
		etag := fmt.Sprintf(`"%s"`, obj.ETag)
		key := obj.Key
		size := obj.Size
		lastMod := obj.UpdatedAt
		result.Contents = append(result.Contents, s3response.Object{
			Key:          &key,
			LastModified: &lastMod,
			ETag:         &etag,
			Size:         &size,
		})
	}
	result.MaxKeys = &maxKeys

	return result, nil
}

func (b *SynapseBackend) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if input.Bucket == nil {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}

	maxKeys := int32(1000)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}

	prefix := derefStr(input.Prefix)
	objects, err := b.repos.Objects.ListByBucket(ctx, bucket.ID, prefix, int(maxKeys)+1)
	if err != nil {
		return s3response.ListObjectsV2Result{}, fmt.Errorf("listing objects v2: %w", err)
	}

	// Apply start-after / continuation-token filter.
	afterKey := ""
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		afterKey = *input.ContinuationToken
	} else if input.StartAfter != nil && *input.StartAfter != "" {
		afterKey = *input.StartAfter
	}
	if afterKey != "" {
		filtered := objects[:0]
		for _, obj := range objects {
			if obj.Key > afterKey {
				filtered = append(filtered, obj)
			}
		}
		objects = filtered
	}

	isTruncated := false
	result := s3response.ListObjectsV2Result{
		Name:        input.Bucket,
		Prefix:      input.Prefix,
		IsTruncated: &isTruncated,
	}

	truncated := len(objects) > int(maxKeys)
	if truncated {
		objects = objects[:maxKeys]
		*result.IsTruncated = true
		token := objects[len(objects)-1].Key
		result.NextContinuationToken = &token
	}

	for _, obj := range objects {
		etag := fmt.Sprintf(`"%s"`, obj.ETag)
		key := obj.Key
		size := obj.Size
		lastMod := obj.UpdatedAt
		result.Contents = append(result.Contents, s3response.Object{
			Key:          &key,
			LastModified: &lastMod,
			ETag:         &etag,
			Size:         &size,
		})
	}
	result.MaxKeys = &maxKeys
	keyCount := int32(len(result.Contents))
	result.KeyCount = &keyCount

	return result, nil
}

// getBucket retrieves an active bucket by name.
func (b *SynapseBackend) getBucket(ctx context.Context, name string) (*model.Bucket, error) {
	bucket, err := b.repos.Buckets.GetByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || bucket.Status == model.BucketStatusDeleted {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return bucket, nil
}

func stringOrDefault(s *string, def string) string {
	if s != nil && *s != "" {
		return *s
	}
	return def
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Ensure Body is consumed for PutObject, as it might come from
// a streaming source. This is a no-op helper.
var _ io.Reader = (*io.LimitedReader)(nil)
