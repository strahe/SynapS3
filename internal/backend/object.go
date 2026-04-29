package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	bucketName := derefStr(input.Bucket)
	keyName := derefStr(input.Key)

	bucket, err := b.requireActiveBucket(ctx, bucketName)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	versionID := model.NewVersionID()
	cacheKey := versionCacheKey(versionID)

	// Write to a version-specific cache key so overwrites cannot affect older tasks.
	staged, err := b.cache.PutStaged(ctx, bucketName, cacheKey, input.Body)
	if err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		return s3response.PutObjectOutput{}, fmt.Errorf("staging object: %w", err)
	}
	defer func() { _ = staged.Rollback() }()

	cacheInfo := staged.Info

	// Build metadata map from input.
	meta := make(map[string]string)
	if input.Metadata != nil {
		meta = input.Metadata
	}
	contentType := stringOrDefault(input.ContentType, "application/octet-stream")

	if err := staged.Commit(); err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		return s3response.PutObjectOutput{}, fmt.Errorf("committing cache file: %w", err)
	}

	// Atomic transaction: upsert object + enqueue task.
	var write repository.ObjectVersionWriteResult
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		version := &model.ObjectVersion{
			VersionID:   versionID,
			BucketID:    bucket.ID,
			Key:         keyName,
			Size:        cacheInfo.Size,
			ETag:        cacheInfo.ETag,
			Checksum:    cacheInfo.Checksum,
			ContentType: contentType,
			Metadata:    meta,
			CacheKey:    cacheKey,
			State:       model.ObjectStateCached,
		}

		var err error
		write, err = txRepos.Objects.CreateVersionAndSetCurrentIfChanged(ctx, version)
		if err != nil {
			return fmt.Errorf("creating object version: %w", err)
		}
		if !write.Created {
			return nil
		}

		task := &model.Task{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          write.ObjectID,
			RefVersionID:   versionID,
			IdempotencyKey: fmt.Sprintf("upload:%s", versionID),
			Status:         model.TaskStatusPending,
			MaxRetries:     b.uploadMaxRetries,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		b.deleteVersionCacheBestEffort(ctx, bucketName, cacheKey, "orphaned version cache file after put tx failure")
		return s3response.PutObjectOutput{}, err
	}

	if !write.Created {
		b.deleteVersionCacheBestEffort(ctx, bucketName, cacheKey, "orphaned duplicate version cache file")
		b.logger.Info("object unchanged", "bucket", bucketName, "key", keyName, "versionID", write.VersionID)
		admin.ObjectOperationsTotal.WithLabelValues("put", "success").Inc()
		etag := fmt.Sprintf(`"%s"`, write.ETag)
		return s3response.PutObjectOutput{ETag: etag}, nil
	}

	b.logger.Info("object stored", "bucket", bucketName, "key", keyName, "size", cacheInfo.Size, "versionID", versionID)
	admin.ObjectOperationsTotal.WithLabelValues("put", "success").Inc()

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	return s3response.PutObjectOutput{
		ETag: etag,
	}, nil
}

func (b *SynapseBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	out, err := b.objectReader.Open(ctx, *input.Bucket, *input.Key, objectreader.S3Visibility)
	if err != nil {
		if errors.Is(err, objectreader.ErrCacheMiss) {
			admin.CacheMissesTotal.Inc()
		}
		admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
		switch {
		case errors.Is(err, objectreader.ErrInvalidArgument):
			return nil, s3err.GetAPIError(s3err.ErrInvalidArgument)
		case errors.Is(err, objectreader.ErrNoSuchBucket):
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		case errors.Is(err, objectreader.ErrNoSuchKey):
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		case errors.Is(err, objectreader.ErrProviderDownload):
			return nil, s3err.GetAPIError(s3err.ErrInternalError)
		default:
			return nil, err
		}
	}

	if out.CacheMiss {
		admin.CacheMissesTotal.Inc()
	}
	switch out.Source {
	case objectreader.SourceCache:
		admin.CacheHitsTotal.Inc()
	}

	admin.ObjectOperationsTotal.WithLabelValues("get", "success").Inc()
	etag := fmt.Sprintf(`"%s"`, out.ETag)
	contentType := out.ContentType
	return &s3.GetObjectOutput{
		Body:          out.Body,
		ContentLength: &out.Size,
		ETag:          &etag,
		ContentType:   &contentType,
	}, nil
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
	marker := derefStr(input.Marker)

	// MaxKeys=0 is valid in S3 (used to probe bucket existence).
	if maxKeys <= 0 {
		isTruncated := false
		result := s3response.ListObjectsResult{
			Name:        input.Bucket,
			Prefix:      input.Prefix,
			Marker:      input.Marker,
			MaxKeys:     &maxKeys,
			IsTruncated: &isTruncated,
		}
		return result, nil
	}

	objects, err := b.repos.Objects.ListByBucket(ctx, bucket.ID, prefix, marker, int(maxKeys)+1)
	if err != nil {
		return s3response.ListObjectsResult{}, fmt.Errorf("listing objects: %w", err)
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

func (b *SynapseBackend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.CopySource == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	// Parse CopySource: "/<bucket>/<key>" or "<bucket>/<key>"
	srcBucketName, srcKey, err := parseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidCopySourceObject)
	}

	dstBucketName := *input.Bucket
	dstKey := *input.Key

	// Validate source and destination buckets
	srcBucket, err := b.getBucket(ctx, srcBucketName)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	dstBucket, err := b.requireActiveBucket(ctx, dstBucketName)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	// Get source object metadata
	srcObj, err := b.repos.Objects.GetByBucketAndKey(ctx, srcBucket.ID, srcKey)
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("querying source object: %w", err)
	}
	if srcObj == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	// Read source data from cache
	srcReader, _, cacheErr := b.cache.Get(ctx, srcBucketName, srcObj.CacheKey)
	if cacheErr != nil {
		if os.IsNotExist(cacheErr) {
			// Cache miss — SP fallback is a future feature
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return s3response.CopyObjectOutput{}, fmt.Errorf("reading source from cache: %w", cacheErr)
	}
	defer func() { _ = srcReader.Close() }()

	// Write to a version-specific destination cache key.
	versionID := model.NewVersionID()
	cacheKey := versionCacheKey(versionID)
	staged, err := b.cache.PutStaged(ctx, dstBucketName, cacheKey, srcReader)
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("staging copy destination: %w", err)
	}
	defer func() { _ = staged.Rollback() }()
	cacheInfo := staged.Info

	// Determine metadata: COPY (default) preserves source, REPLACE uses request metadata
	meta := make(map[string]string)
	contentType := srcObj.ContentType
	if input.MetadataDirective == types.MetadataDirectiveReplace {
		if input.Metadata != nil {
			meta = input.Metadata
		}
		contentType = stringOrDefault(input.ContentType, "application/octet-stream")
	} else {
		if srcObj.Metadata != nil {
			for k, v := range srcObj.Metadata {
				meta[k] = v
			}
		}
	}

	if err := staged.Commit(); err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("committing copy cache: %w", err)
	}

	var write repository.ObjectVersionWriteResult
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		version := &model.ObjectVersion{
			VersionID:   versionID,
			BucketID:    dstBucket.ID,
			Key:         dstKey,
			Size:        cacheInfo.Size,
			ETag:        cacheInfo.ETag,
			Checksum:    cacheInfo.Checksum,
			ContentType: contentType,
			Metadata:    meta,
			CacheKey:    cacheKey,
			State:       model.ObjectStateCached,
		}

		var err error
		write, err = txRepos.Objects.CreateVersionAndSetCurrentIfChanged(ctx, version)
		if err != nil {
			return fmt.Errorf("creating copy destination version: %w", err)
		}
		if !write.Created {
			return nil
		}

		task := &model.Task{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          write.ObjectID,
			RefVersionID:   versionID,
			IdempotencyKey: fmt.Sprintf("upload:%s", versionID),
			Status:         model.TaskStatusPending,
			MaxRetries:     b.uploadMaxRetries,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		b.deleteVersionCacheBestEffort(ctx, dstBucketName, cacheKey, "orphaned version cache file after copy tx failure")
		return s3response.CopyObjectOutput{}, err
	}

	if !write.Created {
		b.deleteVersionCacheBestEffort(ctx, dstBucketName, cacheKey, "orphaned duplicate copy version cache file")
		etag := fmt.Sprintf(`"%s"`, write.ETag)
		lastModified := time.Now()
		b.logger.Info("copy destination unchanged", "src", srcBucketName+"/"+srcKey, "dst", dstBucketName+"/"+dstKey, "versionID", write.VersionID)
		return s3response.CopyObjectOutput{
			CopyObjectResult: &s3response.CopyObjectResult{
				ETag:         &etag,
				LastModified: &lastModified,
			},
		}, nil
	}

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	lastModified := time.Now()
	b.logger.Info("object copied", "src", srcBucketName+"/"+srcKey, "dst", dstBucketName+"/"+dstKey, "versionID", versionID)

	return s3response.CopyObjectOutput{
		CopyObjectResult: &s3response.CopyObjectResult{
			ETag:         &etag,
			LastModified: &lastModified,
		},
	}, nil
}

// parseCopySource parses a CopySource header value into bucket and key.
// Accepts "/<bucket>/<key>" or "<bucket>/<key>" format. URL-decodes per S3 spec.
func parseCopySource(src string) (bucket, key string, err error) {
	src, err = url.PathUnescape(src)
	if err != nil {
		return "", "", fmt.Errorf("url-decoding copy source: %w", err)
	}
	src = strings.TrimPrefix(src, "/")
	idx := strings.IndexByte(src, '/')
	if idx <= 0 || idx == len(src)-1 {
		return "", "", fmt.Errorf("invalid copy source: %q", src)
	}
	return src[:idx], src[idx+1:], nil
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

	// Determine afterKey from continuation token or start-after.
	afterKey := ""
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		afterKey = *input.ContinuationToken
	} else if input.StartAfter != nil && *input.StartAfter != "" {
		afterKey = *input.StartAfter
	}

	// MaxKeys=0 is valid in S3.
	if maxKeys <= 0 {
		isTruncated := false
		zero := int32(0)
		result := s3response.ListObjectsV2Result{
			Name:        input.Bucket,
			Prefix:      input.Prefix,
			MaxKeys:     &maxKeys,
			KeyCount:    &zero,
			IsTruncated: &isTruncated,
		}
		return result, nil
	}

	objects, err := b.repos.Objects.ListByBucket(ctx, bucket.ID, prefix, afterKey, int(maxKeys)+1)
	if err != nil {
		return s3response.ListObjectsV2Result{}, fmt.Errorf("listing objects v2: %w", err)
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

// getBucket retrieves a bucket visible to S3 clients.
// Rejects deleted, create_failed, and delete_failed statuses.
func (b *SynapseBackend) getBucket(ctx context.Context, name string) (*model.Bucket, error) {
	bucket, err := b.repos.Buckets.GetByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || !bucket.Status.IsVisible() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return bucket, nil
}

// requireActiveBucket retrieves a bucket that accepts write operations.
// Active and creating buckets are writable; deleting/failed buckets are rejected.
func (b *SynapseBackend) requireActiveBucket(ctx context.Context, name string) (*model.Bucket, error) {
	bucket, err := b.repos.Buckets.GetByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || !bucket.Status.IsWritable() {
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

func versionCacheKey(versionID string) string {
	return path.Join(".versions", versionID)
}

func (b *SynapseBackend) deleteVersionCacheBestEffort(ctx context.Context, bucketName, cacheKey, message string) {
	if cleanupErr := b.cache.Delete(ctx, bucketName, cacheKey); cleanupErr != nil {
		b.logger.Warn(message, "bucket", bucketName, "cacheKey", cacheKey, "error", cleanupErr)
	}
}

// Ensure Body is consumed for PutObject, as it might come from
// a streaming source. This is a no-op helper.
var _ io.Reader = (*io.LimitedReader)(nil)
