package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	cid "github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
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

	// Write to local cache staging (fsync'd temp file, NOT yet at final path).
	staged, err := b.cache.PutStaged(ctx, bucketName, keyName, input.Body)
	if err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		return s3response.PutObjectOutput{}, fmt.Errorf("staging object: %w", err)
	}
	// Ensure cleanup if we don't commit.
	defer func() { _ = staged.Rollback() }()

	cacheInfo := staged.Info

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
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		// Rollback is handled by defer — staged file is removed, old file untouched.
		return s3response.PutObjectOutput{}, err
	}

	// DB transaction committed — publish the cache file atomically.
	if err := staged.Commit(); err != nil {
		// DB has the new generation but cache rename failed. Return error so the
		// client retries — the retry will create gen N+1 which works correctly.
		// Without this, the client sees 200 OK but the object is unservable
		// (no cache file, uploader will also fail on cache miss).
		b.logger.Error("cache commit failed after DB tx", "bucket", bucketName, "key", keyName, "error", err)
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		return s3response.PutObjectOutput{}, fmt.Errorf("committing cache file: %w", err)
	}

	b.logger.Info("object stored", "bucket", bucketName, "key", keyName, "size", cacheInfo.Size, "gen", newGen)
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

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return nil, err
	}

	obj, err := b.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, *input.Key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if obj == nil {
		admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	// Try local cache first (no TOCTOU — call Get directly, handle ErrNotExist).
	rc, _, cacheErr := b.cache.Get(ctx, *input.Bucket, *input.Key)
	if cacheErr == nil {
		admin.CacheHitsTotal.Inc()
		admin.ObjectOperationsTotal.WithLabelValues("get", "success").Inc()
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
		admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
		return nil, fmt.Errorf("reading from cache: %w", cacheErr)
	}

	// Cache miss — object has been evicted, try SP download if PieceCID is available.
	admin.CacheMissesTotal.Inc()
	if obj.PieceCID != nil && *obj.PieceCID != "" && b.storage != nil {
		// Size guard: prevent OOM from downloading very large objects into memory.
		// go-synapse's Download() loads the entire payload into []byte.
		if b.maxSPDownloadSize > 0 && obj.Size > b.maxSPDownloadSize {
			b.logger.Warn("object too large for SP download",
				"key", *input.Key, "size", obj.Size, "limit", b.maxSPDownloadSize)
			admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
			return nil, s3err.GetAPIError(s3err.ErrEntityTooLarge)
		}

		pieceCID, parseErr := cid.Decode(*obj.PieceCID)
		if parseErr != nil {
			b.logger.Warn("invalid PieceCID, cannot download from SP", "key", *input.Key, "pieceCID", *obj.PieceCID)
			admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}

		// NOTE: storage.Download loads the entire object into memory (SDK limitation).
		// For very large objects this can cause high memory pressure. A streaming
		// download API is needed in go-synapse to fully resolve this.
		data, dlErr := b.storage.Download(ctx, pieceCID, nil)
		if dlErr != nil {
			b.logger.Warn("SP download failed", "key", *input.Key, "err", dlErr)
			admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
			return nil, s3err.GetAPIError(s3err.ErrInternalError)
		}

		// Synchronous best-effort cache rehydration with generation check.
		// Re-check generation to avoid overwriting a concurrent PutObject's newer data.
		cur, _ := b.repos.Objects.GetByID(ctx, obj.ID)
		if cur != nil && cur.Generation == obj.Generation {
			if _, putErr := b.cache.Put(ctx, *input.Bucket, *input.Key, bytes.NewReader(data)); putErr != nil {
				b.logger.Warn("cache rehydration failed (best-effort)", "key", *input.Key, "error", putErr)
			}
		}

		etag := fmt.Sprintf(`"%s"`, obj.ETag)
		contentType := obj.ContentType
		size := int64(len(data))

		admin.ObjectOperationsTotal.WithLabelValues("get", "success").Inc()
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(data)),
			ContentLength: &size,
			ETag:          &etag,
			ContentType:   &contentType,
		}, nil
	}

	admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
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

	admin.ObjectOperationsTotal.WithLabelValues("delete", "success").Inc()
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
	srcReader, _, cacheErr := b.cache.Get(ctx, srcBucketName, srcKey)
	if cacheErr != nil {
		if os.IsNotExist(cacheErr) {
			// Cache miss — SP fallback is a future feature
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return s3response.CopyObjectOutput{}, fmt.Errorf("reading source from cache: %w", cacheErr)
	}
	defer func() { _ = srcReader.Close() }()

	// Write to destination cache (staged — not committed until DB tx succeeds)
	staged, err := b.cache.PutStaged(ctx, dstBucketName, dstKey, srcReader)
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

	var newGen int64
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		obj := &model.Object{
			BucketID:    dstBucket.ID,
			Key:         dstKey,
			Size:        cacheInfo.Size,
			ETag:        cacheInfo.ETag,
			Checksum:    cacheInfo.Checksum,
			ContentType: contentType,
			Metadata:    meta,
			CachePath:   cacheInfo.Path,
			State:       model.ObjectStateCached,
			MaxRetries:  5,
		}

		id, gen, err := txRepos.Objects.UpsertAndBumpGeneration(ctx, obj)
		if err != nil {
			return fmt.Errorf("upserting copy destination: %w", err)
		}
		newGen = gen

		task := &model.Task{
			Type:           model.TaskTypeUploadToSP,
			RefType:        "object",
			RefID:          id,
			RefGeneration:  gen,
			IdempotencyKey: fmt.Sprintf("upload:%d:%d", id, gen),
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		return txRepos.Tasks.Create(ctx, task)
	}); err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	if err := staged.Commit(); err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("committing copy cache: %w", err)
	}

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	lastModified := time.Now()
	b.logger.Info("object copied", "src", srcBucketName+"/"+srcKey, "dst", dstBucketName+"/"+dstKey, "gen", newGen)

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

// Ensure Body is consumed for PutObject, as it might come from
// a streaming source. This is a no-op helper.
var _ io.Reader = (*io.LimitedReader)(nil)
