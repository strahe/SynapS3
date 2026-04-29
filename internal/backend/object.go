package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
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

	// Atomic transaction: create object version + enqueue any needed task.
	var objectID int64
	var createdState model.ObjectState
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		reuse, err := b.resolveVersionReuse(ctx, txRepos.Objects, bucket.ID, cacheInfo.Size, cacheInfo.Checksum)
		if err != nil {
			return err
		}

		version := &model.ObjectVersion{
			VersionID:    versionID,
			BucketID:     bucket.ID,
			Key:          keyName,
			Size:         cacheInfo.Size,
			ETag:         cacheInfo.ETag,
			Checksum:     cacheInfo.Checksum,
			ContentType:  contentType,
			Metadata:     meta,
			CacheKey:     cacheKey,
			PieceCID:     reuse.PieceCID,
			RetrievalURL: reuse.RetrievalURL,
			InCache:      true,
			State:        reuse.State,
		}
		createdState = version.State

		objectID, err = txRepos.Objects.CreateVersionAndSetCurrent(ctx, version)
		if err != nil {
			return fmt.Errorf("creating object version: %w", err)
		}
		return b.enqueuePostWriteTask(ctx, txRepos, objectID, versionID, version.State)
	}); err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		b.deleteVersionCacheBestEffort(ctx, bucketName, cacheKey, "orphaned version cache file after put tx failure")
		return s3response.PutObjectOutput{}, err
	}
	b.completeFollowerIfStoredReuseWonRace(ctx, bucket.ID, bucketName, cacheInfo.Size, cacheInfo.Checksum, objectID, versionID, createdState)

	b.logger.Info("object stored", "bucket", bucketName, "key", keyName, "size", cacheInfo.Size, "versionID", versionID)
	admin.ObjectOperationsTotal.WithLabelValues("put", "success").Inc()

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	return s3response.PutObjectOutput{
		ETag:      etag,
		VersionID: versionID,
	}, nil
}

func (b *SynapseBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	var out *objectreader.Result
	var err error
	if input.VersionId != nil && *input.VersionId != "" {
		out, err = b.objectReader.OpenVersion(ctx, *input.Bucket, *input.Key, *input.VersionId, objectreader.S3Visibility)
	} else {
		out, err = b.objectReader.Open(ctx, *input.Bucket, *input.Key, objectreader.S3Visibility)
	}
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
		case errors.Is(err, objectreader.ErrNoSuchVersion):
			return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
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
		VersionId:     &out.VersionID,
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

	meta, err := b.objectMetadata(ctx, bucket.ID, *input.Key, derefStr(input.VersionId))
	if err != nil {
		return nil, err
	}

	return &s3.HeadObjectOutput{
		ContentLength: &meta.Size,
		ETag:          &meta.QuotedETag,
		ContentType:   &meta.ContentType,
		LastModified:  &meta.LastModified,
		VersionId:     &meta.VersionID,
	}, nil
}

// GetObjectAttributes returns object metadata and honors an explicit versionId.
func (b *SynapseBackend) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.GetObjectAttributesResponse{}, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return s3response.GetObjectAttributesResponse{}, err
	}

	meta, err := b.objectMetadata(ctx, bucket.ID, *input.Key, derefStr(input.VersionId))
	if err != nil {
		return s3response.GetObjectAttributesResponse{}, err
	}

	checksum := types.Checksum{ChecksumSHA256: &meta.Checksum}
	return s3response.GetObjectAttributesResponse{
		ETag:         &meta.QuotedETag,
		ObjectSize:   &meta.Size,
		StorageClass: types.StorageClassStandard,
		Checksum:     &checksum,
		VersionId:    &meta.VersionID,
		LastModified: &meta.LastModified,
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

	objects, commonPrefixes, truncated, nextMarker, err := b.listCurrentObjects(ctx, bucket.ID, prefix, derefStr(input.Delimiter), marker, int(maxKeys))
	if err != nil {
		return s3response.ListObjectsResult{}, fmt.Errorf("listing objects: %w", err)
	}

	isTruncated := false
	result := s3response.ListObjectsResult{
		Name:        input.Bucket,
		Prefix:      input.Prefix,
		Marker:      input.Marker,
		Delimiter:   input.Delimiter,
		IsTruncated: &isTruncated,
	}
	if truncated {
		*result.IsTruncated = true
		if derefStr(input.Delimiter) != "" {
			result.NextMarker = &nextMarker
		}
	}
	result.CommonPrefixes = commonPrefixes

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

// ListObjectVersions lists non-delete object versions with S3 markers and delimiter grouping.
func (b *SynapseBackend) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	if input.Bucket == nil {
		return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return s3response.ListVersionsResult{}, err
	}

	maxKeys := int32(1000)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}
	if maxKeys < 0 {
		return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrNegativeMaxKeys)
	}

	isTruncated := false
	result := s3response.ListVersionsResult{
		Name:            input.Bucket,
		Prefix:          input.Prefix,
		Delimiter:       input.Delimiter,
		KeyMarker:       input.KeyMarker,
		VersionIdMarker: input.VersionIdMarker,
		MaxKeys:         &maxKeys,
		IsTruncated:     &isTruncated,
		DeleteMarkers:   []types.DeleteMarkerEntry{},
	}
	if maxKeys == 0 {
		return result, nil
	}

	rows, commonPrefixes, truncated, nextKey, nextVersion, err := b.listVersions(ctx, bucket.ID, derefStr(input.Prefix), derefStr(input.Delimiter), derefStr(input.KeyMarker), derefStr(input.VersionIdMarker), int(maxKeys))
	if err != nil {
		return s3response.ListVersionsResult{}, fmt.Errorf("listing object versions: %w", err)
	}
	result.CommonPrefixes = commonPrefixes
	if truncated {
		*result.IsTruncated = true
		result.NextKeyMarker = &nextKey
		result.NextVersionIdMarker = &nextVersion
	}

	for _, row := range rows {
		key := row.Key
		versionID := row.VersionID
		etag := fmt.Sprintf(`"%s"`, row.ETag)
		size := row.Size
		lastModified := row.CreatedAt
		isLatest := row.VersionID == row.CurrentVersionID
		result.Versions = append(result.Versions, s3response.ObjectVersion{
			Key:          &key,
			VersionId:    &versionID,
			IsLatest:     &isLatest,
			LastModified: &lastModified,
			ETag:         &etag,
			Size:         &size,
			StorageClass: types.ObjectVersionStorageClassStandard,
		})
	}

	return result, nil
}

func (b *SynapseBackend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.CopySource == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidArgument)
	}

	// Parse CopySource: "/<bucket>/<key>" or "<bucket>/<key>", optionally with ?versionId=...
	srcBucketName, srcKey, srcVersionID, err := parseCopySource(*input.CopySource)
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
	srcVersion, err := b.versionForRead(ctx, srcBucket.ID, srcKey, srcVersionID)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	var srcResult *objectreader.Result
	srcResult, err = b.objectReader.OpenVersion(ctx, srcBucketName, srcKey, srcVersion.VersionID, objectreader.S3Visibility)
	if err != nil {
		return s3response.CopyObjectOutput{}, b.objectReaderError(err)
	}
	defer func() { _ = srcResult.Body.Close() }()

	// Write to a version-specific destination cache key.
	versionID := model.NewVersionID()
	cacheKey := versionCacheKey(versionID)
	staged, err := b.cache.PutStaged(ctx, dstBucketName, cacheKey, srcResult.Body)
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("staging copy destination: %w", err)
	}
	defer func() { _ = staged.Rollback() }()
	cacheInfo := staged.Info

	// Determine metadata: COPY (default) preserves source, REPLACE uses request metadata
	meta := make(map[string]string)
	contentType := srcVersion.ContentType
	if input.MetadataDirective == types.MetadataDirectiveReplace {
		if input.Metadata != nil {
			meta = input.Metadata
		}
		contentType = stringOrDefault(input.ContentType, "application/octet-stream")
	} else {
		if srcVersion.Metadata != nil {
			for k, v := range srcVersion.Metadata {
				meta[k] = v
			}
		}
	}

	if err := staged.Commit(); err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("committing copy cache: %w", err)
	}

	var objectID int64
	var createdState model.ObjectState
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		reuse := versionStorageReuse{State: model.ObjectStateCached}
		if srcBucket.ID == dstBucket.ID {
			var err error
			reuse, err = b.resolveVersionReuse(ctx, txRepos.Objects, dstBucket.ID, cacheInfo.Size, cacheInfo.Checksum)
			if err != nil {
				return err
			}
		}

		version := &model.ObjectVersion{
			VersionID:    versionID,
			BucketID:     dstBucket.ID,
			Key:          dstKey,
			Size:         cacheInfo.Size,
			ETag:         cacheInfo.ETag,
			Checksum:     cacheInfo.Checksum,
			ContentType:  contentType,
			Metadata:     meta,
			CacheKey:     cacheKey,
			PieceCID:     reuse.PieceCID,
			RetrievalURL: reuse.RetrievalURL,
			InCache:      true,
			State:        reuse.State,
		}
		createdState = version.State

		var err error
		objectID, err = txRepos.Objects.CreateVersionAndSetCurrent(ctx, version)
		if err != nil {
			return fmt.Errorf("creating copy destination version: %w", err)
		}
		return b.enqueuePostWriteTask(ctx, txRepos, objectID, versionID, version.State)
	}); err != nil {
		b.deleteVersionCacheBestEffort(ctx, dstBucketName, cacheKey, "orphaned version cache file after copy tx failure")
		return s3response.CopyObjectOutput{}, err
	}
	b.completeFollowerIfStoredReuseWonRace(ctx, dstBucket.ID, dstBucketName, cacheInfo.Size, cacheInfo.Checksum, objectID, versionID, createdState)

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	lastModified := time.Now()
	b.logger.Info("object copied", "src", srcBucketName+"/"+srcKey, "dst", dstBucketName+"/"+dstKey, "versionID", versionID)

	copySourceVersionID := srcVersion.VersionID
	return s3response.CopyObjectOutput{
		CopyObjectResult: &s3response.CopyObjectResult{
			ETag:         &etag,
			LastModified: &lastModified,
		},
		CopySourceVersionId: &copySourceVersionID,
		VersionId:           &versionID,
	}, nil
}

// parseCopySource parses a CopySource header value into bucket and key.
// Accepts "/<bucket>/<key>" or "<bucket>/<key>" format. URL-decodes per S3 spec.
func parseCopySource(src string) (bucket, key, versionID string, err error) {
	pathPart := src
	if before, after, ok := strings.Cut(src, "?"); ok {
		pathPart = before
		values, parseErr := url.ParseQuery(after)
		if parseErr != nil {
			return "", "", "", fmt.Errorf("parsing copy source query: %w", parseErr)
		}
		versionID = values.Get("versionId")
	}
	src, err = url.PathUnescape(pathPart)
	if err != nil {
		return "", "", "", fmt.Errorf("url-decoding copy source: %w", err)
	}
	src = strings.TrimPrefix(src, "/")
	idx := strings.IndexByte(src, '/')
	if idx <= 0 || idx == len(src)-1 {
		return "", "", "", fmt.Errorf("invalid copy source: %q", src)
	}
	return src[:idx], src[idx+1:], versionID, nil
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

	objects, commonPrefixes, truncated, nextToken, err := b.listCurrentObjects(ctx, bucket.ID, prefix, derefStr(input.Delimiter), afterKey, int(maxKeys))
	if err != nil {
		return s3response.ListObjectsV2Result{}, fmt.Errorf("listing objects v2: %w", err)
	}

	isTruncated := false
	result := s3response.ListObjectsV2Result{
		Name:        input.Bucket,
		Prefix:      input.Prefix,
		Delimiter:   input.Delimiter,
		IsTruncated: &isTruncated,
	}
	if truncated {
		*result.IsTruncated = true
		result.NextContinuationToken = &nextToken
	}
	result.CommonPrefixes = commonPrefixes

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
	keyCount := int32(len(result.Contents) + len(result.CommonPrefixes))
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

type objectMetadataResult struct {
	Size         int64
	QuotedETag   string
	Checksum     string
	ContentType  string
	VersionID    string
	LastModified time.Time
}

func (b *SynapseBackend) objectMetadata(ctx context.Context, bucketID int64, key, versionID string) (objectMetadataResult, error) {
	version, err := b.versionForRead(ctx, bucketID, key, versionID)
	if err != nil {
		return objectMetadataResult{}, err
	}
	etag := fmt.Sprintf(`"%s"`, version.ETag)
	return objectMetadataResult{
		Size:         version.Size,
		QuotedETag:   etag,
		Checksum:     version.Checksum,
		ContentType:  version.ContentType,
		VersionID:    version.VersionID,
		LastModified: version.CreatedAt,
	}, nil
}

func (b *SynapseBackend) versionForRead(ctx context.Context, bucketID int64, key, versionID string) (*model.ObjectVersion, error) {
	if versionID != "" {
		version, err := b.repos.Objects.GetVersionByBucketKeyAndID(ctx, bucketID, key, versionID)
		if err != nil {
			return nil, fmt.Errorf("querying object version: %w", err)
		}
		if version == nil {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
		}
		return version, nil
	}

	obj, err := b.repos.Objects.GetByBucketAndKey(ctx, bucketID, key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if obj == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if obj.CurrentVersionID != "" {
		version, err := b.repos.Objects.GetVersionByBucketKeyAndID(ctx, bucketID, key, obj.CurrentVersionID)
		if err != nil {
			return nil, fmt.Errorf("querying current object version: %w", err)
		}
		if version != nil {
			return version, nil
		}
	}
	return objectSnapshotVersion(obj), nil
}

func objectSnapshotVersion(obj *model.Object) *model.ObjectVersion {
	return &model.ObjectVersion{
		VersionID:    obj.CurrentVersionID,
		ObjectID:     obj.ID,
		BucketID:     obj.BucketID,
		Key:          obj.Key,
		Size:         obj.Size,
		ETag:         obj.ETag,
		Checksum:     obj.Checksum,
		ContentType:  obj.ContentType,
		Metadata:     obj.Metadata,
		CacheKey:     obj.CacheKey,
		PieceCID:     obj.PieceCID,
		RetrievalURL: obj.RetrievalURL,
		InCache:      obj.InCache,
		InFilecoin:   obj.InFilecoin,
		State:        obj.State,
		CreatedAt:    obj.UpdatedAt,
		UpdatedAt:    obj.UpdatedAt,
	}
}

type versionStorageReuse struct {
	State        model.ObjectState
	PieceCID     *string
	RetrievalURL *string
}

func (b *SynapseBackend) resolveVersionReuse(ctx context.Context, objects repository.ObjectRepository, bucketID int64, size int64, checksum string) (versionStorageReuse, error) {
	reuse := versionStorageReuse{State: model.ObjectStateCached}
	if checksum == "" {
		return reuse, nil
	}

	stored, err := objects.FindReusableStoredVersion(ctx, bucketID, size, checksum)
	if err != nil {
		return reuse, err
	}
	if stored != nil {
		reuse.State = model.ObjectStateStored
		reuse.PieceCID = stored.PieceCID
		reuse.RetrievalURL = stored.RetrievalURL
		return reuse, nil
	}

	active, err := objects.FindReusableActiveUploadVersion(ctx, bucketID, size, checksum)
	if err != nil {
		return reuse, err
	}
	if active != nil {
		reuse.State = model.ObjectStateUploading
	}
	return reuse, nil
}

func (b *SynapseBackend) reusableStoredVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error) {
	if checksum == "" {
		return nil, nil
	}
	return b.repos.Objects.FindReusableStoredVersion(ctx, bucketID, size, checksum)
}

func (b *SynapseBackend) completeFollowerIfStoredReuseWonRace(ctx context.Context, bucketID int64, bucketName string, size int64, checksum string, objectID int64, versionID string, createdState model.ObjectState) {
	if createdState != model.ObjectStateUploading || checksum == "" {
		return
	}

	reusable, err := b.reusableStoredVersion(ctx, bucketID, size, checksum)
	if err != nil {
		b.logger.Warn("checking stored reuse after active upload follower write", "bucket", bucketName, "versionID", versionID, "error", err)
		return
	}
	if reusable == nil || reusable.PieceCID == nil || reusable.RetrievalURL == nil {
		return
	}

	if err := b.repos.Objects.SetVersionStorageInfoAndTransition(ctx, versionID, *reusable.PieceCID, *reusable.RetrievalURL, model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		b.logger.Debug("active upload follower already handled or still pending", "bucket", bucketName, "versionID", versionID, "error", err)
		return
	}
	if err := b.enqueuePostWriteTask(ctx, b.repos, objectID, versionID, model.ObjectStateStored); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		b.logger.Warn("enqueueing eviction after active upload follower completion", "bucket", bucketName, "versionID", versionID, "error", err)
	}
}

func (b *SynapseBackend) enqueuePostWriteTask(ctx context.Context, repos *repository.Repositories, objectID int64, versionID string, state model.ObjectState) error {
	switch state {
	case model.ObjectStateCached:
		task := &model.Task{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          objectID,
			RefVersionID:   versionID,
			IdempotencyKey: fmt.Sprintf("upload:%s", versionID),
			Status:         model.TaskStatusPending,
			MaxRetries:     b.uploadMaxRetries,
			ScheduledAt:    time.Now(),
		}
		return repos.Tasks.Create(ctx, task)
	case model.ObjectStateStored:
		if !b.autoEvict {
			return nil
		}
		task := &model.Task{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          objectID,
			RefVersionID:   versionID,
			IdempotencyKey: fmt.Sprintf("evict_cache:%s", versionID),
			Status:         model.TaskStatusPending,
			MaxRetries:     b.evictMaxRetries,
			ScheduledAt:    time.Now(),
		}
		return repos.Tasks.Create(ctx, task)
	default:
		return nil
	}
}

func (b *SynapseBackend) objectReaderError(err error) error {
	if errors.Is(err, objectreader.ErrCacheMiss) {
		admin.CacheMissesTotal.Inc()
	}
	switch {
	case errors.Is(err, objectreader.ErrInvalidArgument):
		return s3err.GetAPIError(s3err.ErrInvalidArgument)
	case errors.Is(err, objectreader.ErrNoSuchBucket):
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	case errors.Is(err, objectreader.ErrNoSuchKey):
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	case errors.Is(err, objectreader.ErrNoSuchVersion):
		return s3err.GetAPIError(s3err.ErrNoSuchVersion)
	case errors.Is(err, objectreader.ErrProviderDownload):
		return s3err.GetAPIError(s3err.ErrInternalError)
	default:
		return err
	}
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
