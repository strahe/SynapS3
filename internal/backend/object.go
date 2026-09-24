package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/objectkey"
	"github.com/strahe/synaps3/internal/objectlimits"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/storagepipeline"
	taskengine "github.com/strahe/synaps3/internal/task"
	versitybackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	bucketName := derefStr(input.Bucket)
	keyName := derefStr(input.Key)
	if err := validateObjectKey(keyName); err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		return s3response.PutObjectOutput{}, err
	}

	bucket, err := b.requireWritableBucket(ctx, bucketName)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	versionID := model.NewVersionID()

	if input.ContentLength != nil {
		if err := objectlimits.ValidateFOCUploadSize(*input.ContentLength); err != nil {
			admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
			return s3response.PutObjectOutput{}, objectSizeAPIError(err)
		}
	}

	// Stage beside the content directory: the destination is content-addressed
	// and only nameable once the staged checksum resolves a content row.
	staged, err := b.cache.PutStaged(ctx, bucketName, stagingCacheKey(versionID), objectlimits.LimitFOCUploadReader(input.Body))
	if err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		if errors.Is(err, objectlimits.ErrTooLarge) {
			return s3response.PutObjectOutput{}, objectSizeAPIError(err)
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("staging object: %w", err)
	}
	defer func() { _ = staged.Rollback() }()

	cacheInfo := staged.Info
	if err := objectlimits.ValidateFOCUploadSize(cacheInfo.Size); err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		return s3response.PutObjectOutput{}, objectSizeAPIError(err)
	}

	// Build metadata map from input.
	meta := make(map[string]string)
	if input.Metadata != nil {
		meta = input.Metadata
	}
	contentType := stringOrDefault(input.ContentType, "application/octet-stream")

	content, err := b.ensureContentForBytes(ctx, b.repos, bucket, cacheInfo.Size, cacheInfo.Checksum)
	if err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		return s3response.PutObjectOutput{}, err
	}
	cacheKey := model.ContentCacheKey(content.ID)
	var objectID int64
	cacheCommitted := false
	// The content gate stays held from the physical commit through the database
	// transaction. A deletion therefore observes either the old state or the
	// new file and its committed version/presence together.
	err = b.cacheGate.Commit(cacheKey, func() error {
		if err := staged.CommitAs(bucketName, cacheKey); err != nil {
			return fmt.Errorf("committing cache file: %w", err)
		}
		cacheCommitted = true
		return b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
			state, err := txRepos.Contents.ContentPipelineState(ctx, content.ID)
			if err != nil {
				return err
			}

			version := &model.ObjectVersion{
				VersionID:   versionID,
				BucketID:    bucket.ID,
				Key:         keyName,
				Size:        cacheInfo.Size,
				ETag:        cacheInfo.ETag,
				ContentType: contentType,
				Metadata:    meta,
				ContentID:   &content.ID,
			}
			objectID, err = txRepos.Objects.CreateVersionAndSetCurrent(ctx, version)
			if err != nil {
				return fmt.Errorf("creating object version: %w", err)
			}
			return b.enqueuePostWriteTask(ctx, txRepos, objectID, versionID, version.ContentID, state)
		})
	})
	if err != nil {
		admin.ObjectOperationsTotal.WithLabelValues("put", "failure").Inc()
		if cacheCommitted {
			b.releaseContentCacheIfUnreferenced(ctx, bucketName, content.ID, "orphaned content cache file after put tx failure")
		}
		return s3response.PutObjectOutput{}, contentWriteError(err)
	}

	b.logger.Info("object stored", "bucket", bucketName, "key", keyName, "size", cacheInfo.Size, "versionID", versionID)
	admin.ObjectOperationsTotal.WithLabelValues("put", "success").Inc()

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	return s3response.PutObjectOutput{
		ETag:      etag,
		VersionID: versionID,
		Size:      &cacheInfo.Size,
	}, nil
}

func objectSizeAPIError(err error) s3err.APIError {
	switch {
	case errors.Is(err, objectlimits.ErrTooSmall):
		apiErr := s3err.GetAPIError(s3err.ErrEntityTooSmall)
		apiErr.Description = err.Error()
		return apiErr
	case errors.Is(err, objectlimits.ErrTooLarge):
		apiErr := s3err.GetAPIError(s3err.ErrEntityTooLarge)
		apiErr.Description = err.Error()
		return apiErr
	default:
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
}

func (b *SynapseBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input == nil {
		return nil, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil {
		return nil, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
		)
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
			return nil, invalidArgument("")
		case errors.Is(err, objectreader.ErrNoSuchBucket):
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		case errors.Is(err, objectreader.ErrNoSuchKey):
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		case errors.Is(err, objectreader.ErrNoSuchVersion):
			return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
		case errors.Is(err, objectreader.ErrMethodNotAllowed):
			return nil, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
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

	etag := fmt.Sprintf(`"%s"`, out.ETag)
	contentType := out.ContentType
	acceptRanges := "bytes"
	length := out.Size
	var contentRange *string
	if input.Range != nil && *input.Range != "" {
		start, count, valid, rangeErr := versitybackend.ParseObjectRange(out.Size, *input.Range)
		if rangeErr != nil {
			_ = out.Body.Close()
			admin.ObjectOperationsTotal.WithLabelValues("get", "failure").Inc()
			return nil, rangeErr
		}
		if valid {
			out.Body = newRangeReadCloser(out.Body, start, count, out.Source == objectreader.SourceProvider)
			length = count
			rangeValue := fmt.Sprintf("bytes %d-%d/%d", start, start+count-1, out.Size)
			contentRange = &rangeValue
		}
	}
	admin.ObjectOperationsTotal.WithLabelValues("get", "success").Inc()
	return &s3.GetObjectOutput{
		Body:          out.Body,
		ContentLength: &length,
		ContentRange:  contentRange,
		AcceptRanges:  &acceptRanges,
		ETag:          &etag,
		ContentType:   &contentType,
		VersionId:     &out.VersionID,
		LastModified:  &out.LastModified,
		Metadata:      out.Metadata,
	}, nil
}

func (b *SynapseBackend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input == nil {
		return nil, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil {
		return nil, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
		)
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
		Metadata:      meta.Metadata,
	}, nil
}

// GetObjectAttributes returns object metadata and honors an explicit versionId.
func (b *SynapseBackend) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	if input == nil {
		return s3response.GetObjectAttributesResponse{}, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil {
		return s3response.GetObjectAttributesResponse{}, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
		)
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
	resp := s3response.GetObjectAttributesResponse{
		ETag:         &meta.QuotedETag,
		ObjectSize:   &meta.Size,
		StorageClass: types.StorageClassStandard,
		Checksum:     &checksum,
		VersionId:    &meta.VersionID,
		LastModified: &meta.LastModified,
	}
	if objectPartsRequested(input.ObjectAttributes) {
		objectParts, err := b.getObjectAttributeParts(ctx, meta.MultipartUploadID, input)
		if err != nil {
			return s3response.GetObjectAttributesResponse{}, err
		}
		resp.ObjectParts = objectParts
	}
	return resp, nil
}

func (b *SynapseBackend) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if input == nil || input.Bucket == nil {
		return s3response.ListObjectsResult{}, invalidArgument("Bucket")
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
		lastMod := obj.CreatedAt
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

// ListObjectVersions lists object versions and delete markers with S3 markers and delimiter grouping.
func (b *SynapseBackend) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	if input == nil || input.Bucket == nil {
		return s3response.ListVersionsResult{}, invalidArgument("Bucket")
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
		return s3response.ListVersionsResult{}, s3err.GetInvalidArgumentErr(s3err.InvalidArgNegativeMaxKeys, fmt.Sprint(maxKeys))
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
		lastModified := row.CreatedAt
		isLatest := row.IsCurrent
		if row.IsDeleteMarker {
			result.DeleteMarkers = append(result.DeleteMarkers, types.DeleteMarkerEntry{
				Key:          &key,
				VersionId:    &versionID,
				IsLatest:     &isLatest,
				LastModified: &lastModified,
			})
			continue
		}

		etag := fmt.Sprintf(`"%s"`, row.ETag)
		size := row.Size
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

const deleteObjectsMaxObjects = 1000

func (b *SynapseBackend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if input == nil {
		return nil, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil {
		return nil, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
		)
	}

	bucket, err := b.requireWritableBucket(ctx, *input.Bucket)
	if err != nil {
		return nil, err
	}
	return b.deleteObjectInBucket(ctx, bucket, *input.Key, derefStr(input.VersionId))
}

func (b *SynapseBackend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if input == nil {
		return s3response.DeleteResult{}, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Delete == nil {
		return s3response.DeleteResult{}, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Delete", input.Delete == nil),
		)
	}

	bucket, err := b.requireWritableBucket(ctx, *input.Bucket)
	if err != nil {
		return s3response.DeleteResult{}, err
	}
	if len(input.Delete.Objects) > deleteObjectsMaxObjects {
		return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	// TODO: Support Quiet after upstream issue is resolved: https://github.com/versity/versitygw/issues/2124
	result := s3response.DeleteResult{}
	for _, obj := range input.Delete.Objects {
		if obj.Key == nil || *obj.Key == "" {
			result.Error = append(result.Error, deleteObjectsEntryError(obj.Key, obj.VersionId, invalidArgument("Key")))
			continue
		}

		out, err := b.deleteObjectInBucket(ctx, bucket, *obj.Key, derefStr(obj.VersionId))
		if err != nil {
			result.Error = append(result.Error, deleteObjectsEntryError(obj.Key, obj.VersionId, err))
			continue
		}
		result.Deleted = append(result.Deleted, deleteObjectsDeletedObject(obj, out))
	}

	return result, nil
}

func (b *SynapseBackend) deleteObjectInBucket(ctx context.Context, bucket *model.Bucket, key string, versionID string) (*s3.DeleteObjectOutput, error) {
	if versionID == "" {
		if err := validateObjectKey(key); err != nil {
			return nil, err
		}
		marker, err := b.repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, key, model.NewVersionID())
		if err != nil {
			return nil, fmt.Errorf("creating delete marker: %w", err)
		}
		deleteMarker := true
		return &s3.DeleteObjectOutput{
			DeleteMarker: &deleteMarker,
			VersionId:    &marker.VersionID,
		}, nil
	}

	version, err := b.repos.Objects.GetVersionByBucketKeyAndID(ctx, bucket.ID, key, versionID)
	if err != nil {
		return nil, fmt.Errorf("querying object version for delete: %w", err)
	}
	if version == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
	}
	if !version.IsDeleteMarker {
		var result repository.DeleteObjectVersionResult
		err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
			var deleteErr error
			result, deleteErr = txRepos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
				BucketID: bucket.ID, Key: key, VersionID: versionID,
			})
			if deleteErr != nil {
				return deleteErr
			}
			return b.bindStorageCleanupTask(ctx, txRepos, result.StorageCleanup)
		})
		if err != nil {
			switch {
			case errors.Is(err, repository.ErrNotFound):
				return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
			case errors.Is(err, repository.ErrPermanentDeleteStorageBusy):
				return nil, s3err.APIError{
					Code:           "InvalidRequest",
					Description:    "The object version cannot be deleted while storage is still in progress or a Filecoin transaction is awaiting confirmation. Try again later.",
					HTTPStatusCode: http.StatusBadRequest,
				}
			case errors.Is(err, repository.ErrConflict):
				return nil, s3err.APIError{
					Code:           "InvalidRequest",
					Description:    "This data version is not eligible for deletion.",
					HTTPStatusCode: http.StatusBadRequest,
				}
			default:
				return nil, fmt.Errorf("permanently deleting object version: %w", err)
			}
		}
		b.releaseContentCache(ctx, bucket.Name, result.ContentID)
		return &s3.DeleteObjectOutput{
			VersionId: &versionID,
		}, nil
	}
	if err := b.repos.Objects.DeleteMarkerVersion(ctx, bucket.ID, key, versionID); err != nil {
		return nil, fmt.Errorf("deleting marker version: %w", err)
	}
	deleteMarker := true
	return &s3.DeleteObjectOutput{
		DeleteMarker: &deleteMarker,
		VersionId:    &versionID,
	}, nil
}

func deleteObjectsDeletedObject(obj types.ObjectIdentifier, out *s3.DeleteObjectOutput) types.DeletedObject {
	deleted := types.DeletedObject{
		Key: obj.Key,
	}
	if derefStr(obj.VersionId) != "" {
		deleted.VersionId = obj.VersionId
	}
	if out.DeleteMarker != nil && *out.DeleteMarker {
		deleted.DeleteMarker = out.DeleteMarker
		deleted.DeleteMarkerVersionId = out.VersionId
		return deleted
	}
	if derefStr(obj.VersionId) != "" {
		deleteMarker := false
		deleted.DeleteMarker = &deleteMarker
	}
	return deleted
}

func deleteObjectsEntryError(key *string, versionID *string, err error) types.Error {
	if s3Err, ok := errors.AsType[s3err.S3Error](err); ok {
		apiErr := s3Err.BaseError()
		code := apiErr.Code
		message := apiErr.Description
		return types.Error{
			Key:       key,
			VersionId: versionID,
			Code:      &code,
			Message:   &message,
		}
	}
	code := "InternalError"
	message := err.Error()
	return types.Error{
		Key:       key,
		VersionId: versionID,
		Code:      &code,
		Message:   &message,
	}
}

// releaseContentCache frees cached bytes only once the deletion removed the
// last live reference to that content. Residency is content-addressed, so bytes
// another version still names must survive this deletion.
func (b *SynapseBackend) releaseContentCache(ctx context.Context, bucketName string, contentID *int64) {
	if contentID == nil {
		return
	}
	if _, err := objectdeletion.ReleaseContentCache(
		ctx,
		b.cache,
		b.cacheGate,
		b.cacheAccessTracker,
		b.repos.Objects,
		bucketName,
		*contentID,
	); err != nil {
		b.logger.Warn("releasing content cache failed", "bucket", bucketName, "contentID", *contentID, "error", err)
	}
}

func (b *SynapseBackend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil || input.CopySource == nil {
		return s3response.CopyObjectOutput{}, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
			requiredArg(copySourceArgumentName, input.CopySource == nil),
		)
	}

	// Parse CopySource: "/<bucket>/<key>" or "<bucket>/<key>", optionally with ?versionId=...
	srcBucketName, srcKey, srcVersionID, err := parseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyObjectOutput{}, s3err.GetInvalidArgumentErr(s3err.InvalidArgCopySourceObject, *input.CopySource)
	}

	dstBucketName := *input.Bucket
	dstKey := *input.Key
	if err := validateObjectKey(dstKey); err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	// Validate source and destination buckets
	srcBucket, err := b.getBucket(ctx, srcBucketName)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	dstBucket, err := b.requireWritableBucket(ctx, dstBucketName)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	// Get source object metadata
	srcVersion, err := b.versionForRead(ctx, srcBucket.ID, srcKey, srcVersionID)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	result, err := b.copyObjectVersion(ctx, copyObjectVersionInput{
		SourceBucket:      srcBucket,
		SourceKey:         srcKey,
		SourceVersion:     srcVersion,
		DestinationBucket: dstBucket,
		DestinationKey:    dstKey,
		MetadataDirective: input.MetadataDirective,
		Metadata:          input.Metadata,
		ContentType:       input.ContentType,
		SourceVisibility:  objectreader.S3Visibility,
	})
	if err != nil {
		return s3response.CopyObjectOutput{}, b.copyObjectError(err)
	}
	b.logger.Info("object copied", "src", srcBucketName+"/"+srcKey, "dst", dstBucketName+"/"+dstKey, "versionID", result.VersionID)

	return s3response.CopyObjectOutput{
		CopyObjectResult: &s3response.CopyObjectResult{
			ETag:         &result.ETag,
			LastModified: &result.LastModified,
		},
		CopySourceVersionId: &result.SourceVersionID,
		VersionId:           &result.VersionID,
	}, nil
}

type restoreVersionPrecondition struct {
	SourceVersionID          string
	ExpectedCurrentVersionID string
}

type copyObjectVersionInput struct {
	SourceBucket      *model.Bucket
	SourceKey         string
	SourceVersion     *model.ObjectVersion
	DestinationBucket *model.Bucket
	DestinationKey    string
	MetadataDirective types.MetadataDirective
	Metadata          map[string]string
	ContentType       *string
	SourceVisibility  objectreader.BucketVisibility
	Restore           *restoreVersionPrecondition
}

type copyObjectVersionResult struct {
	SourceVersionID string
	VersionID       string
	ETag            string
	LastModified    time.Time
}

func (b *SynapseBackend) copyObjectVersion(ctx context.Context, input copyObjectVersionInput) (copyObjectVersionResult, error) {
	srcResult, err := b.objectReader.OpenVersionForCopy(
		ctx,
		input.SourceBucket.Name,
		input.SourceKey,
		input.SourceVersion.VersionID,
		input.SourceVisibility,
	)
	if err != nil {
		return copyObjectVersionResult{}, err
	}

	versionID := model.NewVersionID()
	staged, err := b.cache.PutStaged(ctx, input.DestinationBucket.Name, stagingCacheKey(versionID), objectlimits.LimitFOCUploadReader(srcResult.Body))
	_ = srcResult.Body.Close()
	if err != nil {
		return copyObjectVersionResult{}, fmt.Errorf("staging copy destination: %w", err)
	}
	defer func() { _ = staged.Rollback() }()
	cacheInfo := staged.Info
	if err := objectlimits.ValidateFOCUploadSize(cacheInfo.Size); err != nil {
		return copyObjectVersionResult{}, err
	}

	metadata := make(map[string]string)
	contentType := input.SourceVersion.ContentType
	if input.MetadataDirective == types.MetadataDirectiveReplace {
		if input.Metadata != nil {
			metadata = input.Metadata
		}
		contentType = stringOrDefault(input.ContentType, "application/octet-stream")
	} else {
		maps.Copy(metadata, input.SourceVersion.Metadata)
	}

	// Content is bucket-scoped, so a copy always resolves content in the
	// destination bucket, whether or not the source shares it.
	content, err := b.ensureContentForBytes(ctx, b.repos, input.DestinationBucket, cacheInfo.Size, cacheInfo.Checksum)
	if err != nil {
		return copyObjectVersionResult{}, err
	}
	cacheKey := model.ContentCacheKey(content.ID)
	var objectID int64
	cacheCommitted := false
	err = b.cacheGate.Commit(cacheKey, func() error {
		if err := staged.CommitAs(input.DestinationBucket.Name, cacheKey); err != nil {
			return fmt.Errorf("committing copy cache: %w", err)
		}
		cacheCommitted = true
		return b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
			state, err := txRepos.Contents.ContentPipelineState(ctx, content.ID)
			if err != nil {
				return err
			}

			version := &model.ObjectVersion{
				VersionID:   versionID,
				BucketID:    input.DestinationBucket.ID,
				Key:         input.DestinationKey,
				Size:        cacheInfo.Size,
				ETag:        cacheInfo.ETag,
				ContentType: contentType,
				Metadata:    metadata,
				ContentID:   &content.ID,
			}
			if input.Restore == nil {
				objectID, err = txRepos.Objects.CreateVersionAndSetCurrent(ctx, version)
			} else {
				objectID, err = txRepos.Objects.CreateRestoredVersionAndSetCurrent(
					ctx,
					version,
					input.Restore.SourceVersionID,
					input.Restore.ExpectedCurrentVersionID,
				)
			}
			if err != nil {
				return fmt.Errorf("creating copy destination version: %w", err)
			}
			return b.enqueuePostWriteTask(ctx, txRepos, objectID, versionID, version.ContentID, state)
		})
	})
	if err != nil {
		if cacheCommitted {
			b.releaseContentCacheIfUnreferenced(ctx, input.DestinationBucket.Name, content.ID, "orphaned content cache file after copy tx failure")
		}
		return copyObjectVersionResult{}, contentWriteError(err)
	}

	return copyObjectVersionResult{
		SourceVersionID: input.SourceVersion.VersionID,
		VersionID:       versionID,
		ETag:            fmt.Sprintf(`"%s"`, cacheInfo.ETag),
		LastModified:    time.Now(),
	}, nil
}

func (b *SynapseBackend) copyObjectError(err error) error {
	if errors.Is(err, objectlimits.ErrTooSmall) || errors.Is(err, objectlimits.ErrTooLarge) {
		return objectSizeAPIError(err)
	}
	return b.objectReaderError(err)
}

// RestoreObjectVersion copies a selected data version to a new current version of the same object.
func (b *SynapseBackend) RestoreObjectVersion(ctx context.Context, bucketName, key, sourceVersionID, expectedCurrentVersionID string) (string, error) {
	if bucketName == "" || key == "" || sourceVersionID == "" || expectedCurrentVersionID == "" {
		return "", fmt.Errorf("restoring object version: %w", repository.ErrInvalidInput)
	}
	if err := objectkey.Validate(key); err != nil {
		return "", fmt.Errorf("restoring object version: %w: %v", repository.ErrInvalidInput, err)
	}

	bucket, err := b.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		return "", fmt.Errorf("querying restore bucket: %w", err)
	}
	if bucket == nil || !bucket.Status.IsWritable() {
		return "", fmt.Errorf("restoring object version: %w", repository.ErrNotFound)
	}

	sourceVersion, err := b.repos.Objects.GetVersionByBucketKeyAndID(ctx, bucket.ID, key, sourceVersionID)
	if err != nil {
		return "", fmt.Errorf("querying restore source version: %w", err)
	}
	if sourceVersion == nil {
		return "", fmt.Errorf("restoring object version: %w", repository.ErrNotFound)
	}
	if sourceVersion.IsDeleteMarker {
		return "", fmt.Errorf("restoring object version: %w", repository.ErrConflict)
	}
	current, err := b.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, key)
	if err != nil {
		return "", fmt.Errorf("querying current restore version: %w", err)
	}
	if current == nil || current.VersionID != expectedCurrentVersionID {
		return "", fmt.Errorf("restoring object version: %w", repository.ErrConflict)
	}
	if restoreSourceAlreadyCurrent(sourceVersion, current) {
		return "", fmt.Errorf("restoring object version: %w", repository.ErrAlreadyCurrent)
	}

	result, err := b.copyObjectVersion(ctx, copyObjectVersionInput{
		SourceBucket:      bucket,
		SourceKey:         key,
		SourceVersion:     sourceVersion,
		DestinationBucket: bucket,
		DestinationKey:    key,
		SourceVisibility:  objectreader.AdminVisibility,
		Restore: &restoreVersionPrecondition{
			SourceVersionID:          sourceVersionID,
			ExpectedCurrentVersionID: expectedCurrentVersionID,
		},
	})
	if err != nil {
		switch {
		case errors.Is(err, objectreader.ErrInvalidArgument):
			return "", fmt.Errorf("restoring object version: %w", repository.ErrInvalidInput)
		case errors.Is(err, objectreader.ErrNoSuchBucket), errors.Is(err, objectreader.ErrNoSuchKey):
			return "", fmt.Errorf("restoring object version: %w", repository.ErrNotFound)
		case errors.Is(err, objectreader.ErrNoSuchVersion):
			remainingSource, queryErr := b.repos.Objects.GetVersionByBucketKeyAndID(ctx, bucket.ID, key, sourceVersionID)
			if queryErr != nil {
				return "", fmt.Errorf("rechecking restore source version: %w", queryErr)
			}
			if remainingSource == nil {
				return "", fmt.Errorf("restoring object version: %w", repository.ErrNotFound)
			}
			return "", err
		case errors.Is(err, objectreader.ErrMethodNotAllowed):
			return "", fmt.Errorf("restoring object version: %w", repository.ErrConflict)
		default:
			return "", err
		}
	}

	b.logger.Info("object version restored", "bucket", bucketName, "key", key, "sourceVersionID", sourceVersionID, "versionID", result.VersionID)
	return result.VersionID, nil
}

func restoreSourceAlreadyCurrent(source, current *model.ObjectVersion) bool {
	if source == nil || current == nil {
		return false
	}
	if source.VersionID == current.VersionID {
		return true
	}
	if current.State == model.ObjectStateFailed || current.IsDeleteMarker || (!current.InCache && !current.InFilecoin) {
		return false
	}
	return current.Size == source.Size &&
		current.Checksum == source.Checksum &&
		current.ContentType == source.ContentType &&
		maps.Equal(current.Metadata, source.Metadata)
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
	if input == nil || input.Bucket == nil {
		return s3response.ListObjectsV2Result{}, invalidArgument("Bucket")
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
		lastMod := obj.CreatedAt
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

// requireWritableBucket retrieves a bucket that accepts write operations.
func (b *SynapseBackend) requireWritableBucket(ctx context.Context, name string) (*model.Bucket, error) {
	bucket, err := b.repos.Buckets.GetByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || !bucket.Status.IsVisible() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if !bucket.Status.IsWritable() {
		apiErr := s3err.GetAPIError(s3err.ErrSlowDown)
		apiErr.Description = "The bucket is still being prepared. Please retry shortly."
		return nil, apiErr
	}
	return bucket, nil
}

// contentWriteError asks the client to retry a write whose bytes are still
// being removed after their last version was deleted; the retry then stores
// them as new content.
func contentWriteError(err error) error {
	if !errors.Is(err, repository.ErrContentCleanupInProgress) {
		return err
	}
	apiErr := s3err.GetAPIError(s3err.ErrSlowDown)
	apiErr.Description = "The same data is still being removed after an earlier delete. Please retry shortly."
	return apiErr
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

func validateObjectKey(key string) error {
	if err := objectkey.Validate(key); err != nil {
		return s3err.InvalidArgumentError{
			Description:  err.Error(),
			ArgumentName: "Key",
		}
	}
	return nil
}

type objectMetadataResult struct {
	Size              int64
	QuotedETag        string
	Checksum          string
	ContentType       string
	VersionID         string
	MultipartUploadID *string
	LastModified      time.Time
	Metadata          map[string]string
}

func (b *SynapseBackend) objectMetadata(ctx context.Context, bucketID int64, key, versionID string) (objectMetadataResult, error) {
	version, err := b.versionForRead(ctx, bucketID, key, versionID)
	if err != nil {
		return objectMetadataResult{}, err
	}
	etag := fmt.Sprintf(`"%s"`, version.ETag)
	return objectMetadataResult{
		Size:              version.Size,
		QuotedETag:        etag,
		Checksum:          version.Checksum,
		ContentType:       version.ContentType,
		VersionID:         version.VersionID,
		MultipartUploadID: version.MultipartUploadID,
		LastModified:      version.CreatedAt,
		Metadata:          maps.Clone(version.Metadata),
	}, nil
}

func objectPartsRequested(attrs []types.ObjectAttributes) bool {
	if len(attrs) == 0 {
		return true
	}
	return slices.Contains(attrs, types.ObjectAttributesObjectParts)
}

func (b *SynapseBackend) getObjectAttributeParts(ctx context.Context, contentID *string, input *s3.GetObjectAttributesInput) (*s3response.ObjectParts, error) {
	maxParts := 1000
	if input.MaxParts != nil {
		maxParts = int(*input.MaxParts)
	}

	partMarker := 0
	if input.PartNumberMarker != nil {
		if v := *input.PartNumberMarker; v != "" {
			parsed, err := strconv.Atoi(v)
			if err != nil {
				return nil, s3err.GetInvalidArgMaxLimiter("part-number-marker", v)
			}
			if parsed < 0 {
				return nil, s3err.GetInvalidArgNegativeMaxLimiter("part-number-marker", v)
			}
			partMarker = parsed
		}
	}

	result := &s3response.ObjectParts{
		Parts:            []types.ObjectPart{},
		MaxParts:         maxParts,
		PartNumberMarker: partMarker,
	}
	if contentID == nil || *contentID == "" {
		return result, nil
	}

	parts, err := b.repos.Multiparts.GetParts(ctx, *contentID, partMarker, maxParts+1)
	if err != nil {
		return nil, fmt.Errorf("listing object attribute parts: %w", err)
	}
	if len(parts) > maxParts {
		if maxParts > 0 {
			parts = parts[:maxParts]
			result.NextPartNumberMarker = parts[len(parts)-1].PartNumber
		} else {
			parts = nil
		}
		result.IsTruncated = true
	}

	result.Parts = make([]types.ObjectPart, 0, len(parts))
	for _, p := range parts {
		partNumber := int32(p.PartNumber)
		size := p.Size
		part := types.ObjectPart{
			PartNumber: &partNumber,
			Size:       &size,
		}
		if p.Checksum != nil && *p.Checksum != "" {
			checksum := *p.Checksum
			part.ChecksumSHA256 = &checksum
		}
		result.Parts = append(result.Parts, part)
	}

	return result, nil
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
		if version.IsDeleteMarker {
			return nil, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		}
		return version, nil
	}

	version, err := b.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucketID, key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if version == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if version.IsDeleteMarker {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	return version, nil
}

// ensureContentForBytes binds this write to the content identity for its bytes,
// creating that identity on first sight. The bytes have their own row with a
// unique key, so a second write of the same content is a single upsert and the
// pipeline position is read from that content's copies rather than inferred
// from whatever version happened to be found first.
func (b *SynapseBackend) ensureContentForBytes(
	ctx context.Context,
	repos *repository.Repositories,
	bucket *model.Bucket,
	size int64,
	checksum string,
) (*model.StorageContent, error) {
	if checksum == "" {
		return nil, errors.New("cannot identify content without a checksum")
	}
	requestedCopies := b.defaultCopies
	if bucket.DefaultCopies > 0 {
		requestedCopies = bucket.DefaultCopies
	}
	return repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID:        bucket.ID,
		ContentSize:     size,
		Checksum:        checksum,
		RequestedCopies: model.ClampStorageCopies(requestedCopies),
	})
}

func (b *SynapseBackend) enqueuePostWriteTask(ctx context.Context, repos *repository.Repositories, _ int64, versionID string, contentID *int64, state model.ObjectState) error {
	if b.taskService == nil {
		return errors.New("task service is unavailable")
	}
	switch state {
	case model.ObjectStateCached:
		if contentID == nil {
			return nil
		}
		_, _, err := b.taskService.EnqueueOrReactivateTerminalInTransaction(ctx, repos, taskengine.EnqueueRequest{
			Type:           model.TaskTypeUploadPlan,
			IdempotencyKey: storagepipeline.UploadPlanKey(*contentID),
			Input:          storagepipeline.UploadPlanInput{ContentID: *contentID},
			SubjectType:    "storage_content",
			SubjectKey:     strconv.FormatInt(*contentID, 10),
		})
		return err
	case model.ObjectStateStored:
		if !b.evictionPolicy.EnqueuesAfterUploadEviction() || contentID == nil {
			return nil
		}
		return b.enqueueEvictionTask(ctx, repos, *contentID)
	default:
		return nil
	}
}

// enqueueEvictionTask schedules cache removal for one content payload. Several
// versions can name the same bytes, so the unit of eviction is the content.
func (b *SynapseBackend) enqueueEvictionTask(ctx context.Context, repos *repository.Repositories, contentID int64) error {
	reservation, err := repos.CacheEvictions.PrepareEviction(ctx, contentID)
	if err != nil {
		return err
	}
	if reservation.ActiveTaskID != nil {
		return nil
	}
	generation := reservation.Generation
	taskRow, _, err := b.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type:           model.TaskTypeCacheEvict,
		IdempotencyKey: cacheeviction.EvictTaskKey(contentID, generation),
		Input:          cacheeviction.EvictInput{ContentID: contentID, Generation: generation},
		SubjectType:    "storage_content",
		SubjectKey:     strconv.FormatInt(contentID, 10),
	})
	if err != nil {
		return err
	}
	return repos.CacheEvictions.BindEvictionTask(ctx, contentID, generation, taskRow.ID)
}

func (b *SynapseBackend) bindStorageCleanupTask(ctx context.Context, repos *repository.Repositories, cleanup *repository.StorageCleanupReservation) error {
	if cleanup == nil || cleanup.TaskID != nil {
		return nil
	}
	taskRow, _, err := b.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type:           model.TaskTypeStorageCleanup,
		IdempotencyKey: storagecleanup.TaskKey(cleanup.ContentID, cleanup.Generation),
		Input:          storagecleanup.Input{ContentID: cleanup.ContentID, Generation: cleanup.Generation},
		SubjectType:    "storage_content",
		SubjectKey:     strconv.FormatInt(cleanup.ContentID, 10),
	})
	if err != nil {
		return err
	}
	if err := repos.StorageCleanup.BindTask(ctx, cleanup.ContentID, cleanup.Generation, taskRow.ID); err != nil {
		return err
	}
	cleanup.TaskID = &taskRow.ID
	return nil
}

func (b *SynapseBackend) objectReaderError(err error) error {
	if errors.Is(err, objectreader.ErrCacheMiss) {
		admin.CacheMissesTotal.Inc()
	}
	switch {
	case errors.Is(err, objectreader.ErrInvalidArgument):
		return invalidArgument("")
	case errors.Is(err, objectreader.ErrNoSuchBucket):
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	case errors.Is(err, objectreader.ErrNoSuchKey):
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	case errors.Is(err, objectreader.ErrNoSuchVersion):
		return s3err.GetAPIError(s3err.ErrNoSuchVersion)
	case errors.Is(err, objectreader.ErrMethodNotAllowed):
		return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	case errors.Is(err, objectreader.ErrProviderDownload):
		return s3err.GetAPIError(s3err.ErrInternalError)
	default:
		return err
	}
}

// stagingCacheKey names a not-yet-committed write inside the content directory,
// so the staged file and its final content-addressed name share a directory and
// the commit stays a single rename.
func stagingCacheKey(versionID string) string {
	return path.Join(".contents", ".staging-"+versionID)
}

// releaseContentCacheIfUnreferenced drops cached bytes left behind by a failed
// write. Residency is content-addressed, so the file may already back a version
// a concurrent writer created on the same content and can only go when nothing
// names it.
func (b *SynapseBackend) releaseContentCacheIfUnreferenced(ctx context.Context, bucketName string, contentID int64, message string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, err := objectdeletion.ReleaseContentCache(
		cleanupCtx,
		b.cache,
		b.cacheGate,
		b.cacheAccessTracker,
		b.repos.Objects,
		bucketName,
		contentID,
	); err != nil {
		b.logger.Warn(message, "bucket", bucketName, "contentID", contentID, "error", err)
	}
}

// Ensure Body is consumed for PutObject, as it might come from
// a streaming source. This is a no-op helper.
var _ io.Reader = (*io.LimitedReader)(nil)
