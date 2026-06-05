package backend

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectlimits"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *SynapseBackend) CreateMultipartUpload(ctx context.Context, input s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	bucketName := derefStr(input.Bucket)
	keyName := derefStr(input.Key)

	bucket, err := b.requireActiveBucket(ctx, bucketName)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}

	uploadID := uuid.New().String()

	meta := make(map[string]string)
	if input.Metadata != nil {
		meta = input.Metadata
	}

	upload := &model.MultipartUpload{
		BucketID:    bucket.ID,
		Key:         keyName,
		UploadID:    uploadID,
		ContentType: stringOrDefault(input.ContentType, "application/octet-stream"),
		Metadata:    meta,
		Status:      model.MultipartStatusInitiated,
	}
	if err := b.repos.Multiparts.Create(ctx, upload); err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("creating multipart upload: %w", err)
	}

	b.logger.Info("multipart upload initiated", "bucket", bucketName, "key", keyName, "uploadID", uploadID)
	return s3response.InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      keyName,
		UploadId: uploadID,
	}, nil
}

func (b *SynapseBackend) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if input == nil {
		return nil, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil || input.PartNumber == nil {
		return nil, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
			requiredArg("UploadId", input.UploadId == nil),
			requiredArg("PartNumber", input.PartNumber == nil),
		)
	}

	upload, err := b.getActiveUpload(ctx, *input.UploadId, *input.Bucket)
	if err != nil {
		return nil, err
	}
	if upload.Key != *input.Key {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	partNum := int(*input.PartNumber)
	if partNum < 1 || partNum > 10000 {
		return nil, s3err.GetInvalidArgumentErr(s3err.InvalidArgPartNumber, fmt.Sprint(*input.PartNumber))
	}

	cacheInfo, err := b.cache.PutPart(ctx, *input.UploadId, partNum, objectlimits.LimitFOCUploadReader(input.Body))
	if err != nil {
		if errors.Is(err, objectlimits.ErrTooLarge) {
			return nil, objectSizeAPIError(err)
		}
		return nil, fmt.Errorf("caching part: %w", err)
	}

	part := &model.MultipartPart{
		UploadID:   *input.UploadId,
		PartNumber: partNum,
		Size:       cacheInfo.Size,
		ETag:       cacheInfo.ETag,
		Checksum:   &cacheInfo.Checksum,
	}
	if err := b.repos.Multiparts.CreatePart(ctx, part); err != nil {
		return nil, fmt.Errorf("recording part: %w", err)
	}

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	return &s3.UploadPartOutput{
		ETag: &etag,
	}, nil
}

func (b *SynapseBackend) UploadPartCopy(ctx context.Context, input *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	if input == nil {
		return s3response.CopyPartResult{}, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil ||
		input.PartNumber == nil || input.CopySource == nil {
		return s3response.CopyPartResult{}, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
			requiredArg("UploadId", input.UploadId == nil),
			requiredArg("PartNumber", input.PartNumber == nil),
			requiredArg(copySourceArgumentName, input.CopySource == nil),
		)
	}

	upload, err := b.getActiveUpload(ctx, *input.UploadId, *input.Bucket)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	if upload.Key != *input.Key {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	partNum := int(*input.PartNumber)
	if partNum < 1 || partNum > 10000 {
		return s3response.CopyPartResult{}, s3err.GetInvalidArgumentErr(s3err.InvalidArgPartNumber, fmt.Sprint(*input.PartNumber))
	}

	// Parse and validate source object
	srcBucketName, srcKey, srcVersionID, err := parseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyPartResult{}, s3err.GetInvalidArgumentErr(s3err.InvalidArgCopySourceObject, *input.CopySource)
	}

	var srcResult *objectreader.Result
	if srcVersionID != "" {
		srcResult, err = b.objectReader.OpenVersion(ctx, srcBucketName, srcKey, srcVersionID, objectreader.S3Visibility)
	} else {
		srcResult, err = b.objectReader.Open(ctx, srcBucketName, srcKey, objectreader.S3Visibility)
	}
	if err != nil {
		return s3response.CopyPartResult{}, b.objectReaderError(err)
	}
	defer func() { _ = srcResult.Body.Close() }()

	// NOTE: CopySourceRange for partial copies is not yet supported (future enhancement).
	cacheInfo, err := b.cache.PutPart(ctx, *input.UploadId, partNum, objectlimits.LimitFOCUploadReader(srcResult.Body))
	if err != nil {
		if errors.Is(err, objectlimits.ErrTooLarge) {
			return s3response.CopyPartResult{}, objectSizeAPIError(err)
		}
		return s3response.CopyPartResult{}, fmt.Errorf("caching copied part: %w", err)
	}

	part := &model.MultipartPart{
		UploadID:   *input.UploadId,
		PartNumber: partNum,
		Size:       cacheInfo.Size,
		ETag:       cacheInfo.ETag,
		Checksum:   &cacheInfo.Checksum,
	}
	if err := b.repos.Multiparts.CreatePart(ctx, part); err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("recording copied part: %w", err)
	}

	etag := fmt.Sprintf(`"%s"`, cacheInfo.ETag)
	now := time.Now()
	return s3response.CopyPartResult{
		ETag:                &etag,
		LastModified:        now,
		CopySourceVersionId: srcResult.VersionID,
	}, nil
}

func (b *SynapseBackend) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	if input == nil {
		return s3response.CompleteMultipartUploadResult{}, "", invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3response.CompleteMultipartUploadResult{}, "", missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
			requiredArg("UploadId", input.UploadId == nil),
		)
	}
	if input.MultipartUpload == nil || len(input.MultipartUpload.Parts) == 0 {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	bucketName := *input.Bucket
	keyName := *input.Key
	uploadID := *input.UploadId

	upload, err := b.getActiveUpload(ctx, uploadID, bucketName)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if upload.Key != keyName {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	// CAS: initiated → completing
	if err := b.repos.Multiparts.SetStatus(ctx, uploadID, model.MultipartStatusInitiated, model.MultipartStatusCompleting); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	// Rollback CAS on any failure before the tx commits the final status.
	completed := false
	defer func() {
		if !completed {
			_ = b.repos.Multiparts.SetStatus(ctx, uploadID, model.MultipartStatusCompleting, model.MultipartStatusInitiated)
		}
	}()

	// Validate part list: ascending order, all parts exist
	partNumbers := make([]int, len(input.MultipartUpload.Parts))
	for i, p := range input.MultipartUpload.Parts {
		if p.PartNumber == nil {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		partNumbers[i] = int(*p.PartNumber)
		if i > 0 && partNumbers[i] <= partNumbers[i-1] {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
	}

	dbParts, err := b.repos.Multiparts.GetPartsByNumbers(ctx, uploadID, partNumbers)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("querying parts: %w", err)
	}
	if len(dbParts) != len(partNumbers) {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
	}

	// Validate ETags match
	dbPartMap := make(map[int]*model.MultipartPart, len(dbParts))
	for i := range dbParts {
		dbPartMap[dbParts[i].PartNumber] = &dbParts[i]
	}
	for _, p := range input.MultipartUpload.Parts {
		dbPart := dbPartMap[int(*p.PartNumber)]
		if p.ETag != nil {
			inputETag := strings.Trim(*p.ETag, `"`)
			if inputETag != dbPart.ETag {
				return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
			}
		}
	}
	totalSize := int64(0)
	for _, pn := range partNumbers {
		size := dbPartMap[pn].Size
		if size > objectlimits.MaxFOCUploadSize || totalSize > objectlimits.MaxFOCUploadSize-size {
			return s3response.CompleteMultipartUploadResult{}, "", objectSizeAPIError(&objectlimits.SizeError{
				Size: objectlimits.MaxFOCUploadSize + 1,
				Err:  objectlimits.ErrTooLarge,
			})
		}
		totalSize += size
	}
	if err := objectlimits.ValidateFOCUploadSize(totalSize); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", objectSizeAPIError(err)
	}

	versionID := model.NewVersionID()
	cacheKey := versionCacheKey(versionID)

	// Assemble parts into a version-specific cache key.
	cacheInfo, _, err := b.cache.AssembleParts(ctx, bucketName, cacheKey, uploadID, partNumbers)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("assembling parts: %w", err)
	}

	// Compute S3 multipart ETag from DB-recorded ETags (source of truth, not re-derived from files)
	orderedETags := make([]string, len(partNumbers))
	for i, pn := range partNumbers {
		orderedETags[i] = dbPartMap[pn].ETag
	}
	s3ETag, err := computeMultipartETag(orderedETags)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("computing multipart ETag: %w", err)
	}

	// Atomic: create object version + enqueue any needed task + finalize upload status
	var objectID int64
	var createdState model.ObjectState
	if err := b.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		reuse, err := b.resolveVersionReuse(ctx, txRepos.Objects, upload.BucketID, cacheInfo.Size, cacheInfo.Checksum)
		if err != nil {
			return err
		}

		version := &model.ObjectVersion{
			VersionID:         versionID,
			BucketID:          upload.BucketID,
			Key:               keyName,
			Size:              cacheInfo.Size,
			ETag:              s3ETag,
			Checksum:          cacheInfo.Checksum,
			ContentType:       upload.ContentType,
			Metadata:          upload.Metadata,
			CacheKey:          cacheKey,
			MultipartUploadID: &uploadID,
			StorageUploadID:   reuse.StorageUploadID,
			InCache:           true,
			State:             reuse.State,
		}
		createdState = version.State

		objectID, err = txRepos.Objects.CreateVersionAndSetCurrent(ctx, version)
		if err != nil {
			return fmt.Errorf("creating assembled object version: %w", err)
		}
		if err := b.enqueuePostWriteTask(ctx, txRepos, objectID, versionID, version.State); err != nil {
			return err
		}

		return txRepos.Multiparts.SetStatus(ctx, uploadID, model.MultipartStatusCompleting, model.MultipartStatusCompleted)
	}); err != nil {
		b.deleteVersionCacheBestEffort(ctx, bucketName, cacheKey, "orphaned multipart version cache file after complete tx failure")
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	completed = true
	b.completeFollowerIfStoredReuseWonRace(ctx, upload.BucketID, bucketName, cacheInfo.Size, cacheInfo.Checksum, objectID, versionID, createdState)

	// Clean up multipart parts from cache (best-effort)
	_ = b.cache.DeleteUpload(ctx, uploadID)

	etag := fmt.Sprintf(`"%s"`, s3ETag)
	b.logger.Info("multipart upload completed", "bucket", bucketName, "key", keyName, "uploadID", uploadID, "parts", len(partNumbers), "versionID", versionID)

	return s3response.CompleteMultipartUploadResult{
		Bucket: &bucketName,
		Key:    &keyName,
		ETag:   &etag,
	}, versionID, nil
}

func (b *SynapseBackend) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) error {
	if input == nil {
		return invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
			requiredArg("UploadId", input.UploadId == nil),
		)
	}

	upload, err := b.getActiveUpload(ctx, *input.UploadId, *input.Bucket)
	if err != nil {
		return err
	}
	if upload.Key != *input.Key {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	// CAS: initiated → aborted
	if err := b.repos.Multiparts.SetStatus(ctx, *input.UploadId, model.MultipartStatusInitiated, model.MultipartStatusAborted); err != nil {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	// Clean up parts from cache and DB (best-effort for cache)
	_ = b.cache.DeleteUpload(ctx, *input.UploadId)
	_ = b.repos.Multiparts.DeleteParts(ctx, *input.UploadId)

	b.logger.Info("multipart upload aborted", "bucket", *input.Bucket, "key", *input.Key, "uploadID", *input.UploadId)
	return nil
}

func (b *SynapseBackend) ListMultipartUploads(ctx context.Context, input *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	if input == nil {
		return s3response.ListMultipartUploadsResult{}, invalidArgument("Bucket")
	}
	if input.Bucket == nil {
		return s3response.ListMultipartUploadsResult{}, invalidArgument("Bucket")
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, err
	}

	maxUploads := 1000
	if input.MaxUploads != nil {
		maxUploads = int(*input.MaxUploads)
	}

	prefix := derefStr(input.Prefix)
	keyMarker := derefStr(input.KeyMarker)
	uploadIDMarker := derefStr(input.UploadIdMarker)

	uploads, err := b.repos.Multiparts.ListByBucket(ctx, bucket.ID, prefix, keyMarker, uploadIDMarker, maxUploads+1)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, fmt.Errorf("listing multipart uploads: %w", err)
	}

	result := s3response.ListMultipartUploadsResult{
		Bucket:     *input.Bucket,
		Prefix:     prefix,
		KeyMarker:  keyMarker,
		MaxUploads: maxUploads,
	}

	if len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
		result.IsTruncated = true
		result.NextKeyMarker = uploads[len(uploads)-1].Key
		result.NextUploadIDMarker = uploads[len(uploads)-1].UploadID
	}

	for _, u := range uploads {
		result.Uploads = append(result.Uploads, s3response.Upload{
			Key:       u.Key,
			UploadID:  u.UploadID,
			Initiated: u.CreatedAt,
		})
	}

	return result, nil
}

func (b *SynapseBackend) ListParts(ctx context.Context, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	if input == nil {
		return s3response.ListPartsResult{}, invalidArgument("Bucket")
	}
	if input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3response.ListPartsResult{}, missingRequiredArgument(
			requiredArg("Bucket", input.Bucket == nil),
			requiredArg("Key", input.Key == nil),
			requiredArg("UploadId", input.UploadId == nil),
		)
	}

	upload, err := b.getActiveUpload(ctx, *input.UploadId, *input.Bucket)
	if err != nil {
		return s3response.ListPartsResult{}, err
	}
	if upload.Key != *input.Key {
		return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	maxParts := 1000
	if input.MaxParts != nil {
		maxParts = int(*input.MaxParts)
	}

	partMarker := 0
	if input.PartNumberMarker != nil {
		if v := *input.PartNumberMarker; v != "" {
			// PartNumberMarker is a string in the AWS SDK; parse to int
			_, _ = fmt.Sscanf(v, "%d", &partMarker)
		}
	}

	parts, err := b.repos.Multiparts.GetParts(ctx, *input.UploadId, partMarker, maxParts+1)
	if err != nil {
		return s3response.ListPartsResult{}, fmt.Errorf("listing parts: %w", err)
	}

	result := s3response.ListPartsResult{
		Bucket:           *input.Bucket,
		Key:              *input.Key,
		UploadID:         *input.UploadId,
		MaxParts:         maxParts,
		PartNumberMarker: partMarker,
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

	for _, p := range parts {
		etag := fmt.Sprintf(`"%s"`, p.ETag)
		result.Parts = append(result.Parts, s3response.Part{
			PartNumber:   p.PartNumber,
			LastModified: p.CreatedAt,
			ETag:         etag,
			Size:         p.Size,
		})
	}

	return result, nil
}

// getActiveUpload retrieves a multipart upload that is in "initiated" status
// and belongs to the given bucket.
func (b *SynapseBackend) getActiveUpload(ctx context.Context, uploadID, bucketName string) (*model.MultipartUpload, error) {
	bucket, err := b.getBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	upload, err := b.repos.Multiparts.GetByUploadID(ctx, uploadID)
	if err != nil {
		return nil, fmt.Errorf("querying multipart upload: %w", err)
	}
	if upload == nil || upload.BucketID != bucket.ID || upload.Status != model.MultipartStatusInitiated {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	return upload, nil
}

// computeMultipartETag produces the S3 multipart ETag:
// MD5(concat(part-MD5-as-binary-digests))-N
func computeMultipartETag(partETags []string) (string, error) {
	h := md5.New()
	for _, etag := range partETags {
		raw, err := hex.DecodeString(etag)
		if err != nil {
			return "", fmt.Errorf("invalid part ETag %q: %w", etag, err)
		}
		h.Write(raw)
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(partETags)), nil
}
