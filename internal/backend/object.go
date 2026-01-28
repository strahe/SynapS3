package backend

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

	now := time.Now()

	// Build metadata map from input.
	meta := make(map[string]string)
	if input.Metadata != nil {
		meta = input.Metadata
	}

	// Upsert object record and create async task atomically.
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Increment generation for overwrites.
	var existingGen int64
	err = tx.NewSelect().Model((*model.Object)(nil)).
		Column("generation").
		Where("bucket_id = ? AND key = ?", bucket.ID, keyName).
		Scan(ctx, &existingGen)
	newGen := existingGen + 1
	if err != nil {
		newGen = 1 // first write
	}

	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         keyName,
		Generation:  newGen,
		Size:        cacheInfo.Size,
		ETag:        cacheInfo.ETag,
		Checksum:    cacheInfo.Checksum,
		ContentType: stringOrDefault(input.ContentType, "application/octet-stream"),
		Metadata:    meta,
		CachePath:   cacheInfo.Path,
		State:       model.ObjectStateCached,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Upsert: ON CONFLICT (bucket_id, key) DO UPDATE
	if _, err := tx.NewInsert().Model(obj).
		On("CONFLICT (bucket_id, key) DO UPDATE").
		Set("generation = EXCLUDED.generation").
		Set("size = EXCLUDED.size").
		Set("etag = EXCLUDED.etag").
		Set("checksum = EXCLUDED.checksum").
		Set("content_type = EXCLUDED.content_type").
		Set("metadata = EXCLUDED.metadata").
		Set("cache_path = EXCLUDED.cache_path").
		Set("state = EXCLUDED.state").
		Set("retry_count = 0").
		Set("last_error = NULL").
		Set("piece_cid = NULL").
		Set("updated_at = EXCLUDED.updated_at").
		Exec(ctx); err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("upserting object: %w", err)
	}

	// Enqueue upload task.
	task := &model.Task{
		Type:           model.TaskTypeUploadToSP,
		RefType:        "object",
		RefID:          obj.ID,
		RefGeneration:  newGen,
		IdempotencyKey: fmt.Sprintf("upload:%d:%d", bucket.ID, newGen),
		Status:         model.TaskStatusPending,
		ScheduledAt:    now,
	}
	if _, err := tx.NewInsert().Model(task).Exec(ctx); err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("enqueuing upload task: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("commit tx: %w", err)
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

	var obj model.Object
	err = b.db.NewSelect().Model(&obj).
		Where("bucket_id = ? AND key = ?", bucket.ID, *input.Key).
		Scan(ctx)
	if err != nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	// Try local cache first.
	if b.cache.Exists(ctx, *input.Bucket, *input.Key) {
		rc, _, err := b.cache.Get(ctx, *input.Bucket, *input.Key)
		if err != nil {
			return nil, fmt.Errorf("reading from cache: %w", err)
		}

		etag := fmt.Sprintf(`"%s"`, obj.ETag)
		contentType := obj.ContentType

		return &s3.GetObjectOutput{
			Body:          rc,
			ContentLength: &obj.Size,
			ETag:          &etag,
			ContentType:   &contentType,
		}, nil
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

	var obj model.Object
	err = b.db.NewSelect().Model(&obj).
		Where("bucket_id = ? AND key = ?", bucket.ID, *input.Key).
		Scan(ctx)
	if err != nil {
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

	// Delete object record (cancels pending tasks via generation mismatch).
	res, err := b.db.NewDelete().Model((*model.Object)(nil)).
		Where("bucket_id = ? AND key = ?", bucket.ID, *input.Key).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("deleting object: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows > 0 {
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

	q := b.db.NewSelect().Model((*model.Object)(nil)).
		Where("bucket_id = ?", bucket.ID).
		OrderExpr("key ASC")

	if input.Prefix != nil && *input.Prefix != "" {
		q = q.Where("key LIKE ?", *input.Prefix+"%")
	}

	maxKeys := int32(1000)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}
	q = q.Limit(int(maxKeys) + 1)

	if input.Marker != nil && *input.Marker != "" {
		q = q.Where("key > ?", *input.Marker)
	}

	var objects []model.Object
	if err := q.Scan(ctx, &objects); err != nil {
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

func (b *SynapseBackend) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if input.Bucket == nil {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bucket, err := b.getBucket(ctx, *input.Bucket)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}

	q := b.db.NewSelect().Model((*model.Object)(nil)).
		Where("bucket_id = ?", bucket.ID).
		OrderExpr("key ASC")

	if input.Prefix != nil && *input.Prefix != "" {
		q = q.Where("key LIKE ?", *input.Prefix+"%")
	}

	maxKeys := int32(1000)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}
	q = q.Limit(int(maxKeys) + 1)

	if input.StartAfter != nil && *input.StartAfter != "" {
		q = q.Where("key > ?", *input.StartAfter)
	}
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		q = q.Where("key > ?", *input.ContinuationToken)
	}

	var objects []model.Object
	if err := q.Scan(ctx, &objects); err != nil {
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

// getBucket retrieves an active bucket by name.
func (b *SynapseBackend) getBucket(ctx context.Context, name string) (*model.Bucket, error) {
	var bucket model.Bucket
	err := b.db.NewSelect().Model(&bucket).
		Where("name = ?", name).
		Where("status != ?", model.BucketStatusDeleted).
		Scan(ctx)
	if err != nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return &bucket, nil
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
