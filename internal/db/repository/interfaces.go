package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

// BucketRepository defines persistence operations for Bucket entities.
type BucketRepository interface {
	Create(ctx context.Context, bucket *model.Bucket) error
	GetByName(ctx context.Context, name string) (*model.Bucket, error)
	GetByID(ctx context.Context, id int64) (*model.Bucket, error)
	ListActive(ctx context.Context) ([]model.Bucket, error)
	SoftDelete(ctx context.Context, id int64) error
}

// ObjectRepository defines persistence operations for Object entities.
// Soft-deleted objects are automatically excluded from queries by the Bun soft_delete tag.
type ObjectRepository interface {
	// UpsertAndBumpGeneration atomically inserts or updates an object, incrementing its
	// generation counter. If the object was previously soft-deleted, its DeletedAt is cleared.
	// Returns the object ID and new generation.
	UpsertAndBumpGeneration(ctx context.Context, obj *model.Object) (id int64, generation int64, err error)

	GetByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.Object, error)
	ListByBucket(ctx context.Context, bucketID int64, prefix string, maxKeys int) ([]model.Object, error)
	SoftDelete(ctx context.Context, id int64) error
	UpdateState(ctx context.Context, id int64, from, to model.ObjectState) error
	// UpdateStateToFailed transitions an object to ObjectStateFailed while
	// recording which state it failed from (FailedAtState) and the error message.
	UpdateStateToFailed(ctx context.Context, id int64, from model.ObjectState, lastError string) error
}

// TaskRepository defines persistence operations for Task entities.
type TaskRepository interface {
	Create(ctx context.Context, task *model.Task) error
	GetByID(ctx context.Context, id int64) (*model.Task, error)

	// ClaimPending atomically claims one pending task of the given type by
	// transitioning it to running and setting a lease. Returns nil if no task is available.
	ClaimPending(ctx context.Context, taskType model.TaskType, leaseDuration time.Duration) (*model.Task, error)
	// Complete marks a running task as completed.
	Complete(ctx context.Context, taskID int64) error
	// Fail marks a running task as failed, recording the error and incrementing retry count.
	Fail(ctx context.Context, taskID int64, lastError string) error
	// ReleaseExpiredLeases resets running tasks whose lease has expired back to pending.
	ReleaseExpiredLeases(ctx context.Context) (int, error)
}

// MultipartUploadRepository defines persistence operations for multipart upload entities.
type MultipartUploadRepository interface {
	Create(ctx context.Context, upload *model.MultipartUpload) error
	GetByUploadID(ctx context.Context, uploadID string) (*model.MultipartUpload, error)
	ListByBucket(ctx context.Context, bucketID int64, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]model.MultipartUpload, error)
	// SetStatus atomically transitions status using CAS (compare-and-swap) to prevent races.
	SetStatus(ctx context.Context, uploadID string, from, to model.MultipartStatus) error
	Delete(ctx context.Context, uploadID string) error

	// Part operations
	CreatePart(ctx context.Context, part *model.MultipartPart) error
	GetParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]model.MultipartPart, error)
	GetPartsByNumbers(ctx context.Context, uploadID string, numbers []int) ([]model.MultipartPart, error)
	DeleteParts(ctx context.Context, uploadID string) error
}
