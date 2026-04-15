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
	// List returns all buckets regardless of status.
	List(ctx context.Context) ([]model.Bucket, error)
	// CountByStatus returns bucket counts grouped by status.
	CountByStatus(ctx context.Context) ([]BucketStatusCount, error)
	SoftDelete(ctx context.Context, id int64) error
	// UpdateStatus atomically transitions bucket status using CAS.
	UpdateStatus(ctx context.Context, id int64, from, to model.BucketStatus) error
	// SetProofSetID sets the proof set ID for a bucket.
	SetProofSetID(ctx context.Context, id int64, proofSetID string) error
	// HardDelete permanently removes a bucket row (used after proof set deletion).
	HardDelete(ctx context.Context, id int64) error
	// CountWithProofSet returns the number of buckets that have a non-null proof set ID.
	CountWithProofSet(ctx context.Context) (int, error)
}

// ObjectRepository defines persistence operations for Object entities.
// Soft-deleted objects are automatically excluded from queries by the Bun soft_delete tag.
type ObjectRepository interface {
	// UpsertAndBumpGeneration atomically inserts or updates an object, incrementing its
	// generation counter. If the object was previously soft-deleted, its DeletedAt is cleared.
	// Returns the object ID and new generation.
	UpsertAndBumpGeneration(ctx context.Context, obj *model.Object) (id int64, generation int64, err error)

	GetByID(ctx context.Context, id int64) (*model.Object, error)
	GetByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.Object, error)
	ListByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.Object, error)
	SoftDelete(ctx context.Context, id int64) error
	UpdateState(ctx context.Context, id int64, generation int64, from, to model.ObjectState) error
	// UpdateStateToFailed transitions an object to ObjectStateFailed while
	// recording which state it failed from (FailedAtState) and the error message.
	UpdateStateToFailed(ctx context.Context, id int64, generation int64, from model.ObjectState, lastError string) error
	// SetPieceCIDAndTransition atomically sets the PieceCID and transitions state in one CAS update.
	// This prevents split-write races where a stale worker could set PieceCID on a newer generation.
	SetPieceCIDAndTransition(ctx context.Context, id int64, generation int64, pieceCID string, from, to model.ObjectState) error
	// ListByState returns objects in the given state, limited to limit rows.
	ListByState(ctx context.Context, state model.ObjectState, limit int) ([]model.Object, error)
	// ResetStaleStates resets objects stuck in an intermediate state back to a safe state.
	// Used during startup recovery for objects that were mid-transition when the process crashed.
	ResetStaleStates(ctx context.Context, fromState, toState model.ObjectState, staleBefore time.Time) (int, error)
	// CountByState returns object counts grouped by state.
	CountByState(ctx context.Context) ([]ObjectStateCount, error)
	// TotalSize returns the sum of all non-deleted object sizes in bytes.
	TotalSize(ctx context.Context) (int64, error)
	// CountByBucket returns the number of non-deleted objects in a bucket.
	CountByBucket(ctx context.Context, bucketID int64) (int64, error)
	// TotalSizeByBucket returns the sum of non-deleted object sizes in a bucket.
	TotalSizeByBucket(ctx context.Context, bucketID int64) (int64, error)
	// AggregateByBucket returns object count and total size for all buckets in a single query.
	AggregateByBucket(ctx context.Context) (map[int64]BucketObjectStats, error)
}

// BucketObjectStats holds aggregate object metrics for a single bucket.
type BucketObjectStats struct {
	Count     int64 `bun:"count"`
	TotalSize int64 `bun:"total_size"`
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
	// Requeue resets a failed task back to pending with a scheduled backoff delay.
	Requeue(ctx context.Context, taskID int64, backoff time.Duration) error
	// ReleaseExpiredLeases resets running tasks whose lease has expired back to pending.
	ReleaseExpiredLeases(ctx context.Context) (int, error)
	// FailTerminal marks a running task as dead-letter (permanently failed after max retries).
	FailTerminal(ctx context.Context, taskID int64, lastError string) error
	// ListDeadLetters returns dead-letter tasks, ordered by most recent first.
	ListDeadLetters(ctx context.Context, limit int) ([]model.Task, error)
	// RetryDeadLetter resets a dead-letter task back to pending for manual retry.
	RetryDeadLetter(ctx context.Context, taskID int64) error
	// CountByStatus returns task counts grouped by type and status.
	CountByStatus(ctx context.Context) ([]TaskStatusCount, error)
	// CountActiveObjectTasksByBucket returns the number of pending/running object tasks
	// whose referenced objects belong to the given bucket, including soft-deleted
	// objects that still have in-flight work.
	CountActiveObjectTasksByBucket(ctx context.Context, bucketID int64) (int64, error)
	// CountActiveBucketTasksByBucketID returns the number of pending/running tasks
	// that directly reference the given bucket (ref_type=bucket, ref_id=bucketID).
	CountActiveBucketTasksByBucketID(ctx context.Context, bucketID int64) (int64, error)
	// CompleteByRef marks all pending/running tasks matching the given ref as completed.
	CompleteByRef(ctx context.Context, refType string, refID int64, taskType model.TaskType) error
	// List returns tasks with optional filters, paginated by offset/limit.
	// Returns the matching tasks and the total count (for pagination).
	List(ctx context.Context, taskType string, status string, limit, offset int) ([]model.Task, int, error)
}

// TaskStatusCount holds a task count grouped by type and status.
type TaskStatusCount struct {
	Type   string `bun:"type"`
	Status string `bun:"status"`
	Count  int64  `bun:"count"`
}

// ObjectStateCount holds an object count grouped by state.
type ObjectStateCount struct {
	State string `bun:"state"`
	Count int64  `bun:"count"`
}

// BucketStatusCount holds a bucket count grouped by status.
type BucketStatusCount struct {
	Status string `bun:"status"`
	Count  int64  `bun:"count"`
}

// MultipartUploadRepository defines persistence operations for multipart upload entities.
type MultipartUploadRepository interface {
	Create(ctx context.Context, upload *model.MultipartUpload) error
	GetByUploadID(ctx context.Context, uploadID string) (*model.MultipartUpload, error)
	ListByBucket(ctx context.Context, bucketID int64, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]model.MultipartUpload, error)
	// CountActiveByBucket returns initiated/completing multipart uploads for the given bucket.
	CountActiveByBucket(ctx context.Context, bucketID int64) (int64, error)
	// SetStatus atomically transitions status using CAS (compare-and-swap) to prevent races.
	SetStatus(ctx context.Context, uploadID string, from, to model.MultipartStatus) error
	Delete(ctx context.Context, uploadID string) error

	// Part operations
	CreatePart(ctx context.Context, part *model.MultipartPart) error
	GetParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]model.MultipartPart, error)
	GetPartsByNumbers(ctx context.Context, uploadID string, numbers []int) ([]model.MultipartPart, error)
	DeleteParts(ctx context.Context, uploadID string) error
}
