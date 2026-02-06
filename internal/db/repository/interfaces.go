package repository

import (
	"context"

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
}

// TaskRepository defines persistence operations for Task entities.
// Only producer operations are included here; claim/complete/fail belong to the worker layer.
type TaskRepository interface {
	Create(ctx context.Context, task *model.Task) error
	GetByID(ctx context.Context, id int64) (*model.Task, error)
}
