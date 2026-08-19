package cacheeviction

import (
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

const (
	StageLRU                       = "lru"
	StageAfterUpload               = "after_upload"
	StageReconcileBucketDurability = "reconcile_bucket_durability"

	lruAccessedAtPayloadKey       = "cache_accessed_at"
	deleteAuthorizedPayloadKey    = "delete_authorized"
	lruTaskKeyPrefix              = "evict_cache:lru:"
	afterUploadTaskKeyPrefix      = "evict_cache:"
	bucketDurabilityTaskKeyPrefix = "evict_cache:bucket_durability:"
)

// ErrDurabilityThreshold means the current Bucket policy does not authorize deletion.
var ErrDurabilityThreshold = errors.New("minimum durable copies not met")

// ErrNoLongerEligible means a planned cache entry no longer matches the deletion contract.
var ErrNoLongerEligible = errors.New("cache entry is no longer eligible")

// ErrAccessChanged means an LRU candidate was accessed after it was planned.
var ErrAccessChanged = errors.New("cache access snapshot changed")

// Candidate is the persisted snapshot needed to plan one LRU eviction.
type Candidate struct {
	ObjectID   int64     `bun:"object_id"`
	VersionID  string    `bun:"version_id"`
	Size       int64     `bun:"size"`
	AccessedAt time.Time `bun:"cache_accessed_at"`
}

// LRUTaskPayload is the typed boundary for an LRU task's persisted payload.
type LRUTaskPayload struct {
	AccessedAt time.Time
}

// AuthorizedDeletion is the persisted decision needed to remove one cache
// entry outside the database transaction that approved it.
type AuthorizedDeletion struct {
	Version    model.ObjectVersion
	BucketName string
}

// NormalizeAccessTime matches the timestamp precision supported by both
// PostgreSQL and SQLite persistence paths.
func NormalizeAccessTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

// NewLRUTask builds the stable task for one candidate access snapshot.
func NewLRUTask(candidate Candidate, maxRetries int, scheduledAt time.Time) *model.Task {
	stage := StageLRU
	payload := LRUTaskPayload{
		AccessedAt: NormalizeAccessTime(candidate.AccessedAt),
	}
	return &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          candidate.ObjectID,
		RefVersionID:   candidate.VersionID,
		IdempotencyKey: lruTaskKeyPrefix + candidate.VersionID,
		Payload:        payload.taskPayload(),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    scheduledAt,
	}
}

// NewAfterUploadTask builds the stable task for post-upload eviction.
func NewAfterUploadTask(objectID int64, versionID string, maxRetries int, scheduledAt time.Time) *model.Task {
	stage := StageAfterUpload
	return &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: afterUploadTaskKeyPrefix + versionID,
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    scheduledAt,
	}
}

// NewBucketDurabilityTask builds the singleton reconciliation task for one bucket.
func NewBucketDurabilityTask(bucketID int64, maxRetries int, scheduledAt time.Time) *model.Task {
	stage := StageReconcileBucketDurability
	return &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "bucket",
		RefID:          bucketID,
		IdempotencyKey: fmt.Sprintf("%s%d", bucketDurabilityTaskKeyPrefix, bucketID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    scheduledAt,
	}
}

// ParseLRUTaskPayload validates and decodes the persisted LRU access snapshot.
func ParseLRUTaskPayload(task *model.Task) (LRUTaskPayload, error) {
	if task == nil {
		return LRUTaskPayload{}, errors.New("nil LRU eviction task")
	}
	raw, ok := task.Payload[lruAccessedAtPayloadKey]
	if !ok {
		return LRUTaskPayload{}, errors.New("LRU eviction task is missing cache_accessed_at")
	}
	value, ok := raw.(string)
	if !ok {
		return LRUTaskPayload{}, fmt.Errorf("LRU eviction task cache_accessed_at has type %T, want string", raw)
	}
	accessedAt, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return LRUTaskPayload{}, fmt.Errorf("parsing LRU eviction task cache_accessed_at: %w", err)
	}
	return LRUTaskPayload{AccessedAt: NormalizeAccessTime(accessedAt)}, nil
}

// DeleteAuthorized reports whether the task has crossed the durable deletion
// authorization boundary.
func DeleteAuthorized(task *model.Task) (bool, error) {
	if task == nil || task.Payload == nil {
		return false, nil
	}
	raw, ok := task.Payload[deleteAuthorizedPayloadKey]
	if !ok {
		return false, nil
	}
	authorized, ok := raw.(bool)
	if !ok {
		return false, fmt.Errorf("cache eviction task delete_authorized has type %T, want bool", raw)
	}
	return authorized, nil
}

// WithDeleteAuthorization copies payload before recording an authorization.
func WithDeleteAuthorization(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		out[key] = value
	}
	out[deleteAuthorizedPayloadKey] = true
	return out
}

func (p LRUTaskPayload) taskPayload() map[string]any {
	return map[string]any{
		lruAccessedAtPayloadKey: NormalizeAccessTime(p.AccessedAt).Format(time.RFC3339Nano),
	}
}
