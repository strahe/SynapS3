package cacheeviction

import (
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

const (
	StageLRU         = "lru"
	StageAfterUpload = "after_upload"

	lruAccessedAtPayloadKey  = "cache_accessed_at"
	lruTaskKeyPrefix         = "evict_cache:lru:"
	afterUploadTaskKeyPrefix = "evict_cache:"
)

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

func (p LRUTaskPayload) taskPayload() map[string]any {
	return map[string]any{
		lruAccessedAtPayloadKey: NormalizeAccessTime(p.AccessedAt).Format(time.RFC3339Nano),
	}
}
