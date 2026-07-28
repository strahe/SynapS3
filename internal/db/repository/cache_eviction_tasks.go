package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

const (
	CacheEvictionStageLRU         = "lru"
	CacheEvictionStageAfterUpload = "after_upload"
	cacheEvictionLRUKeyPrefix     = "evict_cache:lru:"
	cacheAccessUnaccessed         = "unaccessed"
)

// CacheAccessGeneration returns the stable text generation stored alongside a
// durable cache access timestamp.
func CacheAccessGeneration(accessedAt *time.Time) string {
	if accessedAt == nil {
		return cacheAccessUnaccessed
	}
	return accessedAt.UTC().Format(time.RFC3339Nano)
}

// LRUEvictionTaskKey returns the per-version, per-access-generation task key.
func LRUEvictionTaskKey(versionID, generation string) string {
	if generation == "" {
		generation = cacheAccessUnaccessed
	}
	return cacheEvictionLRUKeyPrefix + versionID + ":" + generation
}

// EnsureAfterUploadEvictionTask creates the stable post-upload eviction task,
// or reactivates it when a policy switch previously cancelled it.
func EnsureAfterUploadEvictionTask(
	ctx context.Context,
	tasks TaskRepository,
	objectID int64,
	versionID string,
	maxRetries int,
) (bool, error) {
	stage := CacheEvictionStageAfterUpload
	return tasks.CreateOrReactivateCancelled(ctx, &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("evict_cache:%s", versionID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	})
}
