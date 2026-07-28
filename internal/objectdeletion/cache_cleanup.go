package objectdeletion

import (
	"context"
	"log/slog"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/model"
)

type cacheCleanupRecorder interface {
	UpdateObjectDeletionCacheCleanup(ctx context.Context, versionID string, status model.CacheCleanupStatus, cacheError string) error
}

func RecordCacheCleanup(
	ctx context.Context,
	c cache.Cache,
	gate *cacheaccess.Gate,
	tracker *cacheaccess.Tracker,
	recorder cacheCleanupRecorder,
	logger *slog.Logger,
	bucketName string,
	versionID string,
	cacheKey string,
) model.CacheCleanupStatus {
	if gate == nil {
		panic("cache cleanup requires a cache access gate")
	}
	if tracker == nil {
		panic("cache cleanup requires a cache access tracker")
	}
	status := model.CacheCleanupStatusSkipped
	cacheErr := ""
	var deleteErr error
	gate.GuardDeletion(versionID, func() {
		if cacheKey != "" {
			deleteErr = c.Delete(ctx, bucketName, cacheKey)
		}
		tracker.Forget(versionID)
	})
	if cacheKey != "" {
		if deleteErr != nil {
			status = model.CacheCleanupStatusFailed
			cacheErr = deleteErr.Error()
			logger.Warn("permanent delete cache cleanup failed", "bucket", bucketName, "versionID", versionID, "cacheKey", cacheKey, "error", deleteErr)
		} else {
			status = model.CacheCleanupStatusDeleted
		}
	}
	if err := recorder.UpdateObjectDeletionCacheCleanup(ctx, versionID, status, cacheErr); err != nil {
		logger.Warn("recording permanent delete cache cleanup failed", "bucket", bucketName, "versionID", versionID, "status", status, "error", err)
	}
	return status
}
