package objectdeletion

import (
	"context"
	"log/slog"
	"time"

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
	access *cacheaccess.Coordinator,
	recorder cacheCleanupRecorder,
	logger *slog.Logger,
	bucketName string,
	versionID string,
	cacheKey string,
) model.CacheCleanupStatus {
	if access == nil {
		panic("cache cleanup requires a cache access coordinator")
	}
	status := model.CacheCleanupStatusSkipped
	cacheErr := ""
	if cacheKey != "" {
		var deleteErr error
		access.GuardDeletion(versionID, func(time.Time) bool {
			deleteErr = c.Delete(ctx, bucketName, cacheKey)
			return deleteErr == nil
		})
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
