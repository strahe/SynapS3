package objectdeletion

import (
	"context"
	"log/slog"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
)

type cacheCleanupRecorder interface {
	UpdateObjectDeletionCacheCleanup(ctx context.Context, versionID string, status model.CacheCleanupStatus, cacheError string) error
}

func RecordCacheCleanup(ctx context.Context, c cache.Cache, recorder cacheCleanupRecorder, logger *slog.Logger, bucketName string, versionID string, cacheKey string) model.CacheCleanupStatus {
	status := model.CacheCleanupStatusSkipped
	cacheErr := ""
	if cacheKey != "" {
		if err := c.Delete(ctx, bucketName, cacheKey); err != nil {
			status = model.CacheCleanupStatusFailed
			cacheErr = err.Error()
			logger.Warn("permanent delete cache cleanup failed", "bucket", bucketName, "versionID", versionID, "cacheKey", cacheKey, "error", err)
		} else {
			status = model.CacheCleanupStatusDeleted
		}
	}
	if err := recorder.UpdateObjectDeletionCacheCleanup(ctx, versionID, status, cacheErr); err != nil {
		logger.Warn("recording permanent delete cache cleanup failed", "bucket", bucketName, "versionID", versionID, "status", status, "error", err)
	}
	return status
}
