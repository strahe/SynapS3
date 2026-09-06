package objectdeletion

import (
	"context"
	"log/slog"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/model"
)

type cachePresenceRecorder interface {
	ClearContentCachePresence(ctx context.Context, contentID int64) error
}

// ReleaseContentCache removes the cached bytes of one content payload after the
// last object version referencing it has been permanently deleted.
//
// Cache residency is content-addressed, so several versions can share a single
// file. Deleting one of them must leave the file alone; only the disappearance
// of the final reference releases it. The caller establishes that in the same
// transaction that removed the version and passes the content here, so this
// function never has to re-derive a decision it cannot make atomically.
//
// The deletion gate is held on the content key for the same reason: two
// versions of identical bytes contend for one file, not one file each.
func ReleaseContentCache(
	ctx context.Context,
	c cache.Cache,
	gate *cacheaccess.Gate,
	tracker *cacheaccess.Tracker,
	recorder cachePresenceRecorder,
	logger *slog.Logger,
	bucketName string,
	contentID int64,
) bool {
	if gate == nil {
		panic("cache release requires a cache access gate")
	}
	if tracker == nil {
		panic("cache release requires a cache access tracker")
	}
	cacheKey := model.ContentCacheKey(contentID)
	var deleteErr error
	gate.GuardDeletion(cacheKey, func() {
		deleteErr = c.Delete(ctx, bucketName, cacheKey)
		tracker.Forget(contentID)
	})
	if deleteErr != nil {
		logger.Warn("releasing unreferenced content cache failed",
			"bucket", bucketName, "contentID", contentID, "cacheKey", cacheKey, "error", deleteErr)
		return false
	}
	if err := recorder.ClearContentCachePresence(ctx, contentID); err != nil {
		logger.Warn("recording released content cache failed",
			"bucket", bucketName, "contentID", contentID, "error", err)
	}
	return true
}
