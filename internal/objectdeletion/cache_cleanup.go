package objectdeletion

import (
	"context"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/model"
)

type cacheReleaseRepository interface {
	ReleaseContentCacheIfUnreferenced(ctx context.Context, contentID int64, release func() error) (bool, error)
}

type CacheReleaseOutcome string

const (
	CacheReleaseRetained CacheReleaseOutcome = "retained"
	CacheReleaseReleased CacheReleaseOutcome = "released"
)

// ReleaseContentCache removes the cached bytes of one content payload after the
// last object version referencing it has been permanently deleted.
//
// Cache residency is content-addressed, so several versions can share a single
// file. Deleting one of them must leave the file alone; only the disappearance
// of the final reference releases it. The reference decision is rechecked while
// the content row and deletion gate are both held.
//
// The deletion gate is held on the content key for the same reason: two
// versions of identical bytes contend for one file, not one file each.
func ReleaseContentCache(
	ctx context.Context,
	c cache.Cache,
	gate *cacheaccess.Gate,
	tracker *cacheaccess.Tracker,
	repository cacheReleaseRepository,
	bucketName string,
	contentID int64,
) (CacheReleaseOutcome, error) {
	if gate == nil {
		panic("cache release requires a cache access gate")
	}
	if tracker == nil {
		panic("cache release requires a cache access tracker")
	}
	cacheKey := model.ContentCacheKey(contentID)
	outcome := CacheReleaseRetained
	var releaseErr error
	gate.GuardDeletion(cacheKey, func() {
		var released bool
		released, releaseErr = repository.ReleaseContentCacheIfUnreferenced(ctx, contentID, func() error {
			return c.Delete(ctx, bucketName, cacheKey)
		})
		if releaseErr == nil && released {
			tracker.Forget(contentID)
			outcome = CacheReleaseReleased
		}
	})
	return outcome, releaseErr
}
