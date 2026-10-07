package transfer

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
)

func copyCacheAvailable(ctx context.Context, repos *repository.Repositories, localCache cache.Cache, copyRow *model.StorageCopy) (bool, error) {
	entry, err := repos.CacheEvictions.GetCacheEntry(ctx, copyRow.ContentID)
	if err != nil || entry == nil || !entry.InCache || localCache == nil {
		return false, err
	}
	bucket, err := repos.Buckets.GetByID(ctx, copyRow.BucketID)
	if err != nil || bucket == nil {
		return false, err
	}
	return localCache.Exists(ctx, bucket.Name, model.ContentCacheKey(copyRow.ContentID)), nil
}

func copyContext(
	ctx context.Context,
	repos *repository.Repositories,
	resolver ReadyDataSetResolver,
	copyRow *model.StorageCopy,
) (*model.StorageDataSet, synapse.DataSetTarget, *model.StorageContent, *model.Bucket, error) {
	if copyRow == nil {
		return nil, nil, nil, nil, errors.New("storage copy has no data set")
	}
	binding, err := repos.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
	if err != nil || binding == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return binding, nil, nil, nil, err
	}
	target, err := resolver.OpenReadyDataSet(ctx, binding)
	if err != nil {
		return binding, nil, nil, nil, err
	}
	content, err := repos.Contents.GetByID(ctx, copyRow.ContentID)
	if err != nil || content == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return binding, target, content, nil, err
	}
	bucket, err := repos.Buckets.GetByID(ctx, content.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return binding, target, content, bucket, err
	}
	return binding, target, content, bucket, nil
}

func commitBacklogFull(ctx context.Context, repos *repository.Repositories, maxBacklog int, copyRow *model.StorageCopy) (bool, error) {
	waiting, err := repos.Contents.CountCommitBacklog(ctx, copyRow.StorageDataSetID)
	if err != nil {
		return false, err
	}
	return waiting >= maxBacklog, nil
}
