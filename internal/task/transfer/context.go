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

func loadCopyContext(ctx context.Context, repos *repository.Repositories, copyRow *model.StorageCopy) (*model.StorageDataSet, *model.StorageContent, *model.Bucket, error) {
	if copyRow == nil {
		return nil, nil, nil, errors.New("replica has no data set")
	}
	binding, err := repos.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
	if err != nil {
		return nil, nil, nil, err
	}
	if binding == nil {
		return nil, nil, nil, repository.ErrNotFound
	}
	content, err := repos.Contents.GetByID(ctx, copyRow.ContentID)
	if err != nil {
		return binding, nil, nil, err
	}
	if content == nil {
		return binding, nil, nil, repository.ErrNotFound
	}
	bucket, err := repos.Buckets.GetByID(ctx, content.BucketID)
	if err != nil {
		return binding, content, nil, err
	}
	if bucket == nil {
		return binding, content, nil, repository.ErrNotFound
	}
	return binding, content, bucket, nil
}

func copyContext(ctx context.Context, repos *repository.Repositories, resolver ReadyDataSetResolver, copyRow *model.StorageCopy) (*model.StorageDataSet, synapse.DataSetTarget, *model.StorageContent, *model.Bucket, error) {
	binding, content, bucket, err := loadCopyContext(ctx, repos, copyRow)
	if err != nil {
		return binding, nil, content, bucket, err
	}
	target, err := resolver.OpenReadyDataSet(ctx, binding)
	return binding, target, content, bucket, err
}

func commitBacklogFull(ctx context.Context, repos *repository.Repositories, maxBacklog int, copyRow *model.StorageCopy) (bool, error) {
	waiting, err := repos.Contents.CountCommitBacklog(ctx, copyRow.StorageDataSetID)
	if err != nil {
		return false, err
	}
	return waiting >= maxBacklog, nil
}
