package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

// CacheLocalState is the content identity and residency observed before checking
// a file. The generations fence a later presence update against cache work.
type CacheLocalState struct {
	ContentID                int64
	BucketName               string
	Checksum                 string
	ContentSize              int64
	InCache                  bool
	Referenced               bool
	CacheOperationGeneration int64
	CachePresenceGeneration  int64
	CacheActiveTaskID        *int64
}

type CacheLocalRepository interface {
	ListStates(context.Context, []int64) (map[int64]CacheLocalState, error)
	RestorePresence(context.Context, CacheLocalState) (bool, error)
	DeleteQuarantinedIfSafe(context.Context, int64, func() error) (bool, error)
}

type BunCacheLocalRepo struct{ db bun.IDB }

func NewBunCacheLocalRepo(db bun.IDB) *BunCacheLocalRepo {
	return &BunCacheLocalRepo{db: db}
}

func (r *BunCacheLocalRepo) ListStates(ctx context.Context, contentIDs []int64) (map[int64]CacheLocalState, error) {
	states := make(map[int64]CacheLocalState, len(contentIDs))
	if len(contentIDs) == 0 {
		return states, nil
	}
	var rows []CacheLocalState
	if err := r.db.NewSelect().TableExpr("storage_contents AS content").
		Join("JOIN buckets AS bucket ON bucket.id = content.bucket_id").
		Join("LEFT JOIN object_cache AS cache ON cache.content_id = content.id").
		ColumnExpr("content.id AS content_id, bucket.name AS bucket_name, content.checksum, content.content_size").
		ColumnExpr("COALESCE(cache.in_cache, false) AS in_cache").
		ColumnExpr("COALESCE(cache.cache_operation_generation, 0) AS cache_operation_generation").
		ColumnExpr("COALESCE(cache.cache_presence_generation, 0) AS cache_presence_generation").
		ColumnExpr("cache.cache_active_task_id").
		ColumnExpr("EXISTS (SELECT 1 FROM object_versions AS version WHERE version.content_id = content.id) AS referenced").
		Where("content.id IN (?)", bun.List(contentIDs)).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("loading local cache states: %w", err)
	}
	for _, row := range rows {
		states[row.ContentID] = row
	}
	return states, nil
}

func (r *BunCacheLocalRepo) RestorePresence(ctx context.Context, observed CacheLocalState) (bool, error) {
	now := time.Now().UTC()
	accessedAt := cacheeviction.NormalizeAccessTime(now)
	result, err := r.db.NewUpdate().Model((*model.ObjectCache)(nil)).
		Set("in_cache = ?", true).
		Set("cache_accessed_at = GREATEST(cache_accessed_at, ?)", accessedAt).
		Set("updated_at = ?", now).
		Where("content_id = ?", observed.ContentID).
		Where("in_cache = ?", false).
		Where("cache_active_task_id IS NULL").
		Where("cache_operation_generation = ?", observed.CacheOperationGeneration).
		Where("cache_presence_generation = ?", observed.CachePresenceGeneration).
		Where(`EXISTS (SELECT 1 FROM storage_contents AS content
			JOIN buckets AS bucket ON bucket.id = content.bucket_id
			WHERE content.id = object_cache.content_id AND bucket.name = ?
			  AND content.checksum = ? AND content.content_size = ?
			  AND EXISTS (SELECT 1 FROM object_versions AS version WHERE version.content_id = content.id))`,
			observed.BucketName, observed.Checksum, observed.ContentSize).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("restoring local cache presence: %w", err)
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

// DeleteQuarantinedIfSafe holds the durability decision through a single-file
// deletion. The caller holds the content deletion gate before its transaction.
func (r *BunCacheLocalRepo) DeleteQuarantinedIfSafe(ctx context.Context, contentID int64, remove func() error) (bool, error) {
	if contentID < 1 || remove == nil {
		return false, ErrInvalidInput
	}
	deleted := false
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		contents, err := lockStorageContentsByID(ctx, db, []int64{contentID})
		if errors.Is(err, ErrNotFound) {
			if err := remove(); err != nil {
				return err
			}
			deleted = true
			return nil
		}
		if err != nil {
			return err
		}
		content := contents[contentID]
		if content == nil {
			return ErrNotFound
		}
		if _, err := lockBucketByID(ctx, db, content.BucketID); err != nil {
			return err
		}
		eligible, err := db.NewSelect().TableExpr("storage_contents AS content").
			Join("JOIN buckets AS bucket ON bucket.id = content.bucket_id").
			Where("content.id = ?", contentID).
			Where(minimumDurabilityMetSQL("content", "bucket")).
			Where(noUnfinishedStoreCacheDependencySQL("content.id")).
			Where(noUnfinishedReplacementCacheDependencySQL("content.id"),
				storagereplacement.ItemStatusPending, storagereplacement.ItemStatusAttention,
				storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
			Exists(ctx)
		if err != nil || !eligible {
			return err
		}
		if err := remove(); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	return deleted, err
}
