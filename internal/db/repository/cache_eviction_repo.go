package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

// Cache residency is keyed by content, not by object version: identical bytes
// written under several keys share one local file, so one eviction decision
// covers every version that references them.
type CacheEvictionRepository interface {
	// GetCacheEntry returns the residency record for one content payload.
	GetCacheEntry(ctx context.Context, contentID int64) (*model.ObjectCache, error)
	NextEvictionGeneration(ctx context.Context, contentID int64) (int64, error)
	PrepareEviction(ctx context.Context, contentID int64) (CacheEvictionReservation, error)
	BindEvictionTask(ctx context.Context, contentID, generation, taskID int64) error
	ListLRUCandidates(ctx context.Context, limit int) ([]cacheeviction.Candidate, error)
	ActiveEvictionBytes(ctx context.Context) (int64, error)
	AuthorizeDeletion(ctx context.Context, contentID, generation, taskID int64, expectedAccess *time.Time) (*cacheeviction.AuthorizedDeletion, error)
	RecordDeletion(ctx context.Context, contentID, generation, taskID int64) error
	DeletionRecorded(ctx context.Context, contentID, generation int64) (bool, error)
	ReleaseEviction(ctx context.Context, contentID, generation, taskID int64) error

	NextDurabilityGeneration(ctx context.Context, bucketID int64) (int64, error)
	BindDurabilityTask(ctx context.Context, bucketID, generation, taskID int64) error
	// NextBucketDurabilityCandidate returns the next cached content in the
	// bucket that now satisfies the bucket's minimum durability. It no longer
	// promotes any lifecycle state: pipeline position is derived from the copy
	// rows, so there is nothing to advance, only cache to reclaim.
	NextBucketDurabilityCandidate(ctx context.Context, bucketID, generation, taskID int64) (*model.StorageContent, error)
	CompleteBucketDurability(ctx context.Context, bucketID, generation, taskID int64) error
}

type CacheEvictionReservation struct {
	Generation   int64
	ActiveTaskID *int64
}

type BunCacheEvictionRepo struct {
	db bun.IDB
}

var _ CacheEvictionRepository = (*BunCacheEvictionRepo)(nil)

func (r *BunCacheEvictionRepo) GetCacheEntry(ctx context.Context, contentID int64) (*model.ObjectCache, error) {
	entry := new(model.ObjectCache)
	err := r.db.NewSelect().Model(entry).Where("content_id = ?", contentID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("selecting content cache entry: %w", err)
	}
	return entry, nil
}

func (r *BunCacheEvictionRepo) NextEvictionGeneration(ctx context.Context, contentID int64) (int64, error) {
	var generation int64
	err := r.db.NewSelect().
		Model((*model.ObjectCache)(nil)).
		ColumnExpr("cache_operation_generation + 1").
		Where("content_id = ?", contentID).
		Scan(ctx, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("reading cache operation generation: %w", err)
	}
	return generation, nil
}

// PrepareEviction locks one cache entry and either reuses its matching live
// eviction or reserves the next generation for the caller to enqueue and bind
// before its surrounding transaction commits.
func (r *BunCacheEvictionRepo) PrepareEviction(ctx context.Context, contentID int64) (CacheEvictionReservation, error) {
	if contentID < 1 {
		return CacheEvictionReservation{}, ErrInvalidInput
	}
	entry, err := lockCacheEntry(ctx, r.db, contentID)
	if err != nil {
		return CacheEvictionReservation{}, err
	}
	if entry.CacheActiveTaskID == nil {
		return CacheEvictionReservation{Generation: entry.CacheOperationGeneration + 1}, nil
	}

	taskRow := new(model.Task)
	err = withTaskPayload(r.db.NewSelect().
		Model(taskRow).
		Where("task.id = ?", *entry.CacheActiveTaskID)).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return CacheEvictionReservation{}, ErrConflict
	}
	if err != nil {
		return CacheEvictionReservation{}, fmt.Errorf("loading active cache eviction task: %w", err)
	}
	if taskRow.Status == model.TaskStatusPending || taskRow.Status == model.TaskStatusRunning {
		expectedSubjectKey := strconv.FormatInt(contentID, 10)
		var taskInput cacheeviction.EvictInput
		inputErr := json.Unmarshal(taskRow.Input, &taskInput)
		if taskRow.Type != model.TaskTypeCacheEvict ||
			taskRow.InputVersion != 1 || inputErr != nil ||
			cacheeviction.ValidateEvictInput(&taskInput) != nil ||
			taskInput.ContentID != contentID || taskInput.Generation != entry.CacheOperationGeneration ||
			taskRow.IdempotencyKey != cacheeviction.EvictTaskKey(contentID, entry.CacheOperationGeneration) ||
			taskRow.SubjectType == nil || *taskRow.SubjectType != "storage_content" ||
			taskRow.SubjectKey == nil || *taskRow.SubjectKey != expectedSubjectKey {
			return CacheEvictionReservation{}, ErrConflict
		}
		return CacheEvictionReservation{
			Generation:   entry.CacheOperationGeneration,
			ActiveTaskID: entry.CacheActiveTaskID,
		}, nil
	}

	result, err := r.db.NewUpdate().
		Model((*model.ObjectCache)(nil)).
		Set("cache_active_task_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("content_id = ?", contentID).
		Where("cache_operation_generation = ?", entry.CacheOperationGeneration).
		Where("cache_active_task_id = ?", *entry.CacheActiveTaskID).
		Exec(ctx)
	if err != nil {
		return CacheEvictionReservation{}, fmt.Errorf("clearing terminal cache eviction owner: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return CacheEvictionReservation{}, ErrConflict
	}
	return CacheEvictionReservation{Generation: entry.CacheOperationGeneration + 1}, nil
}

func (r *BunCacheEvictionRepo) BindEvictionTask(ctx context.Context, contentID, generation, taskID int64) error {
	if contentID < 1 || generation < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	result, err := r.db.NewUpdate().
		Model((*model.ObjectCache)(nil)).
		Set("cache_operation_generation = ?", generation).
		Set("cache_active_task_id = ?", taskID).
		Set("updated_at = ?", time.Now()).
		Where("content_id = ?", contentID).
		Where("cache_operation_generation = ?", generation-1).
		Where("cache_active_task_id IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("binding cache eviction task: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 1 {
		return nil
	}
	var existing struct {
		Generation int64  `bun:"cache_operation_generation"`
		TaskID     *int64 `bun:"cache_active_task_id"`
	}
	err = r.db.NewSelect().
		Model((*model.ObjectCache)(nil)).
		Column("cache_operation_generation", "cache_active_task_id").
		Where("content_id = ?", contentID).
		Scan(ctx, &existing)
	if err != nil {
		return fmt.Errorf("checking cache eviction binding: %w", err)
	}
	if existing.Generation == generation && existing.TaskID != nil && *existing.TaskID == taskID {
		return nil
	}
	return ErrConflict
}

func (r *BunCacheEvictionRepo) ListLRUCandidates(ctx context.Context, limit int) ([]cacheeviction.Candidate, error) {
	var candidates []cacheeviction.Candidate
	query := r.db.NewSelect().
		TableExpr("object_cache AS object_cache").
		ColumnExpr("object_cache.content_id").
		ColumnExpr("storage_content.bucket_id").
		ColumnExpr("storage_content.content_size").
		ColumnExpr("object_cache.cache_accessed_at").
		Join("JOIN storage_contents AS storage_content ON storage_content.id = object_cache.content_id").
		Join("JOIN buckets AS durability_bucket ON durability_bucket.id = storage_content.bucket_id").
		Where("object_cache.cache_active_task_id IS NULL").
		Where("object_cache.in_cache = ?", true).
		Where("storage_content.content_size > 0").
		Where("object_cache.cache_accessed_at IS NOT NULL").
		Where(minimumDurabilityMetSQL("storage_content", "durability_bucket")).
		Where(noUnfinishedReplacementCacheDependencySQL("storage_content.id"),
			storagereplacement.ItemStatusPending,
			storagereplacement.ItemStatusAttention,
			storagereplacement.StatusCompleted,
			storagereplacement.StatusSuperseded).
		OrderExpr("object_cache.cache_accessed_at, object_cache.content_id")
	if limit > 0 {
		query = query.Limit(limit)
	}
	if err := query.Scan(ctx, &candidates); err != nil {
		return nil, fmt.Errorf("listing LRU cache candidates: %w", err)
	}
	return candidates, nil
}

func (r *BunCacheEvictionRepo) ActiveEvictionBytes(ctx context.Context) (int64, error) {
	var total int64
	err := r.db.NewSelect().
		TableExpr("object_cache AS object_cache").
		ColumnExpr("COALESCE(SUM(storage_content.content_size), 0)").
		Join("JOIN storage_contents AS storage_content ON storage_content.id = object_cache.content_id").
		Where("object_cache.in_cache = ?", true).
		Where("object_cache.cache_active_task_id IS NOT NULL").
		Scan(ctx, &total)
	if err != nil {
		return 0, fmt.Errorf("summing active cache eviction bytes: %w", err)
	}
	return total, nil
}

func (r *BunCacheEvictionRepo) AuthorizeDeletion(
	ctx context.Context,
	contentID, generation, taskID int64,
	expectedAccess *time.Time,
) (*cacheeviction.AuthorizedDeletion, error) {
	if contentID < 1 || generation < 1 || taskID < 1 {
		return nil, ErrInvalidInput
	}
	var authorized *cacheeviction.AuthorizedDeletion
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		entry, err := lockCacheEntry(ctx, db, contentID)
		if err != nil {
			return err
		}
		if entry.CacheOperationGeneration != generation || entry.CacheActiveTaskID == nil || *entry.CacheActiveTaskID != taskID {
			return ErrConflict
		}
		if !entry.InCache {
			return cacheeviction.ErrNoLongerEligible
		}
		if expectedAccess != nil && !cacheeviction.NormalizeAccessTime(cacheAccessTime(entry)).Equal(cacheeviction.NormalizeAccessTime(*expectedAccess)) {
			return cacheeviction.ErrAccessChanged
		}
		contents, err := lockStorageContentsByID(ctx, db, []int64{contentID})
		if err != nil {
			return err
		}
		content := contents[contentID]
		if content == nil {
			return cacheeviction.ErrNoLongerEligible
		}
		var pendingReplacement int
		if err := db.NewRaw(`SELECT COUNT(*) FROM storage_replacement_items AS item
			JOIN storage_replacements AS replacement ON replacement.id = item.replacement_id
			WHERE item.content_id = ? AND item.status IN (?, ?)
			  AND replacement.status NOT IN (?, ?)`,
			contentID, storagereplacement.ItemStatusPending,
			storagereplacement.ItemStatusAttention,
			storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
			Scan(ctx, &pendingReplacement); err != nil {
			return err
		}
		if pendingReplacement > 0 {
			return cacheeviction.ErrNoLongerEligible
		}
		bucket, err := lockBucketByID(ctx, db, content.BucketID)
		if err != nil {
			return err
		}
		if bucket == nil {
			return cacheeviction.ErrNoLongerEligible
		}
		if err := requireMinimumDurability(ctx, db, bucket, content); err != nil {
			return err
		}
		authorized = &cacheeviction.AuthorizedDeletion{Content: *content, BucketName: bucket.Name}
		return nil
	})
	return authorized, err
}

func (r *BunCacheEvictionRepo) RecordDeletion(ctx context.Context, contentID, generation, taskID int64) error {
	result, err := r.db.NewUpdate().
		Model((*model.ObjectCache)(nil)).
		Set("in_cache = ?", false).
		Set("cache_presence_generation = cache_presence_generation + 1").
		Set("cache_active_task_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("content_id = ?", contentID).
		Where("cache_operation_generation = ?", generation).
		Where("cache_active_task_id = ?", taskID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("recording cache deletion: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 1 {
		return nil
	}
	recorded, checkErr := r.DeletionRecorded(ctx, contentID, generation)
	if checkErr != nil {
		return checkErr
	}
	if recorded {
		return nil
	}
	return ErrConflict
}

func (r *BunCacheEvictionRepo) DeletionRecorded(ctx context.Context, contentID, generation int64) (bool, error) {
	if contentID < 1 || generation < 1 {
		return false, ErrInvalidInput
	}
	count, err := r.db.NewSelect().
		Model((*model.ObjectCache)(nil)).
		Where("content_id = ?", contentID).
		Where("cache_operation_generation = ?", generation).
		Where("cache_active_task_id IS NULL").
		Where("in_cache = ?", false).
		Count(ctx)
	if err != nil {
		return false, fmt.Errorf("checking recorded cache deletion: %w", err)
	}
	return count == 1, nil
}

func (r *BunCacheEvictionRepo) ReleaseEviction(ctx context.Context, contentID, generation, taskID int64) error {
	result, err := r.db.NewUpdate().
		Model((*model.ObjectCache)(nil)).
		Set("cache_active_task_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("content_id = ?", contentID).
		Where("cache_operation_generation = ?", generation).
		Where("cache_active_task_id = ?", taskID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("releasing cache eviction: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunCacheEvictionRepo) NextDurabilityGeneration(ctx context.Context, bucketID int64) (int64, error) {
	var generation int64
	err := r.db.NewSelect().
		Model((*model.Bucket)(nil)).
		ColumnExpr("durability_generation + 1").
		Where("id = ?", bucketID).
		Scan(ctx, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("reading bucket durability generation: %w", err)
	}
	return generation, nil
}

func (r *BunCacheEvictionRepo) BindDurabilityTask(ctx context.Context, bucketID, generation, taskID int64) error {
	result, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("durability_generation = ?", generation).
		Set("durability_task_id = ?", taskID).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", bucketID).
		Where("durability_generation = ?", generation-1).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("binding bucket durability task: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunCacheEvictionRepo) NextBucketDurabilityCandidate(ctx context.Context, bucketID, generation, taskID int64) (*model.StorageContent, error) {
	var current struct {
		Generation int64  `bun:"durability_generation"`
		TaskID     *int64 `bun:"durability_task_id"`
	}
	if err := r.db.NewSelect().
		Model((*model.Bucket)(nil)).
		Column("durability_generation", "durability_task_id").
		Where("id = ?", bucketID).
		Scan(ctx, &current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("validating bucket durability task: %w", err)
	}
	if current.Generation != generation || current.TaskID == nil || *current.TaskID != taskID {
		return nil, ErrConflict
	}
	return nextBucketDurabilityCandidate(ctx, r.db, bucketID)
}

func (r *BunCacheEvictionRepo) CompleteBucketDurability(ctx context.Context, bucketID, generation, taskID int64) error {
	result, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("durability_task_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND durability_generation = ? AND durability_task_id = ?", bucketID, generation, taskID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing bucket durability reconciliation: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrConflict
	}
	return nil
}

// lockCacheEntry takes the row lock the same way the other repositories do: a
// no-op update, which both dialects serialise without dialect-specific syntax.
func lockCacheEntry(ctx context.Context, db bun.IDB, contentID int64) (*model.ObjectCache, error) {
	lockResult, err := db.NewUpdate().
		Model((*model.ObjectCache)(nil)).
		Set("updated_at = updated_at").
		Where("content_id = ?", contentID).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("locking cache entry: %w", err)
	}
	if rows, _ := lockResult.RowsAffected(); rows == 0 {
		return nil, ErrNotFound
	}
	entry := new(model.ObjectCache)
	if err := db.NewSelect().Model(entry).Where("content_id = ?", contentID).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return entry, nil
}

func requireMinimumDurability(ctx context.Context, db bun.IDB, bucket *model.Bucket, content *model.StorageContent) error {
	if bucket == nil || content == nil || content.BucketID != bucket.ID {
		return ErrInvalidInput
	}
	minimum := minimumDurableCopiesForUpload(bucket, content.RequestedCopies)
	readable, err := countReadableReplicaSlots(ctx, db, content.ID)
	if err != nil {
		return err
	}
	if minimum <= 0 || readable < minimum {
		return cacheeviction.ErrDurabilityThreshold
	}
	return nil
}

// nextBucketDurabilityCandidate finds cached content that now satisfies the
// bucket's minimum durability. A copy-policy change is what schedules this
// scan, so the predicate reads durability from the copy rows rather than from
// any stored lifecycle value.
func nextBucketDurabilityCandidate(ctx context.Context, db bun.IDB, bucketID int64) (*model.StorageContent, error) {
	content := new(model.StorageContent)
	err := db.NewSelect().
		Model(content).
		Join("JOIN object_cache AS cache_entry ON cache_entry.content_id = storage_content.id").
		Join("JOIN buckets AS durability_bucket ON durability_bucket.id = storage_content.bucket_id").
		Where("storage_content.bucket_id = ?", bucketID).
		Where("cache_entry.in_cache = ?", true).
		Where("cache_entry.cache_active_task_id IS NULL").
		Where(minimumDurabilityMetSQL("storage_content", "durability_bucket")).
		Where(noUnfinishedReplacementCacheDependencySQL("storage_content.id"),
			storagereplacement.ItemStatusPending,
			storagereplacement.ItemStatusAttention,
			storagereplacement.StatusCompleted,
			storagereplacement.StatusSuperseded).
		OrderExpr("storage_content.updated_at, storage_content.id").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("selecting bucket durability candidate: %w", err)
	}
	return content, nil
}

func minimumDurabilityMetSQL(contentAlias, bucketAlias string) string {
	return fmt.Sprintf(`%s >= CASE
		WHEN %s.minimum_durable_copies IS NULL
		  OR %s.minimum_durable_copies >= %s.requested_copies
		THEN %s.requested_copies
		ELSE %s.minimum_durable_copies
	END`,
		distinctReadableSlotCountSQL("durable_copy", "durable_data_set", contentAlias+".id"),
		bucketAlias, bucketAlias, contentAlias, contentAlias, bucketAlias,
	)
}

func noUnfinishedReplacementCacheDependencySQL(contentIDExpr string) string {
	return fmt.Sprintf(`NOT EXISTS (
		SELECT 1 FROM storage_replacement_items AS cache_replacement_item
		JOIN storage_replacements AS cache_replacement
		  ON cache_replacement.id = cache_replacement_item.replacement_id
		WHERE cache_replacement_item.content_id = %s
		  AND cache_replacement_item.status IN (?, ?)
		  AND cache_replacement.status NOT IN (?, ?)
	)`, contentIDExpr)
}

func cacheAccessTime(entry *model.ObjectCache) time.Time {
	if entry.CacheAccessedAt != nil {
		return *entry.CacheAccessedAt
	}
	return entry.CreatedAt
}
