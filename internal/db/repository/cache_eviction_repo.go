package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func (r *BunCacheEvictionRepo) AuthorizeDeletion(
	ctx context.Context,
	task *model.Task,
	expectedAccess *time.Time,
) (*cacheeviction.AuthorizedDeletion, error) {
	if task == nil || task.RefVersionID == "" {
		return nil, fmt.Errorf("cache eviction task target is required: %w", ErrInvalidInput)
	}
	preflight, err := r.objectVersionByID(ctx, r.db, task.RefVersionID)
	if err != nil {
		return nil, err
	}
	if preflight.StorageUploadID == nil {
		return nil, cacheeviction.ErrNoLongerEligible
	}

	var authorized *cacheeviction.AuthorizedDeletion
	err = r.runMaybeTx(ctx, func(db bun.IDB) error {
		bucket, upload, version, lockedTask, err := lockCacheEvictionContext(
			ctx,
			db,
			task,
			preflight.BucketID,
			*preflight.StorageUploadID,
			preflight.VersionID,
		)
		if err != nil {
			return err
		}
		if lockedTask.RefVersionID != version.VersionID {
			return fmt.Errorf("cache eviction task target changed: %w", ErrConflict)
		}
		alreadyAuthorized, err := cacheeviction.DeleteAuthorized(lockedTask)
		if err != nil {
			return err
		}
		if !alreadyAuthorized {
			if lockedTask.RefType != "object" {
				return fmt.Errorf("cache eviction task is not an object deletion: %w", ErrConflict)
			}
			if err := requireMinimumDurability(ctx, db, bucket, upload); err != nil {
				return err
			}
			if !cacheDeletionStateEligible(version) {
				return cacheeviction.ErrNoLongerEligible
			}
			if expectedAccess != nil && !cacheeviction.NormalizeAccessTime(cacheAccessTime(version)).Equal(cacheeviction.NormalizeAccessTime(*expectedAccess)) {
				return cacheeviction.ErrAccessChanged
			}
			payload := cacheeviction.WithDeleteAuthorization(lockedTask.Payload)
			if err := updateRunningEvictionTask(ctx, db, task, lockedTask.RefVersionID, payload); err != nil {
				return err
			}
			task.Payload = payload
		}
		authorized = &cacheeviction.AuthorizedDeletion{Version: *version, BucketName: bucket.Name}
		return nil
	})
	return authorized, err
}

func (r *BunCacheEvictionRepo) NextBucketDurabilityCandidate(
	ctx context.Context,
	bucketID int64,
) (*model.ObjectVersion, error) {
	return nextBucketDurabilityCandidate(ctx, r.db, bucketID)
}

func (r *BunCacheEvictionRepo) PromoteBucketDurabilityCandidate(
	ctx context.Context,
	task *model.Task,
	versionID string,
	authorizeDelete bool,
) (*cacheeviction.AuthorizedDeletion, error) {
	if task == nil || task.RefType != "bucket" || task.RefID <= 0 || versionID == "" {
		return nil, fmt.Errorf("bucket durability task and candidate are required: %w", ErrInvalidInput)
	}
	preflight, err := r.objectVersionByID(ctx, r.db, versionID)
	if err != nil {
		return nil, err
	}
	if preflight.BucketID != task.RefID || preflight.StorageUploadID == nil {
		return nil, cacheeviction.ErrNoLongerEligible
	}

	var deletion *cacheeviction.AuthorizedDeletion
	err = r.runMaybeTx(ctx, func(db bun.IDB) error {
		bucket, upload, version, lockedTask, err := lockCacheEvictionContext(
			ctx,
			db,
			task,
			preflight.BucketID,
			*preflight.StorageUploadID,
			versionID,
		)
		if err != nil {
			return err
		}
		if lockedTask.RefType != "bucket" || lockedTask.RefID != bucket.ID {
			return fmt.Errorf("bucket durability task target changed: %w", ErrConflict)
		}
		if version.BucketID != bucket.ID || version.StorageUploadID == nil || *version.StorageUploadID != upload.ID ||
			version.State != model.ObjectStateReplicating || !version.InCache || version.IsDeleteMarker {
			return cacheeviction.ErrNoLongerEligible
		}
		if err := requireMinimumDurability(ctx, db, bucket, upload); err != nil {
			return err
		}
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.ObjectVersion)(nil)).
			Set("state = ?", model.ObjectStateStored).
			Set("updated_at = ?", now).
			Where("version_id = ? AND state = ? AND in_cache = ?", version.VersionID, model.ObjectStateReplicating, true).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("promoting bucket durability candidate: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows != 1 {
			return cacheeviction.ErrNoLongerEligible
		}
		version.State = model.ObjectStateStored
		version.UpdatedAt = now
		if authorizeDelete {
			payload := cacheeviction.WithDeleteAuthorization(lockedTask.Payload)
			if err := updateRunningEvictionTask(ctx, db, task, version.VersionID, payload); err != nil {
				return err
			}
			task.RefVersionID = version.VersionID
			task.Payload = payload
			deletion = &cacheeviction.AuthorizedDeletion{Version: *version, BucketName: bucket.Name}
		}
		return nil
	})
	return deletion, err
}

func (r *BunCacheEvictionRepo) CompleteBucketDurabilityReconciliation(
	ctx context.Context,
	task *model.Task,
) (bool, error) {
	if task == nil || task.RefType != "bucket" || task.RefID <= 0 {
		return false, fmt.Errorf("bucket durability task is required: %w", ErrInvalidInput)
	}
	completed := false
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		bucket, err := lockBucketByID(ctx, db, task.RefID)
		if err != nil {
			return err
		}
		if bucket == nil {
			return cacheeviction.ErrNoLongerEligible
		}
		candidate, err := nextBucketDurabilityCandidate(ctx, db, bucket.ID)
		if err != nil {
			return err
		}
		if candidate != nil {
			return nil
		}
		if err := (&BunTaskRepo{db: db}).LockRunningClaim(ctx, task); err != nil {
			return err
		}
		if err := (&BunTaskRepo{db: db}).Complete(ctx, task); err != nil {
			return err
		}
		completed = true
		return nil
	})
	return completed, err
}

func (r *BunCacheEvictionRepo) RecordAuthorizedDeletion(ctx context.Context, task *model.Task) error {
	if task == nil || task.RefVersionID == "" {
		return fmt.Errorf("authorized cache eviction task is required: %w", ErrInvalidInput)
	}
	preflight, err := r.objectVersionByID(ctx, r.db, task.RefVersionID)
	if err != nil {
		if errors.Is(err, ErrNotFound) && task.RefType == "bucket" {
			return r.clearMissingBucketDurabilityAuthorization(ctx, task)
		}
		return err
	}
	if preflight.StorageUploadID == nil {
		return cacheeviction.ErrNoLongerEligible
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		_, _, version, lockedTask, err := lockCacheEvictionContext(
			ctx,
			db,
			task,
			preflight.BucketID,
			*preflight.StorageUploadID,
			preflight.VersionID,
		)
		if err != nil {
			return err
		}
		authorized, err := cacheeviction.DeleteAuthorized(lockedTask)
		if err != nil {
			return err
		}
		if !authorized || lockedTask.RefVersionID != version.VersionID {
			return fmt.Errorf("cache deletion was not authorized: %w", ErrConflict)
		}
		switch version.State {
		case model.ObjectStateStored:
			_, err = db.NewUpdate().
				Model((*model.ObjectVersion)(nil)).
				Set("state = ?", model.ObjectStateCacheEvicted).
				Set("in_cache = ?", false).
				Set("updated_at = ?", time.Now()).
				Where("version_id = ? AND state = ?", version.VersionID, model.ObjectStateStored).
				Exec(ctx)
		case model.ObjectStateCacheEvicted:
			_, err = db.NewUpdate().
				Model((*model.ObjectVersion)(nil)).
				Set("in_cache = ?", false).
				Where("version_id = ?", version.VersionID).
				Exec(ctx)
		default:
			return cacheeviction.ErrNoLongerEligible
		}
		if err != nil {
			return fmt.Errorf("recording authorized cache deletion: %w", err)
		}
		if lockedTask.RefType == "bucket" {
			if err := updateRunningEvictionTask(ctx, db, task, "", nil); err != nil {
				return err
			}
			task.RefVersionID = ""
			task.Payload = nil
		}
		return nil
	})
}

func (r *BunCacheEvictionRepo) clearMissingBucketDurabilityAuthorization(
	ctx context.Context,
	task *model.Task,
) error {
	if task == nil || task.RefType != "bucket" || task.RefID <= 0 || task.RefVersionID == "" {
		return fmt.Errorf("bucket durability deletion authorization is required: %w", ErrInvalidInput)
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		bucket, err := lockBucketByID(ctx, db, task.RefID)
		if err != nil {
			return err
		}
		if bucket == nil {
			return cacheeviction.ErrNoLongerEligible
		}
		tasks := &BunTaskRepo{db: db}
		if err := tasks.LockRunningClaim(ctx, task); err != nil {
			return err
		}
		lockedTask, err := tasks.GetByID(ctx, task.ID)
		if err != nil {
			return err
		}
		if lockedTask == nil || lockedTask.RefType != "bucket" || lockedTask.RefID != bucket.ID ||
			lockedTask.RefVersionID != task.RefVersionID {
			return fmt.Errorf("bucket durability deletion target changed: %w", ErrConflict)
		}
		authorized, err := cacheeviction.DeleteAuthorized(lockedTask)
		if err != nil {
			return err
		}
		if !authorized {
			return fmt.Errorf("bucket durability deletion was not authorized: %w", ErrConflict)
		}
		if err := updateRunningEvictionTask(ctx, db, task, "", nil); err != nil {
			return err
		}
		task.RefVersionID = ""
		task.Payload = nil
		return nil
	})
}

// CacheEvictionRepository owns persistence operations used only by cache
// eviction planning and policy reconciliation.
type CacheEvictionRepository interface {
	EnsureAfterUploadTask(ctx context.Context, objectID int64, versionID string, maxRetries int) (bool, error)
	EnsureBucketDurabilityReconciliation(ctx context.Context, bucketID int64, maxRetries int) (bool, error)
	ListLRUCandidates(ctx context.Context, terminalSince time.Time, limit int) ([]cacheeviction.Candidate, error)
	PlanLRU(ctx context.Context, candidate cacheeviction.Candidate, maxRetries int, terminalBefore time.Time) (bool, error)
	ActiveLRUBytes(ctx context.Context) (int64, error)
	CancelActiveTasksExcept(ctx context.Context, keepStage string, message string) (int, error)
	AuthorizeDeletion(ctx context.Context, task *model.Task, expectedAccess *time.Time) (*cacheeviction.AuthorizedDeletion, error)
	NextBucketDurabilityCandidate(ctx context.Context, bucketID int64) (*model.ObjectVersion, error)
	PromoteBucketDurabilityCandidate(ctx context.Context, task *model.Task, versionID string, authorizeDelete bool) (*cacheeviction.AuthorizedDeletion, error)
	CompleteBucketDurabilityReconciliation(ctx context.Context, task *model.Task) (bool, error)
	RecordAuthorizedDeletion(ctx context.Context, task *model.Task) error
}

// BunCacheEvictionRepo implements cache eviction planning and reconciliation
// persistence.
type BunCacheEvictionRepo struct {
	db bun.IDB
}

var _ CacheEvictionRepository = (*BunCacheEvictionRepo)(nil)

func (r *BunCacheEvictionRepo) EnsureAfterUploadTask(
	ctx context.Context,
	objectID int64,
	versionID string,
	maxRetries int,
) (bool, error) {
	task := cacheeviction.NewAfterUploadTask(objectID, versionID, maxRetries, time.Now())
	return r.createOrReactivate(ctx, task, taskReactivationRule{
		immediateStatuses: []model.TaskStatus{model.TaskStatusCancelled},
		errorAction:       "reactivating after-upload eviction task",
	})
}

func (r *BunCacheEvictionRepo) EnsureBucketDurabilityReconciliation(
	ctx context.Context,
	bucketID int64,
	maxRetries int,
) (bool, error) {
	task := cacheeviction.NewBucketDurabilityTask(bucketID, maxRetries, time.Now())
	requestedMaxRetries := task.MaxRetries
	activated := false
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		existing, err := loadAndLockTaskByIdempotencyKey(ctx, db, task.IdempotencyKey)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("loading bucket durability task: %w", err)
			}
			res, err := db.NewInsert().Model(task).On("CONFLICT (idempotency_key) DO NOTHING").Exec(ctx)
			if err != nil {
				return fmt.Errorf("creating bucket durability task: %w", err)
			}
			rows, _ := res.RowsAffected()
			activated = rows == 1
			if activated && requestedMaxRetries == 0 {
				// Bun otherwise substitutes the SQL default for this zero-valued field.
				if _, err := db.NewUpdate().
					Model((*model.Task)(nil)).
					Set("max_retries = ?", 0).
					Where("id = ?", task.ID).
					Exec(ctx); err != nil {
					return fmt.Errorf("preserving bucket durability task zero retries: %w", err)
				}
				task.MaxRetries = requestedMaxRetries
			}
			return nil
		}
		switch existing.Status {
		case model.TaskStatusCompleted,
			model.TaskStatusFailed,
			model.TaskStatusExhausted,
			model.TaskStatusCancelled:
		default:
			return nil
		}

		preserveAuthorization, err := cacheeviction.DeleteAuthorized(existing)
		if err != nil {
			return fmt.Errorf("reading bucket durability task authorization: %w", err)
		}
		refVersionID := task.RefVersionID
		payload := task.Payload
		if preserveAuthorization && existing.Status != model.TaskStatusCompleted {
			refVersionID = existing.RefVersionID
			payload = existing.Payload
		}
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("stage = ?", task.Stage).
			Set("ref_type = ?", task.RefType).
			Set("ref_id = ?", task.RefID).
			Set("ref_version_id = ?", refVersionID).
			Set("payload = ?", payload).
			Set("status = ?", model.TaskStatusQueued).
			Set("retry_count = 0").
			Set("max_retries = ?", requestedMaxRetries).
			Set("scheduled_at = ?", now).
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("started_at = NULL").
			Set("completed_at = NULL").
			Set("last_error = NULL").
			Set("wait_reason = NULL").
			Set("status_message = NULL").
			Where("id = ? AND status = ?", existing.ID, existing.Status).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("reactivating bucket durability task %d: %w", bucketID, err)
		}
		rows, _ := res.RowsAffected()
		activated = rows == 1
		return nil
	})
	return activated, err
}

func (r *BunCacheEvictionRepo) ListLRUCandidates(
	ctx context.Context,
	terminalSince time.Time,
	limit int,
) ([]cacheeviction.Candidate, error) {
	var candidates []cacheeviction.Candidate
	q := r.db.NewSelect().
		TableExpr("object_versions AS object_version").
		ColumnExpr("object_version.object_id").
		ColumnExpr("object_version.version_id").
		ColumnExpr("object_version.size").
		ColumnExpr("object_version.cache_accessed_at").
		Join("JOIN storage_uploads AS storage_upload ON storage_upload.id = object_version.storage_upload_id").
		Join("JOIN buckets AS durability_bucket ON durability_bucket.id = storage_upload.bucket_id").
		Where("object_version.in_cache = ?", true).
		Where("object_version.is_delete_marker = ?", false).
		Where("object_version.size > 0").
		Where("object_version.cache_accessed_at IS NOT NULL").
		Where("object_version.state IN (?)", bun.List([]model.ObjectState{
			model.ObjectStateStored,
			model.ObjectStateCacheEvicted,
		})).
		Where("storage_upload.status IN (?)", bun.List([]model.StorageUploadStatus{
			model.StorageUploadStatusReadable,
			model.StorageUploadStatusComplete,
		})).
		Where(minimumDurabilityMetSQL("storage_upload", "durability_bucket")).
		Where(`NOT EXISTS (
			SELECT 1 FROM tasks AS eviction_task
			WHERE eviction_task.type = ?
			  AND eviction_task.ref_type = ?
			  AND eviction_task.ref_version_id = object_version.version_id
			  AND eviction_task.status IN (?)
		)`, model.TaskTypeEvictCache, "object", bun.List(activeTaskStatuses())).
		Where(`NOT EXISTS (
			SELECT 1 FROM tasks AS terminal_lru_task
			WHERE terminal_lru_task.type = ?
			  AND terminal_lru_task.stage = ?
			  AND terminal_lru_task.ref_type = ?
			  AND terminal_lru_task.ref_version_id = object_version.version_id
			  AND terminal_lru_task.status IN (?)
			  AND (terminal_lru_task.completed_at IS NULL OR terminal_lru_task.completed_at > ?)
		)`,
			model.TaskTypeEvictCache,
			cacheeviction.StageLRU,
			"object",
			bun.List([]model.TaskStatus{model.TaskStatusFailed, model.TaskStatusExhausted}),
			terminalSince,
		).
		OrderExpr("object_version.cache_accessed_at ASC").
		OrderExpr("object_version.created_at ASC").
		OrderExpr("object_version.version_id ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Scan(ctx, &candidates); err != nil {
		return nil, fmt.Errorf("listing LRU cache eviction candidates: %w", err)
	}
	return candidates, nil
}

func (r *BunCacheEvictionRepo) PlanLRU(
	ctx context.Context,
	candidate cacheeviction.Candidate,
	maxRetries int,
	terminalBefore time.Time,
) (bool, error) {
	task := cacheeviction.NewLRUTask(candidate, maxRetries, time.Now())
	return r.createOrReactivate(ctx, task, taskReactivationRule{
		immediateStatuses: []model.TaskStatus{
			model.TaskStatusCancelled,
			model.TaskStatusCompleted,
		},
		cooledStatuses: []model.TaskStatus{
			model.TaskStatusFailed,
			model.TaskStatusExhausted,
		},
		terminalBefore: &terminalBefore,
		errorAction:    "reactivating LRU eviction task",
	})
}

type taskReactivationRule struct {
	immediateStatuses []model.TaskStatus
	cooledStatuses    []model.TaskStatus
	terminalBefore    *time.Time
	errorAction       string
}

func (r *BunCacheEvictionRepo) createOrReactivate(
	ctx context.Context,
	task *model.Task,
	rule taskReactivationRule,
) (bool, error) {
	activated := false
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		existing, err := loadAndLockTaskByIdempotencyKey(ctx, db, task.IdempotencyKey)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			res, err := db.NewInsert().Model(task).On("CONFLICT (idempotency_key) DO NOTHING").Exec(ctx)
			if err != nil {
				return err
			}
			rows, _ := res.RowsAffected()
			activated = rows == 1
			return nil
		}
		eligible := taskStatusIn(existing.Status, rule.immediateStatuses)
		if !eligible && taskStatusIn(existing.Status, rule.cooledStatuses) {
			if rule.terminalBefore == nil {
				return errors.New("reactivating task: terminal cutoff is required")
			}
			eligible = existing.CompletedAt != nil && !existing.CompletedAt.After(*rule.terminalBefore)
		}
		if !eligible {
			return nil
		}

		refVersionID := task.RefVersionID
		payload := task.Payload
		preserveAuthorization, err := cacheeviction.DeleteAuthorized(existing)
		if err != nil {
			return err
		}
		if preserveAuthorization && existing.Status != model.TaskStatusCompleted {
			refVersionID = existing.RefVersionID
			payload = existing.Payload
		}
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("type = ?", task.Type).
			Set("stage = ?", task.Stage).
			Set("ref_type = ?", task.RefType).
			Set("ref_id = ?", task.RefID).
			Set("ref_version_id = ?", refVersionID).
			Set("payload = ?", payload).
			Set("status = ?", model.TaskStatusQueued).
			Set("retry_count = 0").
			Set("max_retries = ?", task.MaxRetries).
			Set("scheduled_at = ?", now).
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("started_at = NULL").
			Set("completed_at = NULL").
			Set("last_error = NULL").
			Set("wait_reason = NULL").
			Set("status_message = NULL").
			Where("id = ? AND status = ?", existing.ID, existing.Status).
			Exec(ctx)
		if err != nil {
			return err
		}
		rows, _ := res.RowsAffected()
		activated = rows == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("%s %q: %w", rule.errorAction, task.IdempotencyKey, err)
	}
	return activated, nil
}

func taskStatusIn(status model.TaskStatus, statuses []model.TaskStatus) bool {
	for _, candidate := range statuses {
		if status == candidate {
			return true
		}
	}
	return false
}

func (r *BunCacheEvictionRepo) ActiveLRUBytes(ctx context.Context) (int64, error) {
	var total int64
	err := r.db.NewSelect().
		TableExpr("object_versions AS object_version").
		ColumnExpr("COALESCE(SUM(object_version.size), 0)").
		Where("object_version.in_cache = ?", true).
		Where(`EXISTS (
			SELECT 1 FROM tasks AS eviction_task
			WHERE eviction_task.type = ?
			  AND eviction_task.stage = ?
			  AND eviction_task.ref_type = ?
			  AND eviction_task.ref_version_id = object_version.version_id
			  AND eviction_task.status IN (?)
		)`,
			model.TaskTypeEvictCache,
			cacheeviction.StageLRU,
			"object",
			bun.List(activeTaskStatuses()),
		).
		Scan(ctx, &total)
	if err != nil {
		return 0, fmt.Errorf("summing active LRU eviction bytes: %w", err)
	}
	return total, nil
}

func (r *BunCacheEvictionRepo) CancelActiveTasksExcept(
	ctx context.Context,
	keepStage string,
	message string,
) (int, error) {
	now := time.Now()
	q := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCancelled).
		Set("completed_at = ?", now).
		Set("last_error = NULL").
		Set("wait_reason = NULL").
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("type = ?", model.TaskTypeEvictCache).
		Where("status IN (?)", bun.List(activeTaskStatuses())).
		Where("stage IS NULL OR stage <> ?", cacheeviction.StageReconcileBucketDurability).
		Where("NOT (" + cacheDeletionAuthorizedSQL(r.db.Dialect().Name()) + ")")
	if keepStage != "" {
		q = q.Where("(stage IS NULL OR stage <> ?)", keepStage)
	}
	if message == "" {
		q = q.Set("status_message = NULL")
	} else {
		q = q.Set("status_message = ?", message)
	}
	res, err := q.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("cancelling incompatible cache eviction tasks: %w", err)
	}
	rows, _ := res.RowsAffected()
	return int(rows), nil
}

func (r *BunCacheEvictionRepo) objectVersionByID(
	ctx context.Context,
	db bun.IDB,
	versionID string,
) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	if err := db.NewSelect().Model(version).Where("version_id = ?", versionID).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("loading cache eviction object version: %w", err)
	}
	return version, nil
}

func lockCacheEvictionContext(
	ctx context.Context,
	db bun.IDB,
	claimedTask *model.Task,
	bucketID int64,
	uploadID int64,
	versionID string,
) (*model.Bucket, *model.StorageUpload, *model.ObjectVersion, *model.Task, error) {
	bucket, err := lockBucketByID(ctx, db, bucketID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if bucket == nil {
		return nil, nil, nil, nil, cacheeviction.ErrNoLongerEligible
	}
	uploads, err := lockStorageUploadsByID(ctx, db, []int64{uploadID})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	upload := uploads[uploadID]
	if upload == nil || upload.BucketID != bucket.ID {
		return nil, nil, nil, nil, cacheeviction.ErrNoLongerEligible
	}
	if err := lockObjectVersionsByID(ctx, db, []string{versionID}); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil, nil, nil, cacheeviction.ErrNoLongerEligible
		}
		return nil, nil, nil, nil, err
	}
	version := new(model.ObjectVersion)
	if err := db.NewSelect().Model(version).Where("version_id = ?", versionID).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil, nil, cacheeviction.ErrNoLongerEligible
		}
		return nil, nil, nil, nil, fmt.Errorf("loading locked cache eviction object version: %w", err)
	}
	if version.BucketID != bucket.ID || version.StorageUploadID == nil || *version.StorageUploadID != upload.ID {
		return nil, nil, nil, nil, cacheeviction.ErrNoLongerEligible
	}
	tasks := &BunTaskRepo{db: db}
	if err := tasks.LockRunningClaim(ctx, claimedTask); err != nil {
		return nil, nil, nil, nil, err
	}
	lockedTask, err := tasks.GetByID(ctx, claimedTask.ID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if lockedTask == nil || lockedTask.Type != model.TaskTypeEvictCache {
		return nil, nil, nil, nil, fmt.Errorf("cache eviction task changed: %w", ErrConflict)
	}
	return bucket, upload, version, lockedTask, nil
}

func requireMinimumDurability(
	ctx context.Context,
	db bun.IDB,
	bucket *model.Bucket,
	upload *model.StorageUpload,
) error {
	if bucket == nil || upload == nil || upload.BucketID != bucket.ID {
		return fmt.Errorf("cache eviction durability context is invalid: %w", ErrInvalidInput)
	}
	if upload.Status != model.StorageUploadStatusReadable && upload.Status != model.StorageUploadStatusComplete {
		return cacheeviction.ErrDurabilityThreshold
	}
	minimum := minimumDurableCopiesForUpload(bucket, upload.RequestedCopies)
	readable, err := countReadableCommittedCopies(ctx, db, upload.ID)
	if err != nil {
		return err
	}
	if minimum <= 0 || readable < minimum {
		return cacheeviction.ErrDurabilityThreshold
	}
	return nil
}

func updateRunningEvictionTask(
	ctx context.Context,
	db bun.IDB,
	claimedTask *model.Task,
	refVersionID string,
	payload map[string]any,
) error {
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return err
	}
	now := time.Now()
	res, err := db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("ref_version_id = ?", refVersionID).
		Set("payload = ?", payload).
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("persisting cache deletion authorization: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return fmt.Errorf("persisting cache deletion authorization for task %d: not in active running claim", taskID)
	}
	return nil
}

func nextBucketDurabilityCandidate(
	ctx context.Context,
	db bun.IDB,
	bucketID int64,
) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	err := db.NewSelect().
		Model(version).
		Join("JOIN storage_uploads AS storage_upload ON storage_upload.id = object_version.storage_upload_id").
		Join("JOIN buckets AS durability_bucket ON durability_bucket.id = storage_upload.bucket_id").
		Where("object_version.bucket_id = ?", bucketID).
		Where("object_version.state = ?", model.ObjectStateReplicating).
		Where("object_version.in_cache = ?", true).
		Where("object_version.is_delete_marker = ?", false).
		Where("storage_upload.status IN (?)", bun.List([]model.StorageUploadStatus{
			model.StorageUploadStatusReadable,
			model.StorageUploadStatusComplete,
		})).
		Where(minimumDurabilityMetSQL("storage_upload", "durability_bucket")).
		OrderExpr("object_version.updated_at ASC").
		OrderExpr("object_version.version_id ASC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting bucket durability candidate: %w", err)
	}
	return version, nil
}

func minimumDurabilityMetSQL(uploadAlias, bucketAlias string) string {
	return fmt.Sprintf(`(
		SELECT COUNT(*)
		FROM storage_upload_copies AS durable_copy
		JOIN storage_data_sets AS durable_data_set ON durable_data_set.id = durable_copy.storage_data_set_id
		WHERE durable_copy.upload_id = %s.id
		  AND durable_copy.status = '%s'
		  AND durable_copy.storage_data_set_id IS NOT NULL
		  AND durable_copy.provider_id IS NOT NULL AND durable_copy.provider_id <> ''
		  AND durable_data_set.data_set_id IS NOT NULL AND durable_data_set.data_set_id <> ''
		  AND durable_data_set.status IN (%s)
		  AND durable_copy.piece_id IS NOT NULL AND durable_copy.piece_id <> ''
		  AND durable_copy.retrieval_url IS NOT NULL AND durable_copy.retrieval_url <> ''
	) >= CASE
		WHEN %s.minimum_durable_copies IS NULL
		  OR %s.minimum_durable_copies >= %s.requested_copies
		THEN %s.requested_copies
		ELSE %s.minimum_durable_copies
	END`,
		uploadAlias,
		model.StorageUploadCopyStatusCommitted,
		storageHealthReadyDataSetStatusListSQL(),
		bucketAlias,
		bucketAlias,
		uploadAlias,
		uploadAlias,
		bucketAlias,
	)
}

func cacheDeletionStateEligible(version *model.ObjectVersion) bool {
	if version == nil || version.IsDeleteMarker || !version.InCache || version.StorageUploadID == nil {
		return false
	}
	return version.State == model.ObjectStateStored || version.State == model.ObjectStateCacheEvicted
}

func cacheAccessTime(version *model.ObjectVersion) time.Time {
	if version == nil {
		return time.Time{}
	}
	if version.CacheAccessedAt != nil {
		return *version.CacheAccessedAt
	}
	return version.CreatedAt
}

func cacheDeletionAuthorizedSQL(dialectName dialect.Name) string {
	if dialectName == dialect.PG {
		return "COALESCE(CAST(payload ->> 'delete_authorized' AS BOOLEAN), FALSE)"
	}
	return "COALESCE(CAST(json_extract(payload, '$.delete_authorized') AS INTEGER), 0) = 1"
}

func (r *BunCacheEvictionRepo) runMaybeTx(ctx context.Context, fn func(bun.IDB) error) error {
	if db, ok := r.db.(*bun.DB); ok {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(tx)
		})
	}
	return fn(r.db)
}
