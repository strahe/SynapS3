package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// CacheEvictionRepository owns persistence operations used only by cache
// eviction planning and policy reconciliation.
type CacheEvictionRepository interface {
	EnsureAfterUploadTask(ctx context.Context, objectID int64, versionID string, maxRetries int) (bool, error)
	ListLRUCandidates(ctx context.Context, terminalSince time.Time, limit int) ([]cacheeviction.Candidate, error)
	PlanLRU(ctx context.Context, candidate cacheeviction.Candidate, maxRetries int, terminalBefore time.Time) (bool, error)
	ActiveLRUBytes(ctx context.Context) (int64, error)
	CancelActiveTasksExcept(ctx context.Context, keepStage string, message string) (int, error)
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
		Where("object_version.in_cache = ?", true).
		Where("object_version.is_delete_marker = ?", false).
		Where("object_version.size > 0").
		Where("object_version.cache_accessed_at IS NOT NULL").
		Where("object_version.state IN (?)", bun.List([]model.ObjectState{
			model.ObjectStateStored,
			model.ObjectStateCacheEvicted,
		})).
		Where("storage_upload.status = ?", model.StorageUploadStatusComplete).
		Where(usableCopyExistsSQL("object_version.storage_upload_id")).
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
	tasks := &BunTaskRepo{db: r.db}
	if err := tasks.Create(ctx, task); err == nil {
		return true, nil
	} else if !errors.Is(err, ErrAlreadyExists) {
		return false, err
	}

	now := time.Now()
	q := r.reactivationQuery(task, now)
	if len(rule.cooledStatuses) == 0 {
		q = q.Where("status IN (?)", bun.List(rule.immediateStatuses))
	} else {
		if rule.terminalBefore == nil {
			return false, errors.New("reactivating task: terminal cutoff is required")
		}
		q = q.Where(
			`(
				status IN (?)
				OR (
					status IN (?)
					AND completed_at IS NOT NULL
					AND completed_at <= ?
				)
			)`,
			bun.List(rule.immediateStatuses),
			bun.List(rule.cooledStatuses),
			*rule.terminalBefore,
		)
	}
	res, err := q.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("%s %q: %w", rule.errorAction, task.IdempotencyKey, err)
	}
	rows, _ := res.RowsAffected()
	return rows > 0, nil
}

func (r *BunCacheEvictionRepo) reactivationQuery(task *model.Task, now time.Time) *bun.UpdateQuery {
	return r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("type = ?", task.Type).
		Set("stage = ?", task.Stage).
		Set("ref_type = ?", task.RefType).
		Set("ref_id = ?", task.RefID).
		Set("ref_version_id = ?", task.RefVersionID).
		Set("payload = ?", task.Payload).
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
		Where("idempotency_key = ?", task.IdempotencyKey)
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
		Where("status IN (?)", bun.List(activeTaskStatuses()))
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
