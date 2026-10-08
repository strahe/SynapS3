package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagereplacement"
)

func (r *BunStorageContentRepo) TransferCopyTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("active_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND work_generation = ? AND active_task_id = ?", id, generation, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring storage copy task")
}

func (r *BunStorageContentRepo) TransferEnsureTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("ensure_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND generation = ? AND ensure_task_id = ?", id, generation, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring data set setup task")
}

func (r *BunStorageContentRepo) TransferRetireTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("retirement_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND retirement_generation = ? AND retirement_task_id = ?", id, generation, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring data set retirement task")
}

func (r *BunStorageContentRepo) TransferCommitTaskOwner(ctx context.Context, id string, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*storagecommit.Request)(nil)).Set("task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("request_id = ? AND task_id = ?", id, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring registration task")
}

func (r *BunStorageCleanupRepo) TransferTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("cleanup_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND cleanup_generation = ? AND cleanup_task_id = ?", id, generation, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring cleanup task")
}

func (r *BunWalletOperationRepo) TransferTaskOwner(ctx context.Context, id, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*model.WalletOperation)(nil)).Set("task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND task_id = ?", id, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring wallet task")
}

func (r *BunStorageReplacementRepo) TransferTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND task_generation = ? AND task_id = ?", id, generation, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring replacement task")
}

func (r *BunCacheEvictionRepo) TransferEvictionTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*model.ObjectCache)(nil)).Set("cache_active_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("content_id = ? AND cache_operation_generation = ? AND cache_active_task_id = ?", id, generation, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring cache removal task")
}

func (r *BunCacheEvictionRepo) TransferDurabilityTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*model.Bucket)(nil)).Set("durability_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND durability_generation = ? AND durability_task_id = ?", id, generation, oldID).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring durability task")
}

func (r *BunProviderUploadSpeedRepo) TransferTaskOwner(ctx context.Context, oldID, newID int64) error {
	result, err := r.db.NewUpdate().Model((*providerbenchmark.Result)(nil)).Set("active_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("active_task_id = ? AND state = ?", oldID, providerbenchmark.StateTesting).Exec(ctx)
	return requireTaskFenceRows(result, err, "transferring speed test task")
}

func (r *BunStorageReplacementRepo) ResumeCoordinatorTask(ctx context.Context, id, generation, oldID, newID int64) error {
	row, err := r.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if row == nil || row.TaskID == nil || *row.TaskID != oldID || row.TaskGeneration != generation {
		return ErrConflict
	}
	target, err := (&BunStorageContentRepo{db: r.db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return err
	}
	if target == nil {
		return ErrConflict
	}
	next := storagereplacement.StatusPreparingTarget
	if target.IsCurrent && target.Status == model.StorageDataSetStatusReady {
		next = storagereplacement.StatusMigrating
	}
	result, err := r.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("task_id = ?", newID).Set("status = ?", next).Set("failure_reason = NULL").Set("last_error = NULL").Set("wait_reason = NULL").Set("updated_at = ?", time.Now()).Where("id = ? AND task_generation = ? AND task_id = ?", id, generation, oldID).Where("status NOT IN (?, ?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded, storagereplacement.StatusRetiring).Exec(ctx)
	if err := requireTaskFenceRows(result, err, "resuming replacement task"); err != nil {
		return err
	}
	_, err = r.db.NewUpdate().Model((*storagereplacement.Item)(nil)).Set("status = ?", storagereplacement.ItemStatusPending).Set("last_error = NULL").Set("updated_at = ?", time.Now()).Where("replacement_id = ? AND status = ?", id, storagereplacement.ItemStatusAttention).Exec(ctx)
	return err
}
