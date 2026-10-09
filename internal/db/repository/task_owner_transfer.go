package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/walletoperation"
	"github.com/uptrace/bun"
)

func (r *BunStorageContentRepo) TransferCopyTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		if err := validateCopyTaskBinding(ctx, db, id, generation, newID); err != nil {
			return err
		}
		result, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("active_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND work_generation = ? AND active_task_id = ?", id, generation, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring storage copy task")
	})
}

func (r *BunStorageContentRepo) TransferEnsureTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		if err := validateDataSetTaskBinding(ctx, db, id, 0, newID, model.TaskTypeStorageDataSetEnsure); err != nil {
			return err
		}
		result, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("ensure_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND generation = ? AND ensure_task_id = ?", id, generation, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring data set setup task")
	})
}

func (r *BunStorageContentRepo) TransferRetireTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		if err := validateDataSetTaskBinding(ctx, db, id, generation, newID, model.TaskTypeStorageDataSetRetire); err != nil {
			return err
		}
		result, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("retirement_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND retirement_generation = ? AND retirement_task_id = ?", id, generation, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring data set retirement task")
	})
}

func (r *BunStorageContentRepo) TransferCommitTaskOwner(ctx context.Context, id string, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newID, model.TaskSubjectStorageCommitRequest, id, model.TaskTypeStorageCommit)
		if err != nil {
			return err
		}
		var input storagepipeline.CommitRequestInput
		if err := json.Unmarshal(task.Input, &input); err != nil || input.RequestID != id {
			return ErrConflict
		}
		result, err := db.NewUpdate().Model((*storagecommit.Request)(nil)).Set("task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("request_id = ? AND task_id = ?", id, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring registration task")
	})
}

func (r *BunStorageCleanupRepo) TransferTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newID, model.TaskSubjectStorageContent, strconv.FormatInt(id, 10), model.TaskTypeStorageCleanup)
		if err != nil {
			return err
		}
		var input storagecleanup.Input
		if err := json.Unmarshal(task.Input, &input); err != nil || input.ContentID != id || input.Generation != generation {
			return ErrConflict
		}
		result, err := db.NewUpdate().Model((*model.StorageContent)(nil)).Set("cleanup_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND cleanup_generation = ? AND cleanup_task_id = ?", id, generation, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring cleanup task")
	})
}

func (r *BunWalletOperationRepo) TransferTaskOwner(ctx context.Context, id, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newID, "wallet_operation", strconv.FormatInt(id, 10), model.TaskTypeWalletOperation)
		if err != nil {
			return err
		}
		var input walletoperation.Input
		if err := json.Unmarshal(task.Input, &input); err != nil || input.OperationID != id {
			return ErrConflict
		}
		result, err := db.NewUpdate().Model((*model.WalletOperation)(nil)).Set("task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND task_id = ?", id, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring wallet task")
	})
}

func (r *BunStorageReplacementRepo) TransferTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newID, "storage_replacement", strconv.FormatInt(id, 10), model.TaskTypeProviderReplacementCoordinate)
		if err != nil {
			return err
		}
		input, err := storagereplacement.ParseCoordinateInput(task)
		if err != nil || input.ReplacementID != id || input.Generation != generation {
			return ErrConflict
		}
		result, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND task_generation = ? AND task_id = ?", id, generation, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring replacement task")
	})
}

func (r *BunCacheEvictionRepo) TransferEvictionTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newID, model.TaskSubjectStorageContent, strconv.FormatInt(id, 10), model.TaskTypeCacheEvict)
		if err != nil {
			return err
		}
		input, err := cacheeviction.ParseEvictInput(task)
		if err != nil || input.ContentID != id || input.Generation != generation {
			return ErrConflict
		}
		result, err := db.NewUpdate().Model((*model.ObjectCache)(nil)).Set("cache_active_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("content_id = ? AND cache_operation_generation = ? AND cache_active_task_id = ?", id, generation, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring cache removal task")
	})
}

func (r *BunCacheEvictionRepo) TransferDurabilityTaskOwner(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newID, "bucket", strconv.FormatInt(id, 10), model.TaskTypeCacheReconcileDurability)
		if err != nil {
			return err
		}
		var input cacheeviction.DurabilityInput
		if err := json.Unmarshal(task.Input, &input); err != nil || input.BucketID != id || input.Generation != generation {
			return ErrConflict
		}
		result, err := db.NewUpdate().Model((*model.Bucket)(nil)).Set("durability_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("id = ? AND durability_generation = ? AND durability_task_id = ?", id, generation, oldID).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring durability task")
	})
}

func (r *BunProviderUploadSpeedRepo) TransferTaskOwner(ctx context.Context, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := (&BunTaskRepo{db: db}).GetForUpdate(ctx, newID)
		if err != nil {
			return err
		}
		if task == nil {
			return ErrConflict
		}
		var input providerbenchmark.Input
		if err := json.Unmarshal(task.Input, &input); err != nil || input.ProviderID == "" {
			return ErrConflict
		}
		if _, err := taskForBinding(ctx, db, newID, "provider", input.ProviderID, model.TaskTypeProviderUploadSpeedTest); err != nil {
			return err
		}
		result, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).Set("active_task_id = ?", newID).Set("updated_at = ?", time.Now()).Where("provider_id = ? AND active_task_id = ? AND state = ?", input.ProviderID, oldID, providerbenchmark.StateTesting).Exec(ctx)
		return requireTaskFenceRows(result, err, "transferring speed test task")
	})
}

func (r *BunStorageReplacementRepo) ResumeCoordinatorTask(ctx context.Context, id, generation, oldID, newID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if err := validateTransferredTask(ctx, db, oldID, newID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newID, "storage_replacement", strconv.FormatInt(id, 10), model.TaskTypeProviderReplacementCoordinate)
		if err != nil {
			return err
		}
		input, err := storagereplacement.ParseCoordinateInput(task)
		if err != nil || input.ReplacementID != id || input.Generation != generation {
			return ErrConflict
		}
		local := &BunStorageReplacementRepo{db: db}
		row, err := local.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if row == nil || row.TaskID == nil || *row.TaskID != oldID || row.TaskGeneration != generation {
			return ErrConflict
		}
		target, err := (&BunStorageContentRepo{db: db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
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
		result, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("task_id = ?", newID).Set("status = ?", next).Set("failure_reason = NULL").Set("last_error = NULL").Set("wait_reason = NULL").Set("updated_at = ?", time.Now()).Where("id = ? AND task_generation = ? AND task_id = ?", id, generation, oldID).Where("status NOT IN (?, ?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded, storagereplacement.StatusRetiring).Exec(ctx)
		if err := requireTaskFenceRows(result, err, "resuming replacement task"); err != nil {
			return err
		}
		_, err = db.NewUpdate().Model((*storagereplacement.Item)(nil)).Set("status = ?", storagereplacement.ItemStatusPending).Set("last_error = NULL").Set("updated_at = ?", time.Now()).Where("replacement_id = ? AND status = ?", id, storagereplacement.ItemStatusAttention).Exec(ctx)
		return err
	})
}
