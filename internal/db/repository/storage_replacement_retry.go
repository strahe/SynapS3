package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func (r *BunStorageReplacementRepo) RetryEligibility(ctx context.Context, replacementID int64) error {
	row, err := r.GetByID(ctx, replacementID)
	if err != nil {
		return err
	}
	if row == nil {
		return ErrNotFound
	}
	return replacementRetryEligibility(ctx, r.db, row)
}

func replacementRetryEligibility(ctx context.Context, db bun.IDB, row *storagereplacement.Replacement) error {
	if row.Status == storagereplacement.StatusSuperseded {
		return storagereplacement.ErrSuperseded
	}
	if row.FailureReason != nil {
		if !row.FailureReason.Valid() {
			return storagereplacement.ErrNotRetryable
		}
		//exhaustive:enforce
		switch *row.FailureReason {
		case storagereplacement.FailureReasonTargetInUse:
			return storagereplacement.ErrTargetInUse
		case storagereplacement.FailureReasonTargetRejected:
		default:
			return storagereplacement.ErrNotRetryable
		}
	}
	if !row.Status.Retryable() {
		return storagereplacement.ErrNotRetryable
	}
	target, err := (&BunStorageContentRepo{db: db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return err
	}
	if target == nil || target.Status == model.StorageDataSetStatusRetired || target.Status == model.StorageDataSetStatusFailed {
		return &storagereplacement.NotRetryableError{Message: "Storage setup has ended. Choose another provider for the original replica."}
	}
	source, err := (&BunStorageContentRepo{db: db}).GetDataSetBindingByID(ctx, row.SourceDataSetID)
	if err != nil {
		return err
	}
	if source == nil || (!source.IsCurrent && !target.IsCurrent) {
		return &storagereplacement.NotRetryableError{Message: "This replacement no longer has a current replica. Check its storage setup."}
	}
	if target.EnsureTaskID != nil && target.Status != model.StorageDataSetStatusReady {
		task, err := (&BunTaskRepo{db: db}).GetByID(ctx, *target.EnsureTaskID)
		if err != nil {
			return err
		}
		if task == nil || task.Status == model.TaskStatusCancelled || task.SupersededAt != nil {
			return &storagereplacement.NotRetryableError{Message: "Storage setup stopped. Check its task before retrying the replacement."}
		}
	}
	return nil
}

func (r *BunStorageReplacementRepo) EnsureTaskReplacement(ctx context.Context, taskID int64) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := r.db.NewSelect().Model(row).
		Join("JOIN storage_data_sets AS target ON target.id = storage_replacement.target_data_set_id").
		Join("JOIN storage_data_sets AS source ON source.id = storage_replacement.source_data_set_id").
		Where("source.is_current = ?", true).
		Where("target.ensure_task_id = ? AND target.status <> ?", taskID, model.StorageDataSetStatusReady).
		Where("storage_replacement.status NOT IN (?, ?) AND storage_replacement.task_id IS NOT NULL", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return row, err
}

func (r *BunStorageReplacementRepo) PrepareCoordinatorRetry(ctx context.Context, id, generation, taskID int64) (*model.StorageDataSet, error) {
	row, err := r.GetByID(ctx, id)
	if err != nil || row == nil {
		return nil, errors.Join(err, ErrNotFound)
	}
	if _, err := lockBucketByID(ctx, r.db, row.BucketID); err != nil {
		return nil, err
	}
	row, err = lockReplacementByID(ctx, r.db, id)
	if err != nil {
		return nil, err
	}
	if row.TaskID == nil || *row.TaskID != taskID || row.TaskGeneration != generation {
		return nil, ErrConflict
	}
	if err := lockReplacementDataSets(ctx, r.db, row.SourceDataSetID, row.TargetDataSetID); err != nil {
		return nil, err
	}
	if err := replacementRetryEligibility(ctx, r.db, row); err != nil {
		return nil, err
	}
	target, err := (&BunStorageContentRepo{db: r.db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return nil, err
	}
	if target.EnsureTaskID != nil && target.Status != model.StorageDataSetStatusReady {
		tasks := &BunTaskRepo{db: r.db}
		child, err := tasks.GetByID(ctx, *target.EnsureTaskID)
		if err != nil {
			return nil, err
		}
		if child == nil || child.SupersededAt != nil || child.Status == model.TaskStatusCancelled {
			return nil, ErrConflict
		}
		if child.Status == model.TaskStatusFailed {
			child, err = tasks.GetForUpdate(ctx, child.ID)
			if err != nil {
				return nil, err
			}
			if child == nil || child.Status != model.TaskStatusFailed || child.SupersededAt != nil {
				return nil, ErrConflict
			}
		}
	}
	return target, nil
}

func (r *BunStorageReplacementRepo) MarkInitializationFailed(ctx context.Context, id, generation, taskID, childID int64, message string) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		local := &BunStorageReplacementRepo{db: db}
		row, err := local.GetByID(ctx, id)
		if err != nil || row == nil {
			return errors.Join(err, ErrNotFound)
		}
		if _, err := lockBucketByID(ctx, db, row.BucketID); err != nil {
			return err
		}
		row, err = lockReplacementByID(ctx, db, id)
		if err != nil {
			return err
		}
		if row.TaskID == nil || *row.TaskID != taskID || row.TaskGeneration != generation {
			return ErrConflict
		}
		if err := lockReplacementDataSets(ctx, db, row.SourceDataSetID, row.TargetDataSetID); err != nil {
			return err
		}
		contents := &BunStorageContentRepo{db: db}
		source, err := contents.GetDataSetBindingByID(ctx, row.SourceDataSetID)
		if err != nil {
			return err
		}
		target, err := contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
		if err != nil {
			return err
		}
		if source == nil || !source.IsCurrent ||
			target == nil || target.Status == model.StorageDataSetStatusReady || target.EnsureTaskID == nil || *target.EnsureTaskID != childID {
			return ErrConflict
		}
		tasks := &BunTaskRepo{db: db}
		child, err := tasks.GetByID(ctx, childID)
		if err != nil {
			return err
		}
		if child == nil || child.SupersededAt != nil || (child.Status != model.TaskStatusFailed && child.Status != model.TaskStatusCancelled) {
			return ErrConflict
		}
		child, err = tasks.GetForUpdate(ctx, childID)
		if err != nil {
			return err
		}
		if child == nil || child.SupersededAt != nil || (child.Status != model.TaskStatusFailed && child.Status != model.TaskStatusCancelled) {
			return ErrConflict
		}
		return local.MarkFailed(ctx, id, nil, message)
	})
}
