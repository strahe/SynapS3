package repository

import (
	"context"

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
			return &storagereplacement.NotRetryableError{Message: storagereplacement.ProviderRejectedMessage}
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
	if len(target.CreationRejection) != 0 {
		return &storagereplacement.NotRetryableError{Message: storagereplacement.ProviderRejectedMessage}
	}
	if target.EnsureTaskID != nil && target.Status != model.StorageDataSetStatusReady {
		task, err := (&BunTaskRepo{db: db}).GetByID(ctx, *target.EnsureTaskID)
		if err != nil {
			return err
		}
		if task != nil && (task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusCancelled) {
			return &storagereplacement.NotRetryableError{Message: "Storage setup stopped. Check its task before retrying the replacement."}
		}
	}
	return nil
}
