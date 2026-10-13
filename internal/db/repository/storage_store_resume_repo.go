package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/uptrace/bun"
)

type ResumeFailedStoreCopyInput struct {
	CopyID           int64
	Generation       int64
	SourceTaskID     int64
	TaskID           int64
	SourceCheckpoint json.RawMessage
	Checkpoint       json.RawMessage
}

// ResumeFailedStoreCopy requires the cache read gate and a checkpoint whose
// upload session has been validated by the transfer owner.
func (r *BunStorageContentRepo) ResumeFailedStoreCopy(ctx context.Context, input ResumeFailedStoreCopyInput) error {
	if input.CopyID < 1 || input.Generation < 1 || input.SourceTaskID < 1 || input.TaskID < 1 ||
		input.SourceTaskID == input.TaskID || len(input.SourceCheckpoint) == 0 || len(input.Checkpoint) == 0 {
		return ErrInvalidInput
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		repo := &BunStorageContentRepo{db: db}
		copyRow, err := repo.GetUploadCopyByID(ctx, input.CopyID)
		if err != nil {
			return err
		}
		if copyRow == nil {
			return ErrNotFound
		}
		if _, err := bucketCopyPolicyForReference(ctx, db, copyRow.BucketID); err != nil {
			return err
		}
		if err := lockStorageContentForCopyMutation(ctx, db, copyRow.ContentID); err != nil {
			return err
		}
		copyRow, err = repo.GetUploadCopyByID(ctx, input.CopyID)
		if err != nil {
			return err
		}
		if copyRow == nil || copyRow.WorkGeneration != input.Generation || copyRow.ActiveTaskID != nil ||
			copyRow.Status != model.StorageCopyStatusFailed || copyRow.CommitSealed() ||
			(copyRow.TransferMethod != model.StorageCopyTransferMethodIngress && copyRow.TransferMethod != model.StorageCopyTransferMethodCacheRestore) {
			return ErrConflict
		}
		tasks := &BunTaskRepo{db: db}
		source, err := tasks.GetByID(ctx, input.SourceTaskID)
		if err != nil {
			return err
		}
		next, err := tasks.GetByID(ctx, input.TaskID)
		if err != nil {
			return err
		}
		if source == nil || next == nil || source.Type != model.TaskTypeStorageStore || source.Status != model.TaskStatusFailed ||
			source.SubjectType != model.TaskSubjectStorageCopy || source.SubjectKey != strconv.FormatInt(input.CopyID, 10) ||
			next.RetryOfTaskID == nil || *next.RetryOfTaskID != source.ID ||
			next.Type != model.TaskTypeStorageStore || next.Status != model.TaskStatusPending || next.ResumeMode != model.TaskResumeModeRecover {
			return ErrConflict
		}
		matching, err := db.NewSelect().Model((*model.Task)(nil)).
			Where("(id = ? AND checkpoint_json = ?::jsonb) OR (id = ? AND checkpoint_json = ?::jsonb)",
				input.SourceTaskID, input.SourceCheckpoint, input.TaskID, input.Checkpoint).Count(ctx)
		if err != nil {
			return err
		}
		if matching != 2 {
			return ErrConflict
		}
		var sourceInput storagepipeline.CopyGenerationInput
		if err := json.Unmarshal(source.Input, &sourceInput); err != nil || sourceInput.CopyID != input.CopyID || sourceInput.Generation != input.Generation {
			return ErrConflict
		}
		if err := validateCopyTaskBinding(ctx, db, input.CopyID, input.Generation+1, input.TaskID); err != nil {
			return err
		}
		var latest int64
		if err := db.NewSelect().Model((*model.Task)(nil)).Column("id").
			Where("subject_type = ? AND subject_key = ? AND type IN (?) AND id <> ?", model.TaskSubjectStorageCopy, strconv.FormatInt(input.CopyID, 10),
				bun.List([]model.TaskType{model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull}), input.TaskID).
			OrderExpr("id DESC").Limit(1).Scan(ctx, &latest); err != nil {
			return err
		}
		if latest != input.SourceTaskID {
			return ErrConflict
		}
		states, err := repo.CopyRetryStates(ctx, []int64{input.CopyID})
		if err != nil {
			return err
		}
		if state := states[input.CopyID]; !state.Available {
			if state.Block != "" {
				return &CopyRetryBlockedError{Block: state.Block}
			}
			return ErrConflict
		}
		if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress {
			otherIngress, err := db.NewSelect().Model((*model.StorageCopy)(nil)).
				Where("content_id = ? AND id <> ? AND transfer_method = ? AND status <> ?", copyRow.ContentID, copyRow.ID,
					model.StorageCopyTransferMethodIngress, model.StorageCopyStatusFailed).Exists(ctx)
			if err != nil {
				return err
			}
			if otherIngress {
				return ErrConflict
			}
		}
		now := time.Now()
		result, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
			Set("status = ?", model.StorageCopyStatusPending).Set("work_generation = ?", input.Generation+1).
			Set("active_task_id = ?", input.TaskID).Set("last_error = NULL").Set("updated_at = ?", now).
			Where("id = ? AND work_generation = ? AND active_task_id IS NULL AND status = ? AND commit_position IS NULL AND transfer_method = ?",
				input.CopyID, input.Generation, model.StorageCopyStatusFailed, copyRow.TransferMethod).
			Exec(ctx)
		if err := requireRows(result, err, "resuming failed Store copy"); err != nil {
			return err
		}
		_, err = db.NewUpdate().Model((*model.StorageContent)(nil)).Set("error_message = NULL").Set("updated_at = ?", now).
			Where("id = ? AND accepted_at IS NULL", copyRow.ContentID).Exec(ctx)
		return err
	})
}
