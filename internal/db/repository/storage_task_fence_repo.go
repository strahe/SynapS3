package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/uptrace/bun"
)

func (r *BunStorageContentRepo) GetLiveVersionForUpload(ctx context.Context, contentID int64) (*model.ObjectVersion, error) {
	upload, err := r.GetByID(ctx, contentID)
	if err != nil || upload == nil {
		return nil, err
	}
	return selectLiveObjectVersionForStorageContent(ctx, r.db, upload, nil)
}

func (r *BunStorageContentRepo) BindDataSetEnsureTask(ctx context.Context, dataSetID, taskID int64) error {
	return r.bindDataSetTask(ctx, dataSetID, dataSetEnsureFence, 0, taskID)
}

func (r *BunStorageContentRepo) AuthorizeDataSetEnsureTask(ctx context.Context, dataSetID, taskID int64) (*model.StorageDataSet, error) {
	return r.authorizeDataSetTask(ctx, dataSetID, dataSetEnsureFence, 0, taskID)
}

func (r *BunStorageContentRepo) CompleteDataSetEnsureTask(ctx context.Context, dataSetID, taskID int64) error {
	return r.completeDataSetTask(ctx, dataSetID, dataSetEnsureFence, 0, taskID)
}

func (r *BunStorageContentRepo) NextCopyWorkGeneration(ctx context.Context, copyID int64) (int64, error) {
	var generation int64
	err := r.db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		ColumnExpr("work_generation + 1").
		Where("id = ? AND active_task_id IS NULL", copyID).
		Scan(ctx, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrConflict
	}
	if err != nil {
		return 0, fmt.Errorf("reading copy work generation: %w", err)
	}
	return generation, nil
}

func (r *BunStorageContentRepo) BindCopyTask(ctx context.Context, copyID, generation, taskID int64) error {
	if copyID < 1 || generation < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	result, err := r.db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("work_generation = ?", generation).
		Set("active_task_id = ?", taskID).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND work_generation = ? AND active_task_id IS NULL", copyID, generation-1).
		Exec(ctx)
	return requireTaskFenceRows(result, err, "binding storage copy task")
}

func (r *BunStorageContentRepo) AuthorizeCopyTask(ctx context.Context, copyID, generation, taskID, claimGeneration int64) (*model.StorageCopy, error) {
	copyRow := new(model.StorageCopy)
	q := r.db.NewSelect().Model(copyRow)
	projectActiveCommitAttempt(q, "storage_copy")
	err := q.
		Join("JOIN tasks AS copy_task ON copy_task.id = storage_copy.active_task_id").
		Where("storage_copy.id = ? AND storage_copy.work_generation = ? AND storage_copy.active_task_id = ?", copyID, generation, taskID).
		Where("copy_task.status = ? AND copy_task.claim_generation = ? AND copy_task.lease_until > ?", model.TaskStatusRunning, claimGeneration, time.Now()).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, fmt.Errorf("authorizing storage copy task: %w", err)
	}
	return copyRow, nil
}

// ReservePullRequest records a provider-side copy request before it is sent.
// The attempt lives in its own ledger row, so a crash between writing it and
// sending it leaves a fully identified operation recovery can observe instead
// of nullable columns that may be half written.
func (r *BunStorageContentRepo) ReservePullRequest(ctx context.Context, input ReservePullRequestInput) error {
	if input.CopyID < 1 || input.Generation < 1 || input.TaskID < 1 ||
		input.AttemptID == "" || input.SourcePieceCID == "" ||
		input.SourceProviderID.IsZero() || input.SourceDataSetID.IsZero() || input.SourcePieceID.IsZero() ||
		input.SourceRetrievalURL == "" || input.CommitExtraDataHex == "" {
		return ErrInvalidInput
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		copyRow := new(model.StorageCopy)
		if err := db.NewSelect().
			Model(copyRow).
			Column("content_id", "storage_data_set_id").
			Where("id = ? AND work_generation = ? AND active_task_id = ?", input.CopyID, input.Generation, input.TaskID).
			Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("reserving storage pull request: %w", ErrConflict)
			}
			return fmt.Errorf("reserving storage pull request: %w", err)
		}
		// The extra data is presigned for this target and is reused across
		// recovery, so it stays on the copy where the commit path reads it.
		if _, err := db.NewUpdate().
			Model((*model.StorageCopy)(nil)).
			Set("commit_extra_data_hex = COALESCE(commit_extra_data_hex, ?)", input.CommitExtraDataHex).
			Set("updated_at = ?", now).
			Where("id = ? AND work_generation = ? AND active_task_id = ?", input.CopyID, input.Generation, input.TaskID).
			Where("commit_extra_data_hex IS NULL OR commit_extra_data_hex = ?", input.CommitExtraDataHex).
			Exec(ctx); err != nil {
			return fmt.Errorf("reserving storage pull commit evidence: %w", err)
		}

		existing := new(storagepull.Attempt)
		err := db.NewSelect().
			Model(existing).
			Where("content_id = ? AND storage_data_set_id = ?", copyRow.ContentID, copyRow.StorageDataSetID).
			Where("resolved_at IS NULL").
			Scan(ctx)
		switch {
		case err == nil:
			// Recovery re-reserving the same attempt is a no-op; a different one
			// would mean two live requests for one copy.
			if existing.AttemptID != input.AttemptID {
				return fmt.Errorf("reserving storage pull request: %w", ErrConflict)
			}
			return nil
		case err != sql.ErrNoRows:
			return fmt.Errorf("loading unresolved storage pull attempt: %w", err)
		}

		attempt := &storagepull.Attempt{
			AttemptID: input.AttemptID, ContentID: copyRow.ContentID,
			StorageDataSetID: copyRow.StorageDataSetID, Status: storagepull.AttemptStatusAttempted,
			SourceProviderID: input.SourceProviderID,
			SourceDataSetID:  input.SourceDataSetID, SourcePieceID: input.SourcePieceID,
			SourcePieceCID: input.SourcePieceCID, SourceRetrievalURL: input.SourceRetrievalURL,
			AttemptedAt: now, CreatedAt: now, UpdatedAt: now,
		}
		if _, err := db.NewInsert().Model(attempt).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("reserving storage pull request: %w", ErrConflict)
			}
			return fmt.Errorf("reserving storage pull request: %w", err)
		}
		return nil
	})
}

func (r *BunStorageContentRepo) ReplaceCopyTask(ctx context.Context, copyID, generation, taskID, nextGeneration, nextTaskID int64) error {
	if copyID < 1 || generation < 1 || taskID < 1 || nextGeneration != generation+1 || nextTaskID < 1 {
		return ErrInvalidInput
	}
	result, err := r.db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("work_generation = ?", nextGeneration).
		Set("active_task_id = ?", nextTaskID).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND work_generation = ? AND active_task_id = ?", copyID, generation, taskID).
		Exec(ctx)
	return requireTaskFenceRows(result, err, "advancing storage copy task")
}

func (r *BunStorageContentRepo) CompleteCopyTask(ctx context.Context, copyID, generation, taskID int64) error {
	result, err := r.db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("active_task_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND work_generation = ? AND active_task_id = ?", copyID, generation, taskID).
		Exec(ctx)
	return requireTaskFenceRows(result, err, "completing storage copy task")
}

func (r *BunStorageContentRepo) NextDataSetRetirementGeneration(ctx context.Context, dataSetID int64) (int64, error) {
	return r.nextDataSetTaskGeneration(ctx, dataSetID, dataSetRetirementFence)
}

func (r *BunStorageContentRepo) BindDataSetRetirementTask(ctx context.Context, dataSetID, generation, taskID int64) error {
	return r.bindDataSetTask(ctx, dataSetID, dataSetRetirementFence, generation, taskID)
}

func (r *BunStorageContentRepo) AuthorizeDataSetRetirementTask(ctx context.Context, dataSetID, generation, taskID int64) (*model.StorageDataSet, error) {
	return r.authorizeDataSetTask(ctx, dataSetID, dataSetRetirementFence, generation, taskID)
}

func (r *BunStorageContentRepo) CompleteDataSetRetirementTask(ctx context.Context, dataSetID, generation, taskID int64) error {
	return r.completeDataSetTask(ctx, dataSetID, dataSetRetirementFence, generation, taskID)
}

// dataSetFence names the column pair guarding one kind of data-set task. The
// three fences below are the only valid pairs, so passing a fence value instead
// of raw column names keeps an invalid pair unrepresentable and removes the
// string whitelists this file used to need before building SQL.
type dataSetFence struct {
	taskColumn string
	// generationColumn is empty for fences that carry no generation.
	generationColumn string
}

var (
	dataSetEnsureFence     = dataSetFence{taskColumn: "ensure_task_id"}
	dataSetRetirementFence = dataSetFence{taskColumn: "retirement_task_id", generationColumn: "retirement_generation"}
)

// usesGeneration reports whether this call should fence on a generation, and
// rejects a generation supplied for a fence that has no generation column.
func (f dataSetFence) usesGeneration(generation int64) (bool, error) {
	if generation <= 0 {
		return false, nil
	}
	if f.generationColumn == "" {
		return false, ErrInvalidInput
	}
	return true, nil
}

func (r *BunStorageContentRepo) nextDataSetTaskGeneration(ctx context.Context, dataSetID int64, fence dataSetFence) (int64, error) {
	if fence.generationColumn == "" {
		return 0, ErrInvalidInput
	}
	var generation int64
	err := r.db.NewSelect().
		Model((*model.StorageDataSet)(nil)).
		ColumnExpr(fence.generationColumn+" + 1").
		Where("id = ?", dataSetID).
		Where(fence.taskColumn+" IS NULL").
		Scan(ctx, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrConflict
	}
	if err != nil {
		return 0, fmt.Errorf("reading data set task generation: %w", err)
	}
	return generation, nil
}

func (r *BunStorageContentRepo) bindDataSetTask(ctx context.Context, dataSetID int64, fence dataSetFence, generation, taskID int64) error {
	if dataSetID < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	fenced, err := fence.usesGeneration(generation)
	if err != nil {
		return err
	}
	query := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set(fence.taskColumn+" = ?", taskID).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", dataSetID).
		Where(fence.taskColumn + " IS NULL")
	if fenced {
		query = query.Set(fence.generationColumn+" = ?", generation).
			Where(fence.generationColumn+" = ?", generation-1)
	}
	result, err := query.Exec(ctx)
	return requireTaskFenceRows(result, err, "binding data set task")
}

func (r *BunStorageContentRepo) authorizeDataSetTask(ctx context.Context, dataSetID int64, fence dataSetFence, generation, taskID int64) (*model.StorageDataSet, error) {
	fenced, err := fence.usesGeneration(generation)
	if err != nil {
		return nil, err
	}
	dataSet := new(model.StorageDataSet)
	query := r.db.NewSelect().Model(dataSet).Where("id = ?", dataSetID).Where(fence.taskColumn+" = ?", taskID)
	if fenced {
		query = query.Where(fence.generationColumn+" = ?", generation)
	}
	if err := query.Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("authorizing data set task: %w", err)
	}
	return dataSet, nil
}

func (r *BunStorageContentRepo) completeDataSetTask(ctx context.Context, dataSetID int64, fence dataSetFence, generation, taskID int64) error {
	fenced, err := fence.usesGeneration(generation)
	if err != nil {
		return err
	}
	query := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set(fence.taskColumn+" = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ?", dataSetID).
		Where(fence.taskColumn+" = ?", taskID)
	if fenced {
		query = query.Where(fence.generationColumn+" = ?", generation)
	}
	result, err := query.Exec(ctx)
	return requireTaskFenceRows(result, err, "completing data set task")
}

func requireTaskFenceRows(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrConflict
	}
	return nil
}
