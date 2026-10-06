package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

type CopyRetryState struct {
	CopyID     int64
	Available  bool
	Block      storagepipeline.CopyRetryBlock
	NextMethod model.StorageCopyTransferMethod
}

type CopyRetryBlockedError struct {
	Block storagepipeline.CopyRetryBlock
}

func (e *CopyRetryBlockedError) Error() string {
	return "storage copy retry blocked: " + string(e.Block)
}

type copyRetryRow struct {
	CopyID                         int64
	storagepipeline.CopyRetryFacts `bun:"embed:"`
}

// CopyRetryStates batches the facts for display; callers must recheck admission.
func (r *BunStorageContentRepo) CopyRetryStates(ctx context.Context, copyIDs []int64) (map[int64]CopyRetryState, error) {
	states := make(map[int64]CopyRetryState, len(copyIDs))
	if len(copyIDs) == 0 {
		return states, nil
	}
	var rows []copyRetryRow
	query := `SELECT copy.id AS copy_id, copy.status, copy.active_task_id IS NOT NULL AS owned,
		copy.commit_position IS NOT NULL AS sealed, copy.transfer_method AS method,
		data_set.is_current AS current, data_set.status AS data_set_status,
		(content.cleanup_task_id IS NOT NULL OR NOT EXISTS
			(SELECT 1 FROM object_versions AS version WHERE version.content_id = copy.content_id)) AS object_deleted,
		EXISTS (SELECT 1 FROM storage_replacements AS replacement
			WHERE replacement.status NOT IN (?, ?) AND
			(replacement.source_data_set_id = copy.storage_data_set_id OR replacement.target_data_set_id = copy.storage_data_set_id))
		AS replacement_in_progress,
		EXISTS (SELECT 1 FROM storage_pull_attempts AS attempt
			WHERE attempt.content_id = copy.content_id AND attempt.storage_data_set_id = copy.storage_data_set_id
			AND attempt.resolved_at IS NULL) AS unresolved_pull,
		` + distinctReadableSlotCountSQL("source_copy", "source_data_set", "copy.content_id") + ` AS readable_sources,
		COALESCE(cache.in_cache AND cache.cache_active_task_id IS NULL, FALSE) AS cache_available,
		EXISTS (SELECT 1 FROM storage_copies AS ingress WHERE ingress.content_id = copy.content_id
			AND ingress.id <> copy.id AND ingress.transfer_method = 'ingress' AND ingress.status <> 'failed') AS other_ingress
		FROM storage_copies AS copy
		JOIN storage_contents AS content ON content.id = copy.content_id
		JOIN storage_data_sets AS data_set ON data_set.id = copy.storage_data_set_id
		LEFT JOIN object_cache AS cache ON cache.content_id = copy.content_id
		WHERE copy.id IN (?)`
	if err := r.db.NewRaw(query, storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded, bun.List(copyIDs)).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("loading copy retry facts: %w", err)
	}
	for _, row := range rows {
		next, block, available := storagepipeline.DecideCopyRetry(row.CopyRetryFacts)
		states[row.CopyID] = CopyRetryState{CopyID: row.CopyID, Available: available, Block: block, NextMethod: next}
	}
	return states, nil
}

// RetryFailedCopy requires the shared cache gate before entering its transaction.
// The bucket policy lock also serializes replacement authorization and activation.
func (r *BunStorageContentRepo) RetryFailedCopy(ctx context.Context, copyID int64) (*model.StorageCopy, error) {
	if copyID <= 0 {
		return nil, ErrInvalidInput
	}
	var reopened *model.StorageCopy
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		repo := &BunStorageContentRepo{db: db}
		copyRow, err := repo.GetUploadCopyByID(ctx, copyID)
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
			if errors.Is(err, ErrNotFound) {
				return &CopyRetryBlockedError{Block: storagepipeline.CopyRetryObjectDeleted}
			}
			return err
		}
		states, err := repo.CopyRetryStates(ctx, []int64{copyID})
		if err != nil {
			return err
		}
		state, found := states[copyID]
		if !found {
			return ErrNotFound
		}
		if !state.Available {
			if state.Block != "" {
				return &CopyRetryBlockedError{Block: state.Block}
			}
			return ErrConflict
		}
		// Change the method before reopening to preserve the unique live ingress slot.
		if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
			Set("transfer_method = ?", state.NextMethod).Set("ingress_bytes_transferred = 0").
			Set("ingress_store_attempt = 0").Set("progress_updated_at = NULL").
			Where("id = ?", copyID).Exec(ctx); err != nil {
			return err
		}
		if err := reopenFailedUploadCopy(ctx, db, copyID); err != nil {
			return err
		}
		if _, err := db.NewUpdate().Model((*model.StorageContent)(nil)).
			Set("error_message = NULL").Set("updated_at = ?", time.Now()).
			Where("id = ? AND accepted_at IS NULL", copyRow.ContentID).Exec(ctx); err != nil {
			return err
		}
		reopened, err = repo.GetUploadCopyByID(ctx, copyID)
		return err
	})
	return reopened, err
}

func (r *BunStorageContentRepo) GetLastAbandonedPullAttempt(ctx context.Context, contentID, storageDataSetID int64) (*storagepull.Attempt, error) {
	row := new(storagepull.Attempt)
	err := r.db.NewSelect().Model(row).Where("content_id = ? AND storage_data_set_id = ?", contentID, storageDataSetID).
		Where("status = ? AND resolved_at IS NOT NULL", storagepull.AttemptStatusAbandoned).
		OrderExpr("attempted_at DESC, attempt_id DESC").Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return row, err
}
