package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func cacheDependentCommitMembers(db bun.IDB) *bun.SelectQuery {
	return db.NewSelect().TableExpr("storage_copies AS commit_member").
		Join("JOIN storage_contents AS storage_content ON storage_content.id = commit_member.content_id").
		Join("JOIN object_cache AS object_cache ON object_cache.content_id = storage_content.id").
		Join("JOIN buckets AS durability_bucket ON durability_bucket.id = storage_content.bucket_id").
		Where("object_cache.in_cache = ?", true).
		Where("commit_member.status = ? AND commit_member.commit_position IS NULL", model.StorageCopyStatusPieceReady).
		Where("(NOT ("+minimumDurabilityMetSQL("storage_content", "durability_bucket")+") OR NOT ("+
			noUnfinishedStoreCacheDependencySQL("storage_content.id")+") OR NOT ("+
			noUnfinishedReplacementCacheDependencySQL("storage_content.id")+"))",
			storagereplacement.ItemStatusPending, storagereplacement.ItemStatusAttention,
			storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded)
}

func (r *BunStorageContentRepo) HasCacheDependentCommitMembers(ctx context.Context, requestID string) (bool, error) {
	exists, err := cacheDependentCommitMembers(r.db).Where("commit_member.commit_request_id = ?", requestID).Exists(ctx)
	if err != nil {
		return false, fmt.Errorf("checking cached commit members: %w", err)
	}
	return exists, nil
}

func (r *BunStorageContentRepo) WakeCacheDependentCommitTasks(ctx context.Context) error {
	var taskIDs []int64
	err := cacheDependentCommitMembers(r.db).
		Join("JOIN storage_commit_requests AS commit_request ON commit_request.request_id = commit_member.commit_request_id").
		Where("commit_request.status = ?", storagecommit.RequestStatusCollecting).
		Column("commit_request.task_id").Distinct().Scan(ctx, &taskIDs)
	if err != nil {
		return fmt.Errorf("listing cache-dependent commit tasks: %w", err)
	}
	return wakeCommitTasks(ctx, r.db, taskIDs)
}

// RequestCommitSeal records collection intent; the existing task owns signing.
func (r *BunStorageContentRepo) RequestCommitSeal(ctx context.Context, requestID string) (*storagecommit.Request, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, ErrInvalidInput
	}
	var request *storagecommit.Request
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		var dataSetID int64
		err := db.NewSelect().Model((*storagecommit.Request)(nil)).Column("storage_data_set_id").Where("request_id = ?", requestID).Scan(ctx, &dataSetID)
		if err != nil {
			return mapNotFound(err)
		}
		if _, err := lockCommitDataSet(ctx, db, dataSetID); err != nil {
			return err
		}
		request = new(storagecommit.Request)
		if err := db.NewSelect().Model(request).Where("request_id = ?", requestID).Scan(ctx); err != nil {
			return mapNotFound(err)
		}
		switch request.Status {
		case storagecommit.RequestStatusReady, storagecommit.RequestStatusSubmitted, storagecommit.RequestStatusConfirmed:
			return nil
		case storagecommit.RequestStatusCollecting:
		default:
			return ErrConflict
		}
		request, err = lockCommitRequest(ctx, db, requestID, storagecommit.RequestStatusCollecting)
		if err != nil {
			return err
		}
		if request.TaskID == nil {
			return ErrConflict
		}
		if request.SealRequestedAt != nil {
			return wakeCommitTasks(ctx, db, []int64{*request.TaskID})
		}
		var task model.Task
		if err := db.NewSelect().Model(&task).Column("status").Where("id = ?", *request.TaskID).Scan(ctx); err != nil {
			return err
		}
		if task.Status != model.TaskStatusPending && task.Status != model.TaskStatusRunning {
			return ErrConflict
		}
		count, err := countCommitMembers(ctx, db, requestID)
		if err != nil {
			return err
		}
		if count == 0 {
			return ErrConflict
		}
		now := time.Now().UTC()
		res, err := db.NewUpdate().Model((*storagecommit.Request)(nil)).
			Set("seal_requested_at = ?", now).Set("updated_at = ?", now).
			Where("request_id = ? AND status = ?", requestID, storagecommit.RequestStatusCollecting).Exec(ctx)
		if err := requireCommitRows(res, err, "requesting collection to end"); err != nil {
			return err
		}
		request.SealRequestedAt = &now
		return wakeCommitTasks(ctx, db, []int64{*request.TaskID})
	})
	if err != nil {
		return nil, fmt.Errorf("requesting commit seal: %w", err)
	}
	return request, nil
}

func mapNotFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
