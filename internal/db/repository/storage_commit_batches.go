package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type CommitBatchFilter struct {
	Status          storagecommit.RequestStatus
	Limit           int
	BeforeCreatedAt time.Time
	BeforeRequestID string
}

type CommitBatch struct {
	storagecommit.Request
	BucketName    string
	ProviderID    idtypes.OnChainID
	ProviderName  *string
	DataSetID     *idtypes.OnChainID
	MemberCount   int
	TotalBytes    *int64
	OldestReadyAt *time.Time
	TaskStatus    *model.TaskStatus
	TaskMessage   *string
	TaskError     *string
}

func commitBatchQuery(db bun.IDB, dest any) *bun.SelectQuery {
	return db.NewSelect().Model(dest).ModelTableExpr("storage_commit_requests AS commit_request").
		ColumnExpr("commit_request.*").
		ColumnExpr("batch_bucket.name AS bucket_name").
		ColumnExpr("batch_data_set.provider_id").ColumnExpr("batch_data_set.data_set_id").
		ColumnExpr("batch_provider.name AS provider_name").
		ColumnExpr("batch_task.status AS task_status").
		ColumnExpr("batch_task.status_message AS task_message").ColumnExpr("batch_task.last_error AS task_error").
		ColumnExpr(`CASE WHEN commit_request.status = 'collecting' THEN
			(SELECT COUNT(*) FROM storage_copies AS member WHERE member.commit_request_id = commit_request.request_id)
			ELSE commit_request.piece_count END AS member_count`).
		ColumnExpr(`CASE WHEN commit_request.status = 'collecting' THEN
			(SELECT COALESCE(SUM(member.content_size), 0) FROM storage_copies AS member WHERE member.commit_request_id = commit_request.request_id)
			ELSE (SELECT CASE WHEN COUNT(content.id) = COUNT(*) THEN SUM(content.content_size) ELSE NULL END
				FROM storage_commit_request_pieces AS piece LEFT JOIN storage_contents AS content ON content.id = piece.content_id
				WHERE piece.request_id = commit_request.request_id) END AS total_bytes`).
		ColumnExpr(`(SELECT MIN(member.commit_ready_at) FROM storage_copies AS member
			WHERE member.commit_request_id = commit_request.request_id) AS oldest_ready_at`).
		Join("JOIN storage_data_sets AS batch_data_set ON batch_data_set.id = commit_request.storage_data_set_id").
		Join("JOIN buckets AS batch_bucket ON batch_bucket.id = batch_data_set.bucket_id").
		Join("LEFT JOIN provider_profiles AS batch_provider ON batch_provider.provider_id = batch_data_set.provider_id").
		Join("LEFT JOIN tasks AS batch_task ON batch_task.id = commit_request.task_id")
}

func (r *BunStorageContentRepo) ListCommitBatches(ctx context.Context, filter CommitBatchFilter) ([]CommitBatch, error) {
	if filter.Limit < 1 || filter.Limit > 101 || (filter.Status != "" && !filter.Status.Valid()) {
		return nil, ErrInvalidInput
	}
	rows := make([]CommitBatch, 0)
	query := commitBatchQuery(r.db, &rows).OrderExpr("commit_request.created_at DESC, commit_request.request_id DESC").Limit(filter.Limit)
	if filter.Status != "" {
		query.Where("commit_request.status = ?", filter.Status)
	}
	if filter.BeforeRequestID != "" {
		query.Where("(commit_request.created_at < ? OR (commit_request.created_at = ? AND commit_request.request_id < ?))",
			filter.BeforeCreatedAt, filter.BeforeCreatedAt, filter.BeforeRequestID)
	}
	if err := query.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing registration batches: %w", err)
	}
	return rows, nil
}

func (r *BunStorageContentRepo) GetCommitBatch(ctx context.Context, requestID string) (*CommitBatch, error) {
	row := new(CommitBatch)
	if err := commitBatchQuery(r.db, row).Where("commit_request.request_id = ?", requestID).Scan(ctx); err != nil {
		return nil, mapNotFound(err)
	}
	return row, nil
}
