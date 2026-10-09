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

type CommitBatchMember struct {
	ContentID int64
	PieceCID  string
	Size      *int64
}

func commitBatchQuery(db bun.IDB, dest any) *bun.SelectQuery {
	return db.NewSelect().Model(dest).ModelTableExpr("storage_commit_requests AS commit_request").
		ColumnExpr("commit_request.*").
		ColumnExpr("batch_bucket.name AS bucket_name").
		ColumnExpr("batch_data_set.provider_id").ColumnExpr("batch_data_set.data_set_id").
		ColumnExpr("batch_provider.name AS provider_name").
		ColumnExpr("COALESCE(batch_task.status, history_task.status) AS task_status").
		ColumnExpr("COALESCE(batch_task.status_message, history_task.status_message) AS task_message").
		ColumnExpr("COALESCE(batch_task.last_error, history_task.last_error) AS task_error").
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
		Join("LEFT JOIN tasks AS batch_task ON batch_task.id = commit_request.task_id").
		Join("LEFT JOIN task_history AS history_task ON history_task.task_id = commit_request.task_id")
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

func (r *BunStorageContentRepo) ListCommitBatchMembers(ctx context.Context, requestID string) ([]CommitBatchMember, error) {
	rows := make([]CommitBatchMember, 0)
	// Signed identities outlive the copies and content rows they describe.
	err := r.db.NewRaw(`SELECT member.content_id, member.piece_cid, member.size FROM (
		SELECT copy.content_id, COALESCE(content.piece_cid, '') AS piece_cid, content.content_size AS size,
			copy.commit_ready_at AS ready_at, copy.id AS position
		FROM storage_copies AS copy
		JOIN storage_contents AS content ON content.id = copy.content_id
		JOIN storage_commit_requests AS request ON request.request_id = copy.commit_request_id
		WHERE request.request_id = ? AND request.status = 'collecting'
		UNION ALL
		SELECT piece.content_id, piece.piece_cid, content.content_size AS size,
			NULL AS ready_at, piece.position
		FROM storage_commit_request_pieces AS piece
		LEFT JOIN storage_contents AS content ON content.id = piece.content_id
		JOIN storage_commit_requests AS request ON request.request_id = piece.request_id
		WHERE request.request_id = ? AND request.status <> 'collecting'
	) AS member ORDER BY member.ready_at, member.position`, requestID, requestID).Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("listing batch members: %w", err)
	}
	return rows, nil
}
