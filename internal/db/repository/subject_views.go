package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

// ContentFileSample identifies one version of shared bytes without loading its history.
type ContentFileSample struct {
	BucketID      int64
	Key           string
	Size          int64
	Source        string
	OtherVersions int64
}

// StorageSubject contains the identity fields needed to describe storage work.
type StorageSubject struct {
	BucketID       int64
	ContentID      int64
	Size           *int64
	CopyIndex      *int
	LocalDataSetID *int64
	DataSetID      *types.OnChainID
	ProviderID     *types.OnChainID
	ContentCount   *int64
}

type ReplacementSubject struct {
	BucketID         int64
	CopyIndex        int
	SourceProviderID types.OnChainID
	TargetProviderID types.OnChainID
}

type ProviderSubject struct {
	ProviderID types.OnChainID
	Name       string
	ServiceURL string
}

func scanSubject(ctx context.Context, query *bun.SelectQuery, dest any) error {
	if err := query.Scan(ctx, dest); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else {
		return err
	}
}

func (r *BunObjectRepo) GetContentFileSample(ctx context.Context, contentID int64) (*ContentFileSample, error) {
	sample := new(ContentFileSample)
	err := scanSubject(ctx, r.db.NewSelect().TableExpr("object_versions AS version").
		ColumnExpr("version.bucket_id, version.key, version.size").
		ColumnExpr("CASE WHEN object.current_version_id = version.version_id THEN 'current' ELSE 'historical' END AS source").
		ColumnExpr("COUNT(*) OVER () - 1 AS other_versions").
		Join("LEFT JOIN objects AS object ON object.id = version.object_id").
		Where("version.content_id = ?", contentID).
		OrderExpr("CASE WHEN object.current_version_id = version.version_id THEN 0 ELSE 1 END").
		OrderExpr("version.created_at DESC, version.version_id DESC").Limit(1), sample)
	if !errors.Is(err, ErrNotFound) {
		return sample, err
	}
	err = scanSubject(ctx, r.db.NewSelect().Table("object_deletions").
		Column("bucket_id", "key", "size").ColumnExpr("'deleted' AS source").
		ColumnExpr("(SELECT COUNT(*) - 1 FROM object_deletions WHERE content_id = ?) AS other_versions", contentID).
		Where("content_id = ?", contentID).OrderExpr("deleted_at DESC, id DESC").Limit(1), sample)
	return sample, err
}

func (r *BunStorageContentRepo) GetContentSubject(ctx context.Context, id int64) (*StorageSubject, error) {
	row := new(StorageSubject)
	err := scanSubject(ctx, r.db.NewSelect().Table("storage_contents").
		Column("bucket_id").ColumnExpr("id AS content_id, content_size AS size").Where("id = ?", id), row)
	return row, err
}

func (r *BunStorageContentRepo) GetCopySubject(ctx context.Context, id int64) (*StorageSubject, error) {
	row := new(StorageSubject)
	err := scanSubject(ctx, r.db.NewSelect().Table("storage_copies").
		Column("content_id", "bucket_id", "copy_index", "provider_id").
		ColumnExpr("content_size AS size, storage_data_set_id AS local_data_set_id").Where("id = ?", id), row)
	return row, err
}

func (r *BunStorageContentRepo) GetDataSetSubject(ctx context.Context, id int64) (*StorageSubject, error) {
	row := new(StorageSubject)
	err := scanSubject(ctx, r.db.NewSelect().Table("storage_data_sets").
		Column("bucket_id", "copy_index", "provider_id", "data_set_id").
		ColumnExpr("id AS local_data_set_id").Where("id = ?", id), row)
	return row, err
}

func (r *BunStorageContentRepo) GetCommitSubject(ctx context.Context, requestID string) (*StorageSubject, error) {
	row := new(StorageSubject)
	err := scanSubject(ctx, r.db.NewSelect().TableExpr("storage_commit_requests AS request").
		ColumnExpr("request.storage_data_set_id AS local_data_set_id").
		ColumnExpr("data_set.bucket_id, data_set.copy_index, data_set.provider_id, data_set.data_set_id").
		ColumnExpr(`CASE WHEN request.sealed_at IS NOT NULL THEN request.piece_count
			ELSE (SELECT COUNT(*) FROM storage_copies WHERE commit_request_id = request.request_id) END AS content_count`).
		Join("LEFT JOIN storage_data_sets AS data_set ON data_set.id = request.storage_data_set_id").
		Where("request.request_id = ?", requestID), row)
	return row, err
}

func (r *BunStorageReplacementRepo) GetReplacementSubject(ctx context.Context, id int64) (*ReplacementSubject, error) {
	row := new(ReplacementSubject)
	err := scanSubject(ctx, r.db.NewSelect().TableExpr("storage_replacements AS replacement").
		ColumnExpr("replacement.bucket_id, replacement.copy_index").
		ColumnExpr("source.provider_id AS source_provider_id, target.provider_id AS target_provider_id").
		Join("JOIN storage_data_sets AS source ON source.id = replacement.source_data_set_id").
		Join("JOIN storage_data_sets AS target ON target.id = replacement.target_data_set_id").
		Where("replacement.id = ?", id), row)
	return row, err
}

func (r *BunObservabilityRepo) GetProviderSubject(ctx context.Context, id types.OnChainID) (*ProviderSubject, error) {
	row := new(ProviderSubject)
	err := scanSubject(ctx, r.db.NewSelect().Table("provider_profiles").
		Column("provider_id", "name", "service_url").Where("provider_id = ?", id), row)
	return row, err
}

func (r *BunWalletOperationRepo) GetOperationSubject(ctx context.Context, id int64) (*model.WalletOperation, error) {
	row := new(model.WalletOperation)
	err := scanSubject(ctx, r.db.NewSelect().Model(row).Column("id", "type", "amount").Where("id = ?", id), row)
	return row, err
}
