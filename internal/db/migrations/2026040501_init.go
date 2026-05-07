package migrations

import (
	"context"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up2026040501Init, down2026040501Init)
}

// up2026040501Init is the squashed development baseline schema.
// Follow-up migrations should only be added after the first business release.
func up2026040501Init(ctx context.Context, db *bun.DB) error {
	// IAM and bucket ownership tables come first because buckets reference S3 accounts.
	if _, err := db.NewCreateTable().
		Model((*model.S3Account)(nil)).
		IfNotExists().
		ColumnExpr("CONSTRAINT chk_s3_accounts_role CHECK (role IN ('admin', 'user', 'userplus'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating s3_accounts table: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*model.Bucket)(nil)).
		IfNotExists().
		ForeignKey("(owner_access_key) REFERENCES s3_accounts(access_key) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_buckets_status CHECK (status IN ('active'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating buckets table: %w", err)
	}

	// Objects are stable key identities; all mutable version data lives in object_versions.
	if _, err := db.NewCreateTable().
		Model((*model.Object)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating objects table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*model.Object)(nil)).
		Index("idx_objects_bucket_key").
		Column("bucket_id", "key").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating unique index on objects bucket/key: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.Object)(nil)).
		Index("idx_objects_id_bucket_key").
		Column("id", "bucket_id", "key").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating unique index on objects identity tuple: %w", err)
	}

	if err := createStorageProvenanceTables(ctx, db); err != nil {
		return err
	}

	// Object versions are the source of truth for current data and lifecycle.
	if _, err := db.NewCreateTable().
		Model((*model.ObjectVersion)(nil)).
		IfNotExists().
		ForeignKey("(object_id, bucket_id, key) REFERENCES objects(id, bucket_id, key) ON UPDATE CASCADE ON DELETE CASCADE").
		ForeignKey("(storage_upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_object_versions_state CHECK (state IN ('cached', 'uploading', 'committing', 'replicating', 'stored', 'failed', 'cache_evicted'))").
		ColumnExpr("CONSTRAINT chk_object_versions_size CHECK (size >= 0)").
		// Committing tracks the active upload through storage_uploads.source_version_id.
		// storage_upload_id is set only after primary commit makes the version readable.
		ColumnExpr("CONSTRAINT chk_object_versions_storage_upload_state CHECK ((state IN ('replicating', 'stored', 'cache_evicted') AND storage_upload_id IS NOT NULL) OR (state IN ('cached', 'uploading', 'committing', 'failed') AND storage_upload_id IS NULL))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating object_versions table: %w", err)
	}

	// Current-object reads use partial indexes over object_versions.is_current.
	if _, err := db.NewCreateIndex().
		Model((*model.ObjectVersion)(nil)).
		Index("idx_object_versions_current_unique").
		Column("object_id").
		Where(boolTrueWhere(db, "is_current")).
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating current version unique index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.ObjectVersion)(nil)).
		Index("idx_object_versions_current_bucket_key").
		Column("bucket_id", "key").
		Where(boolTrueWhere(db, "is_current")).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating current version listing index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.ObjectVersion)(nil)).
		Index("idx_object_versions_bucket_key_created").
		ColumnExpr("bucket_id, key, created_at DESC, version_id DESC").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version history index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.ObjectVersion)(nil)).
		Index("idx_object_versions_state_updated").
		Column("state", "updated_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version state index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.ObjectVersion)(nil)).
		Index("idx_object_versions_content_reuse").
		ColumnExpr("bucket_id, size, checksum, state, created_at DESC, version_id DESC").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version content reuse index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.ObjectVersion)(nil)).
		Index("idx_object_versions_storage_upload").
		Column("storage_upload_id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version storage upload index: %w", err)
	}

	// Tasks are queue/audit records with polymorphic references, so no FK is declared here.
	if _, err := db.NewCreateTable().
		Model((*model.Task)(nil)).
		IfNotExists().
		ColumnExpr(`CONSTRAINT chk_tasks_type CHECK ("type" IN ('upload', 'evict_cache'))`).
		ColumnExpr("CONSTRAINT chk_tasks_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'dead_letter'))").
		ColumnExpr("CONSTRAINT chk_tasks_ref_type CHECK (ref_type IN ('object', 'bucket'))").
		ColumnExpr("CONSTRAINT chk_tasks_object_ref_version CHECK (ref_type <> 'object' OR ref_version_id <> '')").
		ColumnExpr("CONSTRAINT chk_tasks_retry_count CHECK (retry_count >= 0)").
		ColumnExpr("CONSTRAINT chk_tasks_max_retries CHECK (max_retries >= 0)").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating tasks table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_type_status_scheduled").
		Column("type", "status", "scheduled_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task polling index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_type_stage_status_scheduled").
		Column("type", "stage", "status", "scheduled_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task stage index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_lease_until").
		Column("lease_until").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task lease index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_ref_status").
		Column("ref_type", "ref_id", "status").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task ref status index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_ref_version_type_status").
		Column("ref_type", "ref_version_id", "type", "status").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task ref version index: %w", err)
	}
	if err := createWalletOperationTables(ctx, db); err != nil {
		return err
	}

	// Secondary IAM and bucket indexes are grouped here to keep table creation order clear.
	if err := createS3AccountIndexes(ctx, db); err != nil {
		return err
	}
	if err := createBucketIndexes(ctx, db); err != nil {
		return err
	}

	// Multipart uploads keep parts separate and cascade-delete parts with their upload.
	if _, err := db.NewCreateTable().
		Model((*model.MultipartUpload)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_multipart_uploads_status CHECK (status IN ('initiated', 'completing', 'completed', 'aborted'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart_uploads table: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*model.MultipartPart)(nil)).
		IfNotExists().
		ForeignKey("(upload_id) REFERENCES multipart_uploads(upload_id) ON UPDATE CASCADE ON DELETE CASCADE").
		ColumnExpr("CONSTRAINT chk_multipart_parts_part_number CHECK (part_number >= 1 AND part_number <= 10000)").
		ColumnExpr("CONSTRAINT chk_multipart_parts_size CHECK (size >= 0)").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart_parts table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*model.MultipartUpload)(nil)).
		Index("idx_multipart_uploads_bucket_status_key_upload").
		Column("bucket_id", "status", "key", "upload_id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart upload listing index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.MultipartPart)(nil)).
		Index("idx_multipart_parts_upload_part").
		Column("upload_id", "part_number").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart part unique index: %w", err)
	}

	return nil
}

// down2026040501Init drops the baseline schema in reverse dependency order.
func down2026040501Init(ctx context.Context, db *bun.DB) error {
	for _, m := range []interface{}{
		(*model.MultipartPart)(nil),
		(*model.MultipartUpload)(nil),
		(*model.WalletOperation)(nil),
		(*model.Task)(nil),
		(*model.StorageUploadFailure)(nil),
		(*model.StorageUploadCopy)(nil),
		(*model.ObjectVersion)(nil),
		(*model.StorageDataSet)(nil),
		(*model.StorageUpload)(nil),
		(*model.Object)(nil),
		(*model.Bucket)(nil),
		(*model.S3Account)(nil),
	} {
		if _, err := db.NewDropTable().Model(m).IfExists().Exec(ctx); err != nil {
			return fmt.Errorf("dropping table %T: %w", m, err)
		}
	}
	return nil
}

func createWalletOperationTables(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewCreateTable().
		Model((*model.WalletOperation)(nil)).
		IfNotExists().
		ColumnExpr(`CONSTRAINT chk_wallet_operations_type CHECK ("type" IN ('fund', 'withdraw'))`).
		ColumnExpr("CONSTRAINT chk_wallet_operations_status CHECK (status IN ('pending', 'running', 'submitted', 'confirmed', 'failed', 'unknown'))").
		ColumnExpr("CONSTRAINT chk_wallet_operations_amount CHECK (" + walletOperationAmountCheck(db) + ")").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating wallet_operations table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.WalletOperation)(nil)).
		Index("idx_wallet_operations_request").
		Column("type", "client_request_id").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating wallet operation request index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.WalletOperation)(nil)).
		Index("idx_wallet_operations_status_created").
		Column("status", "created_at", "id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating wallet operation status index: %w", err)
	}
	return nil
}

func walletOperationAmountCheck(db *bun.DB) string {
	if db.Dialect().Name() == dialect.PG {
		return `amount ~ '^[1-9][0-9]*$'`
	}
	return `amount GLOB '[1-9]*' AND amount NOT GLOB '*[^0-9]*'`
}

func createS3AccountIndexes(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewCreateIndex().
		Model((*model.S3Account)(nil)).
		Index("idx_s3_accounts_is_root").
		Column("is_root").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating s3 account root index: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*model.S3Account)(nil)).
		Index("idx_s3_accounts_single_root").
		Column("is_root").
		Where(boolTrueWhere(db, "is_root")).
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating single root account index: %w", err)
	}
	return nil
}

func createBucketIndexes(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewCreateIndex().
		Model((*model.Bucket)(nil)).
		Index("idx_buckets_owner_access_key").
		Column("owner_access_key").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating bucket owner index: %w", err)
	}
	return nil
}

func createStorageProvenanceTables(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewCreateTable().
		Model((*model.StorageUpload)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_storage_uploads_status CHECK (status IN ('running', 'stored_on_primary', 'primary_committed', 'partial', 'all_copies_committed', 'failed', 'rejected', 'superseded'))").
		ColumnExpr("CONSTRAINT chk_storage_uploads_content_size CHECK (content_size >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_uploads_requested_copies CHECK (requested_copies >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_uploads_primary_bytes CHECK (primary_bytes_uploaded >= 0 AND primary_bytes_uploaded <= content_size)").
		ColumnExpr("CONSTRAINT chk_storage_uploads_primary_attempt CHECK (primary_store_attempt >= 0)").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_uploads table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageUpload)(nil)).
		Index("idx_storage_uploads_task_version_status").
		Column("source_task_id", "source_version_id", "status", "accepted_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload task/version index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageUpload)(nil)).
		Index("idx_storage_uploads_content_status").
		Column("bucket_id", "content_size", "checksum", "status", "accepted_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload content index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageUpload)(nil)).
		Index("idx_storage_uploads_active_source_version").
		Column("source_version_id").
		Where("source_version_id <> '' AND status IN ('running', 'stored_on_primary', 'primary_committed', 'partial')").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating active storage upload source version index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*model.StorageDataSet)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ForeignKey("(created_by_upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE SET NULL").
		ForeignKey("(last_used_upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE SET NULL").
		ColumnExpr("CONSTRAINT chk_storage_data_sets_copy_index CHECK (copy_index >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_data_sets_status CHECK (status IN ('pending', 'creating', 'ready', 'failed'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_data_sets table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageDataSet)(nil)).
		Index("idx_storage_data_sets_provider_data_set").
		Column("provider_id", "data_set_id").
		Where("data_set_id IS NOT NULL AND data_set_id <> ''").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage data set provider unique index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageDataSet)(nil)).
		Index("idx_storage_data_sets_bucket_provider").
		Column("bucket_id", "provider_id").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage data set bucket provider index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageDataSet)(nil)).
		Index("idx_storage_data_sets_bucket_copy_index").
		Column("bucket_id", "copy_index").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage data set bucket copy index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*model.StorageUploadCopy)(nil)).
		IfNotExists().
		ForeignKey("(upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE CASCADE").
		ForeignKey("(storage_data_set_id) REFERENCES storage_data_sets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_storage_upload_copies_copy_index CHECK (copy_index >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_upload_copies_status CHECK (status IN ('pending', 'piece_ready', 'committing', 'committed', 'failed'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_upload_copies table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageUploadCopy)(nil)).
		Index("idx_storage_upload_copies_upload_index").
		Column("upload_id", "copy_index").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload copy unique index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageUploadCopy)(nil)).
		Index("idx_storage_upload_copies_upload_role_index").
		Column("upload_id", "role", "copy_index").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload copy role index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*model.StorageUploadFailure)(nil)).
		IfNotExists().
		ForeignKey("(upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE CASCADE").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_upload_failures table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*model.StorageUploadFailure)(nil)).
		Index("idx_storage_upload_failures_upload_attempt").
		Column("upload_id", "attempt_index").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload failure unique index: %w", err)
	}
	return nil
}

// boolTrueWhere emits portable partial-index predicates for PostgreSQL and SQLite.
func boolTrueWhere(db *bun.DB, column string) string {
	if db.Dialect().Name() == dialect.PG {
		return column + " IS TRUE"
	}
	return column + " = TRUE"
}
