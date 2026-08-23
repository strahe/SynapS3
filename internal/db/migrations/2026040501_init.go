package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026040501Init),
		transactionalMigration(down2026040501Init),
	)
}

type s3Account2026040501 struct {
	bun.BaseModel `bun:"table:s3_accounts"`

	AccessKey string    `bun:",pk"`
	SecretKey string    `bun:",notnull"`
	Role      string    `bun:",notnull"`
	IsRoot    bool      `bun:",notnull,default:false"`
	CreatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

type bucket2026040501 struct {
	bun.BaseModel `bun:"table:buckets"`

	ID             int64     `bun:",pk,autoincrement"`
	Name           string    `bun:",unique,notnull"`
	ACL            []byte    `bun:",nullzero"`
	OwnerAccessKey *string   `bun:",nullzero"`
	Status         string    `bun:",notnull,default:'active'"`
	CreatedAt      time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt      time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

type object2026040501 struct {
	bun.BaseModel `bun:"table:objects"`

	ID        int64     `bun:",pk,autoincrement"`
	BucketID  int64     `bun:",notnull"`
	Key       string    `bun:",notnull"`
	CreatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

type objectVersion2026040501 struct {
	bun.BaseModel `bun:"table:object_versions"`

	VersionID       string            `bun:",pk"`
	ObjectID        int64             `bun:",notnull"`
	BucketID        int64             `bun:",notnull"`
	Key             string            `bun:",notnull"`
	Size            int64             `bun:",notnull"`
	ETag            string            `bun:",notnull"`
	Checksum        string            `bun:",notnull"`
	ContentType     string            `bun:",notnull,default:'application/octet-stream'"`
	Metadata        map[string]string `bun:"type:jsonb"`
	CacheKey        string            `bun:",notnull"`
	StorageUploadID *int64            `bun:",nullzero"`
	PieceCID        *string           `bun:",scanonly"`
	RetrievalURL    *string           `bun:",scanonly"`
	InCache         bool              `bun:",notnull,default:true"`
	InFilecoin      bool              `bun:",scanonly"`
	IsCurrent       bool              `bun:",notnull,default:false"`
	IsDeleteMarker  bool              `bun:",notnull,default:false"`
	State           string            `bun:",notnull,default:'cached'"`
	FailedAtState   *string           `bun:",nullzero"`
	LastError       *string           `bun:",nullzero"`
	CreatedAt       time.Time         `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt       time.Time         `bun:",nullzero,notnull,default:current_timestamp"`
}

type objectDeletion2026040501 struct {
	bun.BaseModel `bun:"table:object_deletions"`

	ID                 int64      `bun:",pk,autoincrement"`
	BucketID           int64      `bun:",notnull"`
	ObjectID           int64      `bun:",notnull"`
	Key                string     `bun:",notnull"`
	VersionID          string     `bun:",unique,notnull"`
	CacheKey           string     `bun:",notnull"`
	StorageUploadID    *int64     `bun:",nullzero"`
	Size               int64      `bun:",notnull"`
	Checksum           string     `bun:",notnull"`
	CacheCleanupStatus string     `bun:",notnull,default:'pending'"`
	CacheError         *string    `bun:",nullzero"`
	CacheCleanedAt     *time.Time `bun:",nullzero"`
	CreatedAt          time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt          time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	DeletedAt          time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
}

type task2026040501 struct {
	bun.BaseModel `bun:"table:tasks"`

	ID             int64          `bun:",pk,autoincrement"`
	Type           string         `bun:",notnull"`
	Stage          *string        `bun:",nullzero"`
	RefType        string         `bun:",notnull"`
	RefID          int64          `bun:",notnull"`
	RefVersionID   string         `bun:",notnull"`
	IdempotencyKey string         `bun:",unique,notnull"`
	Payload        map[string]any `bun:"type:jsonb"`
	Status         string         `bun:",notnull,default:'queued'"`
	RetryCount     int            `bun:",notnull,default:0"`
	MaxRetries     int            `bun:",notnull,default:5"`
	LastError      *string        `bun:",nullzero"`
	StatusMessage  *string        `bun:",nullzero"`
	WaitReason     *string        `bun:",nullzero"`
	ScheduledAt    time.Time      `bun:",nullzero,notnull,default:current_timestamp"`
	ClaimedAt      *time.Time     `bun:",nullzero"`
	LeaseUntil     *time.Time     `bun:",nullzero"`
	StartedAt      *time.Time     `bun:",nullzero"`
	CompletedAt    *time.Time     `bun:",nullzero"`
}

type storageCleanupCopy2026040501 struct {
	bun.BaseModel `bun:"table:storage_cleanup_copies"`

	ID               int64      `bun:",pk,autoincrement"`
	TaskID           int64      `bun:",notnull"`
	UploadID         int64      `bun:",notnull"`
	CopyIndex        int        `bun:",notnull"`
	ProviderID       *string    `bun:"type:text"`
	StorageDataSetID *int64     `bun:",nullzero"`
	DataSetID        *string    `bun:"type:text"`
	ClientDataSetID  *string    `bun:"type:text"`
	PieceID          *string    `bun:"type:text"`
	PieceCID         string     `bun:",notnull"`
	RetrievalURL     *string    `bun:",nullzero"`
	Status           string     `bun:",notnull,default:'pending'"`
	DeleteTxHash     *string    `bun:",nullzero"`
	LastError        *string    `bun:",nullzero"`
	ScheduledAt      *time.Time `bun:",nullzero"`
	RemovedAt        *time.Time `bun:",nullzero"`
	CreatedAt        time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt        time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
}

type multipartUpload2026040501 struct {
	bun.BaseModel `bun:"table:multipart_uploads"`

	ID          int64             `bun:",pk,autoincrement"`
	BucketID    int64             `bun:",notnull"`
	Key         string            `bun:",notnull"`
	UploadID    string            `bun:",notnull,unique"`
	ContentType string            `bun:",notnull,default:'application/octet-stream'"`
	Metadata    map[string]string `bun:"type:jsonb"`
	Status      string            `bun:",notnull,default:'initiated'"`
	CreatedAt   time.Time         `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt   time.Time         `bun:",nullzero,notnull,default:current_timestamp"`
}

type multipartPart2026040501 struct {
	bun.BaseModel `bun:"table:multipart_parts"`

	ID         int64     `bun:",pk,autoincrement"`
	UploadID   string    `bun:",notnull"`
	PartNumber int       `bun:",notnull"`
	Size       int64     `bun:",notnull"`
	ETag       string    `bun:",notnull"`
	CreatedAt  time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

type walletOperation2026040501 struct {
	bun.BaseModel `bun:"table:wallet_operations"`

	ID              int64      `bun:",pk,autoincrement"`
	Type            string     `bun:",notnull"`
	ClientRequestID string     `bun:",notnull"`
	Amount          string     `bun:",notnull"`
	Status          string     `bun:",notnull,default:'pending'"`
	TxHash          *string    `bun:",nullzero"`
	LastError       *string    `bun:",nullzero"`
	LeaseUntil      *time.Time `bun:",nullzero"`
	StartedAt       *time.Time `bun:",nullzero"`
	SubmittedAt     *time.Time `bun:",nullzero"`
	CompletedAt     *time.Time `bun:",nullzero"`
	CreatedAt       time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt       time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
}

type storageUpload2026040501 struct {
	bun.BaseModel `bun:"table:storage_uploads"`

	ID                      int64      `bun:",pk,autoincrement"`
	BucketID                int64      `bun:",notnull"`
	SourceTaskID            *int64     `bun:",nullzero"`
	SourceVersionID         string     `bun:",nullzero"`
	ContentSize             int64      `bun:",notnull"`
	Checksum                string     `bun:",notnull"`
	Status                  string     `bun:",notnull,default:'running'"`
	PieceCID                *string    `bun:",nullzero"`
	RequestedCopies         int        `bun:",notnull"`
	IngressBytesTransferred int64      `bun:",notnull,default:0"`
	IngressStoreAttempt     int        `bun:",notnull,default:0"`
	ProgressUpdatedAt       *time.Time `bun:",nullzero"`
	RawResultJSON           []byte     `bun:"type:jsonb,nullzero"`
	ErrorMessage            *string    `bun:",nullzero"`
	AcceptError             *string    `bun:",nullzero"`
	AcceptedAt              *time.Time `bun:",nullzero"`
	CreatedAt               time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt               time.Time  `bun:",nullzero,notnull,default:current_timestamp"`
}

type storageDataSet2026040501 struct {
	bun.BaseModel `bun:"table:storage_data_sets"`

	ID                  int64     `bun:",pk,autoincrement"`
	BucketID            int64     `bun:",notnull"`
	ProviderID          string    `bun:"type:text,notnull"`
	CopyIndex           int       `bun:",notnull"`
	DataSetID           *string   `bun:"type:text"`
	ClientDataSetID     *string   `bun:"type:text"`
	Status              string    `bun:",notnull,default:'pending'"`
	CreateTransactionID *string   `bun:",nullzero"`
	CreateStatusURL     *string   `bun:",nullzero"`
	CreatedByUploadID   *int64    `bun:",nullzero"`
	LastUsedUploadID    *int64    `bun:",nullzero"`
	LastError           *string   `bun:",nullzero"`
	CreatedAt           time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt           time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

type storageUploadCopy2026040501 struct {
	bun.BaseModel `bun:"table:storage_upload_copies"`

	ID                  int64     `bun:",pk,autoincrement"`
	UploadID            int64     `bun:",notnull"`
	CopyIndex           int       `bun:",notnull"`
	ProviderID          *string   `bun:"type:text"`
	DataSetID           *string   `bun:"type:text,scanonly"`
	PieceID             *string   `bun:"type:text"`
	TransferMethod      string    `bun:",notnull"`
	Status              string    `bun:",notnull,default:'pending'"`
	RetrievalURL        *string   `bun:",nullzero"`
	IsNewDataSet        bool      `bun:",notnull,default:false"`
	StorageDataSetID    *int64    `bun:",nullzero"`
	CommitExtraDataHex  *string   `bun:",nullzero"`
	CommitTransactionID *string   `bun:",nullzero"`
	LastError           *string   `bun:",nullzero"`
	CreatedAt           time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt           time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

type storageUploadFailure2026040501 struct {
	bun.BaseModel `bun:"table:storage_upload_failures"`

	ID             int64     `bun:",pk,autoincrement"`
	UploadID       int64     `bun:",notnull"`
	AttemptIndex   int       `bun:",notnull"`
	ProviderID     *string   `bun:"type:text"`
	TransferMethod string    `bun:",notnull"`
	Stage          *string   `bun:",nullzero"`
	ErrorMessage   *string   `bun:",nullzero"`
	Explicit       bool      `bun:",notnull,default:false"`
	CreatedAt      time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

// up2026040501Init is the frozen development-preview baseline schema.
// Follow-up schema changes must be added as separate migrations.
func up2026040501Init(ctx context.Context, db bun.IDB) error {
	// IAM and bucket ownership tables come first because buckets reference S3 accounts.
	if _, err := db.NewCreateTable().
		Model((*s3Account2026040501)(nil)).
		IfNotExists().
		ColumnExpr("CONSTRAINT chk_s3_accounts_role CHECK (role IN ('admin', 'user', 'userplus'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating s3_accounts table: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*bucket2026040501)(nil)).
		IfNotExists().
		ForeignKey("(owner_access_key) REFERENCES s3_accounts(access_key) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_buckets_status CHECK (status IN ('active'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating buckets table: %w", err)
	}

	// Objects are stable key identities; all mutable version data lives in object_versions.
	if _, err := db.NewCreateTable().
		Model((*object2026040501)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating objects table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*object2026040501)(nil)).
		Index("idx_objects_bucket_key").
		Column("bucket_id", "key").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating unique index on objects bucket/key: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*object2026040501)(nil)).
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
		Model((*objectVersion2026040501)(nil)).
		IfNotExists().
		ForeignKey("(object_id, bucket_id, key) REFERENCES objects(id, bucket_id, key) ON UPDATE CASCADE ON DELETE CASCADE").
		ForeignKey("(storage_upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_object_versions_state CHECK (state IN ('cached', 'uploading', 'committing', 'replicating', 'stored', 'failed', 'cache_evicted'))").
		ColumnExpr("CONSTRAINT chk_object_versions_size CHECK (size >= 0)").
		// Committing tracks the active upload through storage_uploads.source_version_id.
		// storage_upload_id is set only after a committed copy makes the version readable.
		ColumnExpr("CONSTRAINT chk_object_versions_storage_upload_state CHECK ((state IN ('replicating', 'stored', 'cache_evicted') AND storage_upload_id IS NOT NULL) OR (state IN ('cached', 'uploading', 'committing', 'failed') AND storage_upload_id IS NULL))").
		ColumnExpr("CONSTRAINT chk_object_versions_delete_marker_shape CHECK ((is_delete_marker = TRUE AND size = 0 AND e_tag = '' AND checksum = '' AND content_type = '' AND cache_key = '' AND storage_upload_id IS NULL AND in_cache = FALSE AND state = 'cached' AND failed_at_state IS NULL AND last_error IS NULL) OR (is_delete_marker = FALSE AND e_tag <> '' AND checksum <> '' AND cache_key <> ''))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating object_versions table: %w", err)
	}

	// Current-object reads use partial indexes over object_versions.is_current.
	if _, err := db.NewCreateIndex().
		Model((*objectVersion2026040501)(nil)).
		Index("idx_object_versions_current_unique").
		Column("object_id").
		Where(boolTrueWhere(db, "is_current")).
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating current version unique index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectVersion2026040501)(nil)).
		Index("idx_object_versions_current_bucket_key").
		Column("bucket_id", "key").
		Where(boolTrueWhere(db, "is_current")).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating current version listing index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectVersion2026040501)(nil)).
		Index("idx_object_versions_bucket_key_created").
		ColumnExpr("bucket_id, key, created_at DESC, version_id DESC").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version history index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectVersion2026040501)(nil)).
		Index("idx_object_versions_object_created").
		ColumnExpr("object_id, created_at DESC, version_id DESC").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating object version history index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectVersion2026040501)(nil)).
		Index("idx_object_versions_state_updated").
		Column("state", "updated_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version state index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectVersion2026040501)(nil)).
		Index("idx_object_versions_content_reuse").
		ColumnExpr("bucket_id, size, checksum, state, created_at DESC, version_id DESC").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version content reuse index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectVersion2026040501)(nil)).
		Index("idx_object_versions_storage_upload").
		Column("storage_upload_id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating version storage upload index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*objectDeletion2026040501)(nil)).
		IfNotExists().
		ColumnExpr("CONSTRAINT chk_object_deletions_size CHECK (size >= 0)").
		ColumnExpr("CONSTRAINT chk_object_deletions_cache_status CHECK (cache_cleanup_status IN ('pending', 'deleted', 'skipped', 'failed'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating object_deletions table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectDeletion2026040501)(nil)).
		Index("idx_object_deletions_bucket_key_created").
		Column("bucket_id", "key", "created_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating object deletion bucket key index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectDeletion2026040501)(nil)).
		Index("idx_object_deletions_storage_upload").
		Column("storage_upload_id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating object deletion storage upload index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*objectDeletion2026040501)(nil)).
		Index("idx_object_deletions_bucket_created").
		ColumnExpr("bucket_id, created_at DESC, id DESC").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating object deletion bucket created index: %w", err)
	}

	// Tasks are queue/audit records with polymorphic references, so no FK is declared here.
	if _, err := db.NewCreateTable().
		Model((*task2026040501)(nil)).
		IfNotExists().
		ColumnExpr(`CONSTRAINT chk_tasks_type CHECK ("type" IN ('upload', 'evict_cache', 'storage_cleanup'))`).
		ColumnExpr("CONSTRAINT chk_tasks_status CHECK (status IN ('queued', 'scheduled', 'running', 'waiting', 'completed', 'failed', 'exhausted', 'cancelled'))").
		ColumnExpr("CONSTRAINT chk_tasks_wait_reason CHECK (wait_reason IS NULL OR (status = 'waiting' AND wait_reason IN ('dependency', 'external_confirmation')))").
		ColumnExpr("CONSTRAINT chk_tasks_ref_type CHECK (ref_type IN ('object', 'bucket', 'storage_upload'))").
		ColumnExpr("CONSTRAINT chk_tasks_object_ref_version CHECK (ref_type <> 'object' OR ref_version_id <> '')").
		ColumnExpr("CONSTRAINT chk_tasks_retry_count CHECK (retry_count >= 0)").
		ColumnExpr("CONSTRAINT chk_tasks_max_retries CHECK (max_retries >= 0)").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating tasks table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*task2026040501)(nil)).
		Index("idx_tasks_type_status_scheduled").
		Column("type", "status", "scheduled_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task polling index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*task2026040501)(nil)).
		Index("idx_tasks_type_ready_scheduled").
		Column("type", "scheduled_at", "id").
		Where("status IN ('queued', 'scheduled', 'waiting')").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating ready task polling index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*task2026040501)(nil)).
		Index("idx_tasks_type_stage_status_scheduled").
		Column("type", "stage", "status", "scheduled_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task stage index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*task2026040501)(nil)).
		Index("idx_tasks_lease_until").
		Column("lease_until").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task lease index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*task2026040501)(nil)).
		Index("idx_tasks_ref_status").
		Column("ref_type", "ref_id", "status").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task ref status index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*task2026040501)(nil)).
		Index("idx_tasks_ref_version_type_status").
		Column("ref_type", "ref_version_id", "type", "status").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating task ref version index: %w", err)
	}
	if _, err := db.NewCreateTable().
		Model((*storageCleanupCopy2026040501)(nil)).
		IfNotExists().
		ForeignKey("(task_id) REFERENCES tasks(id) ON UPDATE CASCADE ON DELETE CASCADE").
		ColumnExpr("CONSTRAINT chk_storage_cleanup_copies_copy_index CHECK (copy_index >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_cleanup_copies_status CHECK (status IN ('pending', 'delete_scheduled', 'removed', 'failed', 'unsupported'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_cleanup_copies table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageCleanupCopy2026040501)(nil)).
		Index("idx_storage_cleanup_copies_task_copy").
		Column("task_id", "copy_index").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage cleanup copy task index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageCleanupCopy2026040501)(nil)).
		Index("idx_storage_cleanup_copies_upload_status").
		Column("upload_id", "status").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage cleanup copy upload index: %w", err)
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
		Model((*multipartUpload2026040501)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_multipart_uploads_status CHECK (status IN ('initiated', 'completing', 'completed', 'aborted'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart_uploads table: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*multipartPart2026040501)(nil)).
		IfNotExists().
		ForeignKey("(upload_id) REFERENCES multipart_uploads(upload_id) ON UPDATE CASCADE ON DELETE CASCADE").
		ColumnExpr("CONSTRAINT chk_multipart_parts_part_number CHECK (part_number >= 1 AND part_number <= 10000)").
		ColumnExpr("CONSTRAINT chk_multipart_parts_size CHECK (size >= 0)").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart_parts table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*multipartUpload2026040501)(nil)).
		Index("idx_multipart_uploads_bucket_status_key_upload").
		Column("bucket_id", "status", "key", "upload_id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart upload listing index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*multipartPart2026040501)(nil)).
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
func down2026040501Init(ctx context.Context, db bun.IDB) error {
	for _, m := range []interface{}{
		(*multipartPart2026040501)(nil),
		(*multipartUpload2026040501)(nil),
		(*walletOperation2026040501)(nil),
		(*storageCleanupCopy2026040501)(nil),
		(*task2026040501)(nil),
		(*objectDeletion2026040501)(nil),
		(*storageUploadFailure2026040501)(nil),
		(*storageUploadCopy2026040501)(nil),
		(*objectVersion2026040501)(nil),
		(*storageDataSet2026040501)(nil),
		(*storageUpload2026040501)(nil),
		(*object2026040501)(nil),
		(*bucket2026040501)(nil),
		(*s3Account2026040501)(nil),
	} {
		if _, err := db.NewDropTable().Model(m).IfExists().Exec(ctx); err != nil {
			return fmt.Errorf("dropping table %T: %w", m, err)
		}
	}
	return nil
}

func createWalletOperationTables(ctx context.Context, db bun.IDB) error {
	if _, err := db.NewCreateTable().
		Model((*walletOperation2026040501)(nil)).
		IfNotExists().
		ColumnExpr(`CONSTRAINT chk_wallet_operations_type CHECK ("type" IN ('fund', 'withdraw'))`).
		ColumnExpr("CONSTRAINT chk_wallet_operations_status CHECK (status IN ('pending', 'running', 'submitted', 'confirmed', 'failed', 'unknown'))").
		ColumnExpr("CONSTRAINT chk_wallet_operations_amount CHECK (" + walletOperationAmountCheck(db) + ")").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating wallet_operations table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*walletOperation2026040501)(nil)).
		Index("idx_wallet_operations_request").
		Column("type", "client_request_id").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating wallet operation request index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*walletOperation2026040501)(nil)).
		Index("idx_wallet_operations_status_created").
		Column("status", "created_at", "id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating wallet operation status index: %w", err)
	}
	return nil
}

func walletOperationAmountCheck(db bun.IDB) string {
	if db.Dialect().Name() == dialect.PG {
		return `amount ~ '^[1-9][0-9]*$'`
	}
	return `amount GLOB '[1-9]*' AND amount NOT GLOB '*[^0-9]*'`
}

func createS3AccountIndexes(ctx context.Context, db bun.IDB) error {
	if _, err := db.NewCreateIndex().
		Model((*s3Account2026040501)(nil)).
		Index("idx_s3_accounts_is_root").
		Column("is_root").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating s3 account root index: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*s3Account2026040501)(nil)).
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

func createBucketIndexes(ctx context.Context, db bun.IDB) error {
	if _, err := db.NewCreateIndex().
		Model((*bucket2026040501)(nil)).
		Index("idx_buckets_owner_access_key").
		Column("owner_access_key").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating bucket owner index: %w", err)
	}
	return nil
}

func createStorageProvenanceTables(ctx context.Context, db bun.IDB) error {
	if _, err := db.NewCreateTable().
		Model((*storageUpload2026040501)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_storage_uploads_status CHECK (status IN ('running', 'ingress_ready', 'readable', 'complete', 'failed', 'rejected', 'superseded'))").
		ColumnExpr("CONSTRAINT chk_storage_uploads_content_size CHECK (content_size >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_uploads_requested_copies CHECK (requested_copies >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_uploads_ingress_bytes CHECK (ingress_bytes_transferred >= 0 AND ingress_bytes_transferred <= content_size)").
		ColumnExpr("CONSTRAINT chk_storage_uploads_ingress_attempt CHECK (ingress_store_attempt >= 0)").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_uploads table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUpload2026040501)(nil)).
		Index("idx_storage_uploads_task_version_status").
		Column("source_task_id", "source_version_id", "status", "accepted_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload task/version index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUpload2026040501)(nil)).
		Index("idx_storage_uploads_source_version_id").
		Column("source_version_id", "id").
		Where("source_version_id <> ''").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload source version index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUpload2026040501)(nil)).
		Index("idx_storage_uploads_content_status").
		Column("bucket_id", "content_size", "checksum", "status", "accepted_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload content index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUpload2026040501)(nil)).
		Index("idx_storage_uploads_active_source_version").
		Column("source_version_id").
		Where("source_version_id <> '' AND status IN ('running', 'ingress_ready', 'readable')").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating active storage upload source version index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*storageDataSet2026040501)(nil)).
		IfNotExists().
		ForeignKey("(bucket_id) REFERENCES buckets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ForeignKey("(created_by_upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE SET NULL").
		ForeignKey("(last_used_upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE SET NULL").
		ColumnExpr("CONSTRAINT chk_storage_data_sets_copy_index CHECK (copy_index >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_data_sets_status CHECK (status IN ('pending', 'creating', 'ready', 'failed', 'unavailable', 'draining', 'retired'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_data_sets table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageDataSet2026040501)(nil)).
		Index("idx_storage_data_sets_provider_data_set").
		Column("provider_id", "data_set_id").
		Where("data_set_id IS NOT NULL AND data_set_id <> ''").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage data set provider unique index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageDataSet2026040501)(nil)).
		Index("idx_storage_data_sets_bucket_provider").
		Column("bucket_id", "provider_id").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage data set bucket provider index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageDataSet2026040501)(nil)).
		Index("idx_storage_data_sets_bucket_copy_index").
		Column("bucket_id", "copy_index").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage data set bucket copy index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*storageUploadCopy2026040501)(nil)).
		IfNotExists().
		ForeignKey("(upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE CASCADE").
		ForeignKey("(storage_data_set_id) REFERENCES storage_data_sets(id) ON UPDATE CASCADE ON DELETE RESTRICT").
		ColumnExpr("CONSTRAINT chk_storage_upload_copies_copy_index CHECK (copy_index >= 0)").
		ColumnExpr("CONSTRAINT chk_storage_upload_copies_status CHECK (status IN ('pending', 'piece_ready', 'committing', 'committed', 'failed'))").
		ColumnExpr("CONSTRAINT chk_storage_upload_copies_transfer_method CHECK (transfer_method IN ('ingress', 'peer_pull'))").
		ColumnExpr("CONSTRAINT chk_storage_upload_copies_committed_shape CHECK (status <> 'committed' OR (storage_data_set_id IS NOT NULL AND provider_id IS NOT NULL AND provider_id <> '' AND piece_id IS NOT NULL AND piece_id <> '' AND retrieval_url IS NOT NULL AND retrieval_url <> ''))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_upload_copies table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUploadCopy2026040501)(nil)).
		Index("idx_storage_upload_copies_upload_index").
		Column("upload_id", "copy_index").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload copy unique index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUploadCopy2026040501)(nil)).
		Index("idx_storage_upload_copies_upload_transfer_method_index").
		Column("upload_id", "transfer_method", "copy_index").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload copy transfer method index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUploadCopy2026040501)(nil)).
		Index("idx_storage_upload_copies_status_data_set_upload").
		Column("status", "storage_data_set_id", "upload_id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload copy dataset summary index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUploadCopy2026040501)(nil)).
		Index("idx_storage_upload_copies_status_piece_identity_upload").
		Column("status", "provider_id", "piece_id", "upload_id").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage upload copy piece identity index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*storageUploadFailure2026040501)(nil)).
		IfNotExists().
		ForeignKey("(upload_id) REFERENCES storage_uploads(id) ON UPDATE CASCADE ON DELETE CASCADE").
		ColumnExpr("CONSTRAINT chk_storage_upload_failures_transfer_method CHECK (transfer_method IN ('ingress', 'peer_pull'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating storage_upload_failures table: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*storageUploadFailure2026040501)(nil)).
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
func boolTrueWhere(db bun.IDB, column string) string {
	if db.Dialect().Name() == dialect.PG {
		return column + " IS TRUE"
	}
	return column + " = TRUE"
}
