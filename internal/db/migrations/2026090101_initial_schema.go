package migrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026090101InitialSchema),
		func(context.Context, *bun.DB) error {
			return errors.New("the initial schema cannot be rolled back; create a new empty database")
		},
	)
}

func up2026090101InitialSchema(ctx context.Context, db bun.IDB) error {
	complete, err := initialSchemaPostStateComplete(ctx, db)
	if err != nil {
		return fmt.Errorf("checking initial schema post-state: %w", err)
	}
	if complete {
		return nil
	}
	count, err := applicationTableCount(ctx, db)
	if err != nil {
		return fmt.Errorf("checking initial schema pre-state: %w", err)
	}
	if count != 0 {
		return incompatibleDatabaseError()
	}
	steps := []struct {
		name string
		up   migrationBody
	}{
		{"tasks", createTaskSchema},
		{"identity and object roots", createCoreRootSchema},
		{"storage ledgers", createStorageSchema},
		{"object lifecycle", createObjectLifecycleSchema},
		{"wallet operations", createWalletSchema},
		{"observability", createObservabilitySchema},
		{"PostgreSQL prefix indexes", createPostgresPrefixIndexes},
	}
	for _, step := range steps {
		if err := step.up(ctx, db); err != nil {
			return fmt.Errorf("creating %s: %w", step.name, err)
		}
	}
	return nil
}

// taskPayload2026090101 holds the JSON a task carries. It lives beside the task
// rather than in it because lease renewal updates an indexed column on every
// heartbeat, and PostgreSQL copies the whole row — JSON included — each time.
type taskPayload2026090101 struct {
	bun.BaseModel `bun:"table:task_payloads"`

	TaskID     int64           `bun:",pk"`
	Input      json.RawMessage `bun:"input_json,type:jsonb,notnull"`
	Checkpoint json.RawMessage `bun:"checkpoint_json,type:jsonb"`
}

type task2026090101 struct {
	bun.BaseModel `bun:"table:tasks"`

	ID             int64   `bun:",pk,autoincrement,identity"`
	Type           string  `bun:"type:text,notnull"`
	IdempotencyKey string  `bun:"type:text,notnull"`
	InputVersion   int     `bun:"type:integer,notnull"`
	InputHash      string  `bun:"type:text,notnull"`
	SubjectType    *string `bun:"type:text"`
	SubjectKey     *string `bun:"type:text"`

	Status        string    `bun:"type:text,notnull,default:'pending'"`
	ResumeMode    string    `bun:"type:text,notnull,default:'execute'"`
	AvailableAt   time.Time `bun:",notnull"`
	WaitReason    *string   `bun:"type:text"`
	RetryCount    int       `bun:"type:integer,notnull,default:0"`
	RetryLimit    *int      `bun:"type:integer"`
	FailureReason *string   `bun:"type:text"`
	LastError     *string   `bun:"type:text"`
	StatusMessage *string   `bun:"type:text"`

	CancellationRequestedAt *time.Time
	CancellationReason      *string `bun:"type:text"`
	ClaimGeneration         int64   `bun:",notnull,default:0"`
	ClaimedAt               *time.Time
	LeaseUntil              *time.Time
	StartedAt               *time.Time
	FinishedAt              *time.Time
	AcknowledgedAt          *time.Time
	RetentionUntil          *time.Time
	CreatedAt               time.Time `bun:",notnull"`
	UpdatedAt               time.Time `bun:",notnull"`
}

func createTaskSchema(ctx context.Context, db bun.IDB) error {
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:  "tasks",
		model: (*task2026090101)(nil),
		constraints: []string{
			"CONSTRAINT uq_tasks_type_key UNIQUE (type, idempotency_key)",
			"CONSTRAINT chk_tasks_identity CHECK (type <> '' AND idempotency_key <> '' AND input_hash <> '' AND input_version >= 1)",
			"CONSTRAINT chk_tasks_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))",
			"CONSTRAINT chk_tasks_resume_mode CHECK (resume_mode IN ('execute', 'recover'))",
			"CONSTRAINT chk_tasks_subject CHECK ((subject_type IS NULL AND subject_key IS NULL) OR (subject_type IS NOT NULL AND subject_type <> '' AND subject_key IS NOT NULL AND subject_key <> ''))",
			"CONSTRAINT chk_tasks_reason_codes CHECK ((wait_reason IS NULL OR wait_reason <> '') AND (failure_reason IS NULL OR failure_reason <> ''))",
			"CONSTRAINT chk_tasks_retry CHECK (retry_count >= 0 AND (retry_limit IS NULL OR (retry_limit >= 0 AND retry_count <= retry_limit)))",
			"CONSTRAINT chk_tasks_generation CHECK (claim_generation >= 0)",
			`CONSTRAINT chk_tasks_claim CHECK (
				(status = 'running' AND claimed_at IS NOT NULL AND lease_until IS NOT NULL AND claim_generation > 0)
				OR (status <> 'running' AND claimed_at IS NULL AND lease_until IS NULL)
			)`,
			`CONSTRAINT chk_tasks_finished CHECK (
				(status IN ('completed', 'failed', 'cancelled') AND finished_at IS NOT NULL)
				OR (status IN ('pending', 'running') AND finished_at IS NULL)
			)`,
			`CONSTRAINT chk_tasks_retention CHECK (
				(status IN ('completed', 'cancelled') AND retention_until IS NOT NULL)
				OR (status = 'failed' AND ((acknowledged_at IS NULL AND retention_until IS NULL) OR (acknowledged_at IS NOT NULL AND retention_until IS NOT NULL)))
				OR (status IN ('pending', 'running') AND acknowledged_at IS NULL AND retention_until IS NULL)
			)`,
		},
	}); err != nil {
		return err
	}
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:        "task_payloads",
		model:       (*taskPayload2026090101)(nil),
		jsonColumns: initialJSONColumns("task_payloads"),
		foreignKeys: []string{
			"(task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE CASCADE",
		},
	}); err != nil {
		return err
	}
	return createInitialIndexes(ctx, db,
		initialIndexSpec{name: "idx_tasks_pending", table: "tasks", columns: []string{"available_at", "id"}, where: "status = 'pending'"},
		initialIndexSpec{name: "idx_tasks_recovery", table: "tasks", columns: []string{"lease_until", "id"}, where: "status = 'running'"},
		initialIndexSpec{name: "idx_tasks_gc", table: "tasks", columns: []string{"retention_until", "id"}, where: "retention_until IS NOT NULL"},
		initialIndexSpec{name: "idx_tasks_type_status_id", table: "tasks", columns: []string{"type", "status", "id"}},
		initialIndexSpec{name: "idx_tasks_type_id", table: "tasks", columns: []string{"type", "id"}},
		initialIndexSpec{name: "idx_tasks_status_id", table: "tasks", columns: []string{"status", "id"}},
		initialIndexSpec{name: "idx_tasks_subject", table: "tasks", columns: []string{"subject_type", "subject_key", "id"}},
	)
}

type s3Account2026090101 struct {
	bun.BaseModel `bun:"table:s3_accounts"`

	AccessKey string    `bun:"type:text,pk"`
	SecretKey string    `bun:"type:text,notnull"`
	Role      string    `bun:"type:text,notnull"`
	IsRoot    bool      `bun:",notnull,default:false"`
	CreatedAt time.Time `bun:",notnull"`
	UpdatedAt time.Time `bun:",notnull"`
}

type bucket2026090101 struct {
	bun.BaseModel `bun:"table:buckets"`

	ID                   int64  `bun:",pk,autoincrement,identity"`
	Name                 string `bun:"type:text,notnull,unique"`
	ACL                  []byte
	OwnerAccessKey       *string `bun:"type:text"`
	DefaultCopies        int     `bun:"type:integer,notnull"`
	MinimumDurableCopies int     `bun:"type:integer,notnull"`
	DurabilityGeneration int64   `bun:",notnull,default:0"`
	DurabilityTaskID     *int64
	Status               string    `bun:"type:text,notnull,default:'provisioning'"`
	CreatedAt            time.Time `bun:",notnull"`
	UpdatedAt            time.Time `bun:",notnull"`
}

// bucketReplicaSlot2026090101 gives a replica slot a row of its own. copy_index
// used to be a bare integer repeated across five tables and bounded only by a
// range check; as a table it becomes a foreign key target, and a slot keeps its
// history because retired generations still reference it.
type bucketReplicaSlot2026090101 struct {
	bun.BaseModel `bun:"table:bucket_replica_slots"`

	ID        int64     `bun:",pk,autoincrement,identity"`
	BucketID  int64     `bun:",notnull"`
	CopyIndex int       `bun:"type:integer,notnull"`
	Status    string    `bun:"type:text,notnull,default:'active'"`
	CreatedAt time.Time `bun:",notnull"`
	UpdatedAt time.Time `bun:",notnull"`
}

type object2026090101 struct {
	bun.BaseModel `bun:"table:objects"`

	ID       int64  `bun:",pk,autoincrement,identity"`
	BucketID int64  `bun:",notnull"`
	Key      string `bun:"type:text,notnull"`
	// CurrentVersionID points at the version S3 reads serve. One column can hold
	// one value, so "at most one current version" is structural here rather than
	// a partial unique index over every version of the object.
	CurrentVersionID *string   `bun:"type:text"`
	CreatedAt        time.Time `bun:",notnull"`
	UpdatedAt        time.Time `bun:",notnull"`
}

type multipartUpload2026090101 struct {
	bun.BaseModel `bun:"table:multipart_uploads"`

	UploadID    string          `bun:"type:text,pk"`
	BucketID    int64           `bun:",notnull"`
	Key         string          `bun:"type:text,notnull"`
	ContentType string          `bun:"type:text,notnull,default:'application/octet-stream'"`
	Metadata    json.RawMessage `bun:"type:jsonb,notnull,default:'{}'"`
	Status      string          `bun:"type:text,notnull,default:'initiated'"`
	CreatedAt   time.Time       `bun:",notnull"`
	UpdatedAt   time.Time       `bun:",notnull"`
}

type multipartPart2026090101 struct {
	bun.BaseModel `bun:"table:multipart_parts"`

	ID         int64     `bun:",pk,autoincrement,identity"`
	UploadID   string    `bun:"type:text,notnull"`
	PartNumber int       `bun:"type:integer,notnull"`
	Size       int64     `bun:",notnull"`
	ETag       string    `bun:"e_tag,type:text,notnull"`
	Checksum   *string   `bun:"type:text"`
	CreatedAt  time.Time `bun:",notnull"`
}

func createCoreRootSchema(ctx context.Context, db bun.IDB) error {
	tables := []initialTableSpec{
		{
			name:  "s3_accounts",
			model: (*s3Account2026090101)(nil),
			constraints: []string{
				"CONSTRAINT chk_s3_accounts_identity CHECK (access_key <> '' AND secret_key <> '')",
				"CONSTRAINT chk_s3_accounts_role CHECK (role IN ('admin', 'user', 'userplus'))",
			},
		},
		{
			name:  "buckets",
			model: (*bucket2026090101)(nil),
			constraints: []string{
				"CONSTRAINT chk_buckets_identity CHECK (name <> '' AND (owner_access_key IS NULL OR owner_access_key <> ''))",
				"CONSTRAINT chk_buckets_status CHECK (status IN ('provisioning', 'ready'))",
				// The durability policy is materialised from configuration when
				// the bucket is created, so "how many replicas does this bucket
				// want" never depends on reading config at query time.
				"CONSTRAINT chk_buckets_default_copies CHECK (default_copies BETWEEN 1 AND 8)",
				"CONSTRAINT chk_buckets_minimum_durable_copies CHECK (minimum_durable_copies BETWEEN 1 AND 8)",
				"CONSTRAINT chk_buckets_explicit_copy_policy CHECK (minimum_durable_copies <= default_copies)",
				"CONSTRAINT chk_buckets_durability_generation CHECK (durability_generation >= 0)",
			},
			foreignKeys: []string{
				"(owner_access_key) REFERENCES s3_accounts (access_key) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(durability_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
		},
		{
			name:  "bucket_replica_slots",
			model: (*bucketReplicaSlot2026090101)(nil),
			constraints: []string{
				"CONSTRAINT uq_bucket_replica_slots_identity UNIQUE (bucket_id, copy_index)",
				"CONSTRAINT chk_bucket_replica_slots_copy_index CHECK (copy_index BETWEEN 0 AND 7)",
				// A shrunk slot is decommissioned, never deleted: retired data
				// set generations still point at it.
				"CONSTRAINT chk_bucket_replica_slots_status CHECK (status IN ('active', 'decommissioned'))",
			},
			foreignKeys: []string{
				"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
		},
		{
			name:  "objects",
			model: (*object2026090101)(nil),
			constraints: []string{
				"CONSTRAINT uq_objects_identity UNIQUE (id, bucket_id, key)",
				"CONSTRAINT chk_objects_identity CHECK (key <> '')",
			},
			foreignKeys: []string{
				"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
			forwardForeignKeys: []initialForwardForeignKey{objectCurrentVersionForeignKey2026090101()},
		},
		{
			name:        "multipart_uploads",
			model:       (*multipartUpload2026090101)(nil),
			jsonColumns: initialJSONColumns("multipart_uploads"),
			constraints: []string{
				"CONSTRAINT uq_multipart_uploads_identity UNIQUE (upload_id, bucket_id, key)",
				"CONSTRAINT chk_multipart_uploads_identity CHECK (key <> '' AND upload_id <> '')",
				"CONSTRAINT chk_multipart_uploads_status CHECK (status IN ('initiated', 'completing', 'completed', 'aborted'))",
			},
			foreignKeys: []string{
				"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
		},
		{
			name:  "multipart_parts",
			model: (*multipartPart2026090101)(nil),
			constraints: []string{
				"CONSTRAINT chk_multipart_parts_identity CHECK (upload_id <> '' AND e_tag <> '' AND (checksum IS NULL OR checksum <> ''))",
				"CONSTRAINT chk_multipart_parts_part_number CHECK (part_number BETWEEN 1 AND 10000)",
				"CONSTRAINT chk_multipart_parts_size CHECK (size >= 0)",
			},
			foreignKeys: []string{
				"(upload_id) REFERENCES multipart_uploads (upload_id) ON UPDATE RESTRICT ON DELETE CASCADE",
			},
		},
	}
	for _, table := range tables {
		if err := createInitialTable(ctx, db, table); err != nil {
			return err
		}
	}
	indexes := []initialIndexSpec{
		{name: "idx_objects_bucket_key", table: "objects", columns: []string{"bucket_id", "key"}, unique: true},
		{name: "idx_objects_current_version", table: "objects", columns: []string{"current_version_id"}, where: "current_version_id IS NOT NULL"},
		{name: "idx_s3_accounts_single_root", table: "s3_accounts", columns: []string{"is_root"}, where: "is_root = TRUE", unique: true},
		{name: "idx_buckets_owner_access_key", table: "buckets", columns: []string{"owner_access_key"}},
		{name: "idx_buckets_durability_task", table: "buckets", columns: []string{"durability_task_id"}, where: "durability_task_id IS NOT NULL", unique: true},
		{name: "idx_multipart_parts_upload_part", table: "multipart_parts", columns: []string{"upload_id", "part_number"}, unique: true},
	}
	if db.Dialect().Name() != dialect.PG {
		indexes = append(indexes, initialIndexSpec{name: "idx_multipart_uploads_bucket_status_key_upload", table: "multipart_uploads", columns: []string{"bucket_id", "status", "key", "upload_id"}})
	}
	return createInitialIndexes(ctx, db, indexes...)
}

// objectVersion2026090101 carries S3 naming semantics only. Pipeline state is
// a function of the copy rows and stays a query; cache residency belongs to
// object_cache; the bytes themselves are storage_contents. A NULL content_id
// is exactly a delete marker.
type objectVersion2026090101 struct {
	bun.BaseModel `bun:"table:object_versions"`

	VersionID         string `bun:"type:text,pk"`
	ObjectID          int64  `bun:",notnull"`
	BucketID          int64  `bun:",notnull"`
	Key               string `bun:"type:text,notnull"`
	ContentID         *int64
	Size              int64           `bun:",notnull"`
	ETag              string          `bun:"e_tag,type:text,notnull"`
	ContentType       string          `bun:"type:text,notnull,default:'application/octet-stream'"`
	Metadata          json.RawMessage `bun:"type:jsonb,notnull,default:'{}'"`
	MultipartUploadID *string         `bun:"type:text"`
	IsDeleteMarker    bool            `bun:",notnull,default:false"`
	CreatedAt         time.Time       `bun:",notnull"`
	UpdatedAt         time.Time       `bun:",notnull"`
}

// objectCache2026090101 records local cache residency per content, not per
// version, so identical bytes written under several keys share one file. The
// cache key is derived from content_id and therefore not stored.
type objectCache2026090101 struct {
	bun.BaseModel `bun:"table:object_cache"`

	ContentID                int64 `bun:",pk"`
	InCache                  bool  `bun:",notnull"`
	CacheAccessedAt          *time.Time
	CachePresenceGeneration  int64 `bun:",notnull,default:0"`
	CacheOperationGeneration int64 `bun:",notnull,default:0"`
	CacheActiveTaskID        *int64
	CreatedAt                time.Time `bun:",notnull"`
	UpdatedAt                time.Time `bun:",notnull"`
}

// objectDeletion2026090101 is an append-only tombstone. Cache cleanup is not
// tracked here: with content-keyed residency the trigger is the content's
// reference count reaching zero, not the removal of one version.
type objectDeletion2026090101 struct {
	bun.BaseModel `bun:"table:object_deletions"`

	ID        int64  `bun:",pk,autoincrement,identity"`
	BucketID  int64  `bun:",notnull"`
	ObjectID  int64  `bun:",notnull"`
	Key       string `bun:"type:text,notnull"`
	VersionID string `bun:"type:text,notnull,unique"`
	ContentID *int64
	Size      int64     `bun:",notnull"`
	DeletedAt time.Time `bun:",notnull"`
}

func createObjectLifecycleSchema(ctx context.Context, db bun.IDB) error {
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:        "object_versions",
		model:       (*objectVersion2026090101)(nil),
		jsonColumns: initialJSONColumns("object_versions"),
		constraints: []string{
			"CONSTRAINT chk_object_versions_identity CHECK (version_id <> '' AND key <> '' AND (multipart_upload_id IS NULL OR multipart_upload_id <> ''))",
			"CONSTRAINT chk_object_versions_size CHECK (size >= 0)",
			// Candidate key for the objects.current_version_id pointer, which
			// names both the version and the object it must belong to.
			"CONSTRAINT uq_object_versions_object_identity UNIQUE (version_id, object_id)",
			// A delete marker is exactly a version with no content.
			"CONSTRAINT chk_object_versions_delete_marker_shape CHECK ((is_delete_marker = TRUE AND content_id IS NULL AND size = 0 AND e_tag = '' AND content_type = '') OR (is_delete_marker = FALSE AND content_id IS NOT NULL AND e_tag <> ''))",
			"CONSTRAINT fk_object_versions_object FOREIGN KEY (object_id, bucket_id, key) REFERENCES objects (id, bucket_id, key) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
		foreignKeys: []string{
			"(multipart_upload_id, bucket_id, key) REFERENCES multipart_uploads (upload_id, bucket_id, key) ON UPDATE RESTRICT ON DELETE RESTRICT",
			// size repeats the content size so listings need no join; the
			// composite key makes that repetition unable to drift.
			"(content_id, bucket_id, size) REFERENCES storage_contents (id, bucket_id, content_size) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}); err != nil {
		return err
	}
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:  "object_cache",
		model: (*objectCache2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_object_cache_generation CHECK (cache_presence_generation >= 0 AND cache_operation_generation >= 0)",
		},
		foreignKeys: []string{
			"(content_id) REFERENCES storage_contents (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(cache_active_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}); err != nil {
		return err
	}
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:  "object_deletions",
		model: (*objectDeletion2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_object_deletions_identity CHECK (key <> '' AND version_id <> '')",
			"CONSTRAINT chk_object_deletions_size CHECK (size >= 0)",
		},
		// The tombstone keeps content_id as a value: the content row is deleted
		// once its cleanup finishes.
		foreignKeys: []string{
			"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}); err != nil {
		return err
	}
	indexes := []initialIndexSpec{
		{name: "idx_object_versions_object_created", table: "object_versions", columns: []string{"object_id", "created_at DESC", "version_id DESC"}},
		// Content reference count and foreign key coverage in one index.
		{name: "idx_object_versions_content", table: "object_versions", columns: []string{"content_id"}},
		{name: "idx_object_versions_multipart_upload", table: "object_versions", columns: []string{"multipart_upload_id"}},
		{name: "idx_object_cache_lru", table: "object_cache", columns: []string{"cache_accessed_at", "content_id"}, where: "in_cache = TRUE"},
		{name: "idx_object_cache_active_task", table: "object_cache", columns: []string{"cache_active_task_id"}, where: "cache_active_task_id IS NOT NULL", unique: true},
		{name: "idx_object_deletions_bucket_key_deleted", table: "object_deletions", columns: []string{"bucket_id", "key", "deleted_at"}},
		{name: "idx_object_deletions_content", table: "object_deletions", columns: []string{"content_id"}},
		{name: "idx_object_deletions_bucket_deleted", table: "object_deletions", columns: []string{"bucket_id", "deleted_at DESC", "id DESC"}},
	}
	if db.Dialect().Name() != dialect.PG {
		indexes = append(indexes, initialIndexSpec{name: "idx_object_versions_bucket_key_created", table: "object_versions", columns: []string{"bucket_id", "key", "created_at DESC", "version_id DESC"}})
	}
	if err := createInitialIndexes(ctx, db, indexes...); err != nil {
		return err
	}
	// objects was created before object_versions existed, so PostgreSQL takes
	// the pointer constraint here.
	return addForwardForeignKey(ctx, db, "objects", objectCurrentVersionForeignKey2026090101())
}

// objectCurrentVersionForeignKey2026090101 keeps an object from pointing at a
// version of some other object, and keeps the pointed-at version from being
// deleted before the pointer moves.
func objectCurrentVersionForeignKey2026090101() initialForwardForeignKey {
	return initialForwardForeignKey{
		name:       "fk_objects_current_version",
		definition: "(current_version_id, id) REFERENCES object_versions (version_id, object_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
	}
}

func createPostgresPrefixIndexes(ctx context.Context, db bun.IDB) error {
	if db.Dialect().Name() != dialect.PG {
		return nil
	}
	return createInitialIndexes(ctx, db,
		// Listing current objects orders by a C-collated key, so the objects
		// unique index needs a C-collated companion to drive it.
		initialIndexSpec{name: "idx_objects_bucket_key_c", table: "objects", columns: []string{"bucket_id", `(key COLLATE "C")`}},
		initialIndexSpec{name: "idx_object_versions_bucket_key_created", table: "object_versions", columns: []string{"bucket_id", `(key COLLATE "C")`, "created_at DESC", "version_id DESC"}},
		initialIndexSpec{name: "idx_multipart_uploads_bucket_status_key_upload", table: "multipart_uploads", columns: []string{"bucket_id", "status", `(key COLLATE "C")`, "upload_id"}},
	)
}

// storageContent2026090101 is the identity of one bucket-scoped byte payload.
// Durability, pipeline state and ingress progress are deliberately absent: the
// first two are functions of the copy rows and stay queries, while progress
// belongs to the concrete ingress transfer that produced it.
type storageContent2026090101 struct {
	bun.BaseModel `bun:"table:storage_contents"`

	ID                int64   `bun:",pk,autoincrement,identity"`
	BucketID          int64   `bun:",notnull"`
	Checksum          string  `bun:"type:text,notnull"`
	ContentSize       int64   `bun:",notnull"`
	PieceCID          *string `bun:"type:text"`
	RequestedCopies   int     `bun:"type:integer,notnull"`
	ErrorMessage      *string `bun:"type:text"`
	AcceptedAt        *time.Time
	CleanupGeneration int64 `bun:",notnull,default:0"`
	CleanupTaskID     *int64
	CreatedAt         time.Time `bun:",notnull"`
	UpdatedAt         time.Time `bun:",notnull"`
}

type storageDataSet2026090101 struct {
	bun.BaseModel `bun:"table:storage_data_sets"`

	ID                   int64   `bun:",pk,autoincrement,identity"`
	BucketID             int64   `bun:",notnull"`
	ProviderID           string  `bun:"type:text,notnull"`
	CopyIndex            int     `bun:"type:integer,notnull"`
	Generation           int64   `bun:",notnull,default:1"`
	IsCurrent            bool    `bun:",notnull"`
	DataSetID            *string `bun:"type:text"`
	ClientDataSetID      *string `bun:"type:text"`
	Status               string  `bun:"type:text,notnull,default:'pending'"`
	CreateTransactionID  *string `bun:"type:text"`
	CreateStatusURL      *string `bun:"type:text"`
	CreatedByContentID   *int64
	LastUsedContentID    *int64
	LastError            *string `bun:"type:text"`
	EnsureTaskID         *int64
	RetirementGeneration int64 `bun:",notnull,default:0"`
	RetirementTaskID     *int64
	CreatedAt            time.Time `bun:",notnull"`
	UpdatedAt            time.Time `bun:",notnull"`
}

type storageCopy2026090101 struct {
	bun.BaseModel `bun:"table:storage_copies"`

	ID        int64 `bun:",pk,autoincrement,identity"`
	ContentID int64 `bun:",notnull"`
	BucketID  int64 `bun:",notnull"`
	// ContentSize repeats storage_contents.content_size so the ingress bound
	// stays a local CHECK. A composite foreign key keeps the copy from
	// drifting away from the content it transfers.
	ContentSize        int64   `bun:",notnull"`
	StorageDataSetID   int64   `bun:",notnull"`
	CopyIndex          int     `bun:"type:integer,notnull"`
	ProviderID         string  `bun:"type:text,notnull"`
	PieceID            *string `bun:"type:text"`
	TransferMethod     string  `bun:"type:text,notnull"`
	Status             string  `bun:"type:text,notnull,default:'pending'"`
	RetrievalURL       *string `bun:"type:text"`
	CommitExtraDataHex *string `bun:"type:text"`
	CommitReadyAt      *time.Time
	// The confirmed commit attempt this copy projects. Its status is repeated so
	// a composite foreign key can require the referenced attempt to be
	// confirmed, which is what keeps the projection from drifting.
	ConfirmedAttemptID      *string `bun:"type:text"`
	ConfirmedAttemptStatus  *string `bun:"type:text"`
	IngressBytesTransferred int64   `bun:",notnull,default:0"`
	IngressStoreAttempt     int     `bun:"type:integer,notnull,default:0"`
	ProgressUpdatedAt       *time.Time
	WorkGeneration          int64 `bun:",notnull,default:0"`
	ActiveTaskID            *int64
	LastError               *string   `bun:"type:text"`
	CreatedAt               time.Time `bun:",notnull"`
	UpdatedAt               time.Time `bun:",notnull"`
}

type storageCommitAttempt2026090101 struct {
	bun.BaseModel `bun:"table:storage_commit_attempts"`

	AttemptID              string  `bun:"type:text,pk"`
	ContentID              int64   `bun:",notnull"`
	StorageDataSetID       int64   `bun:",notnull"`
	Status                 string  `bun:"type:text,notnull,default:'reserved'"`
	ExtraDataHex           *string `bun:"type:text"`
	TransactionID          *string `bun:"type:text"`
	StatusURL              *string `bun:"type:text"`
	ConfirmedTransactionID *string `bun:"type:text"`
	AttentionCode          *string `bun:"type:text"`
	AttentionAt            *time.Time
	ReleaseReason          *string `bun:"type:text"`
	LastError              *string `bun:"type:text"`
	AttemptedAt            *time.Time
	ResolvedAt             *time.Time
	CreatedAt              time.Time `bun:",notnull"`
	UpdatedAt              time.Time `bun:",notnull"`
}

// storagePullAttempt2026090101 is the ledger of provider-side copy requests.
// Every source column is NOT NULL, which replaces the all-or-nothing check that
// used to guard five nullable columns on the copy.
type storagePullAttempt2026090101 struct {
	bun.BaseModel `bun:"table:storage_pull_attempts"`

	AttemptID          string    `bun:"type:text,pk"`
	ContentID          int64     `bun:",notnull"`
	StorageDataSetID   int64     `bun:",notnull"`
	Status             string    `bun:"type:text,notnull"`
	SourceProviderID   string    `bun:"type:text,notnull"`
	SourceDataSetID    string    `bun:"type:text,notnull"`
	SourcePieceID      string    `bun:"type:text,notnull"`
	SourcePieceCID     string    `bun:"type:text,notnull"`
	SourceRetrievalURL string    `bun:"type:text,notnull"`
	LastError          *string   `bun:"type:text"`
	AttemptedAt        time.Time `bun:",notnull"`
	ResolvedAt         *time.Time
	CreatedAt          time.Time `bun:",notnull"`
	UpdatedAt          time.Time `bun:",notnull"`
}

type storageReplacement2026090101 struct {
	bun.BaseModel `bun:"table:storage_replacements"`

	ID                   int64   `bun:",pk,autoincrement,identity"`
	BucketID             int64   `bun:",notnull"`
	CopyIndex            int     `bun:"type:integer,notnull"`
	SourceDataSetID      int64   `bun:",notnull"`
	TargetDataSetID      int64   `bun:",notnull"`
	SelectionMode        string  `bun:"type:text,notnull"`
	RequestedProviderID  *string `bun:"type:text"`
	ClientRequestID      string  `bun:"type:text,notnull"`
	PriceListFingerprint string  `bun:"type:text,notnull"`
	Status               string  `bun:"type:text,notnull"`
	WaitReason           *string `bun:"type:text"`
	FailureReason        *string `bun:"type:text"`
	LastError            *string `bun:"type:text"`
	ItemsTotal           int     `bun:"type:integer,notnull,default:0"`
	ItemsCopied          int     `bun:"type:integer,notnull,default:0"`
	SeedCursorContentID  int64   `bun:",notnull,default:0"`
	SeedingComplete      bool    `bun:",notnull,default:false"`
	TaskGeneration       int64   `bun:",notnull,default:1"`
	TaskID               *int64
	SupersededByID       *int64
	CreatedAt            time.Time `bun:",notnull"`
	UpdatedAt            time.Time `bun:",notnull"`
}

// storageDataSetTermination2026090101 records one data set's end of term. A
// replacement terminates the source it replaces and, once superseded, the
// target it abandoned; a third kind of termination adds a row here instead of
// another repeated column group on storage_replacements.
type storageDataSetTermination2026090101 struct {
	bun.BaseModel `bun:"table:storage_data_set_terminations"`

	ID            int64  `bun:",pk,autoincrement,identity"`
	ReplacementID int64  `bun:",notnull"`
	Role          string `bun:"type:text,notnull"`
	// Exactly one of the data set columns is set, chosen by role. Each carries a
	// composite foreign key back to the matching column on the replacement, so a
	// row cannot name a data set the replacement never held in that role.
	SourceDataSetID          *int64
	AbandonedTargetDataSetID *int64
	TxHash                   *string   `bun:"type:text"`
	Epoch                    int64     `bun:",notnull"`
	CreatedAt                time.Time `bun:",notnull"`
	UpdatedAt                time.Time `bun:",notnull"`
}

func storageDataSetTerminationTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_data_set_terminations",
		model: (*storageDataSetTermination2026090101)(nil),
		constraints: []string{
			"CONSTRAINT uq_storage_data_set_terminations_role UNIQUE (replacement_id, role)",
			"CONSTRAINT chk_storage_data_set_terminations_role CHECK (role IN ('source', 'abandoned_target'))",
			"CONSTRAINT chk_storage_data_set_terminations_subject CHECK ((role = 'source') = (source_data_set_id IS NOT NULL) AND (role = 'abandoned_target') = (abandoned_target_data_set_id IS NOT NULL))",
			"CONSTRAINT chk_storage_data_set_terminations_epoch CHECK (epoch >= 0)",
			"CONSTRAINT chk_storage_data_set_terminations_tx_hash CHECK (tx_hash IS NULL OR tx_hash <> '')",
		},
		foreignKeys: []string{
			"(replacement_id) REFERENCES storage_replacements (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(replacement_id, source_data_set_id) REFERENCES storage_replacements (id, source_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(replacement_id, abandoned_target_data_set_id) REFERENCES storage_replacements (id, target_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

type storageReplacementItem2026090101 struct {
	bun.BaseModel `bun:"table:storage_replacement_items"`

	ID            int64 `bun:",pk,autoincrement,identity"`
	ReplacementID int64 `bun:",notnull"`
	ContentID     int64 `bun:",notnull"`
	// TargetDataSetID is written when the item is seeded, together with the
	// pending copy it names. A nullable column would make the composite foreign
	// key below skip validation entirely whenever it was unset.
	TargetDataSetID int64     `bun:",notnull"`
	Status          string    `bun:"type:text,notnull,default:'pending'"`
	LastError       *string   `bun:"type:text"`
	CreatedAt       time.Time `bun:",notnull"`
	UpdatedAt       time.Time `bun:",notnull"`
}

type storageCleanupCopy2026090101 struct {
	bun.BaseModel `bun:"table:storage_cleanup_copies"`

	ID               int64   `bun:",pk,autoincrement,identity"`
	ContentID        int64   `bun:",notnull"`
	BucketID         int64   `bun:",notnull"`
	CopyIndex        int     `bun:"type:integer,notnull"`
	ProviderID       string  `bun:"type:text,notnull"`
	StorageDataSetID int64   `bun:",notnull"`
	DataSetID        *string `bun:"type:text"`
	ClientDataSetID  *string `bun:"type:text"`
	PieceID          string  `bun:"type:text,notnull"`
	PieceCID         string  `bun:"type:text,notnull"`
	Checksum         string  `bun:"type:text,notnull"`
	RetrievalURL     *string `bun:"type:text"`
	Status           string  `bun:"type:text,notnull,default:'pending'"`
	DeleteTxHash     *string `bun:"type:text"`
	LastError        *string `bun:"type:text"`
	ScheduledAt      *time.Time
	RemovedAt        *time.Time
	CreatedAt        time.Time `bun:",notnull"`
	UpdatedAt        time.Time `bun:",notnull"`
}

func createStorageSchema(ctx context.Context, db bun.IDB) error {
	checksumCheck := "length(checksum) = 64 AND checksum NOT GLOB '*[^0-9a-f]*'"
	if db.Dialect().Name() == dialect.PG {
		checksumCheck = "checksum ~ '^[0-9a-f]{64}$'"
	}
	tables := []initialTableSpec{
		{
			name:  "storage_contents",
			model: (*storageContent2026090101)(nil),
			constraints: []string{
				// Content dedup is a unique-key lookup, not an index scan.
				"CONSTRAINT uq_storage_contents_bytes UNIQUE (bucket_id, checksum, content_size)",
				// Candidate keys for the composite foreign keys that pin
				// denormalized identity on object_versions and storage_copies.
				"CONSTRAINT uq_storage_contents_id_bucket UNIQUE (id, bucket_id)",
				"CONSTRAINT uq_storage_contents_addr UNIQUE (id, bucket_id, content_size)",
				"CONSTRAINT chk_storage_contents_identity CHECK ((" + checksumCheck + ") AND (piece_cid IS NULL OR piece_cid <> ''))",
				"CONSTRAINT chk_storage_contents_content_size CHECK (content_size >= 0)",
				"CONSTRAINT chk_storage_contents_requested_copies CHECK (requested_copies BETWEEN 1 AND 8)",
				"CONSTRAINT chk_storage_contents_cleanup_generation CHECK (cleanup_generation >= 0)",
			},
			foreignKeys: []string{
				"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(cleanup_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
		},
		{
			name:  "storage_data_sets",
			model: (*storageDataSet2026090101)(nil),
			constraints: []string{
				"CONSTRAINT uq_storage_data_sets_id_bucket_slot UNIQUE (id, bucket_id, copy_index)",
				"CONSTRAINT uq_storage_data_sets_identity UNIQUE (id, bucket_id, copy_index, provider_id)",
				"CONSTRAINT chk_storage_data_sets_identity CHECK (provider_id <> '' AND (data_set_id IS NULL OR data_set_id <> '') AND (client_data_set_id IS NULL OR client_data_set_id <> '') AND (create_transaction_id IS NULL OR create_transaction_id <> '') AND (create_status_url IS NULL OR create_status_url <> ''))",
				// The replica slot is a row, so the index is a foreign key rather than a range check.
				"CONSTRAINT fk_storage_data_sets_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"CONSTRAINT chk_storage_data_sets_generation CHECK (generation >= 1 AND retirement_generation >= 0)",
				"CONSTRAINT chk_storage_data_sets_status CHECK (status IN ('pending', 'creating', 'ready', 'failed', 'draining', 'retired'))",
				"CONSTRAINT chk_storage_data_sets_ready_identity CHECK (status <> 'ready' OR data_set_id IS NOT NULL)",
				"CONSTRAINT chk_storage_data_sets_current_shape CHECK (status NOT IN ('failed', 'draining', 'retired') OR is_current = FALSE)",
			},
			foreignKeys: []string{
				"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(created_by_content_id, bucket_id) REFERENCES storage_contents (id, bucket_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(last_used_content_id, bucket_id) REFERENCES storage_contents (id, bucket_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(ensure_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(retirement_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
		},
		{
			name:  "storage_copies",
			model: (*storageCopy2026090101)(nil),
			constraints: []string{
				"CONSTRAINT uq_storage_copies_content_data_set UNIQUE (content_id, storage_data_set_id)",
				// The replica slot is a row, so the index is a foreign key rather than a range check.
				"CONSTRAINT fk_storage_copies_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"CONSTRAINT chk_storage_copies_work_generation CHECK (work_generation >= 0)",
				"CONSTRAINT chk_storage_copies_status CHECK (status IN ('pending', 'piece_ready', 'committing', 'committed', 'failed'))",
				// The projected status is pinned to a literal so the composite
				// foreign key below can only ever reach a confirmed attempt.
				"CONSTRAINT chk_storage_copies_confirmed_attempt_status CHECK (confirmed_attempt_status IS NULL OR confirmed_attempt_status = 'confirmed')",
				// Committed and "has confirmed evidence" are the same fact. The
				// foreign key alone would still allow a committed copy with no
				// evidence at all, so both directions are stated here.
				"CONSTRAINT chk_storage_copies_committed_evidence CHECK ((status = 'committed') = (confirmed_attempt_id IS NOT NULL))",
				"CONSTRAINT chk_storage_copies_confirmed_attempt_shape CHECK ((confirmed_attempt_id IS NULL AND confirmed_attempt_status IS NULL) OR (confirmed_attempt_id IS NOT NULL AND confirmed_attempt_id <> '' AND confirmed_attempt_status IS NOT NULL))",
				"CONSTRAINT chk_storage_copies_transfer_method CHECK (transfer_method IN ('ingress', 'peer_pull', 'cache_restore'))",
				"CONSTRAINT chk_storage_copies_optional_identity CHECK (provider_id <> '' AND (piece_id IS NULL OR piece_id <> '') AND (retrieval_url IS NULL OR retrieval_url <> '') AND (commit_extra_data_hex IS NULL OR commit_extra_data_hex <> ''))",
				"CONSTRAINT chk_storage_copies_committed_shape CHECK (status <> 'committed' OR (piece_id IS NOT NULL AND piece_id <> '' AND retrieval_url IS NOT NULL AND retrieval_url <> ''))",
				"CONSTRAINT chk_storage_copies_commit_ready CHECK (commit_ready_at IS NULL OR status IN ('piece_ready', 'committing', 'committed'))",
				"CONSTRAINT chk_storage_copies_content_size CHECK (content_size >= 0)",
				// Store progress belongs to the transfer that produces it;
				// peer-pull copies never carry it.
				"CONSTRAINT chk_storage_copies_ingress_progress CHECK (transfer_method IN ('ingress', 'cache_restore') OR (ingress_bytes_transferred = 0 AND ingress_store_attempt = 0 AND progress_updated_at IS NULL))",
				"CONSTRAINT chk_storage_copies_ingress_bytes CHECK (ingress_bytes_transferred >= 0 AND ingress_bytes_transferred <= content_size)",
				"CONSTRAINT chk_storage_copies_ingress_attempt CHECK (ingress_store_attempt >= 0)",
			},
			foreignKeys: []string{
				"(content_id, bucket_id, content_size) REFERENCES storage_contents (id, bucket_id, content_size) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(storage_data_set_id, bucket_id, copy_index, provider_id) REFERENCES storage_data_sets (id, bucket_id, copy_index, provider_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(active_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
			forwardForeignKeys: []initialForwardForeignKey{storageCopyConfirmedAttemptForeignKey2026090101()},
		},
		storageCommitAttemptTable2026090101(),
		storagePullAttemptTable2026090101(),
		storageReplacementTable2026090101(),
		storageDataSetTerminationTable2026090101(),
		storageReplacementItemTable2026090101(),
		storageCleanupCopyTable2026090101(),
	}
	for _, table := range tables {
		if err := createInitialTable(ctx, db, table); err != nil {
			return err
		}
	}
	if err := createInitialIndexes(ctx, db, storageIndexes2026090101()...); err != nil {
		return err
	}
	// storage_copies was created before storage_commit_attempts existed, so
	// PostgreSQL takes the projection constraint here.
	return addForwardForeignKey(ctx, db, "storage_copies", storageCopyConfirmedAttemptForeignKey2026090101())
}

// storageCopyConfirmedAttemptForeignKey2026090101 welds the copy's committed
// state to the ledger row that proves it. storage_copies is created before
// storage_commit_attempts, so the constraint is forward-declared.
func storageCopyConfirmedAttemptForeignKey2026090101() initialForwardForeignKey {
	return initialForwardForeignKey{
		name:       "fk_storage_copies_confirmed_attempt",
		definition: "(confirmed_attempt_id, confirmed_attempt_status) REFERENCES storage_commit_attempts (attempt_id, status) ON UPDATE RESTRICT ON DELETE RESTRICT",
	}
}

func storageCommitAttemptTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_commit_attempts",
		model: (*storageCommitAttempt2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_commit_attempts_identity CHECK (attempt_id <> '' AND (extra_data_hex IS NULL OR extra_data_hex <> '') AND (transaction_id IS NULL OR transaction_id <> '') AND (status_url IS NULL OR status_url <> '') AND (confirmed_transaction_id IS NULL OR confirmed_transaction_id <> '') AND (attention_code IS NULL OR attention_code <> '') AND (release_reason IS NULL OR release_reason <> ''))",
			"CONSTRAINT chk_storage_commit_attempts_status CHECK (status IN ('reserved', 'attempted', 'confirmed', 'released', 'rejected'))",
			// Candidate key for the copy's confirmed-attempt projection.
			"CONSTRAINT uq_storage_commit_attempts_status UNIQUE (attempt_id, status)",
			"CONSTRAINT chk_storage_commit_attempts_resolution CHECK ((status IN ('reserved', 'attempted') AND resolved_at IS NULL) OR (status IN ('confirmed', 'released', 'rejected') AND resolved_at IS NOT NULL))",
			`CONSTRAINT chk_storage_commit_attempts_evidence_shape CHECK (
				(status = 'reserved' AND attempted_at IS NULL AND extra_data_hex IS NULL AND transaction_id IS NULL AND status_url IS NULL AND confirmed_transaction_id IS NULL AND attention_code IS NULL AND attention_at IS NULL AND last_error IS NULL)
				OR (status = 'attempted' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND confirmed_transaction_id IS NULL AND last_error IS NULL)
				OR (status = 'confirmed' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND transaction_id IS NOT NULL AND confirmed_transaction_id IS NOT NULL AND last_error IS NULL)
				OR (status = 'released' AND confirmed_transaction_id IS NULL AND last_error IS NULL AND ((attempted_at IS NULL AND extra_data_hex IS NULL AND transaction_id IS NULL AND status_url IS NULL AND attention_code IS NULL AND attention_at IS NULL) OR (attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL)))
				OR (status = 'rejected' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND confirmed_transaction_id IS NULL AND last_error IS NOT NULL AND last_error <> '')
			)`,
			"CONSTRAINT chk_storage_commit_attempts_submission_evidence CHECK ((transaction_id IS NULL AND status_url IS NULL) OR (transaction_id IS NOT NULL AND status_url IS NOT NULL))",
			"CONSTRAINT chk_storage_commit_attempts_attention CHECK ((attention_code IS NULL AND attention_at IS NULL) OR (attention_code IS NOT NULL AND attention_at IS NOT NULL AND attempted_at IS NOT NULL))",
			"CONSTRAINT chk_storage_commit_attempts_release CHECK ((status = 'released' AND release_reason IS NOT NULL) OR (status <> 'released' AND release_reason IS NULL))",
		},
		// The ledger outlives the copy it was made for: finishing a content's
		// cleanup deletes the copy, and (content_id, storage_data_set_id) stays
		// here as a value.
	}
}

func storagePullAttemptTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_pull_attempts",
		model: (*storagePullAttempt2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_pull_attempts_identity CHECK (attempt_id <> '' AND source_provider_id <> '' AND source_data_set_id <> '' AND source_piece_id <> '' AND source_piece_cid <> '' AND source_retrieval_url <> '')",
			"CONSTRAINT chk_storage_pull_attempts_status CHECK (status IN ('attempted', 'abandoned'))",
			// An error only makes sense on a request nobody will observe again;
			// reopening a copy carries no error string.
			"CONSTRAINT chk_storage_pull_attempts_error CHECK (last_error IS NULL OR (status = 'abandoned' AND last_error <> ''))",
			"CONSTRAINT chk_storage_pull_attempts_abandoned CHECK (status <> 'abandoned' OR resolved_at IS NOT NULL)",
		},
		// Like commit attempts, pull attempts keep the copy identity as values
		// once cleanup has deleted the copy.
	}
}

func storageReplacementTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_replacements",
		model: (*storageReplacement2026090101)(nil),
		constraints: []string{
			"CONSTRAINT uq_storage_replacements_id_source UNIQUE (id, source_data_set_id)",
			"CONSTRAINT uq_storage_replacements_id_target UNIQUE (id, target_data_set_id)",
			"CONSTRAINT chk_storage_replacements_identity CHECK (client_request_id <> '' AND (requested_provider_id IS NULL OR requested_provider_id <> ''))",
			// The replica slot is a row, so the index is a foreign key rather than a range check.
			"CONSTRAINT fk_storage_replacements_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"CONSTRAINT chk_storage_replacements_selection_mode CHECK (selection_mode IN ('automatic', 'manual'))",
			"CONSTRAINT chk_storage_replacements_status CHECK (status IN ('preparing_target', 'migrating', 'waiting', 'retiring', 'cleanup_attention', 'failed', 'completed', 'superseded'))",
			"CONSTRAINT chk_storage_replacements_wait_reason CHECK (wait_reason IS NULL OR wait_reason <> '')",
			"CONSTRAINT chk_storage_replacements_failure_reason CHECK (failure_reason IS NULL OR failure_reason <> '')",
			"CONSTRAINT chk_storage_replacements_client_request_id CHECK (length(client_request_id) BETWEEN 1 AND 128)",
			"CONSTRAINT chk_storage_replacements_distinct_data_sets CHECK (source_data_set_id <> target_data_set_id)",
			"CONSTRAINT chk_storage_replacements_items CHECK (items_total >= 0 AND items_copied >= 0 AND items_copied <= items_total)",
			"CONSTRAINT chk_storage_replacements_generation CHECK (task_generation >= 1)",
		},
		foreignKeys: []string{
			"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(source_data_set_id, bucket_id, copy_index) REFERENCES storage_data_sets (id, bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(target_data_set_id, bucket_id, copy_index) REFERENCES storage_data_sets (id, bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(superseded_by_id) REFERENCES storage_replacements (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

func storageReplacementItemTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_replacement_items",
		model: (*storageReplacementItem2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_replacement_items_status CHECK (status IN ('pending', 'copied', 'cancelled', 'attention'))",
			"CONSTRAINT uq_storage_replacement_items_content UNIQUE (replacement_id, content_id)",
		},
		foreignKeys: []string{
			"(replacement_id) REFERENCES storage_replacements (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(replacement_id, target_data_set_id) REFERENCES storage_replacements (id, target_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
		// content_id stays a value once cleanup deletes the content and its
		// copies; cleanup waits while an item that blocks retirement names it.
	}
}

func storageCleanupCopyTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_cleanup_copies",
		model: (*storageCleanupCopy2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_cleanup_copies_identity CHECK (provider_id <> '' AND piece_id <> '' AND piece_cid <> '' AND checksum <> '' AND (data_set_id IS NULL OR data_set_id <> '') AND (client_data_set_id IS NULL OR client_data_set_id <> '') AND (delete_tx_hash IS NULL OR delete_tx_hash <> ''))",
			// The replica slot is a row, so the index is a foreign key rather than a range check.
			"CONSTRAINT fk_storage_cleanup_copies_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"CONSTRAINT chk_storage_cleanup_copies_status CHECK (status IN ('pending', 'delete_scheduled', 'removed', 'failed', 'unsupported'))",
			"CONSTRAINT chk_storage_cleanup_copies_delete_scheduled CHECK (status <> 'delete_scheduled' OR (delete_tx_hash IS NOT NULL AND scheduled_at IS NOT NULL))",
			"CONSTRAINT uq_storage_cleanup_copies_physical UNIQUE (content_id, storage_data_set_id, piece_id)",
		},
		// The content a cleanup removed is deleted once the cleanup finishes, so
		// its identity and checksum stay here as values rather than a reference.
		foreignKeys: []string{
			"(storage_data_set_id, bucket_id, copy_index, provider_id) REFERENCES storage_data_sets (id, bucket_id, copy_index, provider_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

func storageIndexes2026090101() []initialIndexSpec {
	return []initialIndexSpec{
		{name: "idx_storage_contents_bucket_id", table: "storage_contents", columns: []string{"bucket_id", "id"}},
		{name: "idx_storage_contents_cleanup_task", table: "storage_contents", columns: []string{"cleanup_task_id"}, where: "cleanup_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_data_sets_provider_data_set", table: "storage_data_sets", columns: []string{"provider_id", "data_set_id"}, where: "data_set_id IS NOT NULL", unique: true},
		{name: "idx_storage_data_sets_bucket_copy_current", table: "storage_data_sets", columns: []string{"bucket_id", "copy_index"}, where: "is_current = TRUE", unique: true},
		{name: "idx_storage_data_sets_bucket_copy_generation", table: "storage_data_sets", columns: []string{"bucket_id", "copy_index", "generation"}, unique: true},
		{name: "idx_storage_data_sets_bucket_provider_active", table: "storage_data_sets", columns: []string{"bucket_id", "provider_id"}, where: "status <> 'retired'", unique: true},
		{name: "idx_storage_data_sets_created_by_content", table: "storage_data_sets", columns: []string{"created_by_content_id"}},
		{name: "idx_storage_data_sets_last_used_content", table: "storage_data_sets", columns: []string{"last_used_content_id"}},
		{name: "idx_storage_data_sets_ensure_task", table: "storage_data_sets", columns: []string{"ensure_task_id"}, where: "ensure_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_data_sets_retirement_task", table: "storage_data_sets", columns: []string{"retirement_task_id"}, where: "retirement_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_copies_content_slot", table: "storage_copies", columns: []string{"content_id", "copy_index"}},
		{name: "idx_storage_copies_data_set_identity", table: "storage_copies", columns: []string{"storage_data_set_id", "bucket_id", "copy_index", "provider_id"}},
		{name: "idx_storage_copies_content_transfer_method_index", table: "storage_copies", columns: []string{"content_id", "transfer_method", "copy_index"}},
		{name: "idx_storage_copies_ingress_content", table: "storage_copies", columns: []string{"content_id"}, where: "transfer_method = 'ingress' AND status <> 'failed'", unique: true},
		{name: "idx_storage_copies_status_data_set_content", table: "storage_copies", columns: []string{"status", "storage_data_set_id", "content_id"}},
		{name: "idx_storage_copies_status_piece_identity_content", table: "storage_copies", columns: []string{"status", "provider_id", "piece_id", "content_id"}},
		{name: "idx_storage_copies_commit_ready", table: "storage_copies", columns: []string{"storage_data_set_id", "commit_ready_at", "id"}, where: "status = 'piece_ready' AND commit_ready_at IS NOT NULL"},
		{name: "idx_storage_copies_active_task", table: "storage_copies", columns: []string{"active_task_id"}, where: "active_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_commit_attempts_unresolved_copy", table: "storage_commit_attempts", columns: []string{"content_id", "storage_data_set_id"}, where: "resolved_at IS NULL", unique: true},
		{name: "idx_storage_commit_attempts_unresolved_data_set", table: "storage_commit_attempts", columns: []string{"storage_data_set_id", "created_at", "attempt_id"}, where: "resolved_at IS NULL"},
		{name: "idx_storage_commit_attempts_copy_history", table: "storage_commit_attempts", columns: []string{"content_id", "storage_data_set_id", "created_at DESC", "attempt_id"}},
		{name: "idx_storage_cleanup_copies_replica_slot", table: "storage_cleanup_copies", columns: []string{"bucket_id", "copy_index"}},
		{name: "idx_storage_copies_confirmed_attempt", table: "storage_copies", columns: []string{"confirmed_attempt_id", "confirmed_attempt_status"}, where: "confirmed_attempt_id IS NOT NULL"},
		{name: "idx_storage_copies_replica_slot", table: "storage_copies", columns: []string{"bucket_id", "copy_index"}},
		// One unresolved attempt per copy: a second source can only be tried
		// after the first is abandoned.
		{name: "idx_storage_pull_attempts_unresolved_copy", table: "storage_pull_attempts", columns: []string{"content_id", "storage_data_set_id"}, where: "resolved_at IS NULL", unique: true},
		{name: "idx_storage_pull_attempts_copy_history", table: "storage_pull_attempts", columns: []string{"content_id", "storage_data_set_id", "created_at DESC", "attempt_id"}},
		{name: "idx_storage_replacements_active_source", table: "storage_replacements", columns: []string{"source_data_set_id"}, where: "status NOT IN ('completed', 'superseded')", unique: true},
		{name: "idx_storage_replacements_active_target", table: "storage_replacements", columns: []string{"target_data_set_id"}, where: "status NOT IN ('completed', 'superseded')", unique: true},
		{name: "idx_storage_replacements_active_bucket_slot", table: "storage_replacements", columns: []string{"bucket_id", "copy_index"}, where: "status NOT IN ('completed', 'superseded')", unique: true},
		{name: "idx_storage_replacements_source_identity", table: "storage_replacements", columns: []string{"source_data_set_id", "bucket_id", "copy_index"}},
		{name: "idx_storage_replacements_target_identity", table: "storage_replacements", columns: []string{"target_data_set_id", "bucket_id", "copy_index"}},
		{name: "idx_storage_replacements_superseded_by", table: "storage_replacements", columns: []string{"superseded_by_id"}, where: "superseded_by_id IS NOT NULL"},
		{name: "idx_storage_replacements_bucket_slot", table: "storage_replacements", columns: []string{"bucket_id", "copy_index", "id"}},
		{name: "idx_storage_replacements_bucket_request", table: "storage_replacements", columns: []string{"bucket_id", "client_request_id"}, unique: true},
		{name: "idx_storage_replacements_task", table: "storage_replacements", columns: []string{"task_id"}, where: "task_id IS NOT NULL", unique: true},
		{name: "idx_storage_replacement_items_state", table: "storage_replacement_items", columns: []string{"replacement_id", "status", "id"}},
		{name: "idx_storage_replacement_items_content_id", table: "storage_replacement_items", columns: []string{"content_id"}},
		{name: "idx_storage_replacement_items_target_copy", table: "storage_replacement_items", columns: []string{"content_id", "target_data_set_id"}},
		{name: "idx_storage_cleanup_copies_content_status", table: "storage_cleanup_copies", columns: []string{"content_id", "status", "id"}},
		{name: "idx_storage_cleanup_copies_data_set_identity", table: "storage_cleanup_copies", columns: []string{"storage_data_set_id", "bucket_id", "copy_index", "provider_id"}},
		{name: "idx_storage_cleanup_copies_status_scheduled", table: "storage_cleanup_copies", columns: []string{"status", "scheduled_at", "id"}},
	}
}

type walletOperation2026090101 struct {
	bun.BaseModel `bun:"table:wallet_operations"`

	ID                   int64   `bun:",pk,autoincrement,identity"`
	Type                 string  `bun:"type:text,notnull"`
	ClientRequestID      string  `bun:"type:text,notnull"`
	Amount               string  `bun:"type:text,notnull"`
	Status               string  `bun:"type:text,notnull,default:'pending'"`
	TxHash               *string `bun:"type:text"`
	LastError            *string `bun:"type:text"`
	BroadcastAttemptedAt *time.Time
	TaskID               *int64
	StartedAt            *time.Time
	SubmittedAt          *time.Time
	CompletedAt          *time.Time
	CreatedAt            time.Time `bun:",notnull"`
	UpdatedAt            time.Time `bun:",notnull"`
}

func createWalletSchema(ctx context.Context, db bun.IDB) error {
	amountCheck := `((type = 'approve' AND amount = '0') OR (type IN ('fund', 'withdraw') AND amount GLOB '[1-9]*' AND amount NOT GLOB '*[^0-9]*'))`
	if db.Dialect().Name() == dialect.PG {
		amountCheck = `((type = 'approve' AND amount = '0') OR (type IN ('fund', 'withdraw') AND amount ~ '^[1-9][0-9]*$'))`
	}
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:  "wallet_operations",
		model: (*walletOperation2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_wallet_operations_identity CHECK (client_request_id <> '' AND amount <> '' AND (tx_hash IS NULL OR tx_hash <> ''))",
			"CONSTRAINT chk_wallet_operations_type CHECK (type IN ('fund', 'withdraw', 'approve'))",
			"CONSTRAINT chk_wallet_operations_status CHECK (status IN ('pending', 'submitted', 'confirmed', 'failed', 'unknown'))",
			"CONSTRAINT chk_wallet_operations_submitted_shape CHECK (status <> 'submitted' OR (tx_hash IS NOT NULL AND submitted_at IS NOT NULL))",
			"CONSTRAINT chk_wallet_operations_amount CHECK (" + amountCheck + ")",
			// A uint256 in base 10 is at most 78 digits. The column stays text
			// because the value is returned and compared verbatim.
			"CONSTRAINT chk_wallet_operations_amount_length CHECK (length(amount) BETWEEN 1 AND 78)",
		},
		foreignKeys: []string{
			"(task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}); err != nil {
		return err
	}
	return createInitialIndexes(ctx, db,
		initialIndexSpec{name: "idx_wallet_operations_request", table: "wallet_operations", columns: []string{"type", "client_request_id"}, unique: true},
		initialIndexSpec{name: "idx_wallet_operations_status_created", table: "wallet_operations", columns: []string{"status", "created_at", "id"}},
		initialIndexSpec{name: "idx_wallet_operations_recent", table: "wallet_operations", columns: []string{"created_at DESC", "id DESC"}},
		initialIndexSpec{name: "idx_wallet_operations_task", table: "wallet_operations", columns: []string{"task_id"}, where: "task_id IS NOT NULL", unique: true},
	)
}

type observabilityCollectionState2026090101 struct {
	bun.BaseModel `bun:"table:observability_collection_states"`

	CollectionType string    `bun:"type:text,pk"`
	LastCheckedAt  time.Time `bun:",notnull"`
	CreatedAt      time.Time `bun:",notnull"`
	UpdatedAt      time.Time `bun:",notnull"`
}

type observabilityProviderState2026090101 struct {
	bun.BaseModel `bun:"table:observability_provider_states"`

	ProviderID    string          `bun:"type:text,pk"`
	Status        string          `bun:"type:text,notnull"`
	ReasonCodes   json.RawMessage `bun:"type:jsonb,notnull"`
	Active        *bool
	HasPDP        *bool
	ServiceURL    *string         `bun:"type:text"`
	HealthStatus  *string         `bun:"type:text"`
	LastCheckedAt time.Time       `bun:",notnull"`
	LastAttemptAt time.Time       `bun:",notnull"`
	LastError     *string         `bun:"type:text"`
	Evidence      json.RawMessage `bun:"evidence_json,type:jsonb,notnull"`
}

type providerProfile2026090101 struct {
	bun.BaseModel          `bun:"table:provider_profiles"`
	ProviderID             string          `bun:"type:text,pk"`
	Name                   string          `bun:"type:text,notnull"`
	Description            string          `bun:"type:text,notnull"`
	ServiceProviderAddress string          `bun:"type:text,notnull"`
	PayeeAddress           string          `bun:"type:text,notnull"`
	Active                 bool            `bun:",notnull"`
	ServiceURL             string          `bun:"type:text,notnull"`
	RegistrySnapshot       json.RawMessage `bun:"registry_snapshot_json,type:jsonb,notnull"`
	LastSuccessAt          time.Time       `bun:",notnull"`
}

type providerTierSnapshot2026090101 struct {
	bun.BaseModel `bun:"table:provider_tier_snapshots"`
	Tier          string          `bun:"type:text,pk"`
	ProviderIDs   json.RawMessage `bun:"provider_ids_json,type:jsonb,notnull"`
	CheckedAt     time.Time       `bun:",notnull"`
}

type providerUploadSpeedTest2026090101 struct {
	bun.BaseModel `bun:"table:provider_upload_speed_tests"`

	ProviderID     string `bun:"type:text,pk"`
	State          string `bun:"type:text,notnull"`
	ServiceURLHash string `bun:"type:text,notnull"`
	SampleBytes    int64  `bun:",notnull"`
	DurationMS     *int64
	BytesPerSecond *int64
	TestedAt       *time.Time
	FailureCode    *string `bun:"type:text"`
	ActiveTaskID   *int64
	CreatedAt      time.Time `bun:",notnull"`
	UpdatedAt      time.Time `bun:",notnull"`
}

type observabilityDataSetState2026090101 struct {
	bun.BaseModel `bun:"table:observability_data_set_states"`

	LocalDataSetID  int64   `bun:",pk"`
	BucketID        int64   `bun:",notnull"`
	CopyIndex       int     `bun:"type:integer,notnull"`
	ProviderID      string  `bun:"type:text,notnull"`
	ChainDataSetID  *string `bun:"type:text"`
	ClientDataSetID *string `bun:"type:text"`
	// The bucket's name and the data set's own status are a join away and can
	// disagree with the authorities that own them, so neither is copied here.
	Status           string          `bun:"type:text,notnull"`
	ReasonCodes      json.RawMessage `bun:"type:jsonb,notnull"`
	ActivePieceCount *int64
	LastCheckedAt    time.Time       `bun:",notnull"`
	LastError        *string         `bun:"type:text"`
	Evidence         json.RawMessage `bun:"evidence_json,type:jsonb,notnull"`
}

func createObservabilitySchema(ctx context.Context, db bun.IDB) error {
	tables := []initialTableSpec{
		{
			name:  "observability_collection_states",
			model: (*observabilityCollectionState2026090101)(nil),
			constraints: []string{
				"CONSTRAINT chk_observability_collection_type CHECK (collection_type IN ('providers', 'data_sets'))",
			},
		},
		{
			name:        "observability_provider_states",
			model:       (*observabilityProviderState2026090101)(nil),
			jsonColumns: initialJSONColumns("observability_provider_states"),
			constraints: []string{
				"CONSTRAINT chk_observability_provider_identity CHECK (provider_id <> '')",
				"CONSTRAINT chk_observability_provider_status CHECK (status IN ('available', 'degraded', 'unavailable', 'unknown'))",
			},
		},
		{
			name:        "provider_profiles",
			model:       (*providerProfile2026090101)(nil),
			jsonColumns: initialJSONColumns("provider_profiles"),
			constraints: []string{"CONSTRAINT chk_provider_profiles_id CHECK (provider_id <> '')"},
		},
		{
			name:        "provider_tier_snapshots",
			model:       (*providerTierSnapshot2026090101)(nil),
			jsonColumns: initialJSONColumns("provider_tier_snapshots"),
			constraints: []string{"CONSTRAINT chk_provider_tier_snapshots_tier CHECK (tier IN ('approved', 'endorsed'))"},
		},
		{
			name:  "provider_upload_speed_tests",
			model: (*providerUploadSpeedTest2026090101)(nil),
			constraints: []string{
				"CONSTRAINT chk_provider_upload_speed_tests_state CHECK (state IN ('testing', 'succeeded', 'failed'))",
				"CONSTRAINT chk_provider_upload_speed_tests_identity CHECK (provider_id <> '' AND length(service_url_hash) = 64 AND sample_bytes > 0)",
				"CONSTRAINT chk_provider_upload_speed_tests_result CHECK ((state = 'testing' AND active_task_id IS NOT NULL AND duration_ms IS NULL AND bytes_per_second IS NULL AND tested_at IS NULL AND failure_code IS NULL) OR (state = 'succeeded' AND active_task_id IS NULL AND duration_ms > 0 AND bytes_per_second > 0 AND tested_at IS NOT NULL AND failure_code IS NULL) OR (state = 'failed' AND active_task_id IS NULL AND duration_ms IS NULL AND bytes_per_second IS NULL AND tested_at IS NOT NULL AND failure_code IS NOT NULL))",
			},
			foreignKeys: []string{"(active_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT"},
		},
		{
			name:        "observability_data_set_states",
			model:       (*observabilityDataSetState2026090101)(nil),
			jsonColumns: initialJSONColumns("observability_data_set_states"),
			constraints: []string{
				"CONSTRAINT chk_observability_data_set_identity CHECK (provider_id <> '' AND (chain_data_set_id IS NULL OR chain_data_set_id <> '') AND (client_data_set_id IS NULL OR client_data_set_id <> ''))",
				"CONSTRAINT fk_observability_data_set_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"CONSTRAINT chk_observability_data_set_status CHECK (status IN ('available', 'degraded', 'unavailable', 'unknown'))",
			},
			foreignKeys: []string{
				"(local_data_set_id, bucket_id, copy_index, provider_id) REFERENCES storage_data_sets (id, bucket_id, copy_index, provider_id) ON UPDATE RESTRICT ON DELETE CASCADE",
			},
		},
	}
	for _, table := range tables {
		if err := createInitialTable(ctx, db, table); err != nil {
			return err
		}
	}
	return createInitialIndexes(ctx, db,
		initialIndexSpec{name: "idx_observability_provider_states_status", table: "observability_provider_states", columns: []string{"status", "last_checked_at"}},
		initialIndexSpec{name: "idx_provider_upload_speed_tests_active_task", table: "provider_upload_speed_tests", columns: []string{"active_task_id"}, where: "active_task_id IS NOT NULL", unique: true},
		initialIndexSpec{name: "idx_observability_data_set_states_bucket_status", table: "observability_data_set_states", columns: []string{"bucket_id", "status", "last_checked_at"}},
		initialIndexSpec{name: "idx_observability_data_set_states_provider_status", table: "observability_data_set_states", columns: []string{"provider_id", "status", "last_checked_at"}},
	)
}
