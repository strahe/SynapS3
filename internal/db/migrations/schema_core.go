package migrations

import (
	"context"
	"encoding/json"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

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
