package db

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/testpg"
	"github.com/strahe/synaps3/internal/testutil"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/uptrace/bun"
)

type migrationLockBarrier struct {
	locked  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *migrationLockBarrier) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *migrationLockBarrier) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if event.Err != nil || event.Operation() != "INSERT" || !strings.Contains(event.Query, "bun_migration_locks") {
		return
	}
	h.once.Do(func() {
		close(h.locked)
		select {
		case <-h.release:
		case <-ctx.Done():
		}
	})
}

func TestNewRejectsBlankDSNBeforeConnecting(t *testing.T) {
	database, err := New(config.DatabaseConfig{Driver: "postgres", DSN: " ", MaxOpenConns: 1})
	if database != nil {
		_ = database.Close()
		t.Fatal("New returned a connection for a blank DSN")
	}
	if err == nil || !strings.Contains(err.Error(), "database.dsn") {
		t.Fatalf("New error = %v, want a database.dsn error", err)
	}
}

func TestForceUnlockMigrationsClearsALockLeftByAKilledRun(t *testing.T) {
	db := newPostgresDB(t, 2)
	ctx := context.Background()

	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	// A killed run never reaches its deferred unlock.
	if err := migrations.NewMigrator(db).Lock(ctx); err != nil {
		t.Fatalf("acquiring the leaked lock: %v", err)
	}
	err := RunMigrations(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "--force-unlock") {
		t.Fatalf("RunMigrations() error = %v, want the stale-lock remediation", err)
	}

	if err := ForceUnlockMigrations(ctx, db); err != nil {
		t.Fatalf("ForceUnlockMigrations() error = %v", err)
	}
	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations() after force unlock = %v, want success", err)
	}
}

func TestForceUnlockMigrationsRejectsLegacyDatabaseWithoutModification(t *testing.T) {
	database := newPostgresDB(t, 1)
	if _, err := database.ExecContext(t.Context(), `CREATE TABLE legacy_tasks (id INTEGER PRIMARY KEY, status TEXT NOT NULL)`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO legacy_tasks (id, status) VALUES (1, 'running')`); err != nil {
		t.Fatalf("seed legacy table: %v", err)
	}

	err := ForceUnlockMigrations(t.Context(), database)
	if !errors.Is(err, migrations.ErrIncompatibleDatabase) {
		t.Fatalf("ForceUnlockMigrations() error = %v, want incompatible database", err)
	}
	var status string
	if err := database.NewRaw(`SELECT status FROM legacy_tasks WHERE id = 1`).Scan(t.Context(), &status); err != nil {
		t.Fatalf("read legacy row: %v", err)
	}
	if status != "running" {
		t.Fatalf("legacy row status = %q, want unchanged", status)
	}
	var migrationTables int
	if err := database.NewRaw(`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE 'bun_migration%'`).Scan(t.Context(), &migrationTables); err != nil {
		t.Fatalf("count migration tables: %v", err)
	}
	if migrationTables != 0 {
		t.Fatalf("force unlock created %d migration metadata tables in legacy database", migrationTables)
	}
}

func TestRunMigrationsSerializesConcurrentRunnersAndUnlocksAfterCancellation(t *testing.T) {
	db := newPostgresDB(t, 2)

	barrier := &migrationLockBarrier{locked: make(chan struct{}), release: make(chan struct{})}
	testutil.AddQueryHook(db, barrier)
	ctx, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- RunMigrations(ctx, db)
	}()

	select {
	case <-barrier.locked:
	case <-time.After(5 * time.Second):
		cancel()
		close(barrier.release)
		t.Fatal("first migration runner did not acquire the lock")
	}
	if err := RunMigrations(context.Background(), db); err == nil || !strings.Contains(err.Error(), "already locked") {
		cancel()
		close(barrier.release)
		t.Fatalf("concurrent RunMigrations() error = %v, want migration lock conflict", err)
	}

	cancel()
	close(barrier.release)
	if err := <-firstResult; err == nil {
		t.Fatal("cancelled RunMigrations() succeeded")
	}
	if err := RunMigrations(context.Background(), db); err != nil {
		t.Fatalf("RunMigrations() after cancelled owner = %v, want released lock", err)
	}
}

func TestRunMigrations_ObjectVersionSchema(t *testing.T) {
	db := newMigratedPostgresDB(t)

	objectColumns := schemaColumns(t, db, "objects")
	for _, column := range []string{"size", "e_tag", "checksum", "cache_key", "in_cache", "in_filecoin", "state"} {
		if objectColumns[column] {
			t.Fatalf("objects.%s should not exist", column)
		}
	}
	// "Current" is one pointer on the object, not a flag repeated per version.
	for _, column := range []string{"bucket_id", "key", "current_version_id"} {
		if !objectColumns[column] {
			t.Fatalf("objects.%s should exist", column)
		}
	}

	versionColumns := schemaColumns(t, db, "object_versions")
	for _, column := range []string{"content_id", "multipart_upload_id"} {
		if !versionColumns[column] {
			t.Fatalf("object_versions.%s should exist", column)
		}
	}
	// Bytes, durability and cache residency belong to the content and its cache
	// entry; a version that carried them would be a second authority for facts
	// those rows already own.
	for _, column := range []string{
		"piece_cid", "retrieval_url", "in_filecoin", "checksum", "state",
		"in_cache", "cache_accessed_at", "cache_key", "is_current",
	} {
		if versionColumns[column] {
			t.Fatalf("object_versions.%s should not exist", column)
		}
	}
	cacheColumns := schemaColumns(t, db, "object_cache")
	for _, column := range []string{"content_id", "in_cache", "cache_accessed_at", "cache_active_task_id"} {
		if !cacheColumns[column] {
			t.Fatalf("object_cache.%s should exist", column)
		}
	}

	bucketColumns := schemaColumns(t, db, "buckets")
	if bucketColumns["proof_set_id"] {
		t.Fatal("buckets.proof_set_id should not exist")
	}

	for _, table := range []string{"storage_contents", "storage_data_sets", "storage_copies"} {
		if columns := schemaColumns(t, db, table); len(columns) == 0 {
			t.Fatalf("%s table should exist", table)
		}
	}
	copyColumns := schemaColumns(t, db, "storage_copies")
	if copyColumns["is_new_data_set"] {
		t.Fatal("storage_copies.is_new_data_set should not exist")
	}
	partColumns := schemaColumns(t, db, "multipart_parts")
	if !partColumns["checksum"] {
		t.Fatal("multipart_parts.checksum should exist")
	}

	indexes := schemaIndexes(t, db, "objects")
	if !indexes["idx_objects_bucket_key"] {
		t.Fatal("idx_objects_bucket_key should exist")
	}
	versionIndexes := schemaIndexes(t, db, "object_versions")
	objectIndexes := schemaIndexes(t, db, "objects")
	if !objectIndexes["idx_objects_current_version"] {
		t.Fatal("idx_objects_current_version should exist")
	}
	if !versionIndexes["idx_object_versions_content"] {
		t.Fatal("idx_object_versions_content should exist")
	}
	if !versionIndexes["idx_object_versions_multipart_upload"] {
		t.Fatal("idx_object_versions_multipart_upload should exist")
	}
	if !versionIndexes["idx_object_versions_object_created"] {
		t.Fatal("idx_object_versions_object_created should exist")
	}
	cacheIndexes := schemaIndexes(t, db, "object_cache")
	if !cacheIndexes["idx_object_cache_lru"] {
		t.Fatal("idx_object_cache_lru should exist")
	}
	taskIndexes := schemaIndexes(t, db, "tasks")
	for _, name := range []string{
		"idx_tasks_pending",
		"idx_tasks_recovery",
		"uq_tasks_type_key",
		"uq_tasks_retry_parent",
		"idx_tasks_type_id",
		"idx_tasks_status_id",
		"idx_tasks_status_type_id",
		"idx_tasks_subject",
	} {
		if !taskIndexes[name] {
			t.Fatalf("%s should exist", name)
		}
	}
	uploadCopyIndexes := schemaIndexes(t, db, "storage_copies")
	if !uploadCopyIndexes["idx_storage_copies_status_data_set_content"] {
		t.Fatal("idx_storage_copies_status_data_set_content should exist")
	}
}

func TestRunMigrations_ObjectVersionCurrentAndForeignKeyConstraints(t *testing.T) {
	db := newMigratedPostgresDB(t)

	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (1, 'bucket-a', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 1, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (2, 'bucket-b', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 2, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO objects (id, bucket_id, key, created_at, updated_at) VALUES (1, 1, 'file.txt', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// A data version points at the content holding its bytes, and the composite
	// foreign key welds the size it repeats to that content's size.
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (1, 1, 1, lpad(to_hex(1), 64, '0'), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (2, 1, 2, lpad(to_hex(2), 64, '0'), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (3, 1, 3, lpad(to_hex(3), 64, '0'), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (4, 2, 1, lpad(to_hex(4), 64, '0'), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, created_at, updated_at) VALUES ('v1', 1, 1, 'file.txt', 1, 1, 'etag-1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, created_at, updated_at) VALUES ('v3', 1, 1, 'file.txt', 3, 3, 'etag-3', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// One column holds one value, so "at most one current version" needs no
	// partial unique index; pointing at a version of another object is refused
	// by the pointer's composite foreign key.
	mustExec(t, db, `UPDATE objects SET current_version_id = 'v1' WHERE id = 1`)
	mustReject(t, db, "expected a pointer to a missing version to fail", `UPDATE objects SET current_version_id = 'v-missing' WHERE id = 1`)
	mustReject(t, db, "expected deleting the pointed-at version to fail", `DELETE FROM object_versions WHERE version_id = 'v1'`)
	mustExec(t, db, `INSERT INTO objects (id, bucket_id, key, created_at, updated_at) VALUES (2, 1, 'other.txt', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected pointing at another object's version to fail", `UPDATE objects SET current_version_id = 'v1' WHERE id = 2`)
	mustExec(t, db, `UPDATE objects SET current_version_id = 'v3' WHERE id = 1`)
	mustExec(t, db, `DELETE FROM object_versions WHERE version_id = 'v1'`)
	mustExec(t, db, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, created_at, updated_at) VALUES ('v1', 1, 1, 'file.txt', 1, 1, 'etag-1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `UPDATE objects SET current_version_id = 'v1' WHERE id = 1`)

	mustReject(t, db, "expected object_versions object/bucket/key mismatch to fail", `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, created_at, updated_at) VALUES ('wrong-bucket', 1, 2, 'file.txt', 4, 1, 'etag-x', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected data object version without content_id to fail", `INSERT INTO object_versions (version_id, object_id, bucket_id, key, size, e_tag, created_at, updated_at) VALUES ('data-without-content', 1, 1, 'file.txt', 1, 'etag-x', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected version whose size disagrees with its content to fail", `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, created_at, updated_at) VALUES ('size-drift', 1, 1, 'file.txt', 1, 99, 'etag-x', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected version pointing at another bucket's content to fail", `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, created_at, updated_at) VALUES ('cross-bucket-content', 1, 1, 'file.txt', 4, 1, 'etag-x', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected malformed delete marker insert to fail", `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, content_type, is_delete_marker, created_at, updated_at) VALUES ('bad-marker', 1, 1, 'file.txt', 1, 1, 'etag-marker', '', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected malformed delete marker update to fail", `UPDATE object_versions SET is_delete_marker = TRUE WHERE version_id = 'v3'`)
	mustExec(t, db, `INSERT INTO multipart_uploads (bucket_id, key, upload_id, status, created_at, updated_at) VALUES (1, 'file.txt', 'upload-valid', 'completed', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (5, 1, 1, lpad(to_hex(5), 64, '0'), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, multipart_upload_id, created_at, updated_at) VALUES ('multipart-valid', 1, 1, 'file.txt', 5, 1, 'etag-mp', 'upload-valid', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected object_versions multipart_upload_id without upload to fail", `INSERT INTO object_versions (version_id, object_id, bucket_id, key, content_id, size, e_tag, multipart_upload_id, created_at, updated_at) VALUES ('multipart-missing', 1, 1, 'file.txt', 5, 1, 'etag-missing', 'upload-missing', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, size, e_tag, content_type, is_delete_marker, created_at, updated_at) VALUES ('marker-ok', 1, 1, 'file.txt', 0, '', '', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)

	// Residency belongs to the content, so a cache entry cannot name bytes that
	// do not exist and cannot be recorded twice for one content.
	mustExec(t, db, `INSERT INTO object_cache (content_id, in_cache, created_at, updated_at) VALUES (1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected a second cache entry for one content to fail", `INSERT INTO object_cache (content_id, in_cache, created_at, updated_at) VALUES (1, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected a cache entry for missing content to fail", `INSERT INTO object_cache (content_id, in_cache, created_at, updated_at) VALUES (9999, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
}

func TestRunMigrations_StorageProvenanceConstraints(t *testing.T) {
	db := newMigratedPostgresDB(t)

	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (1, 'bucket-a', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 1, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (2, 'bucket-b', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 2, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, piece_cid, requested_copies, created_at, updated_at) VALUES (1, 1, 10, lpad(to_hex(1), 64, '0'), 'bafk2bzacefake', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (2, 2, 10, lpad(to_hex(2), 64, '0'), 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_by_content_id, created_at, updated_at) VALUES (1, 1, '101', 0, 1, TRUE, '1001', 'ready', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected provider/data_set reuse across buckets to fail", `INSERT INTO storage_data_sets (bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_by_content_id, created_at, updated_at) VALUES (2, '101', 0, 1, TRUE, '1001', 'ready', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected duplicate bucket/copy_index binding to fail", `INSERT INTO storage_data_sets (bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, created_at, updated_at) VALUES (1, '202', 0, 1, TRUE, 'pending', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)

	mustExec(t, db, `INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, created_at, updated_at) VALUES (2, 1, '202', 1, 1, TRUE, 'pending', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// Committed is a projection of a confirmed ledger row, so the evidence has
	// to exist before the copy can claim it.
	mustReject(t, db, "expected a committed copy without confirmed evidence to fail", `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, piece_id, transfer_method, status, retrieval_url, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '101', '2001', 'ingress', 'committed', 'https://provider.example/piece', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, piece_id, transfer_method, status, retrieval_url, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '101', '2001', 'ingress', 'piece_ready', 'https://provider.example/piece', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_commit_requests (request_id, storage_data_set_id, status, piece_count, extra_data_hex, sealed_at, first_sent_at, sends, submitted_at, last_sent_at, transaction_id, status_url, refusals, first_piece_id, confirmed_transaction_id, confirmed_at, created_at, updated_at) VALUES ('request-1', 1, 'confirmed', 1, 'abcd', current_timestamp, current_timestamp, 1, current_timestamp, current_timestamp, 'tx-1', 'https://provider.example/status/request-1', 0, '2001', 'tx-1', current_timestamp, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_commit_request_pieces (request_id, position, content_id, storage_data_set_id, piece_cid, created_at) VALUES ('request-1', 0, 1, 1, 'bafk2bzacefake', CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected a copy to refuse naming a position without its request", `UPDATE storage_copies SET status = 'committed', commit_position = 0, commit_request_status = 'confirmed' WHERE content_id = 1 AND copy_index = 0`)
	mustReject(t, db, "expected a copy to refuse naming a missing request", `UPDATE storage_copies SET status = 'committed', commit_request_id = 'request-missing', commit_position = 0, commit_request_status = 'confirmed' WHERE content_id = 1 AND copy_index = 0`)
	mustExec(t, db, `UPDATE storage_copies SET status = 'committed', commit_request_id = 'request-1', commit_position = 0, commit_request_status = 'confirmed' WHERE content_id = 1 AND copy_index = 0`)
	mustReject(t, db, "expected a referenced request to refuse leaving confirmed", `UPDATE storage_commit_requests SET status = 'abandoned', last_error = 'x' WHERE request_id = 'request-1'`)
	mustReject(t, db, "expected a referenced request to refuse deletion", `DELETE FROM storage_commit_requests WHERE request_id = 'request-1'`)
	mustReject(t, db, "expected a referenced request piece to refuse deletion", `DELETE FROM storage_commit_request_pieces WHERE request_id = 'request-1'`)
	mustReject(t, db, "expected duplicate copy for one data set to fail", `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, transfer_method, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '101', 'peer_pull', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// A replica slot holds both generations while a provider replacement migrates.
	mustExec(t, db, `INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, created_at, updated_at) VALUES (3, 1, '303', 0, 2, FALSE, 'pending', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, transfer_method, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '303', 'peer_pull', 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// A copy is always born bound to exactly one data set; the NOT NULL on
	// storage_data_set_id is what enforces that, so it is the assertion here.
	mustRejectRequiredColumn(t, db, "expected copy without a data set and provider binding to fail", `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, transfer_method, created_at, updated_at) VALUES (1, 1, 10, 3, 'peer_pull', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, transfer_method, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 1, '202', 'peer_pull', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected committed copy without piece identity to fail", `UPDATE storage_copies SET status = 'committed' WHERE content_id = 1 AND copy_index = 1`)
	mustExec(t, db, `INSERT INTO storage_commit_requests (request_id, storage_data_set_id, status, piece_count, extra_data_hex, sealed_at, first_sent_at, sends, submitted_at, last_sent_at, transaction_id, status_url, refusals, first_piece_id, confirmed_transaction_id, confirmed_at, created_at, updated_at) VALUES ('request-2', 2, 'confirmed', 1, 'abcd', current_timestamp, current_timestamp, 1, current_timestamp, current_timestamp, 'tx-2', 'https://provider.example/status/request-2', 0, '0', 'tx-2', current_timestamp, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_commit_request_pieces (request_id, position, content_id, storage_data_set_id, piece_cid, created_at) VALUES ('request-2', 0, 1, 2, 'bafk2bzacefake', CURRENT_TIMESTAMP)`)
	mustExec(t, db, `UPDATE storage_copies SET status = 'committed', piece_id = '0', retrieval_url = 'https://provider.example/zero-piece', commit_request_id = 'request-2', commit_position = 0, commit_request_status = 'confirmed' WHERE content_id = 1 AND copy_index = 1`)
	mustExec(t, db, `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', 1, '2001', 'bafk2bzacefake', lpad(to_hex(1), 64, '0'), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected duplicate physical cleanup identity to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', 1, '2001', 'bafk2bzaceduplicate', lpad(to_hex(1), 64, '0'), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// Cleanup evidence requires the data set, the piece it removed, and the
	// bytes it names; each NOT NULL is the invariant under test, so supply
	// every other column.
	mustRejectRequiredColumn(t, db, "expected cleanup evidence without storage_data_set_id to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, piece_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', '2002', 'bafk2bzacemissingdataset', lpad(to_hex(1), 64, '0'), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustRejectRequiredColumn(t, db, "expected cleanup evidence without piece_id to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', 1, 'bafk2bzacemissingpiece', lpad(to_hex(1), 64, '0'), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustRejectRequiredColumn(t, db, "expected cleanup evidence without checksum to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_id, piece_cid, created_at, updated_at) VALUES (1, 1, 0, '101', 1, '2003', 'bafk2bzacemissingchecksum', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
}

func TestRunMigrations_TaskAndMultipartConstraints(t *testing.T) {
	db := newMigratedPostgresDB(t)
	ctx := context.Background()

	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (1, 'bucket-a', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 1, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)

	mustReject(t, db, "expected task with incomplete subject identity to fail", `INSERT INTO tasks (type,idempotency_key,input_version,input_hash,subject_type,available_at,created_at,updated_at,input_json,policy_json,runtime_json,events_json) VALUES ('custom','invalid-subject',1,'hash','object_version',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{"version":2,"max_attempts":6}','{}','[]')`)
	mustExec(t, db, `INSERT INTO tasks (type,idempotency_key,input_version,input_hash,subject_type,subject_key,available_at,created_at,updated_at,input_json,policy_json,runtime_json,events_json) VALUES ('future_extension','open-type',1,'hash','bucket','1',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{"version":2,"max_attempts":6}','{}','[]')`)

	mustExec(t, db, `INSERT INTO multipart_uploads (bucket_id, key, upload_id, created_at, updated_at) VALUES (1, 'large.bin', 'upload-1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO multipart_parts (upload_id, part_number, size, e_tag, created_at) VALUES ('upload-1', 1, 10, 'part-etag', CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected multipart part_number > 10000 to fail", `INSERT INTO multipart_parts (upload_id, part_number, size, e_tag, created_at) VALUES ('upload-1', 10001, 10, 'bad-part', CURRENT_TIMESTAMP)`)
	mustExec(t, db, `DELETE FROM multipart_uploads WHERE upload_id = 'upload-1'`)

	var partCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM multipart_parts WHERE upload_id = 'upload-1'`).Scan(&partCount); err != nil {
		t.Fatalf("counting multipart parts: %v", err)
	}
	if partCount != 0 {
		t.Fatalf("multipart parts after upload delete = %d, want 0", partCount)
	}
}

// newPostgresDB opens a private PostgreSQL schema through New, as production does.
func newPostgresDB(t *testing.T, maxOpenConns int) *bun.DB {
	t.Helper()
	db, err := New(config.DatabaseConfig{
		Driver: "postgres", DSN: testpg.SchemaDSN(t), MaxOpenConns: maxOpenConns, MaxIdleConns: maxOpenConns,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL test database: %v", err)
		}
	})
	return db
}

func newMigratedPostgresDB(t *testing.T) *bun.DB {
	t.Helper()
	db := newPostgresDB(t, 1)
	if err := RunMigrations(context.Background(), db); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	return db
}

func mustExec(t *testing.T, db *bun.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// mustReject asserts a statement is refused by the constraint it targets. A
// statement that omits a required column is refused by the null constraint
// before that constraint is ever evaluated, so the case would keep passing
// while proving nothing.
func mustReject(t *testing.T, db *bun.DB, message, query string, args ...any) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), query, args...)
	if err == nil {
		t.Fatal(message)
	}
	if rejectedByNullConstraint(err) {
		t.Fatalf("%s: rejected by a null constraint instead of the constraint under test\nquery: %s\nerror: %v", message, query, err)
	}
	if rejectedByMissingSchema(err) {
		t.Fatalf("%s: never reached the constraint under test because the schema has no such table or column\nquery: %s\nerror: %v", message, query, err)
	}
}

// rejectedByMissingSchema reports a statement that never reached the constraint
// under test because it names a table or column the schema does not have. Such a
// statement fails, so a negative assertion keeps passing while proving nothing —
// exactly how three stale cases survived a column being removed.
func rejectedByMissingSchema(err error) bool {
	for _, missing := range []string{
		"does not exist",   // relation/column
		"undefined_table",  // SQLSTATE name
		"undefined_column", // SQLSTATE name
	} {
		if strings.Contains(err.Error(), missing) {
			return true
		}
	}
	return false
}

// mustRejectRequiredColumn asserts the opposite of mustReject: the omitted
// column is itself the invariant under test, so a null-constraint rejection is
// the expected outcome rather than a false pass.
func mustRejectRequiredColumn(t *testing.T, db *bun.DB, message, query string, args ...any) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), query, args...)
	if err == nil {
		t.Fatal(message)
	}
	if !rejectedByNullConstraint(err) {
		t.Fatalf("%s: not rejected by a null constraint\nquery: %s\nerror: %v", message, query, err)
	}
}

func rejectedByNullConstraint(err error) bool {
	return strings.Contains(err.Error(), "null value in column")
}

func schemaColumns(t *testing.T, db *bun.DB, table string) map[string]bool {
	t.Helper()
	var names []string
	if err := db.NewRaw(`SELECT column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = ?`, table).Scan(t.Context(), &names); err != nil {
		t.Fatalf("read columns of %s: %v", table, err)
	}
	return setOf(names)
}

func schemaIndexes(t *testing.T, db *bun.DB, table string) map[string]bool {
	t.Helper()
	var names []string
	if err := db.NewRaw(`SELECT indexname FROM pg_indexes
		WHERE schemaname = current_schema() AND tablename = ?`, table).Scan(t.Context(), &names); err != nil {
		t.Fatalf("read indexes of %s: %v", table, err)
	}
	return setOf(names)
}

func setOf(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}
