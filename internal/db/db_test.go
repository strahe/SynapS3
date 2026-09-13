package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
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

func TestForceUnlockMigrationsClearsALockLeftByAKilledRun(t *testing.T) {
	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.Join(t.TempDir(), "force-unlock.db"),
		MaxOpenConns: 2,
		MaxIdleConns: 2,
	}
	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	// A killed run never reaches its deferred unlock.
	if err := migrations.NewMigrator(db).Lock(ctx); err != nil {
		t.Fatalf("acquiring the leaked lock: %v", err)
	}
	err = RunMigrations(ctx, db)
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
	cfg := config.DatabaseConfig{
		Driver: "sqlite", DSN: "file:" + filepath.Join(t.TempDir(), "legacy-force-unlock.db"),
		MaxOpenConns: 1, MaxIdleConns: 1,
	}
	database, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.ExecContext(t.Context(), `CREATE TABLE legacy_tasks (id INTEGER PRIMARY KEY, status TEXT NOT NULL)`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO legacy_tasks (id, status) VALUES (1, 'running')`); err != nil {
		t.Fatalf("seed legacy table: %v", err)
	}

	err = ForceUnlockMigrations(t.Context(), database)
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
	if err := database.NewRaw(`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name LIKE 'bun_migration%'`).Scan(t.Context(), &migrationTables); err != nil {
		t.Fatalf("count migration tables: %v", err)
	}
	if migrationTables != 0 {
		t.Fatalf("force unlock created %d migration metadata tables in legacy database", migrationTables)
	}
}

func TestNewRejectsLegacySQLiteBeforeApplyingPersistentPragmas(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "preserved-legacy.db")
	legacy, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open legacy sqlite database: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE legacy_tasks (id INTEGER PRIMARY KEY, status TEXT NOT NULL)`); err != nil {
		_ = legacy.Close()
		t.Fatalf("create legacy task table: %v", err)
	}
	if _, err := legacy.Exec(`INSERT INTO legacy_tasks (id, status) VALUES (1, 'running')`); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed legacy task: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy sqlite database: %v", err)
	}
	before, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatalf("read legacy sqlite database before preflight: %v", err)
	}

	database, err := New(config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.ToSlash(databasePath) + "?_pragma=journal_mode(WAL)",
		MaxOpenConns: 2,
		MaxIdleConns: 2,
	})
	if database != nil {
		_ = database.Close()
		t.Fatal("New returned a connection for an incompatible database")
	}
	if !errors.Is(err, migrations.ErrIncompatibleDatabase) {
		t.Fatalf("New error = %v, want incompatible database", err)
	}
	after, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatalf("read legacy sqlite database after preflight: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("legacy sqlite database changed during compatibility preflight")
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, statErr := os.Stat(databasePath + suffix); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("legacy sqlite sidecar %s exists after rejection: %v", suffix, statErr)
		}
	}
}

func TestRunMigrationsSerializesConcurrentRunnersAndUnlocksAfterCancellation(t *testing.T) {
	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.Join(t.TempDir(), "migration-lock.db") + "?_pragma=journal_mode(WAL)",
		MaxOpenConns: 2,
		MaxIdleConns: 2,
	}
	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	barrier := &migrationLockBarrier{locked: make(chan struct{}), release: make(chan struct{})}
	db.AddQueryHook(barrier)
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

func TestNew_SQLiteConcurrentClaimsDoNotBusy(t *testing.T) {
	t.Parallel()

	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.Join(t.TempDir(), "busy.db") + "?_pragma=journal_mode(WAL)",
		MaxOpenConns: 25,
		MaxIdleConns: 5,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}

	repos := repository.NewRepositories(db)
	for i := range 100 {
		versionID := fmt.Sprintf("01J00000000000000000%06d", i+1)
		task := &model.Task{
			Type: model.TaskTypeUploadPlan, IdempotencyKey: fmt.Sprintf("upload-plan:%s", versionID),
			InputVersion: 1, Input: []byte(fmt.Sprintf(`{"version_id":%q}`, versionID)), InputHash: versionID,
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		}
		if _, created, err := repos.Tasks.Enqueue(ctx, task); err != nil || !created {
			t.Fatalf("Enqueue() created=%t error=%v", created, err)
		}
	}

	var busyCount atomic.Int64
	var claimedCount atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				task, err := repos.Tasks.ClaimNext(ctx, time.Minute)
				if err != nil {
					if strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database is locked") {
						busyCount.Add(1)
						return
					}
					t.Errorf("ClaimNext() unexpected error = %v", err)
					return
				}
				if task == nil {
					return
				}
				claimedCount.Add(1)
			}
		})
	}
	wg.Wait()

	if busyCount.Load() != 0 {
		t.Fatalf("expected no SQLITE_BUSY errors during concurrent claim, got %d (claimed=%d)", busyCount.Load(), claimedCount.Load())
	}
	if claimedCount.Load() != 100 {
		t.Fatalf("expected all tasks claimed, got %d", claimedCount.Load())
	}
}

func TestRunMigrations_ObjectVersionSchema(t *testing.T) {
	db := newMigratedSQLiteDB(t, "schema.db")

	objectColumns := sqliteColumns(t, db, "objects")
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

	versionColumns := sqliteColumns(t, db, "object_versions")
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
	cacheColumns := sqliteColumns(t, db, "object_cache")
	for _, column := range []string{"content_id", "in_cache", "cache_accessed_at", "cache_active_task_id"} {
		if !cacheColumns[column] {
			t.Fatalf("object_cache.%s should exist", column)
		}
	}

	bucketColumns := sqliteColumns(t, db, "buckets")
	if bucketColumns["proof_set_id"] {
		t.Fatal("buckets.proof_set_id should not exist")
	}

	for _, table := range []string{"storage_contents", "storage_data_sets", "storage_copies"} {
		if columns := sqliteColumns(t, db, table); len(columns) == 0 {
			t.Fatalf("%s table should exist", table)
		}
	}
	copyColumns := sqliteColumns(t, db, "storage_copies")
	if copyColumns["is_new_data_set"] {
		t.Fatal("storage_copies.is_new_data_set should not exist")
	}
	partColumns := sqliteColumns(t, db, "multipart_parts")
	if !partColumns["checksum"] {
		t.Fatal("multipart_parts.checksum should exist")
	}

	indexes := sqliteIndexes(t, db, "objects")
	if !indexes["idx_objects_bucket_key"] {
		t.Fatal("idx_objects_bucket_key should exist")
	}
	versionIndexes := sqliteIndexes(t, db, "object_versions")
	objectIndexes := sqliteIndexes(t, db, "objects")
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
	cacheIndexes := sqliteIndexes(t, db, "object_cache")
	if !cacheIndexes["idx_object_cache_lru"] {
		t.Fatal("idx_object_cache_lru should exist")
	}
	taskIndexes := sqliteIndexes(t, db, "tasks")
	for _, name := range []string{
		"idx_tasks_pending",
		"idx_tasks_recovery",
		"idx_tasks_gc",
		"idx_tasks_type_status_id",
		"idx_tasks_subject",
	} {
		if !taskIndexes[name] {
			t.Fatalf("%s should exist", name)
		}
	}
	uploadCopyIndexes := sqliteIndexes(t, db, "storage_copies")
	if !uploadCopyIndexes["idx_storage_copies_status_data_set_content"] {
		t.Fatal("idx_storage_copies_status_data_set_content should exist")
	}
}

func TestRunMigrations_ObjectVersionCurrentAndForeignKeyConstraints(t *testing.T) {
	db := newMigratedSQLiteDB(t, "object-version-constraints.db")

	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (1, 'bucket-a', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 1, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (2, 'bucket-b', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 2, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO objects (id, bucket_id, key, created_at, updated_at) VALUES (1, 1, 'file.txt', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// A data version points at the content holding its bytes, and the composite
	// foreign key welds the size it repeats to that content's size.
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (1, 1, 1, printf('%064x', 1), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (2, 1, 2, printf('%064x', 2), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (3, 1, 3, printf('%064x', 3), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (4, 2, 1, printf('%064x', 4), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
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
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (5, 1, 1, printf('%064x', 5), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
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
	db := newMigratedSQLiteDB(t, "storage-provenance-constraints.db")

	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (1, 'bucket-a', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 1, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (2, 'bucket-b', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 2, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, piece_cid, requested_copies, created_at, updated_at) VALUES (1, 1, 10, printf('%064x', 1), 'bafk2bzacefake', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_contents (id, bucket_id, content_size, checksum, requested_copies, created_at, updated_at) VALUES (2, 2, 10, printf('%064x', 2), 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_by_content_id, last_used_content_id, created_at, updated_at) VALUES (1, 1, '101', 0, 1, TRUE, '1001', 'ready', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected provider/data_set reuse across buckets to fail", `INSERT INTO storage_data_sets (bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_by_content_id, last_used_content_id, created_at, updated_at) VALUES (2, '101', 0, 1, TRUE, '1001', 'ready', 2, 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected duplicate bucket/copy_index binding to fail", `INSERT INTO storage_data_sets (bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, last_used_content_id, created_at, updated_at) VALUES (1, '202', 0, 1, TRUE, 'pending', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)

	mustExec(t, db, `INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, last_used_content_id, created_at, updated_at) VALUES (2, 1, '202', 1, 1, TRUE, 'pending', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// Committed is a projection of a confirmed ledger row, so the evidence has
	// to exist before the copy can claim it.
	mustReject(t, db, "expected a committed copy without confirmed evidence to fail", `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, piece_id, transfer_method, status, retrieval_url, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '101', '2001', 'ingress', 'committed', 'https://provider.example/piece', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, piece_id, transfer_method, status, retrieval_url, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '101', '2001', 'ingress', 'piece_ready', 'https://provider.example/piece', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_commit_attempts (attempt_id, content_id, storage_data_set_id, status, extra_data_hex, transaction_id, confirmed_transaction_id, attempted_at, resolved_at, created_at, updated_at) VALUES ('attempt-1', 1, 1, 'confirmed', 'abcd', 'tx-1', 'tx-1', current_timestamp, current_timestamp, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected a copy to refuse projecting an unconfirmed status", `UPDATE storage_copies SET status = 'committed', confirmed_attempt_id = 'attempt-1', confirmed_attempt_status = 'attempted' WHERE content_id = 1 AND copy_index = 0`)
	mustReject(t, db, "expected a copy to refuse projecting a missing attempt", `UPDATE storage_copies SET status = 'committed', confirmed_attempt_id = 'attempt-missing', confirmed_attempt_status = 'confirmed' WHERE content_id = 1 AND copy_index = 0`)
	mustExec(t, db, `UPDATE storage_copies SET status = 'committed', confirmed_attempt_id = 'attempt-1', confirmed_attempt_status = 'confirmed' WHERE content_id = 1 AND copy_index = 0`)
	mustReject(t, db, "expected a referenced attempt to refuse leaving confirmed", `UPDATE storage_commit_attempts SET status = 'released' WHERE attempt_id = 'attempt-1'`)
	mustReject(t, db, "expected a referenced attempt to refuse deletion", `DELETE FROM storage_commit_attempts WHERE attempt_id = 'attempt-1'`)
	mustReject(t, db, "expected duplicate copy for one data set to fail", `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, transfer_method, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '101', 'peer_pull', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// A replica slot holds both generations while a provider replacement migrates.
	mustExec(t, db, `INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, last_used_content_id, created_at, updated_at) VALUES (3, 1, '303', 0, 2, FALSE, 'pending', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, transfer_method, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 0, '303', 'peer_pull', 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// A copy is always born bound to exactly one data set; the NOT NULL on
	// storage_data_set_id is what enforces that, so it is the assertion here.
	mustRejectRequiredColumn(t, db, "expected copy without a data set and provider binding to fail", `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, transfer_method, created_at, updated_at) VALUES (1, 1, 10, 3, 'peer_pull', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO storage_copies (content_id, bucket_id, content_size, copy_index, provider_id, transfer_method, storage_data_set_id, created_at, updated_at) VALUES (1, 1, 10, 1, '202', 'peer_pull', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected committed copy without piece identity to fail", `UPDATE storage_copies SET status = 'committed' WHERE content_id = 1 AND copy_index = 1`)
	mustExec(t, db, `INSERT INTO storage_commit_attempts (attempt_id, content_id, storage_data_set_id, status, extra_data_hex, transaction_id, confirmed_transaction_id, attempted_at, resolved_at, created_at, updated_at) VALUES ('attempt-2', 1, 2, 'confirmed', 'abcd', 'tx-2', 'tx-2', current_timestamp, current_timestamp, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `UPDATE storage_copies SET status = 'committed', piece_id = '0', retrieval_url = 'https://provider.example/zero-piece', confirmed_attempt_id = 'attempt-2', confirmed_attempt_status = 'confirmed' WHERE content_id = 1 AND copy_index = 1`)
	mustExec(t, db, `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', 1, '2001', 'bafk2bzacefake', printf('%064x', 1), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustReject(t, db, "expected duplicate physical cleanup identity to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', 1, '2001', 'bafk2bzaceduplicate', printf('%064x', 1), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	// Cleanup evidence requires the data set, the piece it removed, and the
	// bytes it names; each NOT NULL is the invariant under test, so supply
	// every other column.
	mustRejectRequiredColumn(t, db, "expected cleanup evidence without storage_data_set_id to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, piece_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', '2002', 'bafk2bzacemissingdataset', printf('%064x', 1), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustRejectRequiredColumn(t, db, "expected cleanup evidence without piece_id to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_cid, checksum, created_at, updated_at) VALUES (1, 1, 0, '101', 1, 'bafk2bzacemissingpiece', printf('%064x', 1), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustRejectRequiredColumn(t, db, "expected cleanup evidence without checksum to fail", `INSERT INTO storage_cleanup_copies (content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_id, piece_cid, created_at, updated_at) VALUES (1, 1, 0, '101', 1, '2003', 'bafk2bzacemissingchecksum', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
}

func TestRunMigrations_TaskAndMultipartConstraints(t *testing.T) {
	db := newMigratedSQLiteDB(t, "task-multipart-constraints.db")
	ctx := context.Background()

	mustExec(t, db, `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at) VALUES (1, 'bucket-a', 8, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) SELECT 1, value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (SELECT 0 AS value UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7)`)

	mustReject(t, db, "expected task with incomplete subject identity to fail", `INSERT INTO tasks (type, idempotency_key, input_version, input_hash, subject_type, available_at, created_at, updated_at) VALUES ('custom', 'invalid-subject', 1, 'hash', 'object_version', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO tasks (type, idempotency_key, input_version, input_hash, subject_type, subject_key, available_at, created_at, updated_at) VALUES ('future_extension', 'open-type', 1, 'hash', 'bucket', '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)

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

func TestNew_SQLiteCreatesParentDirectory(t *testing.T) {
	t.Parallel()

	dbDir := filepath.Join(t.TempDir(), "nested", "db")
	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.ToSlash(filepath.Join(dbDir, "synaps3.db")) + "?_pragma=journal_mode(WAL)",
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	info, err := os.Stat(dbDir)
	if err != nil {
		t.Fatalf("expected sqlite directory to exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("sqlite path %s is not a directory", dbDir)
	}
	if runtime.GOOS != "windows" {
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("sqlite directory mode = %o, want 700", got)
		}
	}
}

func TestNew_SQLiteFileDSNAppliesManagedPragmas(t *testing.T) {
	t.Parallel()

	dbDir := filepath.Join(t.TempDir(), "db")
	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.ToSlash(filepath.Join(dbDir, "synaps3.db")),
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}

	var busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", busyTimeout)
	}

	var foreignKeys int
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("foreign_keys = %d, want 1", foreignKeys)
	}
}

func TestNew_SQLiteMemoryDSNDoesNotCreateDirectories(t *testing.T) {
	cwd := t.TempDir()
	oldCWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(cwd); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldCWD); err != nil {
			t.Errorf("restore cwd: %v", err)
		}
	})

	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:memory-dir/named?mode=memory&cache=shared",
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(filepath.Join(cwd, "memory-dir")); !os.IsNotExist(err) {
		t.Fatalf("memory DSN created directory unexpectedly, stat error = %v", err)
	}
}

func TestNew_SQLiteMemoryDSNAppliesManagedConnectionPragmas(t *testing.T) {
	t.Parallel()

	for name, dsn := range map[string]string{
		"colon": ":memory:",
		"uri":   "file:managed-memory?mode=memory&cache=shared",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := config.DatabaseConfig{
				Driver:       "sqlite",
				DSN:          dsn,
				MaxOpenConns: 1,
				MaxIdleConns: 1,
			}

			db, err := New(cfg)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			ctx := context.Background()
			var journalMode string
			if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
				t.Fatalf("PRAGMA journal_mode: %v", err)
			}
			if strings.EqualFold(journalMode, "wal") {
				t.Fatalf("journal_mode = %q, want non-WAL memory journal", journalMode)
			}

			var busyTimeout int
			if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
				t.Fatalf("PRAGMA busy_timeout: %v", err)
			}
			if busyTimeout != 5000 {
				t.Fatalf("busy_timeout = %d, want 5000", busyTimeout)
			}

			var foreignKeys int
			if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
				t.Fatalf("PRAGMA foreign_keys: %v", err)
			}
			if foreignKeys != 1 {
				t.Fatalf("foreign_keys = %d, want 1", foreignKeys)
			}
		})
	}
}

func TestEnsureSQLitePragmasDoesNotDuplicateExistingSettings(t *testing.T) {
	for name, query := range map[string]string{
		"pragma":     "_pragma=journal_mode(WAL)&_pragma=busy_timeout(7000)&_pragma=foreign_keys(1)",
		"assignment": "journal_mode=WAL&busy_timeout=7000&foreign_keys=1",
	} {
		t.Run(name, func(t *testing.T) {
			dsn := "file:" + filepath.ToSlash(filepath.Join(t.TempDir(), "synaps3.db")) + "?" + query

			got := ensureSQLitePragmas(dsn)

			for _, want := range []string{"journal_mode", "busy_timeout", "foreign_keys"} {
				if count := strings.Count(strings.ToLower(got), want); count != 1 {
					t.Fatalf("%s count = %d in %q, want 1", want, count, got)
				}
			}
			if !strings.Contains(got, "busy_timeout=7000") && !strings.Contains(got, "busy_timeout(7000)") {
				t.Fatalf("managed pragmas overwrote existing busy_timeout: %q", got)
			}
		})
	}
}

func TestEnsureSQLitePragmasIgnoresSimilarQueryParameters(t *testing.T) {
	dsn := "file:" + filepath.ToSlash(filepath.Join(t.TempDir(), "synaps3.db")) +
		"?journal_mode_override=WAL&note=foreign_keys(1)&busy_timeout_ms=7000"

	got := ensureSQLitePragmas(dsn)

	for _, want := range []string{"journal_mode(WAL)", "foreign_keys(1)", "busy_timeout(5000)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("ensureSQLitePragmas() = %q, want %s", got, want)
		}
	}
}

func TestEnsureSQLitePragmasDoesNotAddWALForMemoryDSN(t *testing.T) {
	for _, dsn := range []string{":memory:", "file:memory-dir/named?mode=memory&cache=shared"} {
		got := ensureSQLitePragmas(dsn)
		if strings.Contains(strings.ToLower(got), "journal_mode") {
			t.Fatalf("ensureSQLitePragmas(%q) = %q, want no journal_mode", dsn, got)
		}
		for _, want := range []string{"foreign_keys(1)", "busy_timeout(5000)"} {
			if !strings.Contains(got, want) {
				t.Fatalf("ensureSQLitePragmas(%q) = %q, want %s", dsn, got, want)
			}
		}
	}
}

func TestSQLiteFilePathTreatsWindowsDrivePathAsFilePath(t *testing.T) {
	path, ok, err := sqliteFilePath(`C:\synaps3\db\synaps3.db?_pragma=journal_mode(WAL)`)
	if err != nil {
		t.Fatalf("sqliteFilePath() error = %v", err)
	}
	if !ok {
		t.Fatal("sqliteFilePath() ok = false, want true")
	}
	if path == "" {
		t.Fatal("sqliteFilePath() path is empty")
	}
}

func TestSQLiteFilePathTreatsWindowsFileURIAsFilePath(t *testing.T) {
	path, ok, err := sqliteFilePath("file:C:/synaps3/db/synaps3.db?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("sqliteFilePath() error = %v", err)
	}
	if !ok {
		t.Fatal("sqliteFilePath() ok = false, want true")
	}
	if filepath.ToSlash(path) != "C:/synaps3/db/synaps3.db" {
		t.Fatalf("sqliteFilePath() path = %q, want C:/synaps3/db/synaps3.db", filepath.ToSlash(path))
	}
}

func newMigratedSQLiteDB(t *testing.T, filename string) *bun.DB {
	t.Helper()
	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.Join(t.TempDir(), filename) + "?_pragma=journal_mode(WAL)",
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}

	db, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

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
		"no such table",       // SQLite
		"no such column",      // SQLite
		"has no column named", // SQLite, INSERT column list
		"does not exist",      // PostgreSQL, relation/column
		"undefined_table",     // PostgreSQL, SQLSTATE name
		"undefined_column",    // PostgreSQL, SQLSTATE name
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
	for _, nullConstraint := range []string{
		"NOT NULL constraint failed", // SQLite
		"null value in column",       // PostgreSQL
	} {
		if strings.Contains(err.Error(), nullConstraint) {
			return true
		}
	}
	return false
}

type sqliteQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func sqliteColumns(t *testing.T, db sqliteQueryer, table string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info("+table+")")
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer func() { _ = rows.Close() }()

	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan column info for %s: %v", table, err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate columns for %s: %v", table, err)
	}
	return columns
}

func sqliteIndexes(t *testing.T, db sqliteQueryer, table string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "PRAGMA index_list("+table+")")
	if err != nil {
		t.Fatalf("PRAGMA index_list(%s): %v", table, err)
	}
	defer func() { _ = rows.Close() }()

	indexes := make(map[string]bool)
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan index info for %s: %v", table, err)
		}
		indexes[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate indexes for %s: %v", table, err)
	}
	return indexes
}
