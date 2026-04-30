package db

import (
	"context"
	"database/sql"
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
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

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
	for i := 0; i < 100; i++ {
		versionID := fmt.Sprintf("01J00000000000000000%06d", i+1)
		task := &model.Task{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          int64(i + 1),
			RefVersionID:   versionID,
			IdempotencyKey: fmt.Sprintf("upload:%s", versionID),
			Status:         model.TaskStatusPending,
			MaxRetries:     3,
			ScheduledAt:    time.Now(),
		}
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	}

	var busyCount atomic.Int64
	var claimedCount atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
				if err != nil {
					if strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database is locked") {
						busyCount.Add(1)
						return
					}
					t.Errorf("ClaimPending() unexpected error = %v", err)
					return
				}
				if task == nil {
					return
				}
				claimedCount.Add(1)
			}
		}()
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
	for _, column := range []string{"current_version_id", "size", "e_tag", "checksum", "cache_key", "in_cache", "in_filecoin", "state"} {
		if objectColumns[column] {
			t.Fatalf("objects.%s should not exist", column)
		}
	}
	for _, column := range []string{"bucket_id", "key"} {
		if !objectColumns[column] {
			t.Fatalf("objects.%s should exist", column)
		}
	}

	versionColumns := sqliteColumns(t, db, "object_versions")
	for _, column := range []string{"is_current", "in_cache", "in_filecoin"} {
		if !versionColumns[column] {
			t.Fatalf("object_versions.%s should exist", column)
		}
	}

	indexes := sqliteIndexes(t, db, "objects")
	if !indexes["idx_objects_bucket_key"] {
		t.Fatal("idx_objects_bucket_key should exist")
	}
	versionIndexes := sqliteIndexes(t, db, "object_versions")
	if !versionIndexes["idx_object_versions_current_unique"] {
		t.Fatal("idx_object_versions_current_unique should exist")
	}
	if !versionIndexes["idx_object_versions_current_bucket_key"] {
		t.Fatal("idx_object_versions_current_bucket_key should exist")
	}
}

func TestRunMigrations_ObjectVersionCurrentAndForeignKeyConstraints(t *testing.T) {
	db := newMigratedSQLiteDB(t, "object-version-constraints.db")
	ctx := context.Background()

	mustExec(t, db, `INSERT INTO buckets (id, name) VALUES (1, 'bucket-a')`)
	mustExec(t, db, `INSERT INTO buckets (id, name) VALUES (2, 'bucket-b')`)
	mustExec(t, db, `INSERT INTO objects (id, bucket_id, key) VALUES (1, 1, 'file.txt')`)
	mustExec(t, db, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, size, e_tag, checksum, cache_key, is_current) VALUES ('v1', 1, 1, 'file.txt', 1, 'etag-1', 'sum-1', '.versions/v1', TRUE)`)

	if _, err := db.ExecContext(ctx, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, size, e_tag, checksum, cache_key, is_current) VALUES ('v2', 1, 1, 'file.txt', 2, 'etag-2', 'sum-2', '.versions/v2', TRUE)`); err == nil {
		t.Fatal("expected second current version for one object to fail")
	}
	mustExec(t, db, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, size, e_tag, checksum, cache_key, is_current) VALUES ('v3', 1, 1, 'file.txt', 3, 'etag-3', 'sum-3', '.versions/v3', FALSE)`)

	if _, err := db.ExecContext(ctx, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, size, e_tag, checksum, cache_key) VALUES ('wrong-bucket', 1, 2, 'file.txt', 1, 'etag-x', 'sum-x', '.versions/wrong-bucket')`); err == nil {
		t.Fatal("expected object_versions object/bucket/key mismatch to fail")
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO object_versions (version_id, object_id, bucket_id, key, size, e_tag, checksum, cache_key, in_filecoin) VALUES ('missing-provider', 1, 1, 'file.txt', 1, 'etag-x', 'sum-x', '.versions/missing-provider', TRUE)`); err == nil {
		t.Fatal("expected in_filecoin without provider metadata to fail")
	}
}

func TestRunMigrations_TaskAndMultipartConstraints(t *testing.T) {
	db := newMigratedSQLiteDB(t, "task-multipart-constraints.db")
	ctx := context.Background()

	mustExec(t, db, `INSERT INTO buckets (id, name) VALUES (1, 'bucket-a')`)

	if _, err := db.ExecContext(ctx, `INSERT INTO tasks (type, ref_type, ref_id, ref_version_id, idempotency_key) VALUES ('upload', 'object', 1, '', 'upload:missing-version')`); err == nil {
		t.Fatal("expected object task without ref_version_id to fail")
	}
	mustExec(t, db, `INSERT INTO tasks (type, ref_type, ref_id, ref_version_id, idempotency_key) VALUES ('upload', 'bucket', 1, '', 'bucket:allowed')`)

	mustExec(t, db, `INSERT INTO multipart_uploads (bucket_id, key, upload_id) VALUES (1, 'large.bin', 'upload-1')`)
	mustExec(t, db, `INSERT INTO multipart_parts (upload_id, part_number, size, e_tag) VALUES ('upload-1', 1, 10, 'part-etag')`)
	if _, err := db.ExecContext(ctx, `INSERT INTO multipart_parts (upload_id, part_number, size, e_tag) VALUES ('upload-1', 10001, 10, 'bad-part')`); err == nil {
		t.Fatal("expected multipart part_number > 10000 to fail")
	}
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

func mustExec(t *testing.T, db *bun.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

type sqliteQueryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
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
