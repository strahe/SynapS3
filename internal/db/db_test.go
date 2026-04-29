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
	cfg := config.DatabaseConfig{
		Driver:       "sqlite",
		DSN:          "file:" + filepath.Join(t.TempDir(), "schema.db") + "?_pragma=journal_mode(WAL)",
		MaxOpenConns: 1,
		MaxIdleConns: 1,
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

	for _, table := range []string{"objects", "object_versions"} {
		columns := sqliteColumns(t, db, table)
		for _, column := range []string{"retry_count", "max_retries"} {
			if columns[column] {
				t.Fatalf("%s.%s should not exist", table, column)
			}
		}
		for _, column := range []string{"in_cache", "in_filecoin"} {
			if !columns[column] {
				t.Fatalf("%s.%s should exist", table, column)
			}
		}
	}

	indexes := sqliteIndexes(t, db, "objects")
	if indexes["idx_objects_bucket_key_list"] {
		t.Fatal("idx_objects_bucket_key_list should not exist")
	}
	if !indexes["idx_objects_bucket_key"] {
		t.Fatal("idx_objects_bucket_key should exist")
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
