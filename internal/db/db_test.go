package db

import (
	"context"
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
		task := &model.Task{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          int64(i + 1),
			RefGeneration:  1,
			IdempotencyKey: fmt.Sprintf("upload:%d:1", i+1),
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
