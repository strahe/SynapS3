package migrations

import (
	"context"
	"database/sql"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	_ "modernc.org/sqlite"
)

func TestCacheLRUMigrationAddsAccessTrackingAndCancelsLegacyActiveTasks(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:cache_lru_migration?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	for _, statement := range []string{
		`CREATE TABLE object_versions (
			version_id TEXT PRIMARY KEY,
			in_cache BOOLEAN NOT NULL DEFAULT TRUE,
			state TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL
		)`,
		`CREATE TABLE tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			stage TEXT,
			status TEXT NOT NULL,
			completed_at TIMESTAMP,
			claimed_at TIMESTAMP,
			lease_until TIMESTAMP,
			started_at TIMESTAMP,
			wait_reason TEXT,
			status_message TEXT
		)`,
		`INSERT INTO tasks (type, stage, status) VALUES
			('evict_cache', NULL, 'queued'),
			('evict_cache', '', 'running'),
			('evict_cache', NULL, 'failed'),
			('evict_cache', 'after_upload', 'queued'),
			('upload', NULL, 'queued')`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}

	if err := up2026072801CacheLRU(ctx, db); err != nil {
		t.Fatalf("up migration: %v", err)
	}
	if !sqliteColumnExists(t, db, "object_versions", "cache_accessed_at") {
		t.Fatal("object_versions.cache_accessed_at column missing")
	}
	if !sqliteIndexExists(t, db, "idx_object_versions_cache_lru") {
		t.Fatal("idx_object_versions_cache_lru index missing")
	}

	type taskState struct {
		Status        string         `bun:"status"`
		CompletedAt   sql.NullTime   `bun:"completed_at"`
		StatusMessage sql.NullString `bun:"status_message"`
	}
	var tasks []taskState
	if err := db.NewSelect().Table("tasks").Column("status", "completed_at", "status_message").OrderExpr("id ASC").Scan(ctx, &tasks); err != nil {
		t.Fatalf("list migrated tasks: %v", err)
	}
	if len(tasks) != 5 {
		t.Fatalf("task count = %d, want 5", len(tasks))
	}
	for _, index := range []int{0, 1} {
		if tasks[index].Status != "cancelled" || !tasks[index].CompletedAt.Valid || !tasks[index].StatusMessage.Valid {
			t.Fatalf("legacy active task %d = %#v, want cancelled with diagnostics", index, tasks[index])
		}
	}
	if tasks[2].Status != "failed" {
		t.Fatalf("legacy terminal task status = %s, want failed", tasks[2].Status)
	}
	if tasks[3].Status != "queued" || tasks[4].Status != "queued" {
		t.Fatalf("staged/non-eviction tasks changed: %#v", tasks)
	}

	if err := down2026072801CacheLRU(ctx, db); err != nil {
		t.Fatalf("down migration: %v", err)
	}
	if sqliteColumnExists(t, db, "object_versions", "cache_accessed_at") {
		t.Fatal("object_versions.cache_accessed_at still exists after down migration")
	}
	if sqliteIndexExists(t, db, "idx_object_versions_cache_lru") {
		t.Fatal("idx_object_versions_cache_lru still exists after down migration")
	}
	var cancelled int
	if err := db.NewSelect().Table("tasks").ColumnExpr("COUNT(*)").Where("status = 'cancelled'").Scan(ctx, &cancelled); err != nil {
		t.Fatalf("count cancelled tasks after down migration: %v", err)
	}
	if cancelled != 2 {
		t.Fatalf("cancelled task count after down migration = %d, want irreversible count 2", cancelled)
	}
}
