package migrations

import (
	"context"
	"database/sql"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	_ "modernc.org/sqlite"
)

func TestCacheLRUMigrationAddsAccessTracking(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:cache_lru_migration?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE object_versions (
		version_id TEXT PRIMARY KEY,
		in_cache BOOLEAN NOT NULL DEFAULT TRUE,
		state TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL
	)`); err != nil {
		t.Fatalf("create object_versions: %v", err)
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

	if err := down2026072801CacheLRU(ctx, db); err != nil {
		t.Fatalf("down migration: %v", err)
	}
	if sqliteColumnExists(t, db, "object_versions", "cache_accessed_at") {
		t.Fatal("object_versions.cache_accessed_at still exists after down migration")
	}
	if sqliteIndexExists(t, db, "idx_object_versions_cache_lru") {
		t.Fatal("idx_object_versions_cache_lru still exists after down migration")
	}
}
