package migrations

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

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
	createdAt := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(
		ctx,
		`INSERT INTO object_versions (version_id, in_cache, state, created_at) VALUES (?, TRUE, 'stored', ?)`,
		"version-before-lru-migration",
		createdAt,
	); err != nil {
		t.Fatalf("insert pre-migration object version: %v", err)
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
	var accessedAt time.Time
	if err := db.NewRaw(
		`SELECT cache_accessed_at FROM object_versions WHERE version_id = ?`,
		"version-before-lru-migration",
	).Scan(ctx, &accessedAt); err != nil {
		t.Fatalf("select initialized cache access time: %v", err)
	}
	if !accessedAt.Equal(createdAt) {
		t.Fatalf("initialized cache access time = %v, want created_at %v", accessedAt, createdAt)
	}

	rows, err := db.QueryContext(ctx, `PRAGMA index_info('idx_object_versions_cache_lru')`)
	if err != nil {
		t.Fatalf("inspect LRU index: %v", err)
	}
	var indexColumns []string
	for rows.Next() {
		var sequence, columnID int
		var name string
		if err := rows.Scan(&sequence, &columnID, &name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan LRU index column: %v", err)
		}
		indexColumns = append(indexColumns, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close LRU index rows: %v", err)
	}
	wantColumns := []string{"in_cache", "cache_accessed_at", "created_at", "version_id"}
	if strings.Join(indexColumns, ",") != strings.Join(wantColumns, ",") {
		t.Fatalf("LRU index columns = %v, want %v", indexColumns, wantColumns)
	}

	planRows, err := db.QueryContext(ctx, `EXPLAIN QUERY PLAN
		SELECT version_id
		FROM object_versions
		WHERE in_cache = TRUE
			AND state IN ('stored', 'cache_evicted')
			AND cache_accessed_at IS NOT NULL
		ORDER BY cache_accessed_at, created_at, version_id
		LIMIT 100`)
	if err != nil {
		t.Fatalf("explain LRU candidate order: %v", err)
	}
	for planRows.Next() {
		var id, parent, unused int
		var detail string
		if err := planRows.Scan(&id, &parent, &unused, &detail); err != nil {
			_ = planRows.Close()
			t.Fatalf("scan LRU query plan: %v", err)
		}
		if strings.Contains(detail, "USE TEMP B-TREE") {
			_ = planRows.Close()
			t.Fatalf("LRU candidate order uses a temporary sort: %s", detail)
		}
	}
	if err := planRows.Close(); err != nil {
		t.Fatalf("close LRU query plan rows: %v", err)
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
