package repository_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	_ "modernc.org/sqlite"
)

// testDB creates a fresh in-memory SQLite DB with all migrations applied.
func testDB(t *testing.T) *bun.DB {
	t.Helper()

	sqldb, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening test db: %v", err)
	}
	sqldb.SetMaxOpenConns(1)

	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	return db
}

// concurrentTestDB creates a file-backed SQLite DB that allows overlapping connections.
func concurrentTestDB(t *testing.T) *bun.DB {
	t.Helper()

	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "repo.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening concurrent test db: %v", err)
	}
	sqldb.SetMaxOpenConns(8)

	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	return db
}

func mustExec(t *testing.T, db *bun.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.NewRaw(query, args...).Exec(context.Background()); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func onChainID(t *testing.T, value string) types.OnChainID {
	t.Helper()
	id, err := types.ParseOnChainID("test id", value)
	if err != nil {
		t.Fatalf("parse on-chain id %q: %v", value, err)
	}
	return id
}

func onChainIDPtr(t *testing.T, value string) *types.OnChainID {
	t.Helper()
	id := onChainID(t, value)
	return &id
}

// seedBucket inserts a bucket and returns it.
func seedBucket(t *testing.T, db *bun.DB, name string) *model.Bucket {
	t.Helper()
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive}
	_, err := db.NewInsert().Model(bucket).Exec(context.Background())
	if err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	return bucket
}
