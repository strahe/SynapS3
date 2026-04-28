// Package testutil provides shared test infrastructure for SynapS3 packages.
// It is only compiled and linked in test binaries.
package testutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"

	_ "modernc.org/sqlite"
)

// NewTestDB creates a fresh in-memory SQLite DB with all migrations applied.
// The database is closed automatically when the test completes.
func NewTestDB(t *testing.T) *bun.DB {
	t.Helper()

	sqldb, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening test db: %v", err)
	}
	sqldb.SetMaxOpenConns(1)

	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, migrations.Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	return db
}

// NewTestRepos creates a Repositories instance backed by an in-memory SQLite DB.
func NewTestRepos(t *testing.T) *repository.Repositories {
	t.Helper()
	return repository.NewRepositories(NewTestDB(t))
}

// SeedBucket inserts an active bucket and returns it.
func SeedBucket(t *testing.T, db *bun.DB, name string) *model.Bucket {
	t.Helper()
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive}
	_, err := db.NewInsert().Model(bucket).Exec(context.Background())
	if err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	return bucket
}

// SeedBucketWithStatus inserts a bucket with the specified status.
func SeedBucketWithStatus(t *testing.T, db *bun.DB, name string, status model.BucketStatus) *model.Bucket {
	t.Helper()
	bucket := &model.Bucket{Name: name, Status: status}
	_, err := db.NewInsert().Model(bucket).Exec(context.Background())
	if err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	return bucket
}
