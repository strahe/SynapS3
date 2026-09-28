//go:build postgres

package testutil

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/testpg"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	_ "modernc.org/sqlite"
)

// NewTestPostgresDB creates an isolated PostgreSQL schema with all migrations applied.
func NewTestPostgresDB(t *testing.T) *bun.DB {
	t.Helper()
	config, err := pgx.ParseConfig(testpg.SchemaDSN(t))
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	db := bun.NewDB(stdlib.OpenDB(*config), pgdialect.New())
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL test database: %v", err)
		}
	})
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
