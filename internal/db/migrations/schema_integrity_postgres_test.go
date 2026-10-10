//go:build postgres

package migrations

import (
	"database/sql"
	"testing"

	"github.com/strahe/synaps3/internal/testpg"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func newPostgresMigrationDB(t *testing.T) *bun.DB {
	t.Helper()
	sqlDB, err := sql.Open("pgx", testpg.SchemaDSN(t))
	if err != nil {
		t.Fatalf("open PostgreSQL test database: %v", err)
	}
	db := bun.NewDB(sqlDB, pgdialect.New())
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL test database: %v", err)
		}
	})
	return db
}

func testPostgresMigrationDialect(t *testing.T, test func(*testing.T, *bun.DB)) {
	t.Helper()
	t.Run("Postgres", func(t *testing.T) { test(t, newPostgresMigrationDB(t)) })
}
