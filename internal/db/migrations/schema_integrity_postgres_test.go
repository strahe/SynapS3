//go:build postgres

package migrations

import (
	"database/sql"
	"slices"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/testpg"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	_ "modernc.org/sqlite"
)

// A PostgreSQL database is validated against the SQLite reference on what the
// two dialects share, so the baseline must build that shared schema on both.
func TestInitialSchemaMatchesAcrossDialects(t *testing.T) {
	db := newPostgresMigrationDB(t)
	if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
		t.Fatalf("create initial schema: %v", err)
	}
	sqliteDB := newSQLiteMigrationDB(t, "postgres_portable_schema_comparison")
	if err := runMigrationBody(t.Context(), sqliteDB, up2026090101InitialSchema); err != nil {
		t.Fatalf("create comparison SQLite schema: %v", err)
	}
	postgresSchema := portableSchema(t, db)
	sqliteSchema := portableSchema(t, sqliteDB)
	if !slices.Equal(postgresSchema, sqliteSchema) {
		t.Fatalf("portable schema differs by dialect:\n%s", semanticSchemaDifference(sqliteSchema, postgresSchema))
	}
}

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

// portableSchema describes what both dialects share.
func portableSchema(t *testing.T, db *bun.DB) []string {
	t.Helper()
	schema, err := describeSchema(t.Context(), db, true)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func semanticSchemaDifference(wantLines, gotLines []string) string {
	wantSet := make(map[string]struct{}, len(wantLines))
	gotSet := make(map[string]struct{}, len(gotLines))
	for _, line := range wantLines {
		wantSet[line] = struct{}{}
	}
	for _, line := range gotLines {
		gotSet[line] = struct{}{}
	}
	difference := make([]string, 0)
	for _, line := range wantLines {
		if _, ok := gotSet[line]; !ok {
			difference = append(difference, "- "+line)
		}
	}
	for _, line := range gotLines {
		if _, ok := wantSet[line]; !ok {
			difference = append(difference, "+ "+line)
		}
	}
	return strings.Join(difference, "\n")
}
