package migrations

import (
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

func applicationSchemaTables(t *testing.T, db *bun.DB) []string {
	t.Helper()
	tables, err := applicationTableNames(t.Context(), db)
	if err != nil {
		t.Fatalf("read application schema tables: %v", err)
	}
	return tables
}

func appliedTableColumns(t *testing.T, db *bun.DB, table string) []appliedColumn {
	t.Helper()
	columns, err := tableColumns(t.Context(), db, table)
	if err != nil {
		t.Fatal(err)
	}
	return columns
}

// migrateToLevel runs the first level registered migrations that have not run.
func migrateToLevel(t *testing.T, db *bun.DB, level int) {
	t.Helper()
	registry := migrate.NewMigrations()
	for _, migration := range Migrations.Sorted()[:level] {
		registry.Add(migration)
	}
	migrator := newMigrator(db, registry)
	if err := migrator.Init(t.Context()); err != nil {
		t.Fatalf("initialize migrator: %v", err)
	}
	if _, err := migrator.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate to level %d: %v", level, err)
	}
}

func migrationLevel(t *testing.T, name string) int {
	t.Helper()
	for i, migration := range Migrations.Sorted() {
		if migration.Name == name {
			return i + 1
		}
	}
	t.Fatalf("migration %s is not registered", name)
	return 0
}

func migrateThrough(t *testing.T, db *bun.DB, name string) {
	t.Helper()
	migrateToLevel(t, db, migrationLevel(t, name))
}
