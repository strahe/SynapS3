package migrations

import (
	"testing"

	"github.com/uptrace/bun"
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
