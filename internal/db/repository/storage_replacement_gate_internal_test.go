package repository

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	synaps3db "github.com/strahe/synaps3/internal/db"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"
)

func TestRetirementCoverageProductionSQLUsesIndexes(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "retirement-plan.db")+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := synaps3db.RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	rows, err := sqldb.QueryContext(ctx, "EXPLAIN QUERY PLAN "+retirementCoverageGapsSQL(), int64(1), false, int64(2))
	if err != nil {
		t.Fatalf("EXPLAIN production retirement coverage query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var details []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read query plan: %v", err)
	}
	plan := strings.Join(details, "\n")
	for _, index := range []string{
		"idx_storage_upload_copies_status_data_set_upload",
		"idx_object_versions_storage_upload",
	} {
		if !strings.Contains(plan, index) {
			t.Fatalf("production retirement coverage plan =\n%s\nwant %s", plan, index)
		}
	}
}
