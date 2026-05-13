package repository

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	synaps3db "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"
)

func TestClaimReadySQLUsesReadyScheduledIndex(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "claim-ready-plan.db")+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := synaps3db.RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	now := time.Now()
	rows, err := sqldb.QueryContext(ctx, "EXPLAIN QUERY PLAN "+claimReadySQL,
		model.TaskStatusRunning, now, now.Add(time.Minute), now,
		model.TaskTypeUpload,
		now,
	)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN ClaimReady: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var details []string
	for rows.Next() {
		var id int
		var parent int
		var notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate plan rows: %v", err)
	}

	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "idx_tasks_type_ready_scheduled") {
		t.Fatalf("ClaimReady plan =\n%s\nwant idx_tasks_type_ready_scheduled", plan)
	}
	if strings.Contains(plan, "USE TEMP B-TREE") {
		t.Fatalf("ClaimReady plan =\n%s\nwant no temp sort", plan)
	}
}
