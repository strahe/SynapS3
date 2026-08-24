package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/strahe/synaps3/internal/config"
	synaps3db "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
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
	plan := sqliteProductionQueryPlan(t, sqldb, retirementCoverageGapsSQL(), int64(1), false, int64(2))
	for _, index := range []string{
		"idx_storage_upload_copies_status_data_set_upload",
		"idx_object_versions_storage_upload",
	} {
		if !strings.Contains(plan, index) {
			t.Fatalf("production retirement coverage plan =\n%s\nwant %s", plan, index)
		}
	}

	tests := []struct {
		name  string
		query string
		args  []any
		index string
	}{
		{
			name:  "replacement upload window",
			query: storageUploadMigrationWindowSQL(),
			args:  []any{int64(1), int64(0), 100},
			index: "idx_storage_uploads_bucket_id",
		},
		{
			name:  "replacement item lock",
			query: lockReplacementItemsByUploadSQL(),
			args:  []any{int64(1)},
			index: "idx_storage_replacement_items_upload_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := sqliteProductionQueryPlan(t, sqldb, tt.query, tt.args...)
			if !strings.Contains(plan, tt.index) {
				t.Fatalf("production query plan =\n%s\nwant %s", plan, tt.index)
			}
		})
	}

	readyPlan := sqliteProductionQueryPlan(t, sqldb, readyReplacementSelectionSQL(dialect.SQLite),
		storagereplacement.StatusMigrating,
		storagereplacement.StatusWaiting,
		storagereplacement.WaitReasonReadableSource,
		time.Now(),
		time.Now(),
	)
	for _, index := range []string{"idx_storage_replacement_items_due", "idx_storage_replacement_items_lease"} {
		if !strings.Contains(readyPlan, index) {
			t.Fatalf("ready replacement plan =\n%s\nwant %s", readyPlan, index)
		}
	}

	candidateTests := []struct {
		name  string
		query string
		args  []any
		index string
	}{
		{
			name:  "due replacement item",
			query: readyReplacementDueItemSQL(),
			args:  []any{int64(1), time.Now()},
			index: "idx_storage_replacement_items_due",
		},
		{
			name:  "expired replacement item",
			query: readyReplacementExpiredItemSQL(),
			args:  []any{int64(1), time.Now()},
			index: "idx_storage_replacement_items_lease",
		},
	}
	for _, tt := range candidateTests {
		t.Run(tt.name, func(t *testing.T) {
			plan := sqliteProductionQueryPlan(t, sqldb, tt.query, tt.args...)
			if !strings.Contains(plan, tt.index) || strings.Contains(plan, "USE TEMP B-TREE") {
				t.Fatalf("replacement item candidate plan =\n%s\nwant %s without a temp sort", plan, tt.index)
			}
		})
	}
}

func sqliteProductionQueryPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("EXPLAIN production query: %v", err)
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
	return strings.Join(details, "\n")
}

func TestPostgresReplacementProductionSQLUsesNewIndexes(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	adminDB, err := synaps3db.New(config.DatabaseConfig{
		Driver: "postgres", DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	schema := fmt.Sprintf("replacement_query_plan_%d", time.Now().UnixNano())
	quoted := `"` + schema + `"`
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create PostgreSQL schema: %v", err)
	}
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		_, _ = adminDB.ExecContext(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		_ = adminDB.Close()
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	pgConfig.RuntimeParams["search_path"] = schema
	registeredDSN := stdlib.RegisterConnConfig(pgConfig)
	db, err := synaps3db.New(config.DatabaseConfig{
		Driver: "postgres", DSN: registeredDSN, MaxOpenConns: 1, MaxIdleConns: 1,
	})
	if err != nil {
		stdlib.UnregisterConnConfig(registeredDSN)
		_, _ = adminDB.ExecContext(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		_ = adminDB.Close()
		t.Fatalf("open schema-scoped PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		stdlib.UnregisterConnConfig(registeredDSN)
		_, _ = adminDB.ExecContext(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
		_ = adminDB.Close()
	})
	if err := synaps3db.RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	statements := []string{
		`INSERT INTO buckets (id, name) VALUES (1, 'query-plan-target'), (2, 'query-plan-other')`,
		`INSERT INTO storage_uploads (id, bucket_id, content_size, checksum, requested_copies)
		 SELECT n, CASE WHEN n = 1 THEN 1 ELSE 2 END, 1, 'checksum-' || n, 1
		 FROM generate_series(1, 10000) AS n`,
		`INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, status)
		 VALUES (1, 1, '101', 0, 'ready'), (2, 1, '202', 1, 'ready')`,
		`INSERT INTO storage_replacements
			(id, bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, status)
		 SELECT n, 1, 0, 1, 2, 'automatic', 'query-plan-' || n, 'completed'
		 FROM generate_series(1, 10000) AS n`,
		`INSERT INTO storage_replacement_items (id, replacement_id, upload_id, status)
		 SELECT n, n, n, 'pending' FROM generate_series(1, 10000) AS n`,
		`ANALYZE storage_uploads`,
		`ANALYZE storage_replacement_items`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed PostgreSQL query plan data: %v", err)
		}
	}
	tests := []struct {
		name  string
		query string
		args  []any
		index string
	}{
		{"replacement upload window", storageUploadMigrationWindowSQL(), []any{int64(1), int64(0), 100}, "idx_storage_uploads_bucket_id"},
		{"replacement item lock", lockReplacementItemsByUploadSQL(), []any{int64(1)}, "idx_storage_replacement_items_upload_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lines []string
			if err := db.NewRaw("EXPLAIN "+tt.query, tt.args...).Scan(ctx, &lines); err != nil {
				t.Fatalf("EXPLAIN production query: %v", err)
			}
			plan := strings.Join(lines, "\n")
			if !strings.Contains(plan, tt.index) {
				t.Fatalf("production query plan =\n%s\nwant %s", plan, tt.index)
			}
		})
	}
}
