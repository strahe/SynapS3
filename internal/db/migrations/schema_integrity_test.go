package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"
	_ "modernc.org/sqlite"
)

const (
	initialPortableSchemaFingerprint = "45c3ff37abc1d6155e03e23ad4c47a3ae2b46b8e668b358727ade95349b85c70"
)

func TestMigrationRegistryStartsWithUniqueOrderedBaseline(t *testing.T) {
	migrations := Migrations.Sorted()
	if len(migrations) == 0 {
		t.Fatal("migration registry is empty")
	}
	if migrations[0].Name != InitialSchemaName {
		t.Fatalf("migration name = %q, want %q", migrations[0].Name, InitialSchemaName)
	}
	for i := 1; i < len(migrations); i++ {
		if migrations[i-1].Name >= migrations[i].Name {
			t.Fatalf("migration names are not unique and ordered: %q then %q", migrations[i-1].Name, migrations[i].Name)
		}
	}
}

func TestMigrationFilesDoNotImportRuntimePackages(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob migration files: %v", err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", file, err)
			}
			if strings.HasPrefix(path, "github.com/strahe/synaps3/internal/") {
				t.Errorf("%s imports runtime package %q", file, path)
			}
		}
	}
}

func TestInitialSchemaFingerprintPostgres(t *testing.T) {
	db := newPostgresMigrationDB(t)
	if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
		t.Fatalf("create initial schema: %v", err)
	}
	got, schema := portableSchemaFingerprint(t, db)
	sqliteDB := newSQLiteMigrationDB(t, "postgres_portable_schema_comparison")
	if err := runMigrationBody(t.Context(), sqliteDB, up2026090101InitialSchema); err != nil {
		t.Fatalf("create comparison SQLite schema: %v", err)
	}
	sqliteFingerprint, sqliteSchema := portableSchemaFingerprint(t, sqliteDB)
	if got != sqliteFingerprint {
		t.Fatalf(
			"portable schema differs by dialect: PostgreSQL=%s SQLite=%s\n%s",
			got,
			sqliteFingerprint,
			semanticSchemaDifference(sqliteSchema, schema),
		)
	}
	if got != initialPortableSchemaFingerprint {
		t.Fatalf("initial PostgreSQL portable schema fingerprint = %s, want %s\n%s", got, initialPortableSchemaFingerprint, schema)
	}
}

func semanticSchemaDifference(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
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

func TestInitialSchemaPortableFingerprintSQLite(t *testing.T) {
	db := newSQLiteMigrationDB(t, "portable_schema_fingerprint")
	if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
		t.Fatalf("create initial schema: %v", err)
	}
	got, schema := portableSchemaFingerprint(t, db)
	if got != initialPortableSchemaFingerprint {
		t.Fatalf("initial SQLite portable schema fingerprint = %s, want %s\n%s", got, initialPortableSchemaFingerprint, schema)
	}
}

func TestInitialSchemaContractSQLite(t *testing.T) {
	db := newSQLiteMigrationDB(t, "initial_schema_contract")
	if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
		t.Fatalf("create initial schema: %v", err)
	}
	for _, table := range []string{
		"s3_accounts", "buckets", "bucket_replica_slots", "objects", "object_versions", "object_cache", "object_deletions",
		"multipart_uploads", "multipart_parts", "storage_contents", "storage_data_sets",
		"storage_copies", "storage_commit_attempts", "storage_replacements",
		"storage_pull_attempts", "storage_replacement_items", "storage_cleanup_copies", "wallet_operations", "tasks",
		"observability_collection_states", "observability_provider_states", "observability_data_set_states", "provider_profiles", "provider_tier_snapshots", "provider_upload_speed_tests",
		"task_payloads", "storage_data_set_terminations",
	} {
		if exists, err := tableExists(t.Context(), db, table); err != nil || !exists {
			t.Errorf("table %s exists=%t err=%v", table, exists, err)
		}
	}
	for _, column := range []struct{ table, name string }{
		{"tasks", "claim_generation"},
		{"provider_upload_speed_tests", "active_task_id"},
		{"task_payloads", "checkpoint_json"},
		{"storage_data_set_terminations", "epoch"},
		{"storage_copies", "active_task_id"},
		{"storage_copies", "bucket_id"},
		{"storage_copies", "content_size"},
		{"storage_copies", "storage_data_set_id"},
		{"storage_copies", "ingress_bytes_transferred"},
		{"storage_commit_attempts", "attempt_id"},
		{"storage_pull_attempts", "attempt_id"},
		{"storage_pull_attempts", "source_piece_cid"},
		{"storage_contents", "content_size"},
		{"storage_data_sets", "ensure_task_id"},
		{"storage_replacement_items", "target_data_set_id"},
		{"storage_cleanup_copies", "bucket_id"},
		{"storage_cleanup_copies", "checksum"},
		{"object_versions", "content_id"},
		{"object_cache", "cache_active_task_id"},
		{"wallet_operations", "broadcast_attempted_at"},
	} {
		if exists, err := columnExists(t.Context(), db, column.table, column.name); err != nil || !exists {
			t.Errorf("column %s.%s exists=%t err=%v", column.table, column.name, exists, err)
		}
	}
	for _, index := range []string{
		"idx_tasks_pending",
		"idx_tasks_recovery",
		"idx_tasks_gc",
		"idx_storage_copies_commit_ready",
		"idx_storage_commit_attempts_unresolved_copy",
		"idx_storage_copies_ingress_content",
		"idx_storage_data_sets_bucket_provider_active",
		"idx_storage_replacements_active_bucket_slot",
		"idx_wallet_operations_recent",
		"idx_provider_upload_speed_tests_active_task",
	} {
		if exists, err := indexExists(t.Context(), db, index); err != nil || !exists {
			t.Errorf("index %s exists=%t err=%v", index, exists, err)
		}
	}
	for _, index := range []string{
		"idx_storage_data_sets_replica_slot",
		"idx_storage_replacements_replica_slot",
	} {
		if exists, err := indexExists(t.Context(), db, index); err != nil || exists {
			t.Errorf("removed index %s exists=%t err=%v", index, exists, err)
		}
	}
	for _, column := range []struct{ table, name string }{
		{"tasks", "stage"},
		{"tasks", "category"},
		{"tasks", "parent_task_id"},
		{"tasks", "workflow_id"},
		{"tasks", "priority"},
		{"tasks", "lane"},
		{"storage_replacement_items", "claimed_at"},
		{"storage_replacement_items", "lease_until"},
		{"storage_replacement_items", "scheduled_at"},
		{"storage_replacement_items", "retry_count"},
		{"storage_replacement_items", "task_id"},
		{"storage_replacement_items", "target_copy_id"},
		{"multipart_uploads", "id"},
		{"storage_copies", "commit_attempt_id"},
		{"storage_copies", "commit_attempted_at"},
		{"storage_copies", "commit_transaction_id"},
		{"storage_copies", "commit_status_url"},
		{"storage_copies", "commit_confirmed_transaction_id"},
		{"storage_copies", "commit_attention_code"},
		{"storage_copies", "commit_attention_at"},
		{"storage_copies", "is_new_data_set"},
		// Pull identity is a ledger row now, not five nullable copy columns.
		{"storage_copies", "pull_request_id"},
		{"storage_copies", "pull_source_provider_id"},
		{"storage_copies", "pull_source_data_set_id"},
		{"storage_copies", "pull_source_piece_id"},
		{"storage_copies", "pull_source_retrieval_url"},
		{"storage_pull_attempts", "request_id"},
		// Pipeline position, cache residency and the content status column are
		// derived or moved; reintroducing any of them re-creates a second
		// authority for a fact the copies or object_cache already own.
		{"object_versions", "is_current"},
		{"object_versions", "state"},
		{"object_versions", "failed_at_state"},
		{"object_versions", "last_error"},
		{"object_versions", "checksum"},
		{"object_versions", "storage_upload_id"},
		{"object_versions", "cache_key"},
		{"object_versions", "in_cache"},
		{"object_versions", "cache_accessed_at"},
		{"object_versions", "cache_presence_generation"},
		{"object_versions", "cache_operation_generation"},
		{"object_versions", "cache_active_task_id"},
		{"storage_contents", "status"},
		{"storage_contents", "state"},
		{"storage_contents", "disposition"},
		{"storage_contents", "superseded_by_id"},
		{"storage_contents", "committed_slots"},
		{"storage_contents", "ingress_bytes_transferred"},
		{"storage_contents", "created_from_version_id"},
		{"observability_provider_states", "created_at"},
		{"observability_provider_states", "updated_at"},
		{"observability_data_set_states", "created_at"},
		{"observability_data_set_states", "updated_at"},
		{"object_deletions", "cache_cleanup_status"},
		{"object_deletions", "cache_error"},
		// The JSON a task carries and the terminations a replacement records
		// are rows of their own; putting either back re-creates the write
		// amplification and the repeated column group they were split out of.
		{"tasks", "input_json"},
		{"tasks", "checkpoint_json"},
		{"storage_replacements", "termination_tx_hash"},
		{"storage_replacements", "termination_epoch"},
		{"storage_replacements", "termination_observed_at"},
		{"storage_replacements", "abandoned_termination_tx_hash"},
		{"storage_replacements", "abandoned_termination_epoch"},
		{"storage_replacements", "abandoned_termination_observed_at"},
		{"storage_replacements", "confirmed_at"},
	} {
		if exists, err := columnExists(t.Context(), db, column.table, column.name); err != nil || exists {
			t.Errorf("removed column %s.%s exists=%t err=%v", column.table, column.name, exists, err)
		}
	}
}

func TestInitialSchemaTaskOwnerForeignKeysAreRestrictive(t *testing.T) {
	db := newSQLiteMigrationDB(t, "task_owner_foreign_keys")
	if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
		t.Fatalf("create initial schema: %v", err)
	}
	want := map[string]map[string]bool{
		"buckets":              {"durability_task_id": false},
		"object_cache":         {"cache_active_task_id": false},
		"storage_contents":     {"cleanup_task_id": false},
		"storage_data_sets":    {"ensure_task_id": false, "retirement_task_id": false},
		"storage_copies":       {"active_task_id": false},
		"storage_replacements": {"task_id": false},
		"wallet_operations":    {"task_id": false},
	}
	for table, columns := range want {
		rows, err := db.Query(`SELECT "from", "table", on_delete FROM pragma_foreign_key_list(?)`, table)
		if err != nil {
			t.Fatalf("query foreign keys for %s: %v", table, err)
		}
		for rows.Next() {
			var from, target, onDelete string
			if err := rows.Scan(&from, &target, &onDelete); err != nil {
				_ = rows.Close()
				t.Fatalf("scan foreign key for %s: %v", table, err)
			}
			if _, tracked := columns[from]; !tracked || target != "tasks" {
				continue
			}
			if onDelete != "RESTRICT" {
				_ = rows.Close()
				t.Fatalf("%s.%s ON DELETE = %s, want RESTRICT", table, from, onDelete)
			}
			columns[from] = true
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close foreign keys for %s: %v", table, err)
		}
		for column, found := range columns {
			if !found {
				t.Errorf("missing task ownership foreign key %s.%s", table, column)
			}
		}
	}
}

func TestValidateTargetRejectsLegacyDatabaseWithoutModification(t *testing.T) {
	db := newSQLiteMigrationDB(t, "legacy_rejection")
	if _, err := db.Exec(`CREATE TABLE tasks (id INTEGER PRIMARY KEY, status TEXT)`); err != nil {
		t.Fatalf("seed legacy table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (id, status) VALUES (7, 'running')`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	err := ValidateTarget(t.Context(), db)
	if !errors.Is(err, ErrIncompatibleDatabase) {
		t.Fatalf("ValidateTarget error = %v, want ErrIncompatibleDatabase", err)
	}
	var status string
	if err := db.NewRaw(`SELECT status FROM tasks WHERE id = 7`).Scan(t.Context(), &status); err != nil {
		t.Fatalf("read legacy row after rejection: %v", err)
	}
	if status != "running" {
		t.Fatalf("legacy row status = %q, want unchanged", status)
	}
	if exists, err := tableExists(t.Context(), db, "bun_migrations"); err != nil || exists {
		t.Fatalf("migration marker exists=%t err=%v after rejection", exists, err)
	}
}

func TestValidateTargetAcceptsOnlyAppliedMigrationPrefixes(t *testing.T) {
	registry := migrate.NewMigrations()
	registry.Add(migrate.Migration{Name: InitialSchemaName})
	registry.Add(migrate.Migration{Name: "2026090201"})
	registry.Add(migrate.Migration{Name: "2026090301"})

	tests := []struct {
		name    string
		applied []string
		wantErr bool
	}{
		{name: "metadata only"},
		{name: "baseline", applied: []string{InitialSchemaName}},
		{name: "longer prefix", applied: []string{InitialSchemaName, "2026090201"}},
		{name: "full registry", applied: []string{InitialSchemaName, "2026090201", "2026090301"}},
		{name: "legacy marker", applied: []string{"2026040501"}, wantErr: true},
		{name: "unknown marker", applied: []string{InitialSchemaName, "2026090250"}, wantErr: true},
		{name: "duplicate marker", applied: []string{InitialSchemaName, InitialSchemaName}, wantErr: true},
		{name: "out of order", applied: []string{"2026090201", InitialSchemaName}, wantErr: true},
		{name: "gap", applied: []string{InitialSchemaName, "2026090301"}, wantErr: true},
		{name: "longer than registry", applied: []string{InitialSchemaName, "2026090201", "2026090301", "2026090401"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newSQLiteMigrationDB(t, "migration_prefix_"+strings.ReplaceAll(tt.name, " ", "_"))
			if err := newMigrator(db, registry).Init(t.Context()); err != nil {
				t.Fatalf("initialize migration metadata: %v", err)
			}
			if len(tt.applied) > 0 {
				if err := up2026090101InitialSchema(t.Context(), db); err != nil {
					t.Fatalf("create baseline schema: %v", err)
				}
			}
			for _, name := range tt.applied {
				if _, err := db.Exec(`INSERT INTO bun_migrations (name, group_id) VALUES (?, 1)`, name); err != nil {
					t.Fatalf("insert migration marker %q: %v", name, err)
				}
			}

			err := validateTarget(t.Context(), db, registry)
			if tt.wantErr {
				if !errors.Is(err, ErrIncompatibleDatabase) {
					t.Fatalf("validateTarget error = %v, want ErrIncompatibleDatabase", err)
				}
			} else if err != nil {
				t.Fatalf("validateTarget error = %v", err)
			}

			var names []string
			if err := db.NewRaw(`SELECT name FROM bun_migrations ORDER BY id`).Scan(t.Context(), &names); err != nil {
				t.Fatalf("read migration markers: %v", err)
			}
			if !slices.Equal(names, tt.applied) {
				t.Fatalf("migration markers after validation = %v, want unchanged %v", names, tt.applied)
			}
		})
	}
}

func TestValidateTargetRejectsInvalidMigrationRegistry(t *testing.T) {
	tests := []struct {
		name     string
		registry *migrate.Migrations
	}{
		{name: "empty", registry: migrate.NewMigrations()},
		{name: "missing baseline", registry: migrationRegistryForTest("2026090201")},
		{name: "duplicate", registry: migrationRegistryForTest(InitialSchemaName, InitialSchemaName)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newSQLiteMigrationDB(t, "invalid_registry_"+strings.ReplaceAll(tt.name, " ", "_"))
			if err := validateTarget(t.Context(), db, tt.registry); !errors.Is(err, ErrIncompatibleDatabase) {
				t.Fatalf("validateTarget error = %v, want ErrIncompatibleDatabase", err)
			}
			if count, err := applicationTableCount(t.Context(), db); err != nil || count != 0 {
				t.Fatalf("application table count after rejection = %d, err=%v", count, err)
			}
		})
	}
}

func migrationRegistryForTest(names ...string) *migrate.Migrations {
	registry := migrate.NewMigrations()
	for _, name := range names {
		registry.Add(migrate.Migration{Name: name})
	}
	return registry
}

func TestFreshBaselineIsIdempotentAndCannotRollback(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		if err := ValidateTarget(ctx, db); err != nil {
			t.Fatalf("validate empty target: %v", err)
		}
		migrator := NewMigrator(db)
		if err := migrator.Init(ctx); err != nil {
			t.Fatalf("initialize migrator: %v", err)
		}
		first, err := migrator.Migrate(ctx)
		if err != nil {
			t.Fatalf("migrate fresh schema: %v", err)
		}
		if len(first.Migrations) != 2 || first.Migrations[0].Name != InitialSchemaName || first.Migrations[1].Name != "2026092401" {
			t.Fatalf("first migration group = %#v", first.Migrations)
		}
		second, err := migrator.Migrate(ctx)
		if err != nil {
			t.Fatalf("repeat migration: %v", err)
		}
		if len(second.Migrations) != 0 {
			t.Fatalf("repeat migration applied %d migrations", len(second.Migrations))
		}
		if _, err := migrator.Rollback(ctx); err == nil {
			t.Fatal("initial schema rollback succeeded")
		}
		if exists, err := tableExists(ctx, db, "tasks"); err != nil || !exists {
			t.Fatalf("tasks table exists=%t err=%v after rejected rollback", exists, err)
		}
	})
}

func TestValidateTargetRejectsObsoleteCommitLedgerShape(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		migrator := NewMigrator(db)
		if err := migrator.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := migrator.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if err := ValidateTarget(ctx, db); err != nil {
			t.Fatalf("validate current schema: %v", err)
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE storage_commit_attempts RENAME COLUMN status_url TO submission_json`); err != nil {
			t.Fatalf("simulate obsolete commit ledger: %v", err)
		}
		if err := ValidateTarget(ctx, db); !errors.Is(err, ErrIncompatibleDatabase) {
			t.Fatalf("validate obsolete commit ledger = %v, want incompatible database", err)
		}
	})
}

func TestValidateTargetRejectsBaselineWithoutProviderProfiles(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		migrator := NewMigrator(db)
		if err := migrator.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := migrator.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE provider_profiles RENAME TO old_provider_profiles`); err != nil {
			t.Fatal(err)
		}
		if err := ValidateTarget(ctx, db); !errors.Is(err, ErrIncompatibleDatabase) {
			t.Fatalf("validate old baseline = %v, want rebuild guidance", err)
		}
	})
}

func runMigrationBody(ctx context.Context, db *bun.DB, body migrationBody) error {
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		return body(ctx, tx)
	})
}

func newSQLiteMigrationDB(t *testing.T, name string) *bun.DB {
	t.Helper()
	sqldb, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newPostgresMigrationDB(t *testing.T) *bun.DB {
	t.Helper()
	dsn := postgresTestDSN(t)
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	adminSQLDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	adminDB := bun.NewDB(adminSQLDB, pgdialect.New())
	testHash := fmt.Sprintf("%x", sha256.Sum256([]byte(t.Name())))[:16]
	schema := fmt.Sprintf("migration_%s_%x", testHash, time.Now().UnixNano())
	if _, err := adminDB.Exec("CREATE SCHEMA " + quotePostgresName(schema)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create PostgreSQL schema: %v", err)
	}
	pgConfig.RuntimeParams["search_path"] = schema
	db := bun.NewDB(stdlib.OpenDB(*pgConfig), pgdialect.New())
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = adminDB.Exec("DROP SCHEMA " + quotePostgresName(schema) + " CASCADE")
		_ = adminDB.Close()
	})
	return db
}

func postgresTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	return dsn
}

func quotePostgresName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func normalizedFingerprint(lines []string) (string, string) {
	payload := strings.Join(lines, "\n")
	return fmt.Sprintf("%x", sha256.Sum256([]byte(payload))), payload
}
