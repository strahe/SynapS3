package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
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

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

const (
	initialSQLiteSchemaFingerprint   = "a76c972a38ca0fd958625a4e1a6ca69f69a7dc67ffe7abe0bb0bff06481044bb"
	initialPostgresSchemaFingerprint = "91bc1d871d84e05ae3f995a10c14a21276fbe39eb12df4aed3aab9138a5ea964"
)

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

func TestInitialSchemaFingerprintSQLite(t *testing.T) {
	db := newSQLiteMigrationDB(t, "initial_schema_fingerprint")
	ctx := context.Background()
	if err := runMigrationBody(ctx, db, up2026040501Init); err != nil {
		t.Fatalf("create initial schema: %v", err)
	}
	got, _ := sqliteSchemaFingerprint(t, db, false)
	if got != initialSQLiteSchemaFingerprint {
		t.Fatalf("initial SQLite schema fingerprint = %s, want %s", got, initialSQLiteSchemaFingerprint)
	}
}

func TestInitialSchemaFingerprintPostgres(t *testing.T) {
	db := newPostgresMigrationDB(t)
	ctx := context.Background()
	if err := runMigrationBody(ctx, db, up2026040501Init); err != nil {
		t.Fatalf("create initial schema: %v", err)
	}
	got, _ := postgresSchemaFingerprint(t, db, false)
	if got != initialPostgresSchemaFingerprint {
		t.Fatalf("initial PostgreSQL schema fingerprint = %s, want %s", got, initialPostgresSchemaFingerprint)
	}
}

func TestLegacyMigrationUpgradePreservesDataAndIsIdempotent(t *testing.T) {
	testMigrationDialects(t, testLegacyMigrationUpgradePreservesDataAndIsIdempotent)
}

func testLegacyMigrationUpgradePreservesDataAndIsIdempotent(t *testing.T, db *bun.DB) {
	ctx := context.Background()
	migrator := NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("initialize legacy migrator: %v", err)
	}
	if err := runMigrationBody(ctx, db, up2026040501Init); err != nil {
		t.Fatalf("create legacy initial schema: %v", err)
	}
	markAppliedMigration(t, ctx, migrator, "2026040501", 1)
	seedLegacyMigrationData(t, db)
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("upgrade legacy schema: %v", err)
	}

	var generation int
	var current bool
	if err := db.NewRaw("SELECT generation, is_current FROM storage_data_sets WHERE id = 1").Scan(ctx, &generation, &current); err != nil {
		t.Fatalf("read upgraded legacy data: %v", err)
	}
	if generation != 1 || !current {
		t.Fatalf("upgraded data generation/current = %d/%v, want 1/true", generation, current)
	}
	group, err := migrator.Migrate(ctx)
	if err != nil {
		t.Fatalf("repeat migrations: %v", err)
	}
	if len(group.Migrations) != 0 {
		t.Fatalf("repeat migrations applied %d migrations, want none", len(group.Migrations))
	}
}

func TestFreshMigrationRollbackRemovesSchema(t *testing.T) {
	testMigrationDialects(t, testFreshMigrationRollbackRemovesSchema)
}

func testFreshMigrationRollbackRemovesSchema(t *testing.T, db *bun.DB) {
	ctx := context.Background()
	migrator := NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("initialize fresh migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("migrate fresh schema: %v", err)
	}
	if _, err := migrator.Rollback(ctx); err != nil {
		t.Fatalf("rollback empty fresh schema: %v", err)
	}
	domainTables, err := countDomainTables(ctx, db)
	if err != nil {
		t.Fatalf("count tables after rollback: %v", err)
	}
	if domainTables != 0 {
		t.Fatalf("rollback left %d domain tables, want none", domainTables)
	}
	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read migrations after rollback: %v", err)
	}
	if len(applied) != 0 {
		t.Fatalf("rollback left %d applied migrations, want none", len(applied))
	}
}

func markAppliedMigration(t *testing.T, ctx context.Context, migrator interface {
	MarkApplied(context.Context, *migrate.Migration) error
}, name string, groupID int64,
) {
	t.Helper()
	for _, migration := range Migrations.Sorted() {
		if migration.Name != name {
			continue
		}
		migration.GroupID = groupID
		if err := migrator.MarkApplied(ctx, &migration); err != nil {
			t.Fatalf("mark migration %s applied: %v", name, err)
		}
		return
	}
	t.Fatalf("migration %s not found", name)
}

func seedLegacyMigrationData(t *testing.T, db *bun.DB) {
	t.Helper()
	ctx := context.Background()
	statements := []string{
		`INSERT INTO buckets (id, name) VALUES (1, 'legacy-migration-bucket')`,
		`INSERT INTO storage_uploads
			(id, bucket_id, content_size, checksum, requested_copies)
		 VALUES (1, 1, 1, 'checksum', 1)`,
		`INSERT INTO storage_data_sets
			(id, bucket_id, provider_id, copy_index, status)
		 VALUES (1, 1, '101', 0, 'ready')`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed legacy migration data: %v", err)
		}
	}
}

func countDomainTables(ctx context.Context, db *bun.DB) (int, error) {
	query := `SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		  AND name NOT IN ('bun_migrations', 'bun_migration_locks')`
	if db.Dialect().Name() == dialect.PG {
		query = `SELECT COUNT(*) FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name NOT IN ('bun_migrations', 'bun_migration_locks')`
	}
	var count int
	if err := db.NewRaw(query).Scan(ctx, &count); err != nil {
		return 0, err
	}
	return count, nil
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

func sqliteSchemaFingerprint(t *testing.T, db *bun.DB, includeMigrations bool) (string, string) {
	t.Helper()
	query := `SELECT type, name, tbl_name, COALESCE(sql, '')
		FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'`
	if !includeMigrations {
		query += ` AND name NOT IN ('bun_migrations', 'bun_migration_locks')`
	}
	query += ` ORDER BY type, name`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query SQLite schema: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var kind, name, table, ddl string
		if err := rows.Scan(&kind, &name, &table, &ddl); err != nil {
			t.Fatalf("scan SQLite schema: %v", err)
		}
		lines = append(lines, strings.Join([]string{kind, name, table, strings.Join(strings.Fields(ddl), " ")}, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SQLite schema: %v", err)
	}
	return normalizedFingerprint(lines)
}

func postgresSchemaFingerprint(t *testing.T, db *bun.DB, includeMigrations bool) (string, string) {
	t.Helper()
	var lines []string
	queries := []string{
		`SELECT 'column|' || table_name || '|' || lpad(ordinal_position::text, 4, '0') || '|' ||
			column_name || '|' || data_type || '|' || udt_name || '|' || is_nullable || '|' ||
			coalesce(replace(column_default, current_schema() || '.', '<schema>.'), '')
		 FROM information_schema.columns WHERE table_schema = current_schema()`,
		`SELECT 'constraint|' || t.relname || '|' || c.conname || '|' || c.contype::text || '|' ||
			pg_get_constraintdef(c.oid, true)
		 FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
		 JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema()`,
		`SELECT 'index|' || tablename || '|' || indexname || '|' ||
			replace(indexdef, current_schema() || '.', '<schema>.')
		 FROM pg_indexes WHERE schemaname = current_schema()`,
	}
	for _, query := range queries {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("query PostgreSQL schema: %v", err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				_ = rows.Close()
				t.Fatalf("scan PostgreSQL schema: %v", err)
			}
			if !includeMigrations && (strings.Contains(line, "|bun_migrations|") || strings.Contains(line, "|bun_migration_locks|")) {
				continue
			}
			lines = append(lines, strings.Join(strings.Fields(line), " "))
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close PostgreSQL schema rows: %v", err)
		}
	}
	slices.Sort(lines)
	return normalizedFingerprint(lines)
}
