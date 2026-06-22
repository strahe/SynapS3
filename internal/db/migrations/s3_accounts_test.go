package migrations

import (
	"context"
	"database/sql"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"

	_ "modernc.org/sqlite"
)

func TestS3AccountsMigrationCreatesAccountAndOwnerSchema(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	if !sqliteTableExists(t, db, "s3_accounts") {
		t.Fatal("s3_accounts table missing")
	}
	for _, column := range []string{"access_key", "secret_key", "role", "is_root", "created_at", "updated_at"} {
		if !sqliteColumnExists(t, db, "s3_accounts", column) {
			t.Fatalf("s3_accounts.%s column missing", column)
		}
	}
	if !sqliteColumnExists(t, db, "buckets", "owner_access_key") {
		t.Fatal("buckets.owner_access_key column missing")
	}
	if !sqliteIndexExists(t, db, "idx_buckets_owner_access_key") {
		t.Fatal("idx_buckets_owner_access_key index missing")
	}
	if !sqliteIndexExists(t, db, "idx_s3_accounts_is_root") {
		t.Fatal("idx_s3_accounts_is_root index missing")
	}
	if !sqliteIndexExists(t, db, "idx_s3_accounts_single_root") {
		t.Fatal("idx_s3_accounts_single_root index missing")
	}
	if !sqliteForeignKeyExists(t, db, "buckets", "owner_access_key", "s3_accounts") {
		t.Fatal("buckets.owner_access_key foreign key missing")
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO s3_accounts (access_key, secret_key, role, is_root) VALUES ('root-a', 'secret-a', 'admin', TRUE)`); err != nil {
		t.Fatalf("insert first root: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO s3_accounts (access_key, secret_key, role, is_root) VALUES ('root-b', 'secret-b', 'admin', TRUE)`); err == nil {
		t.Fatal("expected duplicate root insert to fail")
	}
}

func TestMigrationCreatesStorageUploadSourceVersionIndex(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if !sqliteIndexExists(t, db, "idx_storage_uploads_source_version_id") {
		t.Fatal("idx_storage_uploads_source_version_id index missing")
	}
}

func TestBucketDefaultCopiesMigrationAddsNullableBoundedColumn(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if !sqliteColumnExists(t, db, "buckets", "default_copies") {
		t.Fatal("buckets.default_copies column missing")
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO buckets (name, status, default_copies) VALUES ('inherit-bucket', 'active', NULL)`); err != nil {
		t.Fatalf("insert inherited bucket: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO buckets (name, status, default_copies) VALUES ('one-copy-bucket', 'active', 1)`); err != nil {
		t.Fatalf("insert one-copy bucket: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO buckets (name, status, default_copies) VALUES ('eight-copy-bucket', 'active', 8)`); err != nil {
		t.Fatalf("insert eight-copy bucket: %v", err)
	}
	for _, tc := range []struct {
		name   string
		copies int
	}{
		{name: "zero-copy-bucket", copies: 0},
		{name: "nine-copy-bucket", copies: 9},
	} {
		if _, err := db.ExecContext(ctx, `INSERT INTO buckets (name, status, default_copies) VALUES (?, 'active', ?)`, tc.name, tc.copies); err == nil {
			t.Fatalf("insert %s with default_copies=%d succeeded, want constraint failure", tc.name, tc.copies)
		}
	}

	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestWalletApproveMigrationUpdatesSQLiteConstraints(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO wallet_operations (type, client_request_id, amount) VALUES ('approve', 'approve-1', '0')`); err != nil {
		t.Fatalf("insert approve amount 0: %v", err)
	}
	for _, tc := range []struct {
		name   string
		opType string
		amount string
	}{
		{name: "approve positive", opType: "approve", amount: "1"},
		{name: "fund zero", opType: "fund", amount: "0"},
		{name: "withdraw zero", opType: "withdraw", amount: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, `INSERT INTO wallet_operations (type, client_request_id, amount) VALUES (?, ?, ?)`, tc.opType, tc.name, tc.amount)
			if err == nil {
				t.Fatalf("insert type=%s amount=%s succeeded, want constraint failure", tc.opType, tc.amount)
			}
		})
	}

	if _, err := migrator.Rollback(ctx); err == nil {
		t.Fatal("rollback with approve operation succeeded, want refusal")
	}
}

func sqliteTableExists(t *testing.T, db *bun.DB, table string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count); err != nil {
		t.Fatalf("query table %s: %v", table, err)
	}
	return count > 0
}

func sqliteColumnExists(t *testing.T, db *bun.DB, table, column string) bool {
	t.Helper()
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatalf("query columns for %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan column for %s: %v", table, err)
		}
		if name == column {
			return true
		}
	}
	return false
}

func sqliteIndexExists(t *testing.T, db *bun.DB, index string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", index).Scan(&count); err != nil {
		t.Fatalf("query index %s: %v", index, err)
	}
	return count > 0
}

func sqliteForeignKeyExists(t *testing.T, db *bun.DB, table, column, refTable string) bool {
	t.Helper()
	rows, err := db.Query("PRAGMA foreign_key_list(" + table + ")")
	if err != nil {
		t.Fatalf("query foreign keys for %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, seq int
		var fkTable, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &fkTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan foreign key for %s: %v", table, err)
		}
		if fkTable == refTable && from == column {
			return true
		}
	}
	return false
}
