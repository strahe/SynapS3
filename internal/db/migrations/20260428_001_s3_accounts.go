package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up20260428001S3Accounts, down20260428001S3Accounts)
}

func up20260428001S3Accounts(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewCreateTable().
		Model((*model.S3Account)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating s3_accounts table: %w", err)
	}

	if !bucketColumnExists(ctx, db, "owner_access_key") {
		columnType := "TEXT"
		if db.Dialect().Name() == dialect.PG {
			columnType = "TEXT"
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"ALTER TABLE buckets ADD COLUMN owner_access_key %s REFERENCES s3_accounts(access_key) ON UPDATE CASCADE ON DELETE RESTRICT",
			columnType,
		)); err != nil {
			return fmt.Errorf("adding bucket owner_access_key column: %w", err)
		}
	} else if !bucketOwnerForeignKeyExists(ctx, db) {
		if err := addBucketOwnerForeignKey(ctx, db); err != nil {
			return err
		}
	}

	if _, err := db.NewCreateIndex().
		TableExpr("buckets").
		Index("idx_buckets_owner_access_key").
		Column("owner_access_key").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating bucket owner index: %w", err)
	}

	if _, err := db.NewCreateIndex().
		TableExpr("s3_accounts").
		Index("idx_s3_accounts_is_root").
		Column("is_root").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating s3 account root index: %w", err)
	}
	if err := createSingleRootIndex(ctx, db); err != nil {
		return err
	}

	return nil
}

func down20260428001S3Accounts(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewDropIndex().
		Index("idx_buckets_owner_access_key").
		IfExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("dropping bucket owner index: %w", err)
	}
	if bucketColumnExists(ctx, db, "owner_access_key") {
		if _, err := db.ExecContext(ctx, "ALTER TABLE buckets DROP COLUMN owner_access_key"); err != nil {
			return fmt.Errorf("dropping bucket owner_access_key column: %w", err)
		}
	}
	if _, err := db.NewDropIndex().
		Index("idx_s3_accounts_single_root").
		IfExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("dropping single root account index: %w", err)
	}
	if _, err := db.NewDropIndex().
		Index("idx_s3_accounts_is_root").
		IfExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("dropping s3 account root index: %w", err)
	}
	if _, err := db.NewDropTable().
		Model((*model.S3Account)(nil)).
		IfExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("dropping s3_accounts table: %w", err)
	}
	return nil
}

func createSingleRootIndex(ctx context.Context, db *bun.DB) error {
	stmt := "CREATE UNIQUE INDEX IF NOT EXISTS idx_s3_accounts_single_root ON s3_accounts (is_root) WHERE is_root = TRUE"
	if db.Dialect().Name() == dialect.PG {
		stmt = "CREATE UNIQUE INDEX IF NOT EXISTS idx_s3_accounts_single_root ON s3_accounts (is_root) WHERE is_root IS TRUE"
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("creating single root account index: %w", err)
	}
	return nil
}

func bucketColumnExists(ctx context.Context, db *bun.DB, column string) bool {
	var count int
	var err error
	if db.Dialect().Name() == dialect.PG {
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'buckets' AND column_name = $1
		`, column).Scan(&count)
		return err == nil && count > 0
	}

	rows, queryErr := db.QueryContext(ctx, "PRAGMA table_info(buckets)")
	if queryErr != nil {
		return false
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if scanErr := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); scanErr != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}

func bucketOwnerForeignKeyExists(ctx context.Context, db *bun.DB) bool {
	if db.Dialect().Name() == dialect.PG {
		var count int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
			WHERE tc.table_name = 'buckets'
				AND tc.constraint_type = 'FOREIGN KEY'
				AND kcu.column_name = 'owner_access_key'
		`).Scan(&count)
		return err == nil && count > 0
	}

	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list(buckets)")
	if err != nil {
		return false
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, seq int
		var tableName, from, to, onUpdate, onDelete, match string
		if scanErr := rows.Scan(&id, &seq, &tableName, &from, &to, &onUpdate, &onDelete, &match); scanErr != nil {
			return false
		}
		if tableName == "s3_accounts" && from == "owner_access_key" {
			return true
		}
	}
	return false
}

func addBucketOwnerForeignKey(ctx context.Context, db *bun.DB) error {
	if db.Dialect().Name() == dialect.PG {
		if _, err := db.ExecContext(ctx, `
			ALTER TABLE buckets
			ADD CONSTRAINT fk_buckets_owner_access_key
			FOREIGN KEY (owner_access_key)
			REFERENCES s3_accounts(access_key)
			ON UPDATE CASCADE
			ON DELETE RESTRICT
		`); err != nil {
			return fmt.Errorf("adding bucket owner foreign key: %w", err)
		}
		return nil
	}
	return rebuildSQLiteBucketsWithOwnerForeignKey(ctx, db)
}

func rebuildSQLiteBucketsWithOwnerForeignKey(ctx context.Context, db *bun.DB) error {
	statements := []string{
		`CREATE TABLE buckets_new (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			proof_set_id TEXT,
			acl BLOB,
			owner_access_key TEXT REFERENCES s3_accounts(access_key) ON UPDATE CASCADE ON DELETE RESTRICT,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
			updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
		)`,
		`INSERT INTO buckets_new (id, name, proof_set_id, acl, owner_access_key, status, created_at, updated_at)
			SELECT id, name, proof_set_id, acl, owner_access_key, status, created_at, updated_at FROM buckets`,
		`DROP TABLE buckets`,
		`ALTER TABLE buckets_new RENAME TO buckets`,
	}
	if _, err := db.ExecContext(ctx, "SAVEPOINT rebuild_buckets_owner_fk"); err != nil {
		return fmt.Errorf("starting buckets table rebuild savepoint: %w", err)
	}
	released := false
	defer func() {
		if !released {
			_, _ = db.ExecContext(ctx, "ROLLBACK TO SAVEPOINT rebuild_buckets_owner_fk")
			_, _ = db.ExecContext(ctx, "RELEASE SAVEPOINT rebuild_buckets_owner_fk")
		}
	}()
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("rebuilding buckets table with owner foreign key: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, "RELEASE SAVEPOINT rebuild_buckets_owner_fk"); err != nil {
		return fmt.Errorf("committing buckets table rebuild savepoint: %w", err)
	}
	released = true
	return nil
}
