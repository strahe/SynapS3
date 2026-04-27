package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up20260427001BucketACL, down20260427001BucketACL)
}

func up20260427001BucketACL(ctx context.Context, db *bun.DB) error {
	if bucketACLColumnExists(ctx, db) {
		return nil
	}

	columnType := "BLOB"
	if db.Dialect().Name() == dialect.PG {
		columnType = "BYTEA"
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE buckets ADD COLUMN acl %s", columnType)); err != nil {
		return fmt.Errorf("adding bucket acl column: %w", err)
	}
	return nil
}

func down20260427001BucketACL(ctx context.Context, db *bun.DB) error {
	if !bucketACLColumnExists(ctx, db) {
		return nil
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE buckets DROP COLUMN acl"); err != nil {
		return fmt.Errorf("dropping bucket acl column: %w", err)
	}
	return nil
}

func bucketACLColumnExists(ctx context.Context, db *bun.DB) bool {
	var count int
	var err error
	if db.Dialect().Name() == dialect.PG {
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'buckets' AND column_name = 'acl'
		`).Scan(&count)
	} else {
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
			if name == "acl" {
				return true
			}
		}
		return false
	}
	return err == nil && count > 0
}
