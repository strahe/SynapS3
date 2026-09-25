package migrations

import (
	"context"
	"errors"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026092401S3AccountName),
		func(context.Context, *bun.DB) error {
			return errors.New("S3 account names cannot be rolled back without losing data")
		},
	)
}

func up2026092401S3AccountName(ctx context.Context, db bun.IDB) error {
	hasColumn, err := columnExists(ctx, db, "s3_accounts", "name")
	if err != nil {
		return fmt.Errorf("checking S3 account name column: %w", err)
	}
	hasIndex, err := indexExists(ctx, db, "uq_s3_accounts_name")
	if err != nil {
		return fmt.Errorf("checking S3 account name index: %w", err)
	}
	if hasColumn && hasIndex {
		return nil
	}
	if hasColumn || hasIndex {
		return fmt.Errorf("incomplete S3 account name migration post-state: %w", ErrIncompatibleDatabase)
	}

	if _, err := db.NewAddColumn().Table("s3_accounts").ColumnExpr("name TEXT NOT NULL DEFAULT ''").Exec(ctx); err != nil {
		return err
	}
	column := `name COLLATE "C"`
	if db.Dialect().Name() == dialect.SQLite {
		column = "name COLLATE BINARY"
	}
	_, err = db.NewCreateIndex().Index("uq_s3_accounts_name").Table("s3_accounts").Unique().ColumnExpr(column).Where("name <> ''").Exec(ctx)
	return err
}
