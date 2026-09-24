package migrations

import (
	"context"
	"errors"

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
	if _, err := db.NewAddColumn().Table("s3_accounts").ColumnExpr("name TEXT NOT NULL DEFAULT ''").Exec(ctx); err != nil {
		return err
	}
	column := `name COLLATE "C"`
	if db.Dialect().Name() == dialect.SQLite {
		column = "name COLLATE BINARY"
	}
	_, err := db.NewCreateIndex().Index("uq_s3_accounts_name").Table("s3_accounts").Unique().ColumnExpr(column).Where("name <> ''").Exec(ctx)
	return err
}
