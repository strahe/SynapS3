package migrations

import (
	"context"
	"errors"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type taskWorkStart2026100501 struct {
	bun.BaseModel `bun:"table:tasks"`
	WorkStartedAt *time.Time
}

func init() {
	Migrations.MustRegister(transactionalMigration(up2026100501TaskWorkStart), func(context.Context, *bun.DB) error {
		return errors.New("task work start migration cannot be rolled back without losing operation timing")
	})
}

func up2026100501TaskWorkStart(ctx context.Context, db bun.IDB) error {
	exists, err := columnExists(ctx, db, "tasks", "work_started_at")
	if err != nil || exists {
		return err
	}
	columnType := "TIMESTAMP"
	if db.Dialect().Name() == dialect.PG {
		columnType = "TIMESTAMPTZ"
	}
	_, err = db.NewAddColumn().Model((*taskWorkStart2026100501)(nil)).ColumnExpr("work_started_at " + columnType).Exec(ctx)
	return err
}
