package migrations

import (
	"context"
	"errors"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up2026100903TaskHistoryIndexes, func(context.Context, *bun.DB) error {
		return errors.New("task history indexes migration requires restoring a database backup to roll back")
	})
}

func up2026100903TaskHistoryIndexes(ctx context.Context, db *bun.DB) error {
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		status, err := indexExists(ctx, tx, "idx_task_history_status_id")
		if err != nil {
			return err
		}
		failed, err := indexExists(ctx, tx, "idx_task_history_current_failed_subject")
		if err != nil {
			return err
		}
		if status && failed {
			return nil
		}
		if status || failed {
			return incompatibleDatabaseError()
		}
		if _, err := tx.ExecContext(ctx, "CREATE INDEX idx_task_history_status_id ON task_history (status, task_id)"); err != nil {
			return err
		}
		ddl := "CREATE INDEX idx_task_history_current_failed_subject ON task_history (subject_type, subject_key, status, superseded_at, task_id)"
		if db.Dialect().Name() == dialect.PG {
			ddl = "CREATE INDEX idx_task_history_current_failed_subject ON task_history (subject_type, subject_key, superseded_at, task_id) WHERE status = 'failed' AND superseded_at IS NULL"
		}
		_, err = tx.ExecContext(ctx, ddl)
		return err
	})
}
