package migrations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

type taskSchedule2026100803 struct {
	bun.BaseModel `bun:"table:task_schedules"`
	Key           string    `bun:"type:text,pk"`
	NextRunAt     time.Time `bun:",notnull"`
	LatestTaskID  *int64
	Generation    int64 `bun:",notnull"`
}

func init() {
	Migrations.MustRegister(transactionalMigration(up2026100803TaskSchedules), func(context.Context, *bun.DB) error {
		return errors.New("task schedule migration cannot be rolled back without losing scheduling identity")
	})
}

func up2026100803TaskSchedules(ctx context.Context, db bun.IDB) error {
	exists, err := tableExists(ctx, db, "task_schedules")
	if err != nil {
		return err
	}
	if exists {
		columns, err := tableColumns(ctx, db, "task_schedules")
		if err != nil {
			return err
		}
		names := []string{"key", "next_run_at", "latest_task_id", "generation"}
		if len(columns) != len(names) {
			return fmt.Errorf("task schedule migration has a partial post-state: %w", ErrIncompatibleDatabase)
		}
		for i, name := range names {
			if columns[i].Name != name {
				return fmt.Errorf("task schedule migration has a partial post-state: %w", ErrIncompatibleDatabase)
			}
		}
		return nil
	}
	return createInitialTable(ctx, db, initialTableSpec{
		name: "task_schedules", model: (*taskSchedule2026100803)(nil),
		constraints: []string{"CONSTRAINT chk_task_schedules_key CHECK (key <> '')", "CONSTRAINT chk_task_schedules_generation CHECK (generation >= 0)", "CONSTRAINT uq_task_schedules_latest UNIQUE (latest_task_id)"},
		foreignKeys: []string{"(latest_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT"},
	})
}
