package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type task2026100802 struct {
	bun.BaseModel `bun:"table:tasks"`

	ID             int64   `bun:",pk,autoincrement,identity"`
	Type           string  `bun:"type:text,notnull"`
	IdempotencyKey string  `bun:"type:text,notnull"`
	InputVersion   int     `bun:"type:integer,notnull"`
	InputHash      string  `bun:"type:text,notnull"`
	SubjectType    *string `bun:"type:text"`
	SubjectKey     *string `bun:"type:text"`

	Status        string    `bun:"type:text,notnull,default:'pending'"`
	ResumeMode    string    `bun:"type:text,notnull,default:'execute'"`
	AvailableAt   time.Time `bun:",notnull"`
	WaitReason    *string   `bun:"type:text"`
	RetryCount    int       `bun:"type:integer,notnull,default:0"`
	RetryLimit    *int      `bun:"type:integer"`
	FailureReason *string   `bun:"type:text"`
	LastError     *string   `bun:"type:text"`
	StatusMessage *string   `bun:"type:text"`

	CancellationRequestedAt *time.Time
	CancellationReason      *string `bun:"type:text"`
	ClaimGeneration         int64   `bun:",notnull,default:0"`
	ClaimedAt               *time.Time
	LeaseUntil              *time.Time
	StartedAt               *time.Time
	FinishedAt              *time.Time
	AcknowledgedAt          *time.Time
	CreatedAt               time.Time `bun:",notnull"`
	UpdatedAt               time.Time `bun:",notnull"`
	WorkStartedAt           *time.Time
	RetryOfTaskID           *int64
	RetryGroupKey           string `bun:"type:text,notnull"`
	SupersededAt            *time.Time
}

func init() {
	Migrations.MustRegister(up2026100802TaskRounds, func(context.Context, *bun.DB) error {
		return errors.New("task execution history migration cannot be rolled back")
	})
}

func taskConstraints2026100802() []string {
	return []string{
		"CONSTRAINT chk_tasks_identity CHECK (type <> '' AND idempotency_key <> '' AND input_hash <> '' AND input_version >= 1)",
		"CONSTRAINT chk_tasks_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))",
		"CONSTRAINT chk_tasks_resume_mode CHECK (resume_mode IN ('execute', 'recover'))",
		"CONSTRAINT chk_tasks_subject CHECK ((subject_type IS NULL AND subject_key IS NULL) OR (subject_type IS NOT NULL AND subject_type <> '' AND subject_key IS NOT NULL AND subject_key <> ''))",
		"CONSTRAINT chk_tasks_reason_codes CHECK ((wait_reason IS NULL OR wait_reason <> '') AND (failure_reason IS NULL OR failure_reason <> ''))",
		"CONSTRAINT chk_tasks_retry CHECK (retry_count >= 0 AND (retry_limit IS NULL OR (retry_limit >= 0 AND retry_count <= retry_limit)))",
		"CONSTRAINT chk_tasks_generation CHECK (claim_generation >= 0)",
		`CONSTRAINT chk_tasks_claim CHECK (
				(status = 'running' AND claimed_at IS NOT NULL AND lease_until IS NOT NULL AND claim_generation > 0)
				OR (status <> 'running' AND claimed_at IS NULL AND lease_until IS NULL)
			)`,
		`CONSTRAINT chk_tasks_finished CHECK (
				(status IN ('completed', 'failed', 'cancelled') AND finished_at IS NOT NULL)
				OR (status IN ('pending', 'running') AND finished_at IS NULL)
			)`,
		"CONSTRAINT chk_tasks_acknowledged CHECK (acknowledged_at IS NULL OR status = 'failed')",
		"CONSTRAINT chk_tasks_retry_group CHECK (retry_group_key <> '')",
		"CONSTRAINT chk_tasks_retry_parent CHECK (retry_of_task_id IS NULL OR retry_of_task_id < id)",
		"CONSTRAINT chk_tasks_superseded CHECK (superseded_at IS NULL OR status IN ('completed', 'failed', 'cancelled'))",
	}
}

func taskIndexes2026100802() []initialIndexSpec {
	return []initialIndexSpec{
		{name: "uq_tasks_type_key", table: "tasks", columns: []string{"type", "idempotency_key"}, where: "superseded_at IS NULL", unique: true},
		{name: "uq_tasks_retry_group", table: "tasks", columns: []string{"retry_group_key"}, where: "superseded_at IS NULL", unique: true},
		{name: "uq_tasks_retry_parent", table: "tasks", columns: []string{"retry_of_task_id"}, where: "retry_of_task_id IS NOT NULL", unique: true},
		{name: "idx_tasks_pending", table: "tasks", columns: []string{"available_at", "id"}, where: "status = 'pending'"},
		{name: "idx_tasks_recovery", table: "tasks", columns: []string{"lease_until", "id"}, where: "status = 'running'"},
		{name: "idx_tasks_type_id", table: "tasks", columns: []string{"type", "id"}},
		{name: "idx_tasks_status_id", table: "tasks", columns: []string{"status", "id"}},
		{name: "idx_tasks_subject", table: "tasks", columns: []string{"subject_type", "subject_key", "id"}},
	}
}

func up2026100802TaskRounds(ctx context.Context, db *bun.DB) error {
	var present int
	for _, column := range []string{"retry_of_task_id", "retry_group_key", "superseded_at"} {
		ok, err := columnExists(ctx, db, "tasks", column)
		if err != nil {
			return err
		}
		if ok {
			present++
		}
	}
	retained, err := columnExists(ctx, db, "tasks", "retention_until")
	if err != nil {
		return err
	}
	if present == 3 && !retained {
		for _, index := range taskIndexes2026100802() {
			ok, err := indexExists(ctx, db, index.name)
			if err != nil {
				return err
			}
			if !ok {
				return incompatibleDatabaseError()
			}
		}
		return nil
	}
	if present != 0 || !retained {
		return fmt.Errorf("task history migration has a partial post-state: %w", ErrIncompatibleDatabase)
	}
	if db.Dialect().Name() == dialect.PG {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			statements := []string{
				"ALTER TABLE tasks DROP CONSTRAINT uq_tasks_type_key",
				"ALTER TABLE tasks DROP CONSTRAINT chk_tasks_retention",
				"DROP INDEX idx_tasks_gc",
				"DROP INDEX idx_tasks_type_status_id",
				"ALTER TABLE tasks DROP COLUMN retention_until",
				"ALTER TABLE tasks ADD COLUMN retry_of_task_id BIGINT REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE SET NULL",
				"ALTER TABLE tasks ADD COLUMN retry_group_key TEXT",
				"ALTER TABLE tasks ADD COLUMN superseded_at TIMESTAMPTZ",
				"UPDATE tasks SET retry_group_key = 'legacy:' || id::text",
				"ALTER TABLE tasks ALTER COLUMN retry_group_key SET NOT NULL",
			}
			for _, statement := range statements {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			for _, constraint := range taskConstraints2026100802() {
				if strings.Contains(constraint, "chk_tasks_acknowledged") || strings.Contains(constraint, "chk_tasks_retry_group") || strings.Contains(constraint, "chk_tasks_retry_parent") || strings.Contains(constraint, "chk_tasks_superseded") {
					if _, err := tx.ExecContext(ctx, "ALTER TABLE tasks ADD "+constraint); err != nil {
						return err
					}
				}
			}
			return createInitialIndexes(ctx, tx, taskIndexes2026100802()[:3]...)
		})
	}
	var indexes []string
	for _, index := range taskIndexes2026100802() {
		q := db.NewCreateIndex().Index(index.name).Table(index.table)
		if index.unique {
			q.Unique()
		}
		for _, column := range index.columns {
			q.ColumnExpr(column)
		}
		if index.where != "" {
			q.Where(index.where)
		}
		ddl, err := q.AppendQuery(db.QueryGen(), nil)
		if err != nil {
			return err
		}
		indexes = append(indexes, string(ddl))
	}
	return rebuildSQLiteTables(ctx, db, sqliteTableRebuild{
		table: "tasks", indexes: indexes,
		create: func(ctx context.Context, tx bun.Tx, name string) error {
			q := tx.NewCreateTable().Model((*task2026100802)(nil)).ModelTableExpr("?", bun.Ident(name))
			for _, constraint := range taskConstraints2026100802() {
				q.ColumnExpr(constraint)
			}
			q.ForeignKey("(retry_of_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE SET NULL")
			_, err := q.Exec(ctx)
			return err
		},
		copy: func(ctx context.Context, tx bun.Tx, from, to string) error {
			var cols []string
			if err := tx.NewRaw("SELECT name FROM pragma_table_info(?) WHERE name <> 'retention_until' ORDER BY cid", from).Scan(ctx, &cols); err != nil {
				return err
			}
			for i := range cols {
				cols[i] = quoteSQLiteName(cols[i])
			}
			list := strings.Join(cols, ", ")
			_, err := tx.ExecContext(ctx, "INSERT INTO "+quoteSQLiteName(to)+" ("+list+", retry_group_key) SELECT "+list+", 'legacy:' || id FROM "+quoteSQLiteName(from))
			return err
		},
	})
}
