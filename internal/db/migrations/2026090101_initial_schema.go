package migrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026090101InitialSchema),
		func(context.Context, *bun.DB) error {
			return errors.New("the initial schema cannot be rolled back; create a new empty database")
		},
	)
}

func up2026090101InitialSchema(ctx context.Context, db bun.IDB) error {
	complete, err := initialSchemaPostStateComplete(ctx, db)
	if err != nil {
		return fmt.Errorf("checking initial schema post-state: %w", err)
	}
	if complete {
		return nil
	}
	count, err := applicationTableCount(ctx, db)
	if err != nil {
		return fmt.Errorf("checking initial schema pre-state: %w", err)
	}
	if count != 0 {
		return incompatibleDatabaseError()
	}
	steps := []struct {
		name string
		up   migrationBody
	}{
		{"tasks", createTaskSchema},
		{"identity and object roots", createCoreRootSchema},
		{"storage ledgers", createStorageSchema},
		{"object lifecycle", createObjectLifecycleSchema},
		{"wallet operations", createWalletSchema},
		{"observability", createObservabilitySchema},
		{"PostgreSQL prefix indexes", createPostgresPrefixIndexes},
	}
	for _, step := range steps {
		if err := step.up(ctx, db); err != nil {
			return fmt.Errorf("creating %s: %w", step.name, err)
		}
	}
	return nil
}

// taskPayload2026090101 holds the JSON a task carries. It lives beside the task
// rather than in it because lease renewal updates an indexed column on every
// heartbeat, and PostgreSQL copies the whole row — JSON included — each time.
type taskPayload2026090101 struct {
	bun.BaseModel `bun:"table:task_payloads"`

	TaskID     int64           `bun:",pk"`
	Input      json.RawMessage `bun:"input_json,type:jsonb,notnull"`
	Checkpoint json.RawMessage `bun:"checkpoint_json,type:jsonb"`
}

type task2026090101 struct {
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
	RetentionUntil          *time.Time
	CreatedAt               time.Time `bun:",notnull"`
	UpdatedAt               time.Time `bun:",notnull"`
}

func createTaskSchema(ctx context.Context, db bun.IDB) error {
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:  "tasks",
		model: (*task2026090101)(nil),
		constraints: []string{
			"CONSTRAINT uq_tasks_type_key UNIQUE (type, idempotency_key)",
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
			`CONSTRAINT chk_tasks_retention CHECK (
				(status IN ('completed', 'cancelled') AND retention_until IS NOT NULL)
				OR (status = 'failed' AND ((acknowledged_at IS NULL AND retention_until IS NULL) OR (acknowledged_at IS NOT NULL AND retention_until IS NOT NULL)))
				OR (status IN ('pending', 'running') AND acknowledged_at IS NULL AND retention_until IS NULL)
			)`,
		},
	}); err != nil {
		return err
	}
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:        "task_payloads",
		model:       (*taskPayload2026090101)(nil),
		jsonColumns: initialJSONColumns("task_payloads"),
		foreignKeys: []string{
			"(task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE CASCADE",
		},
	}); err != nil {
		return err
	}
	return createInitialIndexes(ctx, db,
		initialIndexSpec{name: "idx_tasks_pending", table: "tasks", columns: []string{"available_at", "id"}, where: "status = 'pending'"},
		initialIndexSpec{name: "idx_tasks_recovery", table: "tasks", columns: []string{"lease_until", "id"}, where: "status = 'running'"},
		initialIndexSpec{name: "idx_tasks_gc", table: "tasks", columns: []string{"retention_until", "id"}, where: "retention_until IS NOT NULL"},
		initialIndexSpec{name: "idx_tasks_type_status_id", table: "tasks", columns: []string{"type", "status", "id"}},
		initialIndexSpec{name: "idx_tasks_type_id", table: "tasks", columns: []string{"type", "id"}},
		initialIndexSpec{name: "idx_tasks_status_id", table: "tasks", columns: []string{"status", "id"}},
		initialIndexSpec{name: "idx_tasks_subject", table: "tasks", columns: []string{"subject_type", "subject_key", "id"}},
	)
}
