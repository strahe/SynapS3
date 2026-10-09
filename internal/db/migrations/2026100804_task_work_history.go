package migrations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type task2026100804 struct {
	bun.BaseModel `bun:"table:tasks"`

	ID             int64   `bun:",pk,autoincrement,identity"`
	Type           string  `bun:"type:text,notnull"`
	IdempotencyKey string  `bun:"type:text,notnull"`
	InputVersion   int     `bun:"type:integer,notnull"`
	InputHash      string  `bun:"type:text,notnull"`
	SubjectType    *string `bun:"type:text,nullzero"`
	SubjectKey     *string `bun:"type:text,nullzero"`
	RetryOfTaskID  *int64  `bun:",nullzero"`

	Input      json.RawMessage `bun:"input_json,type:jsonb,notnull"`
	Checkpoint json.RawMessage `bun:"checkpoint_json,type:jsonb,nullzero"`
	Policy     json.RawMessage `bun:"policy_json,type:jsonb,notnull"`
	Runtime    json.RawMessage `bun:"runtime_json,type:jsonb,notnull"`
	Events     json.RawMessage `bun:"events_json,type:jsonb,notnull"`

	Status        string    `bun:"type:text,notnull,default:'pending'"`
	ResumeMode    string    `bun:"type:text,notnull,default:'execute'"`
	AvailableAt   time.Time `bun:",nullzero,notnull"`
	WaitReason    *string   `bun:"type:text,nullzero"`
	RetryCount    int       `bun:"type:integer,notnull,default:0"`
	FailureReason *string   `bun:"type:text,nullzero"`
	LastError     *string   `bun:"type:text,nullzero"`
	StatusMessage *string   `bun:"type:text,nullzero"`

	CancellationRequestedAt *time.Time `bun:",nullzero"`
	CancellationReason      *string    `bun:"type:text,nullzero"`
	ClaimGeneration         int64      `bun:",notnull,default:0"`
	ClaimedAt               *time.Time `bun:",nullzero"`
	LeaseUntil              *time.Time `bun:",nullzero"`
	StartedAt               *time.Time `bun:",nullzero"`
	FinishedAt              *time.Time `bun:",nullzero"`
	CreatedAt               time.Time  `bun:",nullzero,notnull"`
	UpdatedAt               time.Time  `bun:",nullzero,notnull"`
	WorkStartedAt           *time.Time `bun:",nullzero"`
}

type taskHistory2026100804 struct {
	bun.BaseModel `bun:"table:task_history"`

	TaskID         int64   `bun:",pk"`
	Type           string  `bun:"type:text,notnull"`
	IdempotencyKey string  `bun:"type:text,notnull"`
	InputVersion   int     `bun:"type:integer,notnull"`
	InputHash      string  `bun:"type:text,notnull"`
	SubjectType    *string `bun:"type:text,nullzero"`
	SubjectKey     *string `bun:"type:text,nullzero"`
	RetryOfTaskID  *int64  `bun:",nullzero"`

	Input      json.RawMessage `bun:"input_json,type:jsonb,notnull"`
	Checkpoint json.RawMessage `bun:"checkpoint_json,type:jsonb,nullzero"`
	Policy     json.RawMessage `bun:"policy_json,type:jsonb,notnull"`
	Runtime    json.RawMessage `bun:"runtime_json,type:jsonb,notnull"`
	Events     json.RawMessage `bun:"events_json,type:jsonb,notnull"`

	Status        string    `bun:"type:text,notnull"`
	ResumeMode    string    `bun:"type:text,notnull,default:'execute'"`
	AvailableAt   time.Time `bun:",nullzero,notnull"`
	WaitReason    *string   `bun:"type:text,nullzero"`
	RetryCount    int       `bun:"type:integer,notnull,default:0"`
	FailureReason *string   `bun:"type:text,nullzero"`
	LastError     *string   `bun:"type:text,nullzero"`
	StatusMessage *string   `bun:"type:text,nullzero"`

	CancellationRequestedAt *time.Time `bun:",nullzero"`
	CancellationReason      *string    `bun:"type:text,nullzero"`
	ClaimGeneration         int64      `bun:",notnull,default:0"`
	ClaimedAt               *time.Time `bun:",nullzero"`
	LeaseUntil              *time.Time `bun:",nullzero"`
	StartedAt               *time.Time `bun:",nullzero"`
	FinishedAt              *time.Time `bun:",nullzero"`
	CreatedAt               time.Time  `bun:",nullzero,notnull"`
	UpdatedAt               time.Time  `bun:",nullzero,notnull"`
	WorkStartedAt           *time.Time `bun:",nullzero"`
	AcknowledgedAt          *time.Time
	SupersededAt            *time.Time
}

func init() {
	Migrations.MustRegister(up2026100804TaskWorkHistory, func(context.Context, *bun.DB) error {
		return errors.New("task work/history migration cannot be rolled back without losing execution history")
	})
}

func taskJSONColumns2026100804() []initialJSONColumnSpec {
	return []initialJSONColumnSpec{
		{name: "input_json", shape: initialJSONObject},
		{name: "checkpoint_json", shape: initialJSONObject, nullable: true},
		{name: "policy_json", shape: initialJSONObject},
		{name: "runtime_json", shape: initialJSONObject},
		{name: "events_json", shape: initialJSONArray},
	}
}

func taskConstraints2026100804(name string, d dialect.Name) []string {
	id := "id"
	if name == "task_history" {
		id = "task_id"
	}
	prefix := "CONSTRAINT chk_" + name + "_"
	constraints := []string{
		prefix + "identity CHECK (type <> '' AND idempotency_key <> '' AND input_hash <> '' AND input_version >= 1)",
		prefix + "resume_mode CHECK (resume_mode IN ('execute','recover'))",
		prefix + "subject CHECK ((subject_type IS NULL AND subject_key IS NULL) OR (subject_type IS NOT NULL AND subject_type <> '' AND subject_key IS NOT NULL AND subject_key <> ''))",
		prefix + "reason_codes CHECK ((wait_reason IS NULL OR wait_reason <> '') AND (failure_reason IS NULL OR failure_reason <> ''))",
		prefix + "retry CHECK (retry_count >= 0)",
		prefix + "generation CHECK (claim_generation >= 0)",
		prefix + "retry_parent CHECK (retry_of_task_id IS NULL OR retry_of_task_id < " + id + ")",
	}
	if name == "tasks" {
		constraints = append(constraints,
			prefix+"status CHECK (status IN ('pending','running','failed'))",
			prefix+"claim CHECK ((status = 'running' AND claimed_at IS NOT NULL AND lease_until IS NOT NULL AND claim_generation > 0) OR (status <> 'running' AND claimed_at IS NULL AND lease_until IS NULL))",
			prefix+"finished CHECK ((status = 'failed' AND finished_at IS NOT NULL) OR (status IN ('pending','running') AND finished_at IS NULL))")
	} else {
		constraints = append(constraints,
			prefix+"status CHECK (status IN ('completed','failed','cancelled'))",
			prefix+"claim CHECK (claimed_at IS NULL AND lease_until IS NULL)",
			prefix+"finished CHECK (finished_at IS NOT NULL)",
			prefix+"acknowledged CHECK (acknowledged_at IS NULL OR status = 'failed')",
			prefix+"placement CHECK (status <> 'failed' OR acknowledged_at IS NOT NULL OR superseded_at IS NOT NULL)")
	}
	if d == dialect.PG {
		constraints = append(constraints,
			prefix+"events_count CHECK (jsonb_array_length(events_json) <= 128)",
			prefix+"policy_budget CHECK (CASE WHEN policy_json->>'version' = '2' THEN COALESCE(jsonb_typeof(policy_json->'max_attempts') = 'number' AND (policy_json->>'max_attempts') ~ '^[1-9][0-9]*$' AND retry_count < (policy_json->>'max_attempts')::BIGINT,FALSE) ELSE TRUE END)")
	} else {
		constraints = append(constraints,
			prefix+"events_count CHECK (CASE WHEN json_valid(events_json) AND json_type(events_json) = 'array' THEN json_array_length(events_json) <= 128 ELSE FALSE END)",
			prefix+"policy_budget CHECK (CASE WHEN json_valid(policy_json) AND json_extract(policy_json,'$.version') = 2 THEN COALESCE(json_type(policy_json,'$.max_attempts') = 'integer' AND json_extract(policy_json,'$.max_attempts') > 0 AND retry_count < json_extract(policy_json,'$.max_attempts'),FALSE) ELSE TRUE END)")
	}
	return constraints
}

func taskIndexes2026100804() []initialIndexSpec {
	return []initialIndexSpec{
		{name: "uq_tasks_type_key", table: "tasks", columns: []string{"type", "idempotency_key"}, unique: true},
		{name: "uq_tasks_retry_parent", table: "tasks", columns: []string{"retry_of_task_id"}, where: "retry_of_task_id IS NOT NULL", unique: true},
		{name: "idx_tasks_pending", table: "tasks", columns: []string{"available_at", "id"}, where: "status = 'pending'"},
		{name: "idx_tasks_recovery", table: "tasks", columns: []string{"lease_until", "id"}, where: "status = 'running'"},
		{name: "idx_tasks_status_id", table: "tasks", columns: []string{"status", "id"}},
		{name: "idx_tasks_status_type_id", table: "tasks", columns: []string{"status", "type", "id"}},
		{name: "idx_tasks_type_id", table: "tasks", columns: []string{"type", "id"}},
		{name: "idx_tasks_subject", table: "tasks", columns: []string{"subject_type", "subject_key", "id"}},
		{name: "uq_task_history_type_key", table: "task_history", columns: []string{"type", "idempotency_key"}, where: "superseded_at IS NULL", unique: true},
		{name: "uq_task_history_retry_parent", table: "task_history", columns: []string{"retry_of_task_id"}, where: "retry_of_task_id IS NOT NULL", unique: true},
		{name: "idx_task_history_type_id", table: "task_history", columns: []string{"type", "task_id"}},
		{name: "idx_task_history_subject", table: "task_history", columns: []string{"subject_type", "subject_key", "task_id"}},
	}
}

func createTaskTable2026100804(ctx context.Context, db bun.IDB, table, actual string) error {
	var model any = (*task2026100804)(nil)
	if table == "task_history" {
		model = (*taskHistory2026100804)(nil)
	}
	q := db.NewCreateTable().Model(model).ModelTableExpr("?", bun.Ident(actual))
	for _, constraint := range taskConstraints2026100804(table, db.Dialect().Name()) {
		q.ColumnExpr(constraint)
	}
	for _, column := range taskJSONColumns2026100804() {
		q.ColumnExpr(initialJSONConstraint(db.Dialect().Name(), table, column))
	}
	ddl := q.String()
	if db.Dialect().Name() == dialect.SQLite {
		for _, column := range taskJSONColumns2026100804() {
			ddl = strings.Replace(ddl, `"`+column.name+`" jsonb`, `"`+column.name+`" text`, 1)
		}
	}
	_, err := db.ExecContext(ctx, ddl)
	return err
}

func up2026100804TaskWorkHistory(ctx context.Context, db *bun.DB) error {
	history, err := tableExists(ctx, db, "task_history")
	if err != nil {
		return err
	}
	level := 0
	for i, m := range Migrations.Sorted() {
		if m.Name == "2026100804" {
			level = i + 1
			break
		}
	}
	if level == 0 {
		return ErrIncompatibleDatabase
	}
	if history {
		if err := validateSchema(ctx, db, Migrations, level); err != nil {
			return err
		}
		return validateTaskReferences2026100804(ctx, db, "tasks")
	}
	if err := validateSchema(ctx, db, Migrations, level-1); err != nil {
		return fmt.Errorf("task work/history migration requires a complete pre-state: %w", err)
	}
	if db.Dialect().Name() == dialect.PG {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error { return migrateTaskWorkHistoryPostgres2026100804(ctx, tx) })
	}
	return migrateTaskWorkHistorySQLite2026100804(ctx, db)
}

func taskIndexDDL2026100804(db *bun.DB, table string) ([]string, error) {
	var statements []string
	for _, spec := range taskIndexes2026100804() {
		if spec.table != table {
			continue
		}
		q := db.NewCreateIndex().Index(spec.name).Table(spec.table)
		if spec.unique {
			q.Unique()
		}
		for _, col := range spec.columns {
			q.ColumnExpr(col)
		}
		if spec.where != "" {
			q.Where(spec.where)
		}
		ddl, err := q.AppendQuery(db.QueryGen(), nil)
		if err != nil {
			return nil, err
		}
		statements = append(statements, string(ddl))
	}
	return statements, nil
}

func migrateTaskWorkHistorySQLite2026100804(ctx context.Context, db *bun.DB) error {
	// The validated pre-state supplies the exact domain DDL; only task references
	// are removed, preserving later domain constraints and unrelated references.
	var names []string
	if err := db.NewRaw(`SELECT DISTINCT parent.name FROM sqlite_schema AS parent,pragma_foreign_key_list(parent.name) AS reference WHERE parent.type='table' AND reference."table"='tasks' AND parent.name NOT IN ('tasks','task_payloads','task_events') ORDER BY parent.name`).Scan(ctx, &names); err != nil {
		return err
	}
	var rebuilds []sqliteTableRebuild
	for _, table := range names {
		ddl, err := sqliteTableDDL(ctx, db, table)
		if err != nil {
			return err
		}
		stripped, err := withoutTaskForeignKeys2026100804(ddl)
		if err != nil {
			return err
		}
		rebuilds = append(rebuilds, sqliteTableRebuild{table: table, create: func(ctx context.Context, tx bun.Tx, name string) error {
			open := strings.IndexByte(stripped, '(')
			_, err := tx.ExecContext(ctx, "CREATE TABLE "+quoteSQLiteName(name)+" "+stripped[open:])
			return err
		}})
	}
	indexes, err := taskIndexDDL2026100804(db, "tasks")
	if err != nil {
		return err
	}
	rebuilds = append(rebuilds, sqliteTableRebuild{
		table: "tasks", indexes: indexes,
		create: func(ctx context.Context, tx bun.Tx, name string) error {
			if err := createTaskTable2026100804(ctx, tx, "task_history", "task_history"); err != nil {
				return err
			}
			return createTaskTable2026100804(ctx, tx, "tasks", name)
		},
		copy: func(ctx context.Context, tx bun.Tx, from, to string) error {
			if err := copyTaskRounds2026100804(ctx, tx, from, to); err != nil {
				return err
			}
			if err := createInitialIndexes(ctx, tx, taskIndexes2026100804()[8:]...); err != nil {
				return err
			}
			for _, table := range []string{"task_events", "task_payloads"} {
				if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteSQLiteName(table)); err != nil {
					return err
				}
			}
			return validateTaskReferences2026100804(ctx, tx, to)
		},
	})
	return rebuildSQLiteTables(ctx, db, rebuilds...)
}

func withoutTaskForeignKeys2026100804(ddl string) (string, error) {
	open := strings.IndexByte(ddl, '(')
	if open < 0 {
		return "", ErrIncompatibleDatabase
	}
	close, err := closingParenthesis(ddl, open)
	if err != nil {
		return "", err
	}
	body := ddl[open+1 : close]
	var parts []string
	start, depth := 0, 0
	var quote byte
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(body) && body[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, body[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, body[start:])
	ref := regexp.MustCompile(`(?i)\bREFERENCES\s+"?tasks"?\s*\(`)
	var kept []string
	removed := 0
	for _, part := range parts {
		if ref.MatchString(part) {
			if !strings.Contains(strings.ToUpper(part), "FOREIGN KEY") {
				return "", fmt.Errorf("inline task reference cannot be rebuilt safely: %w", ErrIncompatibleDatabase)
			}
			removed++
			continue
		}
		kept = append(kept, part)
	}
	if removed == 0 {
		return "", fmt.Errorf("task reference not found: %w", ErrIncompatibleDatabase)
	}
	return ddl[:open+1] + strings.Join(kept, ",") + ddl[close:], nil
}

func migrateTaskWorkHistoryPostgres2026100804(ctx context.Context, tx bun.Tx) error {
	var references []struct{ TableName, ConstraintName string }
	if err := tx.NewRaw(`SELECT child.relname AS table_name,c.conname AS constraint_name FROM pg_constraint AS c JOIN pg_class AS child ON child.oid=c.conrelid JOIN pg_class AS parent ON parent.oid=c.confrelid JOIN pg_namespace AS ns ON ns.oid=child.relnamespace WHERE c.contype='f' AND ns.nspname=current_schema() AND parent.oid='tasks'::regclass`).Scan(ctx, &references); err != nil {
		return err
	}
	for _, reference := range references {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE "+quoteSQLiteName(reference.TableName)+" DROP CONSTRAINT "+quoteSQLiteName(reference.ConstraintName)); err != nil {
			return err
		}
	}
	var sequence string
	if err := tx.NewRaw(`SELECT pg_get_serial_sequence('tasks','id')`).Scan(ctx, &sequence); err != nil {
		return err
	}
	var highwater int64
	var called bool
	if err := tx.NewRaw("SELECT last_value,is_called FROM ?", bun.Ident(sequence)).Scan(ctx, &highwater, &called); err != nil {
		return err
	}
	var maximum int64
	if err := tx.NewRaw(`SELECT COALESCE(MAX(id),0) FROM tasks`).Scan(ctx, &maximum); err != nil {
		return err
	}
	if maximum > highwater {
		highwater = maximum
		called = true
	}
	if err := createTaskTable2026100804(ctx, tx, "task_history", "task_history"); err != nil {
		return err
	}
	if err := createTaskTable2026100804(ctx, tx, "tasks", "tasks__rebuild"); err != nil {
		return err
	}
	if err := copyTaskRounds2026100804(ctx, tx, "tasks", "tasks__rebuild"); err != nil {
		return err
	}
	for _, table := range []string{"task_events", "task_payloads", "tasks"} {
		if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteSQLiteName(table)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE tasks__rebuild RENAME TO tasks`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER INDEX tasks__rebuild_pkey RENAME TO tasks_pkey`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER SEQUENCE tasks__rebuild_id_seq RENAME TO tasks_id_seq`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT setval(pg_get_serial_sequence('tasks','id'),?,?)`, highwater, called); err != nil {
		return err
	}
	if err := createInitialIndexes(ctx, tx, taskIndexes2026100804()...); err != nil {
		return err
	}
	return validateTaskReferences2026100804(ctx, tx, "tasks")
}

type taskPolicy2026100804 struct {
	Version int  `json:"version"`
	Legacy  bool `json:"legacy,omitempty"`
	Backoff struct {
		InitialDelay int64   `json:"initial_delay"`
		Multiplier   float64 `json:"multiplier"`
		MaximumDelay int64   `json:"maximum_delay"`
		Jitter       float64 `json:"jitter"`
	} `json:"backoff"`
	InvocationTimeout int64 `json:"invocation_timeout"`
	ObservationWindow int64 `json:"observation_window"`
}

func upgradeTaskPolicy2026100804(raw json.RawMessage, limit *int, retryCount int) (json.RawMessage, error) {
	var p taskPolicy2026100804
	var fields map[string]json.RawMessage
	valid := json.Unmarshal(raw, &fields) == nil
	for _, key := range []string{"version", "backoff", "invocation_timeout", "observation_window"} {
		valid = valid && len(fields[key]) > 0 && !bytes.Equal(fields[key], []byte("null"))
	}
	var backoff map[string]json.RawMessage
	valid = valid && json.Unmarshal(fields["backoff"], &backoff) == nil
	for _, key := range []string{"initial_delay", "multiplier", "maximum_delay", "jitter"} {
		valid = valid && len(backoff[key]) > 0 && !bytes.Equal(backoff[key], []byte("null"))
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	valid = valid && dec.Decode(&p) == nil && errors.Is(dec.Decode(new(any)), io.EOF)
	b := p.Backoff
	valid = valid && p.InvocationTimeout >= 0 && p.ObservationWindow >= 0 && b.InitialDelay > 0 && b.MaximumDelay >= b.InitialDelay && !math.IsNaN(b.Multiplier) && !math.IsInf(b.Multiplier, 0) && b.Multiplier >= 1 && !math.IsNaN(b.Jitter) && !math.IsInf(b.Jitter, 0) && b.Jitter >= 0 && b.Jitter <= 1
	legacy := valid && p.Version == 0 && p.Legacy
	current := valid && p.Version == 1 && !p.Legacy && limit != nil && *limit >= 0 && *limit < math.MaxInt && retryCount <= *limit
	if !legacy && !current {
		return json.Marshal(struct {
			Version            int             `json:"version"`
			OriginalPolicy     json.RawMessage `json:"original_policy"`
			OriginalRetryLimit *int            `json:"original_retry_limit"`
		}{-1, raw, limit})
	}
	if current {
		fields["version"] = json.RawMessage(`2`)
	}
	fields["max_attempts"] = json.RawMessage(`null`)
	if limit != nil {
		budget, err := json.Marshal(*limit + 1)
		if err != nil {
			return nil, err
		}
		fields["max_attempts"] = budget
	}
	return json.Marshal(fields)
}

type taskEvent2026100804 struct {
	Sequence  int64           `json:"sequence"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Details   json.RawMessage `bun:"details_json" json:"details"`
}

func taskColumns2026100804(id string) []string {
	columns := strings.Fields(id + ` type idempotency_key input_version input_hash subject_type subject_key retry_of_task_id input_json checkpoint_json policy_json runtime_json events_json status resume_mode available_at wait_reason retry_count failure_reason last_error status_message cancellation_requested_at cancellation_reason claim_generation claimed_at lease_until started_at finished_at created_at updated_at work_started_at`)
	if id == "task_id" {
		columns = append(columns, "acknowledged_at", "superseded_at")
	}
	return columns
}

func copyTaskRounds2026100804(ctx context.Context, db bun.IDB, from, to string) error {
	var after, count int64
	for {
		var rows []struct {
			task2026100802
			Input      json.RawMessage `bun:"input_json"`
			Checkpoint json.RawMessage `bun:"checkpoint_json"`
			Policy     json.RawMessage `bun:"policy_json"`
			Runtime    json.RawMessage `bun:"runtime_json"`
		}
		if err := db.NewRaw(`SELECT t.*,p.input_json,p.checkpoint_json,p.policy_json,p.runtime_json FROM ? AS t JOIN task_payloads AS p ON p.task_id=t.id WHERE t.id>? ORDER BY t.id LIMIT 500`, bun.Ident(from), after).Scan(ctx, &rows); err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		var eventRows []struct {
			TaskID int64
			taskEvent2026100804
		}
		if err := db.NewRaw(`SELECT task_id,sequence,type,created_at,details_json FROM (SELECT task_id,sequence,type,created_at,details_json,ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY sequence DESC) AS position FROM task_events WHERE task_id>? AND task_id<=?) AS recent WHERE position<=128 ORDER BY task_id,sequence`, after, rows[len(rows)-1].ID).Scan(ctx, &eventRows); err != nil {
			return err
		}
		eventsByID := make(map[int64][]taskEvent2026100804)
		for _, event := range eventRows {
			eventsByID[event.TaskID] = append(eventsByID[event.TaskID], event.taskEvent2026100804)
		}
		var hot []task2026100804
		var history []taskHistory2026100804
		for _, row := range rows {
			after = row.ID
			count++
			policy, err := upgradeTaskPolicy2026100804(row.Policy, row.RetryLimit, row.RetryCount)
			if err != nil {
				return err
			}
			events := eventsByID[row.ID]
			if events == nil {
				events = []taskEvent2026100804{}
			}
			eventJSON, err := json.Marshal(events)
			if err != nil {
				return err
			}
			body := task2026100804{
				ID:                      row.ID,
				Type:                    row.Type,
				IdempotencyKey:          row.IdempotencyKey,
				InputVersion:            row.InputVersion,
				InputHash:               row.InputHash,
				SubjectType:             row.SubjectType,
				SubjectKey:              row.SubjectKey,
				RetryOfTaskID:           row.RetryOfTaskID,
				Input:                   row.Input,
				Checkpoint:              row.Checkpoint,
				Policy:                  policy,
				Runtime:                 row.Runtime,
				Events:                  json.RawMessage(eventJSON),
				Status:                  row.Status,
				ResumeMode:              row.ResumeMode,
				AvailableAt:             row.AvailableAt,
				WaitReason:              row.WaitReason,
				RetryCount:              row.RetryCount,
				FailureReason:           row.FailureReason,
				LastError:               row.LastError,
				StatusMessage:           row.StatusMessage,
				CancellationRequestedAt: row.CancellationRequestedAt,
				CancellationReason:      row.CancellationReason,
				ClaimGeneration:         row.ClaimGeneration,
				ClaimedAt:               row.ClaimedAt,
				LeaseUntil:              row.LeaseUntil,
				StartedAt:               row.StartedAt,
				FinishedAt:              row.FinishedAt,
				CreatedAt:               row.CreatedAt,
				UpdatedAt:               row.UpdatedAt,
				WorkStartedAt:           row.WorkStartedAt,
			}
			if row.Status == "completed" || row.Status == "cancelled" || row.AcknowledgedAt != nil || row.SupersededAt != nil {
				history = append(history, taskHistory2026100804{
					TaskID:                  body.ID,
					Type:                    body.Type,
					IdempotencyKey:          body.IdempotencyKey,
					InputVersion:            body.InputVersion,
					InputHash:               body.InputHash,
					SubjectType:             body.SubjectType,
					SubjectKey:              body.SubjectKey,
					RetryOfTaskID:           body.RetryOfTaskID,
					Input:                   body.Input,
					Checkpoint:              body.Checkpoint,
					Policy:                  body.Policy,
					Runtime:                 body.Runtime,
					Events:                  body.Events,
					Status:                  body.Status,
					ResumeMode:              body.ResumeMode,
					AvailableAt:             body.AvailableAt,
					WaitReason:              body.WaitReason,
					RetryCount:              body.RetryCount,
					FailureReason:           body.FailureReason,
					LastError:               body.LastError,
					StatusMessage:           body.StatusMessage,
					CancellationRequestedAt: body.CancellationRequestedAt,
					CancellationReason:      body.CancellationReason,
					ClaimGeneration:         body.ClaimGeneration,
					ClaimedAt:               body.ClaimedAt,
					LeaseUntil:              body.LeaseUntil,
					StartedAt:               body.StartedAt,
					FinishedAt:              body.FinishedAt,
					CreatedAt:               body.CreatedAt,
					UpdatedAt:               body.UpdatedAt,
					WorkStartedAt:           body.WorkStartedAt,
					AcknowledgedAt:          row.AcknowledgedAt,
					SupersededAt:            row.SupersededAt,
				})
			} else {
				hot = append(hot, body)
			}
		}
		if len(hot) > 0 {
			if _, err := db.NewInsert().Model(&hot).Column(taskColumns2026100804("id")...).ModelTableExpr("?", bun.Ident(to)).Exec(ctx); err != nil {
				return fmt.Errorf("preserving live task batch: %w", err)
			}
		}
		if len(history) > 0 {
			if _, err := db.NewInsert().Model(&history).Column(taskColumns2026100804("task_id")...).Exec(ctx); err != nil {
				return fmt.Errorf("preserving history batch: %w", err)
			}
		}
		if len(rows) < 500 {
			break
		}
	}

	var before, afterCount int64
	if err := db.NewRaw("SELECT COUNT(*) FROM ?", bun.Ident(from)).Scan(ctx, &before); err != nil {
		return err
	}
	if err := db.NewRaw("SELECT (SELECT COUNT(*) FROM ?)+(SELECT COUNT(*) FROM task_history)", bun.Ident(to)).Scan(ctx, &afterCount); err != nil {
		return err
	}
	if count != before || count != afterCount {
		return errors.New("task snapshot count differs after migration")
	}
	return nil
}

func validateTaskReferences2026100804(ctx context.Context, db bun.IDB, hot string) error {
	var invalid int64
	if err := db.NewRaw(`SELECT COUNT(*) FROM ? AS hot JOIN task_history AS cold ON cold.task_id=hot.id`, bun.Ident(hot)).Scan(ctx, &invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return errors.New("task ID appears in both work and history")
	}
	var duplicates int64
	if err := db.NewRaw(`SELECT COUNT(*) FROM (SELECT type,idempotency_key FROM (SELECT type,idempotency_key FROM ? UNION ALL SELECT type,idempotency_key FROM task_history WHERE superseded_at IS NULL) AS identities GROUP BY type,idempotency_key HAVING COUNT(*)>1) AS duplicated`, bun.Ident(hot)).Scan(ctx, &duplicates); err != nil {
		return err
	}
	if duplicates != 0 {
		return errors.New("current task identity appears in both work and history")
	}
	if err := db.NewRaw(`SELECT COUNT(*) FROM (SELECT retry_of_task_id FROM (SELECT retry_of_task_id FROM ? WHERE retry_of_task_id IS NOT NULL UNION ALL SELECT retry_of_task_id FROM task_history WHERE retry_of_task_id IS NOT NULL) AS successors GROUP BY retry_of_task_id HAVING COUNT(*)>1) AS duplicated`, bun.Ident(hot)).Scan(ctx, &duplicates); err != nil {
		return err
	}
	if duplicates != 0 {
		return errors.New("task has multiple direct successors across work and history")
	}

	for _, entry := range []struct{ table, id string }{{hot, "id"}, {"task_history", "task_id"}} {
		if err := db.NewRaw(`SELECT COUNT(*) FROM ? AS child WHERE child.retry_of_task_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ? AS parent WHERE parent.id=child.retry_of_task_id) AND NOT EXISTS (SELECT 1 FROM task_history AS parent WHERE parent.task_id=child.retry_of_task_id)`, bun.Ident(entry.table), bun.Ident(hot)).Scan(ctx, &invalid); err != nil {
			return err
		}
		if invalid != 0 {
			return errors.New("task predecessor missing after migration")
		}
	}
	// The removed database references become stable task IDs across both bodies.
	for _, r := range []struct{ table, column string }{
		{"buckets", "durability_task_id"},
		{"object_cache", "cache_active_task_id"},
		{"storage_contents", "cleanup_task_id"},
		{"storage_data_sets", "ensure_task_id"},
		{"storage_data_sets", "retirement_task_id"},
		{"storage_copies", "active_task_id"},
		{"storage_commit_requests", "task_id"},
		{"storage_replacements", "task_id"},
		{"wallet_operations", "task_id"},
		{"provider_upload_speed_tests", "active_task_id"},
		{"task_schedules", "latest_task_id"},
	} {
		var missing int64
		query := `SELECT COUNT(*) FROM ? AS owner WHERE owner.? IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ? AS task WHERE task.id=owner.?) AND NOT EXISTS (SELECT 1 FROM task_history AS history WHERE history.task_id=owner.?)`
		if err := db.NewRaw(query, bun.Ident(r.table), bun.Ident(r.column), bun.Ident(hot), bun.Ident(r.column), bun.Ident(r.column)).Scan(ctx, &missing); err != nil {
			return err
		}
		if missing != 0 {
			return fmt.Errorf("task references missing from %s.%s", r.table, r.column)
		}
	}
	return nil
}
