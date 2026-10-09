package migrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

const legacyTaskPolicy2026100801 = `{"version":0,"legacy":true,"backoff":{"initial_delay":10000000000,"multiplier":2,"maximum_delay":300000000000,"jitter":0.2},"invocation_timeout":0,"observation_window":0}`

type taskEvent2026100801 struct {
	bun.BaseModel `bun:"table:task_events"`
	TaskID        int64           `bun:",pk"`
	Sequence      int64           `bun:",pk"`
	Type          string          `bun:"type:text,notnull"`
	CreatedAt     time.Time       `bun:",notnull"`
	Details       json.RawMessage `bun:"details_json,type:jsonb,notnull"`
}

func init() {
	Migrations.MustRegister(transactionalMigration(up2026100801TaskPolicyEvents), func(context.Context, *bun.DB) error {
		return errors.New("task policy and events migration cannot be rolled back without losing execution evidence")
	})
}

func up2026100801TaskPolicyEvents(ctx context.Context, db bun.IDB) error {
	policy, err := columnExists(ctx, db, "task_payloads", "policy_json")
	if err != nil {
		return err
	}
	runtime, err := columnExists(ctx, db, "task_payloads", "runtime_json")
	if err != nil {
		return err
	}
	events, err := tableExists(ctx, db, "task_events")
	if err != nil {
		return err
	}
	if policy && runtime && events {
		return nil
	}
	if policy || runtime || events {
		return fmt.Errorf("task policy migration has a partial post-state: %w", ErrIncompatibleDatabase)
	}
	jsonType := "TEXT"
	if db.Dialect().Name() == dialect.PG {
		jsonType = "JSONB"
	}
	for _, col := range []struct{ name, value string }{{"policy_json", "{}"}, {"runtime_json", "{}"}} {
		spec := initialJSONColumnSpec{name: col.name, shape: initialJSONObject}
		ddl := "ALTER TABLE task_payloads ADD COLUMN " + col.name + " " + jsonType + " NOT NULL DEFAULT '" + col.value + "' " + initialJSONConstraint(db.Dialect().Name(), "task_payloads", spec)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	if _, err := db.NewRaw("UPDATE task_payloads SET policy_json = ?", json.RawMessage(legacyTaskPolicy2026100801)).Exec(ctx); err != nil {
		return err
	}
	for _, task := range []struct {
		kind   string
		window time.Duration
	}{{"storage_store", 30 * time.Minute}, {"storage_cleanup", 24 * time.Hour}} {
		var policy map[string]any
		if err := json.Unmarshal([]byte(legacyTaskPolicy2026100801), &policy); err != nil {
			return err
		}
		policy["observation_window"] = int64(task.window)
		raw, err := json.Marshal(policy)
		if err != nil {
			return err
		}
		if _, err = db.NewRaw(`UPDATE task_payloads SET policy_json = ? WHERE task_id IN (SELECT id FROM tasks WHERE type = ?)`, json.RawMessage(raw), task.kind).Exec(ctx); err != nil {
			return err
		}
	}
	if err := backfillTaskSubjects2026100801(ctx, db); err != nil {
		return err
	}
	return createInitialTable(ctx, db, initialTableSpec{
		name: "task_events", model: (*taskEvent2026100801)(nil),
		constraints: []string{"CONSTRAINT chk_task_events_sequence CHECK (sequence > 0)", "CONSTRAINT chk_task_events_type CHECK (type <> '')"},
		foreignKeys: []string{"(task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE CASCADE"},
		jsonColumns: []initialJSONColumnSpec{{name: "details_json", shape: initialJSONObject}},
	})
}

// Older entry points omitted subjects or used the measurement input hash.
// Only canonical IDs proved by the persisted input may repair that identity.
func backfillTaskSubjects2026100801(ctx context.Context, db bun.IDB) error {
	var after int64
	for {
		var rows []struct {
			ID          int64
			Type        string
			InputHash   string
			SubjectType *string
			SubjectKey  *string
			Input       json.RawMessage
		}
		if err := db.NewRaw(`SELECT t.id,t.type,t.input_hash,t.subject_type,t.subject_key,p.input_json AS input
			FROM tasks AS t JOIN task_payloads AS p ON p.task_id=t.id
			WHERE t.id>? AND t.type IN ('provider_upload_speed_test','provider_replacement_coordinate','storage_dataset_retire','bucket_provision')
			AND (t.subject_type IS NULL OR t.subject_key IS NULL OR (t.type='provider_upload_speed_test' AND t.subject_type='provider' AND t.subject_key=t.input_hash))
			ORDER BY t.id LIMIT 1000`, after).Scan(ctx, &rows); err != nil {
			return err
		}
		for _, row := range rows {
			after = row.ID
			var input map[string]json.RawMessage
			if json.Unmarshal(row.Input, &input) != nil {
				continue
			}
			var kind, field, key string
			switch row.Type {
			case "provider_upload_speed_test":
				kind, field = "provider", "provider_id"
				if json.Unmarshal(input[field], &key) != nil || len(key) == 0 || key[0] < '1' || key[0] > '9' {
					continue
				}
				valid := true
				for _, digit := range key {
					if digit < '0' || digit > '9' {
						valid = false
						break
					}
				}
				if !valid {
					continue
				}
			case "provider_replacement_coordinate":
				kind, field = "storage_replacement", "replacement_id"
			case "storage_dataset_retire":
				kind, field = "storage_data_set", "data_set_id"
			case "bucket_provision":
				kind, field = "bucket", "bucket_id"
			}
			if key == "" {
				id, err := strconv.ParseInt(string(input[field]), 10, 64)
				if err != nil || id <= 0 {
					continue
				}
				key = strconv.FormatInt(id, 10)
			}
			if _, err := db.ExecContext(ctx, `UPDATE tasks SET subject_type=?,subject_key=? WHERE id=?`, kind, key, row.ID); err != nil {
				return err
			}
		}
		if len(rows) < 1000 {
			return nil
		}
	}
}
