package migrations

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestTaskHistoryMigrationsPreserveLegacyEvidence(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, len(Migrations.Sorted())-4)
		store := insertBaselineTestTask(t, db, "legacy-store")
		cleanup := insertBaselineTestTask(t, db, "legacy-cleanup")
		deleted := insertBaselineTestTask(t, db, "deleted-high-water")
		if _, err := db.ExecContext(t.Context(), "DELETE FROM tasks WHERE id = ?", deleted); err != nil {
			t.Fatal(err)
		}
		oldTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET type='storage_store',status='failed',retry_count=2,retry_limit=5,finished_at=?,work_started_at=?,acknowledged_at=?,retention_until=?,last_error='provider unavailable' WHERE id=?`, oldTime, oldTime, oldTime, oldTime, store); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET type='storage_cleanup' WHERE id=?`, cleanup); err != nil {
			t.Fatal(err)
		}
		if _, err := db.NewRaw("UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?", json.RawMessage(`{"submitted":true,"transaction":"0x123"}`), store).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		migrateToLevel(t, db, len(Migrations.Sorted())-1)
		if err := validateSchema(t.Context(), db, Migrations, len(Migrations.Sorted())-1); err != nil {
			t.Fatal(err)
		}
		var stored struct {
			Status         string
			RetryCount     int
			RetryLimit     *int
			LastError      *string
			RetryGroupKey  string
			AcknowledgedAt *time.Time
			WorkStartedAt  *time.Time
		}
		if err := db.NewRaw(`SELECT status,retry_count,retry_limit,last_error,retry_group_key,acknowledged_at,work_started_at FROM tasks WHERE id=?`, store).Scan(t.Context(), &stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status != "failed" || stored.RetryCount != 2 || stored.RetryLimit == nil || *stored.RetryLimit != 5 || stored.AcknowledgedAt == nil || !stored.AcknowledgedAt.Equal(oldTime) || stored.WorkStartedAt == nil || !stored.WorkStartedAt.Equal(oldTime) || stored.LastError == nil || *stored.LastError != "provider unavailable" || stored.RetryGroupKey != fmt.Sprintf("legacy:%d", store) {
			t.Fatalf("legacy evidence changed: %#v", stored)
		}
		for _, tc := range []struct {
			id     int64
			window time.Duration
		}{{store, 30 * time.Minute}, {cleanup, 24 * time.Hour}} {
			var raw string
			if err := db.NewRaw("SELECT policy_json FROM task_payloads WHERE task_id=?", tc.id).Scan(t.Context(), &raw); err != nil {
				t.Fatal(err)
			}
			var policy struct {
				Version           int
				Legacy            bool
				ObservationWindow time.Duration `json:"observation_window"`
			}
			if err := json.Unmarshal([]byte(raw), &policy); err != nil || policy.Version != 0 || !policy.Legacy || policy.ObservationWindow != tc.window {
				t.Fatalf("legacy policy=%s,%v", raw, err)
			}
		}
		var checkpoint string
		if err := db.NewRaw("SELECT checkpoint_json FROM task_payloads WHERE task_id=?", store).Scan(t.Context(), &checkpoint); err != nil {
			t.Fatal(err)
		}
		var evidence map[string]any
		if err := json.Unmarshal([]byte(checkpoint), &evidence); err != nil || evidence["transaction"] != "0x123" {
			t.Fatalf("checkpoint=%s,%v", checkpoint, err)
		}
		next := insertBaselineTestTask(t, db, "new-after-upgrade")
		if next <= deleted {
			t.Fatalf("sequence reused deleted id: %d <= %d", next, deleted)
		}
		before, err := describeSchema(t.Context(), db, db.Dialect().Name() == dialect.PG)
		if err != nil {
			t.Fatal(err)
		}
		if err := runMigrationBody(t.Context(), db, up2026100801TaskPolicyEvents); err != nil {
			t.Fatal(err)
		}
		if err := up2026100802TaskRounds(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		if err := runMigrationBody(t.Context(), db, up2026100803TaskSchedules); err != nil {
			t.Fatal(err)
		}
		after, err := describeSchema(t.Context(), db, db.Dialect().Name() == dialect.PG)
		if err != nil || strings.Join(before, "\n") != strings.Join(after, "\n") {
			t.Fatalf("replay changed schema: %v", err)
		}
		if exists, err := columnExists(t.Context(), db, "tasks", "retention_until"); err != nil || exists {
			t.Fatalf("retention exists=%v,%v", exists, err)
		}
	})
}

func TestTaskHistoryConstraintsAndMinimalIndexes(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, len(Migrations.Sorted())-1)
		source := insertBaselineTestTask(t, db, "source")
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET acknowledged_at=CURRENT_TIMESTAMP WHERE id=?`, source); err == nil {
			t.Fatal("non-failed task accepted acknowledgement")
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET superseded_at=CURRENT_TIMESTAMP WHERE id=?`, source); err == nil {
			t.Fatal("active task accepted supersession")
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET status='failed',finished_at=CURRENT_TIMESTAMP,superseded_at=CURRENT_TIMESTAMP WHERE id=?`, source); err != nil {
			t.Fatal(err)
		}
		child := insertBaselineTestTask(t, db, "child")
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET idempotency_key='source',retry_group_key=?,retry_of_task_id=? WHERE id=?`, "test:source", source, child); err != nil {
			t.Fatal(err)
		}
		other := insertBaselineTestTask(t, db, "other")
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET retry_of_task_id=? WHERE id=?`, source, other); err == nil {
			t.Fatal("source accepted a second direct successor")
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET retry_of_task_id=? WHERE id=?`, other, source); err == nil {
			t.Fatal("source accepted a newer parent")
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO task_schedules (key,next_run_at,latest_task_id,generation) VALUES ('schedule',CURRENT_TIMESTAMP,?,0)`, child); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `DELETE FROM tasks WHERE id=?`, child); err == nil {
			t.Fatal("schedule head was deletable")
		}
		if _, err := db.ExecContext(t.Context(), `DELETE FROM tasks WHERE id=?`, source); err != nil {
			t.Fatal(err)
		}
		var parent *int64
		if err := db.NewRaw("SELECT retry_of_task_id FROM tasks WHERE id=?", child).Scan(t.Context(), &parent); err != nil || parent != nil {
			t.Fatalf("deleted parent=%v,%v", parent, err)
		}
		for _, name := range []string{"idx_tasks_gc", "idx_tasks_type_status_id"} {
			if exists, err := indexExists(t.Context(), db, name); err != nil || exists {
				t.Fatalf("obsolete index %s exists=%v,%v", name, exists, err)
			}
		}
		for _, spec := range taskIndexes2026100802() {
			if exists, err := indexExists(t.Context(), db, spec.name); err != nil || !exists {
				t.Fatalf("required index %s exists=%v,%v", spec.name, exists, err)
			}
		}
	})
}

func TestLegacyTaskSubjectsAreRepairedOnlyFromVerifiedInput(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, len(Migrations.Sorted())-4)
		tests := []struct {
			kind, input, oldType, oldKey, wantType, wantKey string
		}{
			{"provider_upload_speed_test", `{"provider_id":"123456789012345678901234567890"}`, "", "", "provider", "123456789012345678901234567890"},
			{"provider_upload_speed_test", `{"provider_id":"42"}`, "provider", "hash", "provider", "42"},
			{"provider_upload_speed_test", `{"provider_id":"042"}`, "", "", "", ""},
			{"provider_replacement_coordinate", `{"replacement_id":17}`, "", "", "storage_replacement", "17"},
			{"storage_dataset_retire", `{"data_set_id":18}`, "", "", "storage_data_set", "18"},
			{"bucket_provision", `{"bucket_id":19}`, "", "", "bucket", "19"},
			{"bucket_provision", `{"bucket_id":"19"}`, "", "", "", ""},
			{"bucket_provision", `{"bucket_id":-1}`, "", "", "", ""},
			{"bucket_provision", `{"bucket_id":20}`, "bucket", "99", "bucket", "99"},
		}
		ids := make([]int64, len(tests))
		for i, tc := range tests {
			ids[i] = insertBaselineTestTask(t, db, fmt.Sprintf("subject-%d", i))
			var kind, key *string
			if tc.oldType != "" {
				kind, key = &tc.oldType, &tc.oldKey
			}
			if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET type=?,subject_type=?,subject_key=? WHERE id=?`, tc.kind, kind, key, ids[i]); err != nil {
				t.Fatal(err)
			}
			if _, err := db.NewRaw(`UPDATE task_payloads SET input_json=? WHERE task_id=?`, json.RawMessage(tc.input), ids[i]).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		migrateToLevel(t, db, len(Migrations.Sorted())-1)
		for i, tc := range tests {
			var actual struct{ SubjectType, SubjectKey *string }
			if err := db.NewRaw(`SELECT subject_type,subject_key FROM tasks WHERE id=?`, ids[i]).Scan(t.Context(), &actual); err != nil {
				t.Fatal(err)
			}
			if tc.wantType == "" {
				if actual.SubjectType != nil || actual.SubjectKey != nil {
					t.Fatalf("invalid input acquired a subject: %#v", actual)
				}
			} else if actual.SubjectType == nil || actual.SubjectKey == nil || *actual.SubjectType != tc.wantType || *actual.SubjectKey != tc.wantKey {
				t.Fatalf("subject for %s = %#v, want %s/%s", tc.input, actual, tc.wantType, tc.wantKey)
			}
		}
	})
}

func TestTaskIndexesServeSmallActiveSetsBesidePermanentHistory(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, len(Migrations.Sorted()))
		if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE sequence(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM sequence WHERE value<30000)
   INSERT INTO task_history (task_id,type,idempotency_key,input_version,input_hash,status,subject_type,subject_key,available_at,finished_at,created_at,updated_at,input_json,policy_json,runtime_json,events_json)
   SELECT value,CASE WHEN value%500=0 THEN 'bucket_provision' ELSE 'observability_refresh' END,'history-'||value,1,'hash','completed','bucket',CAST(value%500 AS TEXT),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{"version":2,"max_attempts":6}','{}','[]' FROM sequence`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE sequence(value) AS (SELECT 30001 UNION ALL SELECT value+1 FROM sequence WHERE value<40000)
   INSERT INTO tasks (id,type,idempotency_key,input_version,input_hash,status,subject_type,subject_key,available_at,finished_at,created_at,updated_at,input_json,policy_json,runtime_json,events_json)
   SELECT value,CASE WHEN value%500=0 THEN 'bucket_provision' ELSE 'observability_refresh' END,'work-'||value,1,'hash',CASE WHEN value%600=0 THEN 'failed' ELSE 'pending' END,'bucket',CAST(value%500 AS TEXT),CURRENT_TIMESTAMP,CASE WHEN value%600=0 THEN CURRENT_TIMESTAMP ELSE NULL END,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{"version":2,"max_attempts":6}','{}','[]' FROM sequence`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET available_at=?`, time.Now().UTC().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET available_at=CURRENT_TIMESTAMP WHERE id BETWEEN 30020 AND 30024`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET status='running',finished_at=NULL,claim_generation=1,claimed_at=CURRENT_TIMESTAMP,lease_until=CURRENT_TIMESTAMP WHERE id BETWEEN 30030 AND 30034`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET retry_of_task_id=1 WHERE id=30002`); err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"tasks", "task_history"} {
			if _, err := db.ExecContext(t.Context(), "ANALYZE "+table); err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct{ query, index string }{
			{`SELECT id FROM tasks WHERE status='pending' AND available_at<=CURRENT_TIMESTAMP ORDER BY available_at,id LIMIT 1`, "idx_tasks_pending"},
			{`SELECT id FROM tasks WHERE status='running' AND lease_until<=CURRENT_TIMESTAMP ORDER BY lease_until,id LIMIT 1`, "idx_tasks_recovery"},
			{`SELECT id FROM tasks WHERE type='bucket_provision' ORDER BY id DESC LIMIT 20`, "idx_tasks_type_id"},
			{`SELECT id FROM tasks WHERE status='failed' ORDER BY id DESC LIMIT 20`, "idx_tasks_status_id"},
			{`SELECT id FROM tasks WHERE subject_type='bucket' AND subject_key='17' ORDER BY id DESC LIMIT 20`, "idx_tasks_subject"},
			{`SELECT id FROM tasks WHERE type='observability_refresh' AND idempotency_key='work-30002'`, "uq_tasks_type_key"},
			{`SELECT id FROM tasks WHERE retry_of_task_id=1`, "uq_tasks_retry_parent"},
			{`SELECT task_id FROM task_history WHERE type='bucket_provision' ORDER BY task_id DESC LIMIT 20`, "idx_task_history_type_id"},
			{`SELECT task_id FROM task_history WHERE subject_type='bucket' AND subject_key='17' ORDER BY task_id DESC LIMIT 20`, "idx_task_history_subject"},
		} {
			var plan string
			if db.Dialect().Name() == dialect.PG {
				var lines []string
				if err := db.NewRaw("EXPLAIN (COSTS OFF) "+tc.query).Scan(t.Context(), &lines); err != nil {
					t.Fatal(err)
				}
				plan = strings.Join(lines, "\n")
			} else {
				plan = explainQueryPlan(t, db, tc.query)
			}
			if !strings.Contains(plan, tc.index) {
				t.Fatalf("query did not use %s: %s\n%s", tc.index, tc.query, plan)
			}
			if strings.Contains(tc.query, " FROM tasks ") && strings.Contains(plan, "task_history") {
				t.Fatalf("work query scanned history: %s", plan)
			}
		}
	})
}

func TestTaskPolicyMigrationRejectsPartialPostState(t *testing.T) {
	db := newSQLiteMigrationDB(t, "task_partial")
	migrateToLevel(t, db, len(Migrations.Sorted())-4)
	if _, err := db.ExecContext(t.Context(), "ALTER TABLE task_payloads ADD COLUMN policy_json TEXT"); err != nil {
		t.Fatal(err)
	}
	if err := runMigrationBody(t.Context(), db, up2026100801TaskPolicyEvents); err == nil {
		t.Fatal("partial post-state accepted")
	}
	if exists, err := columnExists(t.Context(), db, "task_payloads", "runtime_json"); err != nil || exists {
		t.Fatalf("partial migration wrote runtime=%v,%v", exists, err)
	}
}

func TestTaskScheduleMigrationRejectsPartialPostState(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, len(Migrations.Sorted())-2)
		if _, err := db.ExecContext(t.Context(), `CREATE TABLE task_schedules (key TEXT PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if err := runMigrationBody(t.Context(), db, up2026100803TaskSchedules); err == nil {
			t.Fatal("partial schedule table accepted")
		}
		if columns, err := tableColumns(t.Context(), db, "task_schedules"); err != nil || len(columns) != 1 {
			t.Fatalf("partial table changed: %#v, %v", columns, err)
		}
	})
}
