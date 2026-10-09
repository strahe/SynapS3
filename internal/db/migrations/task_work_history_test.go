package migrations

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

const currentPolicyBeforeWorkHistory = `{"version":1,"backoff":{"initial_delay":10000000000,"multiplier":2,"maximum_delay":300000000000,"jitter":0.2},"invocation_timeout":0,"observation_window":1800000000000}`

func TestTaskWorkHistoryMigrationPreservesRoundsAndReferences(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, migrationLevel(t, "2026100803"))
		now := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
		ids := map[string]int64{}
		for _, state := range []string{"pending", "running", "failed", "acknowledged", "completed", "cancelled", "superseded", "invalid", "unknown", "legacy"} {
			id := insertBaselineTestTask(t, db, state)
			ids[state] = id
			if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET retry_limit=5,retry_count=2,available_at=?,created_at=?,updated_at=?,work_started_at=?,cancellation_requested_at=?,cancellation_reason='operator cancelled' WHERE id=?`, now, now, now, now, now, id); err != nil {
				t.Fatal(err)
			}
			if _, err := db.NewRaw(`UPDATE task_payloads SET input_json=?,checkpoint_json=?,runtime_json=?,policy_json=? WHERE task_id=?`, json.RawMessage(`{"copy_id":42}`), json.RawMessage(`{"submitted":true,"transaction":"0x123"}`), json.RawMessage(`{"operation_key":"store:42","last_admitted_attempt":3}`), json.RawMessage(currentPolicyBeforeWorkHistory), id).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			switch state {
			case "running":
				if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET status='running',claim_generation=7,claimed_at=?,lease_until=?,started_at=? WHERE id=?`, now, now.Add(time.Minute), now, id); err != nil {
					t.Fatal(err)
				}
			case "failed", "acknowledged", "superseded":
				if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET status='failed',finished_at=?,last_error='provider unavailable',failure_reason='attempts_exhausted' WHERE id=?`, now, id); err != nil {
					t.Fatal(err)
				}
				if state == "acknowledged" {
					if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET acknowledged_at=? WHERE id=?`, now, id); err != nil {
						t.Fatal(err)
					}
				}
				if state == "superseded" {
					if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET superseded_at=? WHERE id=?`, now, id); err != nil {
						t.Fatal(err)
					}
				}
			case "completed", "cancelled":
				if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET status=?,finished_at=? WHERE id=?`, state, now, id); err != nil {
					t.Fatal(err)
				}
			case "invalid":
				if _, err := db.NewRaw(`UPDATE task_payloads SET policy_json=? WHERE task_id=?`, json.RawMessage(`{"version":1,"unexpected":"retained"}`), id).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				if _, err := db.NewRaw(`UPDATE task_payloads SET policy_json=? WHERE task_id=?`, json.RawMessage(`{"version":42,"future":{"budget":99}}`), id).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "legacy":
				if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET retry_limit=NULL WHERE id=?`, id); err != nil {
					t.Fatal(err)
				}
				if _, err := db.NewRaw(`UPDATE task_payloads SET policy_json=? WHERE task_id=?`, json.RawMessage(legacyTaskPolicy2026100801), id).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		}
		bucket := insertBaselineTestBucket(t, db, "history-owner")
		if _, err := db.ExecContext(t.Context(), `UPDATE buckets SET durability_task_id=? WHERE id=?`, ids["acknowledged"], bucket); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO task_schedules (key,next_run_at,latest_task_id,generation) VALUES ('refresh',?,?,9)`, now.Add(time.Hour), ids["completed"]); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET retry_of_task_id=? WHERE id=?`, ids["failed"], ids["acknowledged"]); err != nil {
			t.Fatal(err)
		}
		if _, err := db.NewRaw(`WITH RECURSIVE sequence(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM sequence WHERE value<130) INSERT INTO task_events (task_id,sequence,type,created_at,details_json) SELECT ?,value,'retry',?,? FROM sequence`, ids["acknowledged"], now, json.RawMessage(`{"attempt":3}`)).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		highwater := insertBaselineTestTask(t, db, "deleted-high-water")
		if _, err := db.ExecContext(t.Context(), `DELETE FROM tasks WHERE id=?`, highwater); err != nil {
			t.Fatal(err)
		}
		migrateToLevel(t, db, len(Migrations.Sorted()))
		if err := validateCurrentSchema(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		for state, id := range ids {
			table, idColumn := "tasks", "id"
			if state == "acknowledged" || state == "completed" || state == "cancelled" || state == "superseded" {
				table, idColumn = "task_history", "task_id"
			}
			var row struct {
				RetryCount                                                    int
				RetryOfTaskID                                                 *int64
				Input, Checkpoint, Policy, Runtime, Events                    json.RawMessage
				CreatedAt, UpdatedAt, AvailableAt                             time.Time
				WorkStartedAt, CancellationRequestedAt, ClaimedAt, LeaseUntil *time.Time
				ClaimGeneration                                               int64
			}
			if err := db.NewRaw(`SELECT retry_count,retry_of_task_id,input_json AS input,checkpoint_json AS checkpoint,policy_json AS policy,runtime_json AS runtime,events_json AS events,created_at,updated_at,available_at,work_started_at,cancellation_requested_at,claim_generation,claimed_at,lease_until FROM ? WHERE ?=?`, bun.Ident(table), bun.Ident(idColumn), id).Scan(t.Context(), &row); err != nil {
				t.Fatal(err)
			}
			if row.RetryCount != 2 || !row.CreatedAt.Equal(now) || !row.UpdatedAt.Equal(now) || !row.AvailableAt.Equal(now) || row.WorkStartedAt == nil || !row.WorkStartedAt.Equal(now) || row.CancellationRequestedAt == nil || !row.CancellationRequestedAt.Equal(now) {
				t.Fatalf("%s evidence changed: %#v", state, row)
			}
			assertTaskMigrationJSON(t, row.Input, `{"copy_id":42}`)
			assertTaskMigrationJSON(t, row.Checkpoint, `{"submitted":true,"transaction":"0x123"}`)
			assertTaskMigrationJSON(t, row.Runtime, `{"operation_key":"store:42","last_admitted_attempt":3}`)
			var policy map[string]any
			if err := json.Unmarshal(row.Policy, &policy); err != nil {
				t.Fatal(err)
			}
			switch state {
			case "invalid", "unknown":
				if policy["version"] != float64(-1) || policy["original_retry_limit"] != float64(5) || policy["legacy"] != nil || policy["original_policy"] == nil {
					t.Fatalf("invalid policy became executable: %s", row.Policy)
				}
			case "legacy":
				if policy["version"] != float64(0) || policy["legacy"] != true || policy["max_attempts"] != nil {
					t.Fatalf("unknown legacy budget changed: %s", row.Policy)
				}
			default:
				if policy["version"] != float64(2) || policy["max_attempts"] != float64(6) || policy["observation_window"] != float64(1800000000000) {
					t.Fatalf("frozen policy changed: %s", row.Policy)
				}
			}
			if state == "running" && (row.ClaimGeneration != 7 || row.ClaimedAt == nil || !row.ClaimedAt.Equal(now) || row.LeaseUntil == nil || !row.LeaseUntil.Equal(now.Add(time.Minute))) {
				t.Fatalf("lease changed: %#v", row)
			}
			if state == "acknowledged" {
				if row.RetryOfTaskID == nil || *row.RetryOfTaskID != ids["failed"] {
					t.Fatalf("predecessor changed: %v", row.RetryOfTaskID)
				}
				var events []taskEvent2026100804
				if err := json.Unmarshal(row.Events, &events); err != nil {
					t.Fatal(err)
				}
				if len(events) != 128 || events[0].Sequence != 3 || events[127].Sequence != 130 || events[0].Type != "retry" || !events[0].CreatedAt.Equal(now) {
					t.Fatalf("events changed: %s", row.Events)
				}
				assertTaskMigrationJSON(t, events[0].Details, `{"attempt":3}`)
			} else {
				assertTaskMigrationJSON(t, row.Events, `[]`)
			}
		}
		var owner, head int64
		if err := db.NewRaw(`SELECT durability_task_id FROM buckets WHERE id=?`, bucket).Scan(t.Context(), &owner); err != nil {
			t.Fatal(err)
		}
		if err := db.NewRaw(`SELECT latest_task_id FROM task_schedules WHERE key='refresh'`).Scan(t.Context(), &head); err != nil {
			t.Fatal(err)
		}
		if owner != ids["acknowledged"] || head != ids["completed"] {
			t.Fatalf("task owner changed: %d/%d", owner, head)
		}
		for _, table := range []string{"task_payloads", "task_events"} {
			if present, err := tableExists(t.Context(), db, table); err != nil || present {
				t.Fatalf("obsolete table %s remains: %v", table, err)
			}
		}
		tables, err := applicationTableNames(t.Context(), db)
		if err != nil {
			t.Fatal(err)
		}
		references, err := foreignKeyLines(t.Context(), db, tables)
		if err != nil {
			t.Fatal(err)
		}
		for _, reference := range references {
			if strings.Contains(reference, "|tasks|") || strings.Contains(reference, "|task_history|") {
				t.Fatalf("task database reference remains: %s", reference)
			}
		}
		next := insertBaselineTestTask(t, db, "next-round")
		if next <= highwater {
			t.Fatalf("allocator reused id: %d <= %d", next, highwater)
		}
		before, err := describeSchema(t.Context(), db, db.Dialect().Name() == dialect.PG)
		if err != nil {
			t.Fatal(err)
		}
		if err := up2026100804TaskWorkHistory(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		after, err := describeSchema(t.Context(), db, db.Dialect().Name() == dialect.PG)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("complete post-state replay changed schema: %v", err)
		}
	})
}

func assertTaskMigrationJSON(t *testing.T, actual json.RawMessage, want string) {
	t.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal(actual, &gotValue); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON = %s, want %s", actual, want)
	}
}

func TestTaskWorkHistoryMigrationRejectsPartialPostStateWithoutWrites(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, migrationLevel(t, "2026100803"))
		id := insertBaselineTestTask(t, db, "unchanged")
		if _, err := db.ExecContext(t.Context(), `CREATE TABLE task_history (task_id BIGINT PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if err := up2026100804TaskWorkHistory(t.Context(), db); !errors.Is(err, ErrIncompatibleDatabase) {
			t.Fatalf("partial post-state = %v", err)
		}
		var stored int64
		if err := db.NewRaw(`SELECT id FROM tasks WHERE id=?`, id).Scan(t.Context(), &stored); err != nil || stored != id {
			t.Fatalf("source changed: %d/%v", stored, err)
		}
		if present, err := tableExists(t.Context(), db, "task_payloads"); err != nil || !present {
			t.Fatalf("payload removed: %v", err)
		}
	})
}

func TestTaskWorkHistoryConstraintsAndExactIndexSet(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateThrough(t, db, "2026100804")
		id := insertBaselineTestTask(t, db, "working")
		for _, statement := range []string{
			`UPDATE tasks SET status='completed',finished_at=CURRENT_TIMESTAMP WHERE id=?`,
			`UPDATE tasks SET status='cancelled',finished_at=CURRENT_TIMESTAMP WHERE id=?`,
			`UPDATE tasks SET retry_count=-1 WHERE id=?`,
			`UPDATE tasks SET policy_json='{"version":2,"max_attempts":0}' WHERE id=?`,
			`UPDATE tasks SET policy_json='{"version":2}' WHERE id=?`,
			`UPDATE tasks SET policy_json='{"version":2,"max_attempts":null}' WHERE id=?`,
		} {
			mustRejectStatement(t, db, statement, id)
		}
		if _, err := db.NewRaw(`UPDATE tasks SET policy_json=? WHERE id=?`, json.RawMessage(`{"version":2,"max_attempts":6}`), id).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		mustRejectStatement(t, db, `UPDATE tasks SET retry_count=6 WHERE id=?`, id)
		var got []string
		for _, table := range []string{"tasks", "task_history"} {
			if db.Dialect().Name() == dialect.PG {
				var names []string
				if err := db.NewRaw(`SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() AND tablename=? AND indexname NOT LIKE '%_pkey' ORDER BY indexname`, table).Scan(t.Context(), &names); err != nil {
					t.Fatal(err)
				}
				got = append(got, names...)
			} else {
				var names []string
				if err := db.NewRaw(`SELECT name FROM sqlite_schema WHERE type='index' AND tbl_name=? AND sql IS NOT NULL ORDER BY name`, table).Scan(t.Context(), &names); err != nil {
					t.Fatal(err)
				}
				got = append(got, names...)
			}
		}
		want := make([]string, 0, 12)
		for _, spec := range taskIndexes2026100804() {
			want = append(want, spec.name)
		}
		slices.Sort(got)
		slices.Sort(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("indexes = %v, want %v", got, want)
		}
	})
}
