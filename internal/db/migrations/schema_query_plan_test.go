package migrations

import (
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"
)

func TestTaskIndexesServeSmallActiveSetsBesidePermanentHistory(t *testing.T) {
	withMigratedDB(t, func(t *testing.T, db *bun.DB) {
		// One live round in 200 among permanent history, with rare history
		// statuses and a rare operation.
		for _, statement := range []string{
			`INSERT INTO tasks (id,type,generation,input_version,status,subject_type,subject_key,available_at,finished_at,acknowledged_at,created_at,updated_at,input_json,runtime_json)
			 SELECT n,type,n,1,status,'bucket',CAST(n%500 AS TEXT),CURRENT_TIMESTAMP+interval '1 hour',
			        CASE WHEN status<>'pending' THEN CURRENT_TIMESTAMP END,
			        CASE WHEN status='failed' AND n%1000<>0 THEN CURRENT_TIMESTAMP END,
			        CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{}'
			 FROM (SELECT n,
			         CASE WHEN n%500=7 THEN 'bucket_provision' ELSE 'observability_refresh' END AS type,
			         CASE WHEN n%1000=0 THEN 'failed' WHEN n%200=0 THEN 'pending' WHEN n%991=2 THEN 'failed' WHEN n%997=1 THEN 'cancelled' ELSE 'completed' END AS status
			       FROM generate_series(1,40000) AS series(n)) AS seed`,
			`UPDATE tasks SET available_at=CURRENT_TIMESTAMP WHERE id IN (200,400,600)`,
			`UPDATE tasks SET status='running',claim_generation=1,lease_until=CURRENT_TIMESTAMP+(id%2000/1000)*interval '1 hour' WHERE id%1000=800`,
			`UPDATE tasks SET retry_of_task_id=1 WHERE id=1200`,
			// Analyze all fixture rows to keep selectivity estimates stable.
			`ALTER TABLE tasks ALTER COLUMN status SET STATISTICS 200`,
			`ANALYZE tasks`,
		} {
			if _, err := db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
		const history = `NOT (status IN ('pending','running','failed') AND acknowledged_at IS NULL AND superseded_at IS NULL)`
		for _, tc := range []struct {
			name, query string
			indexes     []string
		}{
			{"claim", `SELECT id FROM tasks WHERE status='pending' AND available_at<=CURRENT_TIMESTAMP ORDER BY available_at,id LIMIT 1`, []string{"idx_tasks_pending"}},
			{"recovery", `SELECT id FROM tasks WHERE status='running' AND lease_until<=CURRENT_TIMESTAMP ORDER BY lease_until,id LIMIT 1`, []string{"idx_tasks_recovery"}},
			{"work", `SELECT id FROM live_tasks ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_live"}},
			{"work by status", `SELECT id FROM live_tasks WHERE status='failed' ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_live", "idx_tasks_status_id"}},
			{"work by operation", `SELECT id FROM live_tasks WHERE type='bucket_provision' ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_live", "idx_tasks_type_id"}},
			{"history", `SELECT id FROM tasks WHERE ` + history + ` ORDER BY id DESC LIMIT 20`, []string{"tasks_pkey"}},
			{"history by operation", `SELECT id FROM tasks WHERE ` + history + ` AND type='bucket_provision' ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_type_id"}},
			{"history completed", `SELECT id FROM tasks WHERE ` + history + ` AND status='completed' ORDER BY id DESC LIMIT 20`, []string{"tasks_pkey"}},
			{"history failed", `SELECT id FROM tasks WHERE ` + history + ` AND status='failed' ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_status_id"}},
			{"history completed by operation", `SELECT id FROM tasks WHERE ` + history + ` AND status='completed' AND type='bucket_provision' ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_type_id"}},
			{"history cancelled by operation", `SELECT id FROM tasks WHERE ` + history + ` AND status='cancelled' AND type='bucket_provision' ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_status_id", "idx_tasks_type_id"}},
			{"subject", `SELECT id FROM tasks WHERE subject_type='bucket' AND subject_key='17' ORDER BY id DESC LIMIT 20`, []string{"idx_tasks_subject"}},
			{"current failed", `SELECT id FROM tasks WHERE status='failed' AND superseded_at IS NULL AND subject_type='bucket' AND subject_key='17' ORDER BY superseded_at DESC,id DESC LIMIT 20`, []string{"idx_tasks_current_failed"}},
			{"identity", `SELECT id FROM tasks WHERE type='observability_refresh' AND subject_type='bucket' AND subject_key='2' AND generation=30002 AND superseded_at IS NULL`, []string{"uq_tasks_identity"}},
			{"retry successor", `SELECT id FROM tasks WHERE retry_of_task_id=1`, []string{"uq_tasks_retry_parent"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var lines []string
				if err := db.NewRaw("EXPLAIN (COSTS OFF) "+tc.query).Scan(t.Context(), &lines); err != nil {
					t.Fatal(err)
				}
				plan := strings.Join(lines, "\n")
				if !slices.ContainsFunc(tc.indexes, func(index string) bool { return strings.Contains(plan, index) }) {
					t.Fatalf("query did not use %v: %s\n%s", tc.indexes, tc.query, plan)
				}
			})
		}
	})
}
