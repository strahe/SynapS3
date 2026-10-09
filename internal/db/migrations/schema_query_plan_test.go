package migrations

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestCurrentSchemaRepresentativeQueriesUseSupportingIndexes(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		seedObjectPlanBacklog(t, db)
		seedStorageCommitPlanBacklog(t, db)
		seedTaskPlanBacklog(t, db)
		seedWalletPlanBacklog(t, db)

		plans := []struct {
			name       string
			indexNames []string
			query      string
		}{
			{
				name:       "deleted content sample",
				indexNames: []string{"idx_object_deletions_content_deleted"},
				query: `SELECT key, size FROM object_deletions
					WHERE content_id = 1 ORDER BY deleted_at DESC, id DESC LIMIT 1`,
			},
			{
				// Residency is content-addressed, so the eviction scan reads
				// object_cache and never touches object_versions.
				name:       "cache LRU",
				indexNames: []string{"idx_object_cache_lru"},
				query: `SELECT content_id FROM object_cache
					WHERE in_cache = TRUE
					ORDER BY cache_accessed_at, content_id LIMIT 100`,
			},
			{
				name:       "expired task recovery",
				indexNames: []string{"idx_tasks_recovery"},
				query: `SELECT id FROM tasks
					WHERE status = 'running' AND lease_until <= '9999-12-31 00:00:00'
					ORDER BY lease_until, id LIMIT 1`,
			},
			{
				name:       "pending task claim",
				indexNames: []string{"idx_tasks_pending"},
				query: `SELECT id FROM tasks
					WHERE status = 'pending' AND available_at <= '9999-12-31 00:00:00'
					ORDER BY available_at, id LIMIT 1`,
			},
			{
				// The next request to send is found among the data set's open
				// requests, not by scanning its settled history.
				name:       "commit queue head",
				indexNames: []string{"idx_storage_commit_requests_data_set_status", "idx_storage_commit_requests_status_created"},
				query: `SELECT request_id FROM storage_commit_requests
					WHERE storage_data_set_id = 1 AND status = 'ready'
					  AND (retry_at IS NULL OR retry_at <= '9999-12-31 00:00:00')
					ORDER BY sealed_at ASC, request_id ASC LIMIT 1`,
			},
			{
				name:       "commit request members",
				indexNames: []string{"idx_storage_copies_commit_request"},
				query: `SELECT id FROM storage_copies
					WHERE commit_request_id = 'collecting-1'
					ORDER BY commit_ready_at ASC, id ASC`,
			},
			{
				name:       "replacement progress",
				indexNames: []string{"idx_storage_replacement_items_state"},
				query: `SELECT id FROM storage_replacement_items
					WHERE replacement_id = 1 AND status = 'pending' ORDER BY id LIMIT 100`,
			},
			{
				name:       "recent wallet operations",
				indexNames: []string{"idx_wallet_operations_recent"},
				query: `SELECT id FROM wallet_operations
					ORDER BY created_at DESC, id DESC LIMIT 100`,
			},
		}

		for _, plan := range plans {
			t.Run(plan.name, func(t *testing.T) {
				got := explainQueryPlan(t, db, plan.query)
				usedSupportingIndex := false
				for _, indexName := range plan.indexNames {
					if strings.Contains(got, indexName) {
						usedSupportingIndex = true
						break
					}
				}
				if !usedSupportingIndex {
					t.Fatalf("plan does not use a supporting index %v:\n%s", plan.indexNames, got)
				}
			})
		}
	})
}

func seedObjectPlanBacklog(t *testing.T, db *bun.DB) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `INSERT INTO buckets (id, name, default_copies, minimum_durable_copies, created_at, updated_at)
		VALUES (1, 'query-plan-bucket', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`); err != nil {
		t.Fatalf("seed query-plan bucket: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) VALUES (1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`); err != nil {
		t.Fatalf("seed query-plan replica slot: %v", err)
	}
	var insertObjects string
	if db.Dialect().Name() == dialect.PG {
		insertObjects = `INSERT INTO objects (id, bucket_id, key, created_at, updated_at)
			SELECT value, 1,
			       CASE WHEN value = 1 THEN 'key' ELSE 'key-' || lpad(value::text, 6, '0') END, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM generate_series(1, 512) AS series(value)`
	} else {
		insertObjects = `WITH RECURSIVE sequence(value) AS (
				SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 512
			)
			INSERT INTO objects (id, bucket_id, key, created_at, updated_at)
			SELECT value, 1,
			       CASE WHEN value = 1 THEN 'key' ELSE 'key-' || printf('%06d', value) END, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM sequence`
	}
	if _, err := db.ExecContext(t.Context(), insertObjects); err != nil {
		t.Fatalf("seed query-plan objects: %v", err)
	}
	// Cache residency belongs to the content.
	checksumExpression := "printf('%064x', id)"
	if db.Dialect().Name() == dialect.PG {
		checksumExpression = "lpad(to_hex(id), 64, '0')"
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO storage_contents (
			id, bucket_id, checksum, content_size, requested_copies
		, created_at, updated_at)
		SELECT id, bucket_id, `+checksumExpression+`, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM objects`); err != nil {
		t.Fatalf("seed query-plan contents: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO object_cache (
			content_id, in_cache, cache_accessed_at
		, created_at, updated_at)
		SELECT id, TRUE, '2026-01-01 00:00:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM storage_contents`); err != nil {
		t.Fatalf("seed query-plan cache entries: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO object_deletions
		(bucket_id, object_id, key, version_id, content_id, size, deleted_at)
		SELECT bucket_id, id, key, 'deleted-' || id, id, 1, CURRENT_TIMESTAMP FROM objects`); err != nil {
		t.Fatalf("seed query-plan deletions: %v", err)
	}
	for _, table := range []string{"object_cache", "object_deletions"} {
		if _, err := db.ExecContext(t.Context(), "ANALYZE "+table); err != nil {
			t.Fatalf("analyze query-plan table %s: %v", table, err)
		}
	}
}

func seedStorageCommitPlanBacklog(t *testing.T, db *bun.DB) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `INSERT INTO storage_data_sets (
		id, bucket_id, provider_id, copy_index, generation, is_current,
		data_set_id, status, created_at, updated_at)
		VALUES (1, 1, 'query-plan-provider', 0, 1, TRUE,
		'query-plan-data-set', 'ready', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`); err != nil {
		t.Fatalf("seed query-plan storage data set: %v", err)
	}

	// A representative settled history makes the open-request lookups
	// materially cheaper through the status index than by scanning.
	var insertCopies, insertHistory string
	if db.Dialect().Name() == dialect.PG {
		insertCopies = `INSERT INTO storage_copies (
			content_id, bucket_id, content_size, storage_data_set_id, copy_index,
			provider_id, piece_id, transfer_method, status, commit_ready_at,
			created_at, updated_at)
			SELECT value, 1, 1, 1, 0,
			       'query-plan-provider', 'piece-' || value, 'ingress', 'piece_ready',
			       '2026-01-01 00:00:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM generate_series(1, 512) AS series(value)`
		insertHistory = `INSERT INTO storage_commit_requests (
			request_id, storage_data_set_id, status, piece_count, sends, refusals,
			last_error, created_at, updated_at)
			SELECT 'history-' || value, 1, 'abandoned', 0, 0, 0,
			       'query plan history', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM generate_series(1, 4096) AS series(value)`
	} else {
		insertCopies = `WITH RECURSIVE contents(content_id) AS (
			SELECT 1 UNION ALL SELECT content_id + 1 FROM contents WHERE content_id < 512
		)
		INSERT INTO storage_copies (
			content_id, bucket_id, content_size, storage_data_set_id, copy_index,
			provider_id, piece_id, transfer_method, status, commit_ready_at,
			created_at, updated_at)
			SELECT content_id, 1, 1, 1, 0,
			       'query-plan-provider', 'piece-' || content_id, 'ingress', 'piece_ready',
			       '2026-01-01 00:00:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM contents`
		insertHistory = `WITH RECURSIVE requests(value) AS (
			SELECT 1 UNION ALL SELECT value + 1 FROM requests WHERE value < 4096
		)
		INSERT INTO storage_commit_requests (
			request_id, storage_data_set_id, status, piece_count, sends, refusals,
			last_error, created_at, updated_at)
			SELECT 'history-' || value, 1, 'abandoned', 0, 0, 0,
			       'query plan history', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM requests`
	}

	statements := []struct {
		name      string
		statement string
	}{
		{"storage copies", insertCopies},
		{"storage commit history", insertHistory},
		{"collecting request task", `INSERT INTO tasks (id, type, idempotency_key, input_version, input_hash, available_at, created_at, updated_at, input_json, policy_json, runtime_json, events_json)
			VALUES (900001, 'storage_commit', 'query-plan-collecting', 1, 'hash', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{}', '{"version":2,"max_attempts":6}', '{}', '[]')`},
		{"collecting request", `INSERT INTO storage_commit_requests (
			request_id, storage_data_set_id, status, task_id, piece_count, sends, refusals, created_at, updated_at)
			VALUES ('collecting-1', 1, 'collecting', 900001, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`},
		{"collecting members", `UPDATE storage_copies SET commit_request_id = 'collecting-1' WHERE content_id <= 32`},
	}
	for _, item := range statements {
		if _, err := db.ExecContext(t.Context(), item.statement); err != nil {
			t.Fatalf("seed query-plan %s: %v", item.name, err)
		}
	}
	for _, table := range []string{"storage_copies", "storage_commit_requests"} {
		if _, err := db.ExecContext(t.Context(), "ANALYZE "+table); err != nil {
			t.Fatalf("analyze query-plan table %s: %v", table, err)
		}
	}
}

func seedTaskPlanBacklog(t *testing.T, db *bun.DB) {
	t.Helper()
	var statements []string
	if db.Dialect().Name() == dialect.PG {
		statements = []string{
			`INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status, available_at, created_at, updated_at, input_json, policy_json, runtime_json, events_json)
			 SELECT 'plan', 'pending-' || value, 1, 'hash', 'pending', '2026-01-01 00:00:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{}', '{"version":2,"max_attempts":6}', '{}', '[]'
			 FROM generate_series(1, 512) AS series(value)`,
			`INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status,
			 available_at, resume_mode, claim_generation, claimed_at, lease_until, created_at, updated_at, input_json, policy_json, runtime_json, events_json)
			 SELECT 'plan', 'running-' || value, 1, 'hash', 'running', '2026-01-01 00:00:00',
			 'recover', 1, '2026-01-01 00:00:00', '2026-01-01 00:01:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{}', '{"version":2,"max_attempts":6}', '{}', '[]'
			 FROM generate_series(1, 512) AS series(value)`,
		}
	} else {
		statements = []string{
			`WITH RECURSIVE sequence(value) AS (
				SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 512
			 )
			 INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status, available_at, created_at, updated_at, input_json, policy_json, runtime_json, events_json)
			 SELECT 'plan', 'pending-' || value, 1, 'hash', 'pending', '2026-01-01 00:00:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{}', '{"version":2,"max_attempts":6}', '{}', '[]'
			 FROM sequence`,
			`WITH RECURSIVE sequence(value) AS (
				SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 512
			 )
			 INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status,
			 available_at, resume_mode, claim_generation, claimed_at, lease_until, created_at, updated_at, input_json, policy_json, runtime_json, events_json)
			 SELECT 'plan', 'running-' || value, 1, 'hash', 'running', '2026-01-01 00:00:00',
			 'recover', 1, '2026-01-01 00:00:00', '2026-01-01 00:01:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{}', '{"version":2,"max_attempts":6}', '{}', '[]'
			 FROM sequence`,
		}
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatalf("seed task query-plan backlog: %v", err)
		}
	}
	if _, err := db.ExecContext(t.Context(), "ANALYZE tasks"); err != nil {
		t.Fatalf("analyze task query-plan backlog: %v", err)
	}
}

func seedWalletPlanBacklog(t *testing.T, db *bun.DB) {
	t.Helper()
	var statement string
	if db.Dialect().Name() == dialect.PG {
		statement = `INSERT INTO wallet_operations
			(type, client_request_id, amount, created_at, updated_at)
			SELECT 'fund', 'wallet-' || value, '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM generate_series(1, 512) AS series(value)`
	} else {
		statement = `WITH RECURSIVE sequence(value) AS (
				SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 512
			)
			INSERT INTO wallet_operations
				(type, client_request_id, amount, created_at, updated_at)
			SELECT 'fund', 'wallet-' || value, '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			FROM sequence`
	}
	if _, err := db.ExecContext(t.Context(), statement); err != nil {
		t.Fatalf("seed wallet query-plan backlog: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "ANALYZE wallet_operations"); err != nil {
		t.Fatalf("analyze wallet query-plan backlog: %v", err)
	}
}

func explainQueryPlan(t *testing.T, db *bun.DB, query string) string {
	t.Helper()
	if db.Dialect().Name() == dialect.PG {
		var plan []string
		err := db.RunInTx(t.Context(), nil, func(ctx context.Context, tx bun.Tx) error {
			if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
				return err
			}
			return tx.NewRaw("EXPLAIN (COSTS OFF) "+query).Scan(ctx, &plan)
		})
		if err != nil {
			t.Fatalf("explain PostgreSQL query: %v", err)
		}
		return strings.Join(plan, "\n")
	}

	var rows []struct {
		ID      int    `bun:"id"`
		Parent  int    `bun:"parent"`
		NotUsed int    `bun:"notused"`
		Detail  string `bun:"detail"`
	}
	if err := db.NewRaw("EXPLAIN QUERY PLAN "+query).Scan(t.Context(), &rows); err != nil {
		t.Fatalf("explain SQLite query: %v", err)
	}
	details := make([]string, 0, len(rows))
	for _, row := range rows {
		details = append(details, row.Detail)
	}
	return strings.Join(details, "\n")
}

func TestTaskIndexesServeSmallActiveSetsBesidePermanentHistory(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
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
		}
	})
}
