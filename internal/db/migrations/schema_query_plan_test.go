package migrations

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"
)

func TestCurrentSchemaRepresentativeQueriesUseSupportingIndexes(t *testing.T) {
	withMigratedDB(t, func(t *testing.T, db *bun.DB) {
		seedObjectPlanBacklog(t, db)
		seedStorageCommitPlanBacklog(t, db)
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
	insertObjects := `INSERT INTO objects (id, bucket_id, key, created_at, updated_at)
		SELECT value, 1,
		       CASE WHEN value = 1 THEN 'key' ELSE 'key-' || lpad(value::text, 6, '0') END, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
		FROM generate_series(1, 512) AS series(value)`
	if _, err := db.ExecContext(t.Context(), insertObjects); err != nil {
		t.Fatalf("seed query-plan objects: %v", err)
	}
	// Cache residency belongs to the content.
	checksumExpression := "lpad(to_hex(id), 64, '0')"
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

	statements := []struct {
		name      string
		statement string
	}{
		{"storage copies", insertCopies},
		{"storage commit history", insertHistory},
		{"collecting request task", `INSERT INTO tasks (id, type, subject_type, subject_key, generation, input_version, input_hash, available_at, created_at, updated_at, input_json, policy_json, runtime_json)
			VALUES (900001, 'storage_commit', 'storage_commit_request', 'collecting-1', 0, 1, 'hash', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{}', '{"version":2,"max_attempts":6}', '{}')`},
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

func seedWalletPlanBacklog(t *testing.T, db *bun.DB) {
	t.Helper()
	statement := `INSERT INTO wallet_operations
		(type, client_request_id, amount, created_at, updated_at)
		SELECT 'fund', 'wallet-' || value, '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
		FROM generate_series(1, 512) AS series(value)`
	if _, err := db.ExecContext(t.Context(), statement); err != nil {
		t.Fatalf("seed wallet query-plan backlog: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "ANALYZE wallet_operations"); err != nil {
		t.Fatalf("analyze wallet query-plan backlog: %v", err)
	}
}

func explainQueryPlan(t *testing.T, db *bun.DB, query string) string {
	t.Helper()
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

func TestTaskIndexesServeSmallActiveSetsBesidePermanentHistory(t *testing.T) {
	withMigratedDB(t, func(t *testing.T, db *bun.DB) {
		// One live round in 200 among permanent history, with rare history
		// statuses and a rare operation.
		for _, statement := range []string{
			`INSERT INTO tasks (id,type,generation,input_version,input_hash,status,subject_type,subject_key,available_at,finished_at,acknowledged_at,created_at,updated_at,input_json,policy_json,runtime_json)
			 SELECT n,type,n,1,'hash',status,'bucket',CAST(n%500 AS TEXT),CURRENT_TIMESTAMP+interval '1 hour',
			        CASE WHEN status<>'pending' THEN CURRENT_TIMESTAMP END,
			        CASE WHEN status='failed' AND n%1000<>0 THEN CURRENT_TIMESTAMP END,
			        CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{"version":2,"max_attempts":6}','{}'
			 FROM (SELECT n,
			         CASE WHEN n%500=7 THEN 'bucket_provision' ELSE 'observability_refresh' END AS type,
			         CASE WHEN n%1000=0 THEN 'failed' WHEN n%200=0 THEN 'pending' WHEN n%991=2 THEN 'failed' WHEN n%997=1 THEN 'cancelled' ELSE 'completed' END AS status
			       FROM generate_series(1,40000) AS series(n)) AS seed`,
			`UPDATE tasks SET available_at=CURRENT_TIMESTAMP WHERE id IN (200,400,600)`,
			`UPDATE tasks SET status='running',claim_generation=1,claimed_at=CURRENT_TIMESTAMP,lease_until=CURRENT_TIMESTAMP+(id%2000/1000)*interval '1 hour' WHERE id%1000=800`,
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
