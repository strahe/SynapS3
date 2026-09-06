package migrations

import (
	"context"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestBaselineRepresentativeQueriesUseSupportingIndexes(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		seedObjectPlanBacklog(t, db)
		seedTaskPlanBacklog(t, db)
		seedWalletPlanBacklog(t, db)

		// Listing current objects walks the objects unique key and follows each
		// pointer, so the driving index is on objects rather than a partial
		// index over every version.
		currentListQuery := `SELECT object_version.version_id
			FROM objects AS current_object
			JOIN object_versions AS object_version
			  ON object_version.version_id = current_object.current_version_id
			WHERE current_object.bucket_id = 1 AND object_version.is_delete_marker = FALSE
			  AND current_object.key >= 'a'
			ORDER BY current_object.key ASC LIMIT 100`
		versionListQuery := `SELECT version_id FROM object_versions
			WHERE bucket_id = 1 AND key >= 'a'
			ORDER BY key ASC, created_at DESC, version_id DESC LIMIT 100`
		currentListIndex := "idx_objects_bucket_key"
		if db.Dialect().Name() == dialect.PG {
			currentListQuery = `SELECT object_version.version_id
				FROM objects AS current_object
				JOIN object_versions AS object_version
				  ON object_version.version_id = current_object.current_version_id
				WHERE current_object.bucket_id = 1 AND object_version.is_delete_marker = FALSE
				  AND current_object.key COLLATE "C" >= 'a' COLLATE "C"
				ORDER BY current_object.key COLLATE "C" ASC LIMIT 100`
			versionListQuery = `SELECT version_id FROM object_versions
				WHERE bucket_id = 1 AND key COLLATE "C" >= 'a' COLLATE "C"
				ORDER BY key COLLATE "C" ASC, created_at DESC, version_id DESC LIMIT 100`
			currentListIndex = "idx_objects_bucket_key_c"
		}

		plans := []struct {
			name       string
			indexNames []string
			query      string
		}{
			{
				name:       "ListObjectsV2",
				indexNames: []string{currentListIndex},
				query:      currentListQuery,
			},
			{
				name:       "current object lookup",
				indexNames: []string{"idx_objects_bucket_key"},
				query: `SELECT current_version_id FROM objects
					WHERE bucket_id = 1 AND key = 'key' LIMIT 1`,
			},
			{
				name:       "version listing",
				indexNames: []string{"idx_object_versions_bucket_key_created"},
				query:      versionListQuery,
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
				name: "commit FIFO",
				indexNames: []string{
					"idx_storage_copies_commit_ready",
					"idx_storage_commit_attempts_unresolved_copy",
				},
				query: `SELECT storage_copy.id FROM storage_copies AS storage_copy
					WHERE storage_copy.storage_data_set_id = 1
					  AND storage_copy.status = 'piece_ready'
					  AND storage_copy.commit_ready_at IS NOT NULL
					  AND NOT EXISTS (
						SELECT 1 FROM storage_commit_attempts AS active_attempt
						WHERE active_attempt.content_id = storage_copy.content_id
						  AND active_attempt.storage_data_set_id = storage_copy.storage_data_set_id
						  AND active_attempt.resolved_at IS NULL
					  )
					ORDER BY storage_copy.commit_ready_at, storage_copy.id LIMIT 1`,
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
				for _, indexName := range plan.indexNames {
					if !strings.Contains(got, indexName) {
						t.Fatalf("plan does not use %s:\n%s", indexName, got)
					}
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
	// Bytes own their identity, so each seeded version needs a content row and
	// residency belongs to that content rather than to the version.
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
	if _, err := db.ExecContext(t.Context(), `INSERT INTO object_versions (
			version_id, object_id, bucket_id, key, content_id, size, e_tag,
			is_delete_marker
		, created_at, updated_at)
		SELECT 'version-' || id, id, bucket_id, key, id, 1, 'etag-' || id, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
		FROM objects`); err != nil {
		t.Fatalf("seed query-plan object versions: %v", err)
	}
	// "Current" is the object's pointer now.
	if _, err := db.ExecContext(t.Context(), `
		UPDATE objects SET current_version_id = 'version-' || id`); err != nil {
		t.Fatalf("point query-plan objects at their versions: %v", err)
	}
	var insertHistory string
	if db.Dialect().Name() == dialect.PG {
		insertHistory = `INSERT INTO object_versions (
				version_id, object_id, bucket_id, key, content_id, size, e_tag,
				is_delete_marker, created_at
			, updated_at)
			SELECT 'history-' || object_info.id || '-' || generation, object_info.id,
			       object_info.bucket_id, object_info.key, object_info.id, 1,
			       'history-etag-' || generation, FALSE, '2025-01-01 00:00:00', CURRENT_TIMESTAMP
			FROM objects AS object_info CROSS JOIN generate_series(1, 4) AS series(generation)`
	} else {
		insertHistory = `WITH RECURSIVE generations(generation) AS (
				SELECT 1 UNION ALL SELECT generation + 1 FROM generations WHERE generation < 4
			)
			INSERT INTO object_versions (
				version_id, object_id, bucket_id, key, content_id, size, e_tag,
				is_delete_marker, created_at
			, updated_at)
			SELECT 'history-' || object_info.id || '-' || generation, object_info.id,
			       object_info.bucket_id, object_info.key, object_info.id, 1,
			       'history-etag-' || generation, FALSE, '2025-01-01 00:00:00', CURRENT_TIMESTAMP
			FROM objects AS object_info CROSS JOIN generations`
	}
	if _, err := db.ExecContext(t.Context(), insertHistory); err != nil {
		t.Fatalf("seed query-plan version history: %v", err)
	}
	for _, table := range []string{"objects", "object_versions", "object_cache"} {
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
			`INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status, available_at, created_at, updated_at)
			 SELECT 'plan', 'pending-' || value, 1, 'hash', 'pending', '2026-01-01 00:00:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			 FROM generate_series(1, 512) AS series(value)`,
			`INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status,
			 available_at, resume_mode, claim_generation, claimed_at, lease_until, created_at, updated_at)
			 SELECT 'plan', 'running-' || value, 1, 'hash', 'running', '2026-01-01 00:00:00',
			 'recover', 1, '2026-01-01 00:00:00', '2026-01-01 00:01:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			 FROM generate_series(1, 512) AS series(value)`,
		}
	} else {
		statements = []string{
			`WITH RECURSIVE sequence(value) AS (
				SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 512
			 )
			 INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status, available_at, created_at, updated_at)
			 SELECT 'plan', 'pending-' || value, 1, 'hash', 'pending', '2026-01-01 00:00:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			 FROM sequence`,
			`WITH RECURSIVE sequence(value) AS (
				SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 512
			 )
			 INSERT INTO tasks (type, idempotency_key, input_version, input_hash, status,
			 available_at, resume_mode, claim_generation, claimed_at, lease_until, created_at, updated_at)
			 SELECT 'plan', 'running-' || value, 1, 'hash', 'running', '2026-01-01 00:00:00',
			 'recover', 1, '2026-01-01 00:00:00', '2026-01-01 00:01:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
			 FROM sequence`,
		}
	}
	statements = append(statements,
		`INSERT INTO task_payloads (task_id, input_json) SELECT id, '{}' FROM tasks`)
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
