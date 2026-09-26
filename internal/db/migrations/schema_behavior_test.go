package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestBaselineConstraintsRejectInvalidWrites(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		bucketID := insertBaselineTestBucket(t, db, "constraint-bucket")

		mustRejectStatement(t, db, `INSERT INTO tasks
			(type, idempotency_key, input_version, input_hash, status, available_at, created_at, updated_at)
			VALUES ('test', 'invalid-status', 1, 'hash', 'unknown', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
		for _, checksum := range []string{
			"",
			"checksum",
			strings.Repeat("A", 64),
			"sha256:" + strings.Repeat("a", 64),
			strings.Repeat("a", 63) + "g",
		} {
			mustRejectStatement(t, db, `INSERT INTO storage_contents
				(bucket_id, content_size, checksum, requested_copies, created_at, updated_at)
				VALUES (?, 1, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID, checksum)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, create_transaction_id, created_at, updated_at)
			VALUES (?, 'provider', 0, 1, FALSE, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID)
		mustRejectStatement(t, db, `INSERT INTO wallet_operations
			(type, client_request_id, amount, created_at, updated_at)
			VALUES ('fund', 'invalid-amount', '0', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
		mustRejectStatement(t, db, `INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_at, updated_at)
			VALUES (?, 'ready-without-id', 1, 1, FALSE, 'ready', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID)
		mustRejectStatement(t, db, `INSERT INTO wallet_operations
			(type, client_request_id, amount, status, tx_hash, created_at, updated_at)
			VALUES ('fund', 'submitted-without-time', '1', 'submitted', 'tx-1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
		mustRejectStatement(t, db, `INSERT INTO wallet_operations
			(type, client_request_id, amount, status, submitted_at, created_at, updated_at)
			VALUES ('fund', 'submitted-without-hash', '1', 'submitted', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
		if _, err := db.Exec(`INSERT INTO wallet_operations
			(type, client_request_id, amount, status, tx_hash, submitted_at, created_at, updated_at)
			VALUES ('fund', 'submitted-complete', '1', 'submitted', 'tx-complete', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`); err != nil {
			t.Fatalf("insert valid submitted wallet operation: %v", err)
		}

		// A data version is bytes plus a name, so it cannot exist without the
		// content that holds those bytes, and a delete marker cannot carry one.
		if _, err := db.ExecContext(t.Context(),
			`INSERT INTO objects (id, bucket_id, key, created_at, updated_at) VALUES (1, ?, 'failure-shape.txt', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID); err != nil {
			t.Fatalf("insert object: %v", err)
		}
		contentID := insertBaselineTestContent(t, db, bucketID, "v-shape-origin")
		mustRejectStatement(t, db, `INSERT INTO object_versions
			(version_id, object_id, bucket_id, key, content_id, size, e_tag, content_type,
			 metadata, is_delete_marker, created_at, updated_at)
			VALUES ('v-data-without-content', 1, ?, 'failure-shape.txt', NULL, 1, 'etag',
			 'application/octet-stream', '{}', FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID)
		mustRejectStatement(t, db, `INSERT INTO object_versions
			(version_id, object_id, bucket_id, key, content_id, size, e_tag, content_type,
			 metadata, is_delete_marker, created_at, updated_at)
			VALUES ('v-marker-with-content', 1, ?, 'failure-shape.txt', ?, 0, '',
			 '', '{}', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID, contentID)

		dataSetID := insertBaselineTestDataSet(t, db, bucketID, "cleanup-provider", 2, 1, false)
		var cleanupID int64
		if err := db.QueryRow(`INSERT INTO storage_cleanup_copies
			(content_id, bucket_id, copy_index, provider_id, storage_data_set_id, piece_id, piece_cid, checksum, created_at, updated_at)
			VALUES (?, ?, 2, 'cleanup-provider', ?, 'piece-1', 'piece-cid-1', 'checksum-1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			RETURNING id`, contentID, bucketID, dataSetID).Scan(&cleanupID); err != nil {
			t.Fatalf("insert cleanup copy: %v", err)
		}
		mustRejectStatement(t, db, `UPDATE storage_cleanup_copies
			SET status = 'delete_scheduled', delete_tx_hash = 'delete-tx' WHERE id = ?`, cleanupID)
		mustRejectStatement(t, db, `UPDATE storage_cleanup_copies
			SET status = 'delete_scheduled', scheduled_at = CURRENT_TIMESTAMP WHERE id = ?`, cleanupID)
		if _, err := db.Exec(`UPDATE storage_cleanup_copies
			SET status = 'delete_scheduled', delete_tx_hash = 'delete-tx', scheduled_at = CURRENT_TIMESTAMP
			WHERE id = ?`, cleanupID); err != nil {
			t.Fatalf("schedule valid cleanup copy: %v", err)
		}

		taskID := insertBaselineTestTask(t, db, "valid-before-update")
		mustRejectStatement(t, db, `UPDATE tasks SET claim_generation = -1 WHERE id = ?`, taskID)
		mustRejectStatement(t, db, `UPDATE tasks SET retry_limit = 0, retry_count = 1 WHERE id = ?`, taskID)
		mustRejectStatement(t, db, `UPDATE buckets SET default_copies = 2, minimum_durable_copies = 3 WHERE id = ?`, bucketID)
		var generation int64
		if err := db.NewRaw(`SELECT claim_generation FROM tasks WHERE id = ?`, taskID).Scan(t.Context(), &generation); err != nil {
			t.Fatalf("read task after rejected update: %v", err)
		}
		if generation != 0 {
			t.Fatalf("claim_generation = %d after rejected update, want 0", generation)
		}
	})
}

func TestBaselineStorageIdentityAndLedgerConstraints(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		bucketA := insertBaselineTestBucket(t, db, "identity-a")
		bucketB := insertBaselineTestBucket(t, db, "identity-b")
		contentA := insertBaselineTestContent(t, db, bucketA, "upload-a")
		contentA2 := insertBaselineTestContent(t, db, bucketA, "upload-a2")
		contentB := insertBaselineTestContent(t, db, bucketB, "upload-b")
		source := insertBaselineTestDataSet(t, db, bucketA, "101", 0, 1, true)
		copyID := insertBaselineTestCopy(t, db, contentA, bucketA, source, 0, "101", "ingress")
		mustRejectStatement(t, db, `UPDATE storage_copies
			SET confirmed_attempt_status = 'confirmed' WHERE id = ?`, copyID)

		mustRejectStatement(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, ?, 0, '101', 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentB, bucketB, source)
		mustRejectStatement(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, ?, 1, '101', 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA2, bucketA, source)
		mustRejectStatement(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, ?, 0, '202', 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA2, bucketA, source)
		// A copy is always born bound to exactly one data set; the NOT NULL on
		// storage_data_set_id is what enforces that, so it is the assertion here.
		mustRejectRequiredColumn(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, copy_index, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, 0, 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA2, bucketA)

		mustRejectStatement(t, db, `INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_at, updated_at)
			VALUES (?, '101', 1, 1, FALSE, 'draining', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA)
		if _, err := db.Exec(`INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_at, updated_at)
			VALUES (?, '101', 1, 1, FALSE, 'retired', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA); err != nil {
			t.Fatalf("reuse retired provider: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_at, updated_at)
			VALUES (?, '202', 0, 2, TRUE, '2002', 'ready', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA)
		// A generation that failed, drained or retired has given up its slot.
		// Letting one stay current is what stalled bucket provisioning forever.
		for _, endedStatus := range []string{"failed", "draining", "retired"} {
			mustRejectStatement(t, db, `INSERT INTO storage_data_sets
				(bucket_id, provider_id, copy_index, generation, is_current, status, created_at, updated_at)
				VALUES (?, '909', 5, 1, TRUE, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA, endedStatus)
		}
		bucketBSource := insertBaselineTestDataSet(t, db, bucketB, "101", 0, 1, true)

		createdByContent := insertBaselineTestContent(t, db, bucketB, "provenance-created-by")
		if _, err := db.Exec(`INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, created_at, updated_at)
			VALUES (?, 'provenance-created-by', 1, 1, FALSE, 'retired', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketB, createdByContent); err != nil {
			t.Fatalf("insert created-by provenance: %v", err)
		}
		mustRejectStatement(t, db, `DELETE FROM storage_contents WHERE id = ?`, createdByContent)

		lastUsedContent := insertBaselineTestContent(t, db, bucketB, "provenance-last-used")
		if _, err := db.Exec(`INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, last_used_content_id, created_at, updated_at)
			VALUES (?, 'provenance-last-used', 2, 1, FALSE, 'retired', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketB, lastUsedContent); err != nil {
			t.Fatalf("insert last-used provenance: %v", err)
		}
		mustRejectStatement(t, db, `DELETE FROM storage_contents WHERE id = ?`, lastUsedContent)

		if _, err := db.Exec(`INSERT INTO storage_commit_attempts
			(attempt_id, content_id, storage_data_set_id, created_at, updated_at)
			VALUES ('attempt-1', ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA, source); err != nil {
			t.Fatalf("insert first unresolved attempt: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_commit_attempts
			(attempt_id, content_id, storage_data_set_id, created_at, updated_at)
			VALUES ('attempt-2', ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA, source)
		if _, err := db.Exec(`UPDATE storage_commit_attempts
			SET status = 'released', release_reason = 'before_submit_canceled', resolved_at = current_timestamp
			WHERE attempt_id = 'attempt-1'`); err != nil {
			t.Fatalf("release reserved attempt: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO storage_commit_attempts
			(attempt_id, content_id, storage_data_set_id, created_at, updated_at)
			VALUES ('attempt-2', ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA, source); err != nil {
			t.Fatalf("insert unresolved attempt after release: %v", err)
		}
		mustRejectStatement(t, db, `UPDATE storage_commit_attempts
			SET status = 'attempted', attempted_at = current_timestamp
			WHERE attempt_id = 'attempt-2'`)
		mustRejectStatement(t, db, `UPDATE storage_commit_attempts
			SET extra_data_hex = 'abcd'
			WHERE attempt_id = 'attempt-2'`)
		mustRejectStatement(t, db, `UPDATE storage_commit_attempts
			SET status = 'attempted', attempted_at = current_timestamp,
			    extra_data_hex = 'abcd', status_url = 'https://provider.example/status'
			WHERE attempt_id = 'attempt-2'`)
		mustRejectStatement(t, db, `UPDATE storage_commit_attempts
			SET status = 'attempted', attempted_at = current_timestamp,
			    extra_data_hex = 'abcd', transaction_id = '0xabc'
			WHERE attempt_id = 'attempt-2'`)
		if _, err := db.Exec(`UPDATE storage_commit_attempts
			SET status = 'attempted', attempted_at = current_timestamp,
			    extra_data_hex = 'abcd', transaction_id = '0xabc',
			    status_url = 'https://provider.example/status'
			WHERE attempt_id = 'attempt-2'`); err != nil {
			t.Fatalf("record complete commit submission: %v", err)
		}

		target := insertBaselineTestDataSet(t, db, bucketA, "202", 0, 2, false)
		var replacementID int64
		if err := db.QueryRow(`INSERT INTO storage_replacements
			(bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, price_list_fingerprint, status, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'replacement-1', 'test-price', 'preparing_target', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			RETURNING id`, bucketA, source, target).Scan(&replacementID); err != nil {
			t.Fatalf("insert replacement: %v", err)
		}
		bucketBTarget := insertBaselineTestDataSet(t, db, bucketB, "303", 0, 2, false)
		if _, err := db.Exec(`INSERT INTO storage_replacements
			(bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, price_list_fingerprint, status, superseded_by_id, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'replacement-superseded', 'test-price', 'superseded', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			bucketB, bucketBSource, bucketBTarget, replacementID); err != nil {
			t.Fatalf("insert superseded replacement provenance: %v", err)
		}
		mustRejectStatement(t, db, `DELETE FROM storage_replacements WHERE id = ?`, replacementID)
		mustRejectStatement(t, db, `INSERT INTO storage_replacements
			(bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, price_list_fingerprint, status, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'replacement-2', 'test-price', 'waiting', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA, source, target)
		if _, err := db.Exec(`UPDATE storage_replacements SET status = 'failed' WHERE id = ?`, replacementID); err != nil {
			t.Fatalf("mark replacement retryable: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_replacements
			(bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, price_list_fingerprint, status, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'replacement-3', 'test-price', 'preparing_target', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA, source, target)
		mustRejectStatement(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, ?, 0, '202', 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA, bucketA, target)
		insertBaselineTestCopy(t, db, contentA, bucketA, target, 0, "202", "peer_pull")
		if _, err := db.Exec(`INSERT INTO storage_replacement_items
			(replacement_id, content_id, target_data_set_id, created_at, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, contentA, target); err != nil {
			t.Fatalf("insert replacement item after target copy: %v", err)
		}

		// A copy can have only one unresolved pull attempt: a second source can
		// be tried only after the first attempt is abandoned.
		if _, err := db.Exec(`INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, attempted_at, created_at, updated_at)
			VALUES ('pull-1', ?, ?, 'attempted', '301', '3001', '4001', 'bafk2bzacepull', 'https://source.example/piece', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			contentA, source); err != nil {
			t.Fatalf("insert first pull attempt: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, attempted_at, created_at, updated_at)
			VALUES ('pull-2', ?, ?, 'attempted', '302', '3002', '4002', 'bafk2bzacepull2', 'https://source.example/other', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			contentA, source)
		mustRejectStatement(t, db, `UPDATE storage_pull_attempts SET status = 'abandoned' WHERE attempt_id = 'pull-1'`)
		if _, err := db.Exec(`UPDATE storage_pull_attempts
			SET status = 'abandoned', resolved_at = current_timestamp WHERE attempt_id = 'pull-1'`); err != nil {
			t.Fatalf("abandon first pull attempt: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, attempted_at, created_at, updated_at)
			VALUES ('pull-2', ?, ?, 'attempted', '302', '3002', '4002', 'bafk2bzacepull2', 'https://source.example/other', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			contentA, source); err != nil {
			t.Fatalf("insert second pull attempt after abandon: %v", err)
		}
		// A successful pull resolves without changing status, and that also
		// frees the slot.
		if _, err := db.Exec(`UPDATE storage_pull_attempts
			SET resolved_at = current_timestamp WHERE attempt_id = 'pull-2'`); err != nil {
			t.Fatalf("resolve second pull attempt: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, attempted_at, created_at, updated_at)
			VALUES ('', ?, ?, 'attempted', '303', '3003', '4003', 'bafk2bzacepull3', 'https://source.example/third', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			contentA, source)

		taskID := insertBaselineTestTask(t, db, "owner-unique")
		if _, err := db.Exec(`UPDATE buckets SET durability_task_id = ? WHERE id = ?`, taskID, bucketA); err != nil {
			t.Fatalf("bind first task owner: %v", err)
		}
		mustRejectStatement(t, db, `UPDATE buckets SET durability_task_id = ? WHERE id = ?`, taskID, bucketB)
	})
}

func TestBaselineFailedIngressAllowsOneReplacementIngress(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		bucketID := insertBaselineTestBucket(t, db, "retired-ingress")
		contentID := insertBaselineTestContent(t, db, bucketID, "retired-ingress-content")
		retiredDataSetID := insertBaselineTestDataSet(t, db, bucketID, "101", 0, 1, true)
		copyID := insertBaselineTestCopy(t, db, contentID, bucketID, retiredDataSetID, 0, "101", "ingress")
		if _, err := db.Exec(`UPDATE storage_copies SET status = 'failed', last_error = 'retired generation' WHERE id = ?`, copyID); err != nil {
			t.Fatalf("fail original ingress copy: %v", err)
		}
		if _, err := db.Exec(`UPDATE storage_data_sets SET status = 'retired', is_current = FALSE WHERE id = ?`, retiredDataSetID); err != nil {
			t.Fatalf("retire original ingress data set: %v", err)
		}

		currentDataSetID := insertBaselineTestDataSet(t, db, bucketID, "202", 0, 2, true)
		insertBaselineTestCopy(t, db, contentID, bucketID, currentDataSetID, 0, "202", "ingress")
		otherDataSetID := insertBaselineTestDataSet(t, db, bucketID, "303", 1, 1, true)
		mustRejectStatement(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, ?, 1, '303', 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentID, bucketID, otherDataSetID)
	})
}

func TestBaselineIdentitySupportsGenerationAndBackfill(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		first := insertBaselineTestTask(t, db, "identity-first")
		if _, err := db.Exec(`DELETE FROM tasks WHERE id = ?`, first); err != nil {
			t.Fatalf("delete first identity row: %v", err)
		}
		second := insertBaselineTestTask(t, db, "identity-second")
		if second <= first {
			t.Fatalf("generated ID %d reused deleted ID %d", second, first)
		}

		const backfilledID int64 = 5_000_000_000
		if _, err := db.Exec(`INSERT INTO tasks
			(id, type, idempotency_key, input_version, input_hash, available_at, created_at, updated_at)
			VALUES (?, 'test', 'identity-backfill', 1, 'hash', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, backfilledID); err != nil {
			t.Fatalf("backfill explicit identity: %v", err)
		}
		var storedID int64
		if err := db.NewRaw(`SELECT id FROM tasks WHERE id = ?`, backfilledID).Scan(t.Context(), &storedID); err != nil {
			t.Fatalf("read backfilled identity: %v", err)
		}
		if storedID != backfilledID {
			t.Fatalf("backfilled ID = %d, want %d", storedID, backfilledID)
		}

		afterBackfill := insertBaselineTestTask(t, db, "identity-after-backfill")
		if afterBackfill <= 0 || afterBackfill == backfilledID {
			t.Fatalf("generated ID after backfill = %d", afterBackfill)
		}
		if db.Dialect().Name() == dialect.SQLite && afterBackfill <= backfilledID {
			t.Fatalf("SQLite generated ID after backfill = %d, want > %d", afterBackfill, backfilledID)
		}

		assertBaselineIdentityTypes(t, db)
	})
}

func TestBaselineStoresLargeGeneration(t *testing.T) {
	const large = int64(5_000_000_000)
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		bucketID := insertBaselineTestBucket(t, db, "large-value-bucket")
		sourceID := insertBaselineTestDataSet(t, db, bucketID, "provider-source", 0, large, true)
		var generation int64
		if err := db.NewRaw(`SELECT generation FROM storage_data_sets WHERE id = ?`, sourceID).Scan(t.Context(), &generation); err != nil {
			t.Fatalf("read large generation: %v", err)
		}
		if generation != large {
			t.Fatalf("generation = %d, want %d", generation, large)
		}
	})
}

func insertBaselineTestTask(t *testing.T, db *bun.DB, key string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO tasks
		(type, idempotency_key, input_version, input_hash, available_at, created_at, updated_at)
		VALUES ('test', ?, 1, 'hash', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING id`, key).Scan(&id); err != nil {
		t.Fatalf("insert baseline test task %q: %v", key, err)
	}
	if _, err := db.Exec(`INSERT INTO task_payloads (task_id, input_json) VALUES (?, '{}')`, id); err != nil {
		t.Fatalf("insert baseline test task payload %q: %v", key, err)
	}
	return id
}

func insertBaselineTestBucket(t *testing.T, db *bun.DB, name string) int64 {
	t.Helper()
	return insertBaselineTestBucketWithSlots(t, db, name, 8)
}

// insertBaselineTestBucketWithSlots opens exactly the given number of replica
// slots, so a test can reach for an index the bucket never opened.
func insertBaselineTestBucketWithSlots(t *testing.T, db *bun.DB, name string, slots int) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO buckets (name, default_copies, minimum_durable_copies, created_at, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id`, name, slots, slots).Scan(&id); err != nil {
		t.Fatalf("insert baseline test bucket %q: %v", name, err)
	}
	for copyIndex := range slots {
		if _, err := db.Exec(`INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, id, copyIndex); err != nil {
			t.Fatalf("insert baseline test replica slot %d: %v", copyIndex, err)
		}
	}
	return id
}

func insertBaselineTestDataSet(t *testing.T, db *bun.DB, bucketID int64, provider string, copyIndex int, generation int64, current bool) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO storage_data_sets
		(bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 'ready', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING id`, bucketID, provider, copyIndex, generation, current,
		fmt.Sprintf("%d-%s-%d-%d", bucketID, provider, copyIndex, generation)).Scan(&id); err != nil {
		t.Fatalf("insert baseline test data set: %v", err)
	}
	return id
}

func insertBaselineTestContent(t *testing.T, db *bun.DB, bucketID int64, identity string) int64 {
	t.Helper()
	digest := sha256.Sum256([]byte(identity))
	var id int64
	if err := db.QueryRow(`INSERT INTO storage_contents
		(bucket_id, content_size, checksum, requested_copies, created_at, updated_at)
		VALUES (?, 1, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING id`, bucketID, hex.EncodeToString(digest[:])).Scan(&id); err != nil {
		t.Fatalf("insert baseline test content: %v", err)
	}
	return id
}

func insertBaselineTestCopy(
	t *testing.T,
	db *bun.DB,
	contentID int64,
	bucketID int64,
	storageDataSetID int64,
	copyIndex int,
	provider string,
	transferMethod string,
) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO storage_copies
		(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
		VALUES (?, ?, 1, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING id`, contentID, bucketID, storageDataSetID, copyIndex, provider, transferMethod).Scan(&id); err != nil {
		t.Fatalf("insert baseline test copy: %v", err)
	}
	return id
}

func mustRejectStatement(t *testing.T, db *bun.DB, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	if err == nil {
		t.Fatalf("statement unexpectedly succeeded: %s", query)
	}
	requireConstraintRejection(t, err, query)
}

// mustRejectRequiredColumn asserts the opposite of mustRejectStatement: the
// omitted column is itself the invariant under test, so a null-constraint
// rejection is the expected outcome rather than a false pass.
func mustRejectRequiredColumn(t *testing.T, db *bun.DB, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	if err == nil {
		t.Fatalf("statement unexpectedly succeeded: %s", query)
	}
	if !rejectedByNullConstraint(err) {
		t.Fatalf("statement was not rejected by a null constraint: %s\nerror: %v", query, err)
	}
}

// requireConstraintRejection keeps a negative assertion honest. A statement
// that omits a required column is rejected by the null constraint before the
// constraint under test is ever evaluated, so the case would keep passing
// while proving nothing.
func requireConstraintRejection(t *testing.T, err error, query string) {
	t.Helper()
	if rejectedByNullConstraint(err) {
		t.Fatalf("statement was rejected by a null constraint instead of the constraint under test: %s\nerror: %v", query, err)
	}
	if rejectedByMissingSchema(err) {
		t.Fatalf("statement never reached the constraint under test because the schema has no such table or column: %s\nerror: %v", query, err)
	}
}

// rejectedByMissingSchema reports a statement that never reached the constraint
// under test because it names a table or column the schema does not have. Such a
// statement fails, so a negative assertion keeps passing while proving nothing —
// exactly how three stale cases survived a column being removed.
func rejectedByMissingSchema(err error) bool {
	for _, missing := range []string{
		"no such table",       // SQLite
		"no such column",      // SQLite
		"has no column named", // SQLite, INSERT column list
		"does not exist",      // PostgreSQL, relation/column
		"undefined_table",     // PostgreSQL, SQLSTATE name
		"undefined_column",    // PostgreSQL, SQLSTATE name
	} {
		if strings.Contains(err.Error(), missing) {
			return true
		}
	}
	return false
}

func rejectedByNullConstraint(err error) bool {
	for _, nullConstraint := range []string{
		"NOT NULL constraint failed", // SQLite
		"null value in column",       // PostgreSQL
	} {
		if strings.Contains(err.Error(), nullConstraint) {
			return true
		}
	}
	return false
}

func assertBaselineIdentityTypes(t *testing.T, db *bun.DB) {
	t.Helper()
	if db.Dialect().Name() == dialect.PG {
		var idType, inputVersionType, generationType, identity string
		if err := db.QueryRow(`SELECT
			(SELECT data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'tasks' AND column_name = 'id'),
			(SELECT data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'tasks' AND column_name = 'input_version'),
			(SELECT data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'storage_data_sets' AND column_name = 'generation'),
			(SELECT is_identity FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'tasks' AND column_name = 'id')`).
			Scan(&idType, &inputVersionType, &generationType, &identity); err != nil {
			t.Fatalf("read PostgreSQL identity types: %v", err)
		}
		if idType != "bigint" || inputVersionType != "integer" || generationType != "bigint" || identity != "YES" {
			t.Fatalf("PostgreSQL types = id:%s input:%s generation:%s identity:%s", idType, inputVersionType, generationType, identity)
		}
		return
	}
	for table, column := range map[string]string{
		"tasks":             "id",
		"tasks/input":       "input_version",
		"storage_data_sets": "generation",
	} {
		tableName := table
		if table == "tasks/input" {
			tableName = "tasks"
		}
		var sqlType string
		if err := db.NewRaw(`SELECT type FROM pragma_table_info(?) WHERE name = ?`, tableName, column).Scan(t.Context(), &sqlType); err != nil {
			t.Fatalf("read SQLite type for %s.%s: %v", tableName, column, err)
		}
		if sqlType != "INTEGER" {
			t.Fatalf("SQLite type for %s.%s = %s, want INTEGER", tableName, column, sqlType)
		}
	}
}

// A replica index is a foreign key into the bucket's own slots, not a bare
// integer bounded by a range check. Every other seed in this package opens all
// eight slots, so this is the only case that reaches the constraint.
func TestBaselineReplicaIndexRequiresAnOpenSlot(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		bucketID := insertBaselineTestBucketWithSlots(t, db, "two-slot-bucket", 2)

		// The bucket opened 0 and 1, so a data set on 5 has no slot to belong to.
		mustRejectStatement(t, db, `INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_at, updated_at)
			VALUES (?, 'provider-a', 5, 1, TRUE, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID)

		// Same index on a bucket that did open it is accepted, which is what
		// makes the rejection above about the slot rather than the range.
		wideBucketID := insertBaselineTestBucketWithSlots(t, db, "eight-slot-bucket", 8)
		if _, err := db.Exec(`INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_at, updated_at)
			VALUES (?, 'provider-a', 5, 1, TRUE, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, wideBucketID); err != nil {
			t.Fatalf("insert data set on an open slot: %v", err)
		}

		// A slot cannot be dropped while a generation still stands on it.
		mustRejectStatement(t, db, `DELETE FROM bucket_replica_slots WHERE bucket_id = ? AND copy_index = 5`, wideBucketID)
	})
}

// A termination names one data set through the role it ended, and the composite
// foreign keys keep that data set inside the replacement it belongs to.
func TestBaselineTerminationBelongsToItsReplacementRole(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		bucketID := insertBaselineTestBucket(t, db, "termination-bucket")
		source := insertBaselineTestDataSet(t, db, bucketID, "101", 0, 1, true)
		target := insertBaselineTestDataSet(t, db, bucketID, "202", 0, 2, false)
		stranger := insertBaselineTestDataSet(t, db, bucketID, "303", 1, 1, true)
		var replacementID int64
		if err := db.QueryRow(`INSERT INTO storage_replacements
			(bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, price_list_fingerprint, status, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'termination-request', 'test-price', 'retiring', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			RETURNING id`, bucketID, source, target).Scan(&replacementID); err != nil {
			t.Fatalf("insert replacement: %v", err)
		}

		// The role decides which data set column carries the subject.
		mustRejectStatement(t, db, `INSERT INTO storage_data_set_terminations
			(replacement_id, role, abandoned_target_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'source', ?, 84, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, target)
		mustRejectStatement(t, db, `INSERT INTO storage_data_set_terminations
			(replacement_id, role, source_data_set_id, abandoned_target_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'source', ?, ?, 84, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, source, target)
		mustRejectStatement(t, db, `INSERT INTO storage_data_set_terminations
			(replacement_id, role, source_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'both', ?, 84, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, source)

		// A source termination cannot name a data set this replacement never
		// held as its source, even one that exists in the same bucket.
		mustRejectStatement(t, db, `INSERT INTO storage_data_set_terminations
			(replacement_id, role, source_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'source', ?, 84, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, stranger)
		// The replacement's own target is not its source either.
		mustRejectStatement(t, db, `INSERT INTO storage_data_set_terminations
			(replacement_id, role, source_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'source', ?, 84, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, target)

		if _, err := db.Exec(`INSERT INTO storage_data_set_terminations
			(replacement_id, role, source_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'source', ?, 84, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, source); err != nil {
			t.Fatalf("insert source termination: %v", err)
		}
		// One end of term per role: a second would mean paying twice.
		mustRejectStatement(t, db, `INSERT INTO storage_data_set_terminations
			(replacement_id, role, source_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'source', ?, 90, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, source)
		// The abandoned target is a separate role and still fits.
		if _, err := db.Exec(`INSERT INTO storage_data_set_terminations
			(replacement_id, role, abandoned_target_data_set_id, epoch, created_at, updated_at)
			VALUES (?, 'abandoned_target', ?, 90, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, target); err != nil {
			t.Fatalf("insert abandoned target termination: %v", err)
		}
	})
}
