package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestCurrentSchemaConstraintsRejectInvalidWrites(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		bucketID := insertSchemaTestBucket(t, db, "constraint-bucket")
		var strategy string
		if err := db.NewRaw("SELECT provider_selection_strategy FROM buckets WHERE id = ?", bucketID).Scan(t.Context(), &strategy); err != nil || strategy != "distribution" {
			t.Fatalf("default provider strategy = %q, err=%v", strategy, err)
		}
		if _, err := db.Exec("UPDATE buckets SET provider_selection_strategy = 'speed' WHERE id = ?", bucketID); err != nil {
			t.Fatal(err)
		}
		mustRejectStatement(t, db, `UPDATE buckets SET provider_selection_strategy = 'unknown' WHERE id = ?`, bucketID)

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
			VALUES ('fund', 'invalid-amount', '01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`)
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
		walletTaskID := insertSchemaTestTask(t, db, "wallet-owner")
		for name, statement := range map[string]string{
			"confirmed without completion": `INSERT INTO wallet_operations
				(type, client_request_id, amount, status, tx_hash, created_at, updated_at)
				VALUES ('fund', 'confirmed-open', '1', 'confirmed', 'tx-open', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			"pending with completion": `INSERT INTO wallet_operations
				(type, client_request_id, amount, completed_at, created_at, updated_at)
				VALUES ('fund', 'pending-completed', '1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			"failed without error": `INSERT INTO wallet_operations
				(type, client_request_id, amount, status, completed_at, created_at, updated_at)
				VALUES ('fund', 'failed-silent', '1', 'failed', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			"unknown without broadcast": `INSERT INTO wallet_operations
				(type, client_request_id, amount, status, last_error, completed_at, created_at, updated_at)
				VALUES ('fund', 'unknown-unsent', '1', 'unknown', 'lost', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			"settled holding its task": `INSERT INTO wallet_operations
				(type, client_request_id, amount, status, last_error, task_id, completed_at, created_at, updated_at)
				VALUES ('fund', 'failed-owned', '1', 'failed', 'rejected', ` + strconv.FormatInt(walletTaskID, 10) + `, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			"fund confirmed without transaction": `INSERT INTO wallet_operations
				(type, client_request_id, amount, status, completed_at, created_at, updated_at)
				VALUES ('fund', 'fund-confirmed-untracked', '1', 'confirmed', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			"speed test succeeded without duration": `INSERT INTO provider_upload_speed_tests
				(provider_id, state, service_url_hash, sample_bytes, bytes_per_second, tested_at, created_at, updated_at)
				VALUES ('speed-no-duration', 'succeeded', '` + strings.Repeat("a", 64) + `', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			"speed test succeeded without throughput": `INSERT INTO provider_upload_speed_tests
				(provider_id, state, service_url_hash, sample_bytes, duration_ms, tested_at, created_at, updated_at)
				VALUES ('speed-no-throughput', 'succeeded', '` + strings.Repeat("a", 64) + `', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		} {
			t.Run(name, func(t *testing.T) { mustRejectStatement(t, db, statement) })
		}
		// An approval already in place is confirmed without a transaction.
		if _, err := db.Exec(`INSERT INTO wallet_operations
			(type, client_request_id, amount, status, completed_at, created_at, updated_at)
			VALUES ('approve', 'approve-in-place', '0', 'confirmed', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`); err != nil {
			t.Fatalf("insert approval confirmed without transaction: %v", err)
		}

		// A data version is bytes plus a name, so it cannot exist without the
		// content that holds those bytes, and a delete marker cannot carry one.
		if _, err := db.ExecContext(t.Context(),
			`INSERT INTO objects (id, bucket_id, key, created_at, updated_at) VALUES (1, ?, 'failure-shape.txt', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID); err != nil {
			t.Fatalf("insert object: %v", err)
		}
		contentID := insertSchemaTestContent(t, db, bucketID, "v-shape-origin")
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

		dataSetID := insertSchemaTestDataSet(t, db, bucketID, "cleanup-provider", 2, 1, false)
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

		taskID := insertSchemaTestTask(t, db, "valid-before-update")
		mustRejectStatement(t, db, `UPDATE tasks SET claim_generation = -1 WHERE id = ?`, taskID)
		mustRejectStatement(t, db, `UPDATE tasks SET status = 'unknown' WHERE id = ?`, taskID)
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

func TestCurrentSchemaStorageIdentityAndLedgerConstraints(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		bucketA := insertSchemaTestBucket(t, db, "identity-a")
		bucketB := insertSchemaTestBucket(t, db, "identity-b")
		contentA := insertSchemaTestContent(t, db, bucketA, "upload-a")
		contentA2 := insertSchemaTestContent(t, db, bucketA, "upload-a2")
		contentB := insertSchemaTestContent(t, db, bucketB, "upload-b")
		source := insertSchemaTestDataSet(t, db, bucketA, "101", 0, 1, true)
		if _, err := db.Exec(`UPDATE storage_data_sets SET creation_rejection = '{"version":99}' WHERE id = ?`, source); err != nil {
			t.Fatal(err)
		}
		for _, invalid := range []string{"{", "[]", "null"} {
			mustRejectStatement(t, db, `UPDATE storage_data_sets SET creation_rejection = ? WHERE id = ?`, invalid, source)
		}
		copyID := insertSchemaTestCopy(t, db, contentA, bucketA, source, 0, "101", "ingress")
		// A committed copy must name a confirmed request in its own data set,
		// at a position recorded for its own content.
		mustRejectStatement(t, db, `UPDATE storage_copies
			SET commit_request_status = 'confirmed' WHERE id = ?`, copyID)
		requestTask := insertSchemaTestTask(t, db, "commit-request-a")
		if _, err := db.Exec(`INSERT INTO storage_commit_requests
			(request_id, storage_data_set_id, status, task_id, piece_count, sends, refusals, created_at, updated_at)
			VALUES ('request-a', ?, 'collecting', ?, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, source, requestTask); err != nil {
			t.Fatalf("insert collecting request: %v", err)
		}
		// A request that is not settled keeps its task.
		mustRejectStatement(t, db, `UPDATE storage_commit_requests SET task_id = NULL WHERE request_id = 'request-a'`)
		// A collecting request carries no signature yet.
		mustRejectStatement(t, db, `UPDATE storage_commit_requests SET extra_data_hex = 'abcd' WHERE request_id = 'request-a'`)
		// Attention is raised only on a request that may be on chain.
		mustRejectStatement(t, db, `UPDATE storage_commit_requests
			SET attention_code = 'confirmation_timeout', attention_at = CURRENT_TIMESTAMP WHERE request_id = 'request-a'`)
		if _, err := db.Exec(`UPDATE storage_copies SET status = 'piece_ready', commit_request_id = 'request-a' WHERE id = ?`, copyID); err != nil {
			t.Fatalf("join collecting request: %v", err)
		}
		// A transferred copy waiting in a request has no position until signed.
		mustRejectStatement(t, db, `UPDATE storage_copies SET commit_position = 0 WHERE id = ?`, copyID)
		if _, err := db.Exec(`INSERT INTO storage_commit_request_pieces
			(request_id, position, content_id, storage_data_set_id, piece_cid, created_at)
			VALUES ('request-a', 0, ?, ?, 'baga-own', CURRENT_TIMESTAMP)`, contentA, source); err != nil {
			t.Fatalf("insert request piece: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_commit_request_pieces
			(request_id, position, content_id, storage_data_set_id, piece_cid, created_at)
			VALUES ('request-a', 0, ?, ?, 'baga-other', CURRENT_TIMESTAMP)`, contentA2, source)
		// The position belongs to its own content: another content's copy
		// cannot name it.
		otherCopy := insertSchemaTestCopy(t, db, contentA2, bucketA, source, 0, "101", "ingress")
		mustRejectStatement(t, db, `UPDATE storage_copies
			SET status = 'committing', commit_request_id = 'request-a', commit_position = 0 WHERE id = ?`, otherCopy)
		if _, err := db.Exec(`UPDATE storage_commit_requests SET seal_requested_at = CURRENT_TIMESTAMP WHERE request_id = 'request-a'`); err != nil {
			t.Fatal(err)
		}
		mustRejectStatement(t, db, `UPDATE storage_commit_requests SET status = 'ready', piece_count = 1, extra_data_hex = 'abcd', sealed_at = CURRENT_TIMESTAMP WHERE request_id = 'request-a'`)
		if _, err := db.Exec(`UPDATE storage_commit_requests SET seal_requested_at = NULL WHERE request_id = 'request-a'`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE storage_commit_requests
			SET status = 'ready', piece_count = 1, extra_data_hex = 'abcd', sealed_at = CURRENT_TIMESTAMP
			WHERE request_id = 'request-a'`); err != nil {
			t.Fatalf("seal request: %v", err)
		}
		if _, err := db.Exec(`UPDATE storage_copies SET status = 'committing', commit_position = 0 WHERE id = ?`, copyID); err != nil {
			t.Fatalf("position sealed member: %v", err)
		}
		commitCopy := `UPDATE storage_copies
			SET status = 'committed', piece_id = '1', retrieval_url = 'https://provider.example/piece',
			    commit_request_status = 'confirmed'
			WHERE id = ?`
		// The request is not confirmed yet.
		mustRejectStatement(t, db, commitCopy, copyID)
		// A submitted request has been sent at least once.
		mustRejectStatement(t, db, `UPDATE storage_commit_requests SET status = 'submitted' WHERE request_id = 'request-a'`)
		if _, err := db.Exec(`UPDATE storage_commit_requests
			SET status = 'submitted', first_sent_at = CURRENT_TIMESTAMP, submitted_at = CURRENT_TIMESTAMP,
			    last_sent_at = CURRENT_TIMESTAMP, sends = 1
			WHERE request_id = 'request-a'`); err != nil {
			t.Fatalf("submit request: %v", err)
		}
		// A receipt names its transaction and its status together.
		mustRejectStatement(t, db, `UPDATE storage_commit_requests SET transaction_id = '0xown' WHERE request_id = 'request-a'`)
		if _, err := db.Exec(`UPDATE storage_commit_requests
			SET transaction_id = '0xown', status_url = 'https://provider.example/status/own',
			    attention_code = 'confirmation_timeout', attention_at = CURRENT_TIMESTAMP
			WHERE request_id = 'request-a'`); err != nil {
			t.Fatalf("record submission evidence and attention: %v", err)
		}
		// A confirmation proven only by the nonce names no confirmed transaction.
		mustRejectStatement(t, db, `UPDATE storage_commit_requests
			SET status = 'confirmed', task_id = NULL, confirmed_at = CURRENT_TIMESTAMP, attention_code = NULL, attention_at = NULL
			WHERE request_id = 'request-a'`)
		if _, err := db.Exec(`UPDATE storage_commit_requests
			SET status = 'confirmed', task_id = NULL, first_piece_id = '1', confirmed_at = CURRENT_TIMESTAMP,
			    attention_code = NULL, attention_at = NULL
			WHERE request_id = 'request-a'`); err != nil {
			t.Fatalf("confirm request by nonce: %v", err)
		}
		if _, err := db.Exec(commitCopy, copyID); err != nil {
			t.Fatalf("commit copy with its confirmed request: %v", err)
		}
		// A confirmed request stays confirmed while a copy names it.
		mustRejectStatement(t, db, `UPDATE storage_commit_requests
			SET status = 'abandoned', last_error = 'x' WHERE request_id = 'request-a'`)
		if _, err := db.Exec(`UPDATE storage_copies
			SET status = 'pending', piece_id = NULL, retrieval_url = NULL,
			    commit_request_id = NULL, commit_position = NULL, commit_request_status = NULL
			WHERE id = ?`, copyID); err != nil {
			t.Fatalf("reopen copy: %v", err)
		}
		if _, err := db.Exec(`DELETE FROM storage_copies WHERE id = ?`, otherCopy); err != nil {
			t.Fatalf("remove second copy: %v", err)
		}

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
		bucketBSource := insertSchemaTestDataSet(t, db, bucketB, "101", 0, 1, true)

		createdByContent := insertSchemaTestContent(t, db, bucketB, "provenance-created-by")
		if _, err := db.Exec(`INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_by_content_id, created_at, updated_at)
			VALUES (?, 'provenance-created-by', 1, 1, FALSE, 'retired', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketB, createdByContent); err != nil {
			t.Fatalf("insert created-by provenance: %v", err)
		}
		mustRejectStatement(t, db, `DELETE FROM storage_contents WHERE id = ?`, createdByContent)

		target := insertSchemaTestDataSet(t, db, bucketA, "202", 0, 2, false)
		var replacementID int64
		if err := db.QueryRow(`INSERT INTO storage_replacements
			(bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, price_list_fingerprint, status, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'replacement-1', 'test-price', 'preparing_target', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			RETURNING id`, bucketA, source, target).Scan(&replacementID); err != nil {
			t.Fatalf("insert replacement: %v", err)
		}
		bucketBTarget := insertSchemaTestDataSet(t, db, bucketB, "303", 0, 2, false)
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
			 selection_mode, client_request_id, price_list_fingerprint, status, wait_reason, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'replacement-2', 'test-price', 'waiting', 'provider', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA, source, target)
		// Status and its reasons move together: only a waiting replacement
		// has a wait reason, a failure reason survives only into superseded,
		// and only a superseded replacement names its successor.
		for _, statement := range []string{
			`UPDATE storage_replacements SET status = 'waiting' WHERE id = ?`,
			`UPDATE storage_replacements SET wait_reason = 'provider' WHERE id = ?`,
			`UPDATE storage_replacements SET failure_reason = 'target_in_use' WHERE id = ?`,
			`UPDATE storage_replacements SET superseded_by_id = id WHERE id = ?`,
		} {
			mustRejectStatement(t, db, statement, replacementID)
		}
		if _, err := db.Exec(`UPDATE storage_replacements SET status = 'waiting', wait_reason = 'provider' WHERE id = ?`, replacementID); err != nil {
			t.Fatalf("mark replacement waiting: %v", err)
		}
		if _, err := db.Exec(`UPDATE storage_replacements
			SET status = 'failed', wait_reason = NULL, failure_reason = 'target_in_use' WHERE id = ?`, replacementID); err != nil {
			t.Fatalf("mark replacement retryable: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_replacements
			(bucket_id, copy_index, source_data_set_id, target_data_set_id,
			 selection_mode, client_request_id, price_list_fingerprint, status, created_at, updated_at)
			VALUES (?, 0, ?, ?, 'manual', 'replacement-3', 'test-price', 'preparing_target', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketA, source, target)
		mustRejectStatement(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, ?, 0, '202', 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentA, bucketA, target)
		insertSchemaTestCopy(t, db, contentA, bucketA, target, 0, "202", "peer_pull")
		if _, err := db.Exec(`INSERT INTO storage_replacement_items
			(replacement_id, content_id, target_data_set_id, created_at, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, replacementID, contentA, target); err != nil {
			t.Fatalf("insert replacement item after target copy: %v", err)
		}

		// A copy can have only one unresolved pull attempt: a second source can
		// be tried only after the first attempt is abandoned.
		if _, err := db.Exec(`INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, extra_data_hex, attempted_at, created_at, updated_at)
			VALUES ('pull-1', ?, ?, 'attempted', '301', '3001', '4001', 'bafk2bzacepull', 'https://source.example/piece', 'ab', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			contentA, source); err != nil {
			t.Fatalf("insert first pull attempt: %v", err)
		}
		mustRejectStatement(t, db, `INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, extra_data_hex, attempted_at, created_at, updated_at)
			VALUES ('pull-2', ?, ?, 'attempted', '302', '3002', '4002', 'bafk2bzacepull2', 'https://source.example/other', 'ab', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			contentA, source)
		mustRejectStatement(t, db, `UPDATE storage_pull_attempts SET storage_data_set_id = -1 WHERE attempt_id = 'pull-1'`)
		mustRejectRequiredColumn(t, db, `UPDATE storage_pull_attempts SET extra_data_hex = NULL WHERE attempt_id = 'pull-1'`)
		mustRejectStatement(t, db, `UPDATE storage_pull_attempts SET extra_data_hex = '' WHERE attempt_id = 'pull-1'`)
		mustRejectStatement(t, db, `UPDATE storage_pull_attempts SET status = 'abandoned' WHERE attempt_id = 'pull-1'`)
		if _, err := db.Exec(`UPDATE storage_pull_attempts
			SET status = 'abandoned', resolved_at = current_timestamp WHERE attempt_id = 'pull-1'`); err != nil {
			t.Fatalf("abandon first pull attempt: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, extra_data_hex, attempted_at, created_at, updated_at)
			VALUES ('pull-2', ?, ?, 'attempted', '302', '3002', '4002', 'bafk2bzacepull2', 'https://source.example/other', 'ab', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
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
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, extra_data_hex, attempted_at, created_at, updated_at)
			VALUES ('', ?, ?, 'attempted', '303', '3003', '4003', 'bafk2bzacepull3', 'https://source.example/third', 'ab', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			contentA, source)

		ledgerBucket := insertSchemaTestBucket(t, db, "pull-ledger-bucket")
		ledgerContent := insertSchemaTestContent(t, db, ledgerBucket, "pull-ledger-content")
		ledgerDataSet := insertSchemaTestDataSet(t, db, ledgerBucket, "401", 0, 1, true)
		if _, err := db.Exec(`INSERT INTO storage_pull_attempts
			(attempt_id, content_id, storage_data_set_id, status,
			 source_provider_id, source_data_set_id, source_piece_id, source_piece_cid, source_retrieval_url, extra_data_hex, attempted_at, created_at, updated_at)
			VALUES ('pull-ledger', ?, ?, 'attempted', '301', '3001', '4001', 'bafk2bzacepull', 'https://source.example/piece', 'ab', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			ledgerContent, ledgerDataSet); err != nil {
			t.Fatalf("insert retained pull ledger: %v", err)
		}
		mustRejectStatement(t, db, `DELETE FROM storage_data_sets WHERE id = ?`, ledgerDataSet)
		mustRejectStatement(t, db, `UPDATE storage_data_sets SET id = -1 WHERE id = ?`, ledgerDataSet)
		if _, err := db.Exec(`DELETE FROM storage_contents WHERE id = ?`, ledgerContent); err != nil {
			t.Fatalf("content cleanup should retain pull ledger: %v", err)
		}
		var retainedContent, retainedDataSet int64
		if err := db.QueryRow(`SELECT content_id, storage_data_set_id FROM storage_pull_attempts WHERE attempt_id = 'pull-ledger'`).Scan(&retainedContent, &retainedDataSet); err != nil {
			t.Fatalf("read retained pull ledger: %v", err)
		}
		if retainedContent != ledgerContent || retainedDataSet != ledgerDataSet {
			t.Fatalf("cleanup changed pull identity: content=%d, data set=%d", retainedContent, retainedDataSet)
		}

		taskID := insertSchemaTestTask(t, db, "owner-unique")
		if _, err := db.Exec(`UPDATE buckets SET durability_task_id = ? WHERE id = ?`, taskID, bucketA); err != nil {
			t.Fatalf("bind first task owner: %v", err)
		}
		mustRejectStatement(t, db, `UPDATE buckets SET durability_task_id = ? WHERE id = ?`, taskID, bucketB)
	})
}

func TestCurrentSchemaFailedIngressAllowsOneReplacementIngress(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		bucketID := insertSchemaTestBucket(t, db, "retired-ingress")
		contentID := insertSchemaTestContent(t, db, bucketID, "retired-ingress-content")
		retiredDataSetID := insertSchemaTestDataSet(t, db, bucketID, "101", 0, 1, true)
		copyID := insertSchemaTestCopy(t, db, contentID, bucketID, retiredDataSetID, 0, "101", "ingress")
		if _, err := db.Exec(`UPDATE storage_copies SET status = 'failed', last_error = 'retired generation' WHERE id = ?`, copyID); err != nil {
			t.Fatalf("fail original ingress copy: %v", err)
		}
		if _, err := db.Exec(`UPDATE storage_data_sets SET status = 'retired', is_current = FALSE WHERE id = ?`, retiredDataSetID); err != nil {
			t.Fatalf("retire original ingress data set: %v", err)
		}

		currentDataSetID := insertSchemaTestDataSet(t, db, bucketID, "202", 0, 2, true)
		insertSchemaTestCopy(t, db, contentID, bucketID, currentDataSetID, 0, "202", "ingress")
		otherDataSetID := insertSchemaTestDataSet(t, db, bucketID, "303", 1, 1, true)
		mustRejectStatement(t, db, `INSERT INTO storage_copies
			(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, created_at, updated_at)
			VALUES (?, ?, 1, ?, 1, '303', 'ingress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, contentID, bucketID, otherDataSetID)
	})
}

func TestCurrentSchemaIdentitySupportsGenerationAndBackfill(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		first := insertSchemaTestTask(t, db, "identity-first")
		if _, err := db.Exec(`DELETE FROM tasks WHERE id = ?`, first); err != nil {
			t.Fatalf("delete first identity row: %v", err)
		}
		second := insertSchemaTestTask(t, db, "identity-second")
		if second <= first {
			t.Fatalf("generated ID %d reused deleted ID %d", second, first)
		}

		const backfilledID int64 = 5_000_000_000
		if _, err := db.Exec(`INSERT INTO tasks
			(id, type, idempotency_key, input_version, input_hash, available_at, created_at, updated_at, input_json, policy_json, runtime_json, events_json)
			VALUES (?, 'test', 'identity-backfill', 1, 'hash', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{}', '{"version":2,"max_attempts":6}', '{}', '[]')`, backfilledID); err != nil {
			t.Fatalf("backfill explicit identity: %v", err)
		}
		var storedID int64
		if err := db.NewRaw(`SELECT id FROM tasks WHERE id = ?`, backfilledID).Scan(t.Context(), &storedID); err != nil {
			t.Fatalf("read backfilled identity: %v", err)
		}
		if storedID != backfilledID {
			t.Fatalf("backfilled ID = %d, want %d", storedID, backfilledID)
		}

		afterBackfill := insertSchemaTestTask(t, db, "identity-after-backfill")
		if afterBackfill <= 0 || afterBackfill == backfilledID {
			t.Fatalf("generated ID after backfill = %d", afterBackfill)
		}
		if db.Dialect().Name() == dialect.SQLite && afterBackfill <= backfilledID {
			t.Fatalf("SQLite generated ID after backfill = %d, want > %d", afterBackfill, backfilledID)
		}
	})
}

func insertSchemaTestTask(t *testing.T, db *bun.DB, key string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO tasks
		(type,idempotency_key,input_version,input_hash,available_at,created_at,updated_at,input_json,policy_json,runtime_json,events_json)
		VALUES ('test',?,1,'hash',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{"version":2,"max_attempts":6}','{}','[]') RETURNING id`, key).Scan(&id); err != nil {
		t.Fatalf("insert task %q: %v", key, err)
	}
	return id
}

func insertSchemaTestBucket(t *testing.T, db *bun.DB, name string) int64 {
	t.Helper()
	return insertSchemaTestBucketWithSlots(t, db, name, 8)
}

// insertSchemaTestBucketWithSlots opens exactly the given number of replica
// slots, so a test can reach for an index the bucket never opened.
func insertSchemaTestBucketWithSlots(t *testing.T, db *bun.DB, name string, slots int) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO buckets (name, default_copies, minimum_durable_copies, created_at, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id`, name, slots, slots).Scan(&id); err != nil {
		t.Fatalf("insert schema test bucket %q: %v", name, err)
	}
	for copyIndex := range slots {
		if _, err := db.Exec(`INSERT INTO bucket_replica_slots (bucket_id, copy_index, created_at, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, id, copyIndex); err != nil {
			t.Fatalf("insert schema test replica slot %d: %v", copyIndex, err)
		}
	}
	return id
}

func insertSchemaTestDataSet(t *testing.T, db *bun.DB, bucketID int64, provider string, copyIndex int, generation int64, current bool) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO storage_data_sets
		(bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 'ready', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING id`, bucketID, provider, copyIndex, generation, current,
		fmt.Sprintf("%d-%s-%d-%d", bucketID, provider, copyIndex, generation)).Scan(&id); err != nil {
		t.Fatalf("insert schema test data set: %v", err)
	}
	return id
}

func insertSchemaTestContent(t *testing.T, db *bun.DB, bucketID int64, identity string) int64 {
	t.Helper()
	digest := sha256.Sum256([]byte(identity))
	var id int64
	if err := db.QueryRow(`INSERT INTO storage_contents
		(bucket_id, content_size, checksum, requested_copies, created_at, updated_at)
		VALUES (?, 1, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING id`, bucketID, hex.EncodeToString(digest[:])).Scan(&id); err != nil {
		t.Fatalf("insert schema test content: %v", err)
	}
	return id
}

func insertSchemaTestCopy(
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
		t.Fatalf("insert schema test copy: %v", err)
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

func TestCurrentSchemaReplicaIndexRequiresAnOpenSlot(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		bucketID := insertSchemaTestBucketWithSlots(t, db, "two-slot-bucket", 2)

		// The bucket opened 0 and 1, so a data set on 5 has no slot to belong to.
		mustRejectStatement(t, db, `INSERT INTO storage_data_sets
			(bucket_id, provider_id, copy_index, generation, is_current, status, created_at, updated_at)
			VALUES (?, 'provider-a', 5, 1, TRUE, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID)

		// Same index on a bucket that did open it is accepted, which is what
		// makes the rejection above about the slot rather than the range.
		wideBucketID := insertSchemaTestBucketWithSlots(t, db, "eight-slot-bucket", 8)
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
func TestCurrentSchemaTerminationBelongsToItsReplacementRole(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		bucketID := insertSchemaTestBucket(t, db, "termination-bucket")
		source := insertSchemaTestDataSet(t, db, bucketID, "101", 0, 1, true)
		target := insertSchemaTestDataSet(t, db, bucketID, "202", 0, 2, false)
		stranger := insertSchemaTestDataSet(t, db, bucketID, "303", 1, 1, true)
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

// Accounts without a name share the empty default; set names are unique byte
// for byte, so names differing only in case are distinct.
func TestCurrentSchemaAccountNamesAreUniqueOnlyWhenSet(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		const unnamed = `INSERT INTO s3_accounts (access_key, secret_key, role, is_root, created_at, updated_at)
			VALUES (?, 'secret', 'user', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
		const named = `INSERT INTO s3_accounts (access_key, name, secret_key, role, is_root, created_at, updated_at)
			VALUES (?, ?, 'secret', 'user', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
		for _, accessKey := range []string{"unnamed-1", "unnamed-2"} {
			if _, err := db.Exec(unnamed, accessKey); err != nil {
				t.Fatalf("unnamed account %s: %v", accessKey, err)
			}
		}
		var name string
		if err := db.NewRaw(`SELECT name FROM s3_accounts WHERE access_key = 'unnamed-1'`).Scan(ctx, &name); err != nil || name != "" {
			t.Fatalf("unnamed account name = %q, err=%v", name, err)
		}
		for accessKey, accountName := range map[string]string{"alice-upper": "Alice", "alice-lower": "alice"} {
			if _, err := db.Exec(named, accessKey, accountName); err != nil {
				t.Fatalf("account named %s: %v", accountName, err)
			}
		}
		mustRejectStatement(t, db, named, "alice-again", "Alice")
	})
}

func TestTaskTablesEnforceStateAndBudget(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		id := insertSchemaTestTask(t, db, "working")
		for _, statement := range []string{
			`UPDATE tasks SET status='completed',finished_at=CURRENT_TIMESTAMP WHERE id=?`,
			`UPDATE tasks SET status='cancelled',finished_at=CURRENT_TIMESTAMP WHERE id=?`,
			`UPDATE tasks SET retry_count=-1 WHERE id=?`,
			`UPDATE tasks SET input_json='[]' WHERE id=?`,
			`UPDATE tasks SET input_json='{' WHERE id=?`,
			`UPDATE tasks SET events_json='{}' WHERE id=?`,
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
		if _, err := db.Exec(`INSERT INTO task_history
			(task_id,type,idempotency_key,input_version,input_hash,input_json,policy_json,runtime_json,events_json,status,available_at,finished_at,created_at,updated_at)
			VALUES (1001,'test','history',1,'hash','{}','{"version":2,"max_attempts":6}','{}','[]','completed',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`); err != nil {
			t.Fatal(err)
		}
		for _, status := range []string{"pending", "running", "failed"} {
			mustRejectStatement(t, db, `UPDATE task_history SET status = ? WHERE task_id = 1001`, status)
		}
		mustRejectStatement(t, db, `UPDATE task_history SET acknowledged_at = CURRENT_TIMESTAMP WHERE task_id = 1001`)
	})
}
