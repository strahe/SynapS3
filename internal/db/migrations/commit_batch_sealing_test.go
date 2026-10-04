package migrations

import (
	"testing"

	"github.com/uptrace/bun"
)

func TestCommitBatchMigrationPreservesLedgerAndReplays(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		if err := runMigrationBody(ctx, db, up2026090101InitialSchema); err != nil {
			t.Fatal(err)
		}
		bucket := insertBaselineTestBucket(t, db, "batch-migration")
		content := insertBaselineTestContent(t, db, bucket, "migration-content")
		dataSet := insertBaselineTestDataSet(t, db, bucket, "901", 0, 1, true)
		copyID := insertBaselineTestCopy(t, db, content, bucket, dataSet, 0, "901", "ingress")
		task := insertBaselineTestTask(t, db, "migration-request")
		if _, err := db.ExecContext(ctx, `INSERT INTO storage_commit_requests
			(request_id, storage_data_set_id, status, task_id, piece_count, sends, refusals, created_at, updated_at)
			VALUES ('migration-request', ?, 'collecting', ?, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, dataSet, task); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE storage_copies SET status = 'piece_ready', commit_request_id = 'migration-request' WHERE id = ?`, copyID); err != nil {
			t.Fatal(err)
		}
		signedTask := insertBaselineTestTask(t, db, "signed-migration-request")
		if _, err := db.ExecContext(ctx, `INSERT INTO storage_commit_requests
			(request_id, storage_data_set_id, status, task_id, piece_count, extra_data_hex, sealed_at, sends, refusals, created_at, updated_at)
			VALUES ('signed-request', ?, 'ready', ?, 1, 'abcd', CURRENT_TIMESTAMP, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, dataSet, signedTask); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO storage_commit_request_pieces (request_id, position, content_id, storage_data_set_id, piece_cid, created_at)
			VALUES ('signed-request', 0, ?, ?, 'signed-piece', CURRENT_TIMESTAMP)`, content, dataSet); err != nil {
			t.Fatal(err)
		}
		if err := up2026100401CommitBatchSealing(ctx, db); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := db.NewRaw(`SELECT COUNT(*) FROM storage_commit_requests WHERE request_id = 'migration-request' AND seal_requested_at IS NULL`).Scan(ctx, &count); err != nil || count != 1 {
			t.Fatalf("old ledger = %d, %v", count, err)
		}
		if err := db.NewRaw(`SELECT COUNT(*) FROM storage_copies WHERE id = ? AND commit_request_id = 'migration-request'`, copyID).Scan(ctx, &count); err != nil || count != 1 {
			t.Fatalf("old membership = %d, %v", count, err)
		}
		if err := db.NewRaw(`SELECT COUNT(*) FROM storage_commit_request_pieces AS piece JOIN storage_commit_requests AS request ON request.request_id = piece.request_id
			WHERE piece.request_id = 'signed-request' AND piece.position = 0 AND piece.piece_cid = 'signed-piece' AND request.piece_count = 1 AND request.status = 'ready' AND request.extra_data_hex = 'abcd'`).Scan(ctx, &count); err != nil || count != 1 {
			t.Fatalf("signed ledger = %d, %v", count, err)
		}
		for _, index := range []string{"idx_storage_commit_requests_data_set_status", "idx_storage_commit_requests_task", "idx_storage_commit_requests_created", "idx_storage_commit_requests_status_created"} {
			if exists, err := indexExists(ctx, db, index); err != nil || !exists {
				t.Fatalf("index %s = %v, %v", index, exists, err)
			}
		}
		if _, err := db.ExecContext(ctx, `UPDATE storage_commit_requests SET seal_requested_at = CURRENT_TIMESTAMP WHERE request_id = 'migration-request'`); err != nil {
			t.Fatal(err)
		}
		mustRejectStatement(t, db, `UPDATE storage_commit_requests SET status = 'ready', piece_count = 1, extra_data_hex = 'abcd', sealed_at = CURRENT_TIMESTAMP WHERE request_id = 'migration-request'`)
		if err := up2026100401CommitBatchSealing(ctx, db); err != nil {
			t.Fatalf("replay: %v", err)
		}
		if err := db.NewRaw(`SELECT COUNT(*) FROM storage_commit_requests WHERE request_id = 'migration-request' AND seal_requested_at IS NOT NULL`).Scan(ctx, &count); err != nil || count != 1 {
			t.Fatalf("replay lost intent = %d, %v", count, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE storage_commit_requests SET status = 'ready', piece_count = 1, extra_data_hex = 'abcd', sealed_at = CURRENT_TIMESTAMP, seal_requested_at = NULL WHERE request_id = 'migration-request'`); err != nil {
			t.Fatal(err)
		}
		mustRejectStatement(t, db, `DELETE FROM tasks WHERE id = ?`, task)
	})
}
