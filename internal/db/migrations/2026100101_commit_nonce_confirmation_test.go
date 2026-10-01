package migrations

import (
	"fmt"
	"slices"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// Attempts recorded before the migration keep their rows. Afterwards a
// confirmed attempt may carry no confirmed transaction, but never one without
// the submission it answered, and rerunning the migration changes nothing.
func TestCommitNonceConfirmationMigration(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		migrateToLevel(t, db, 1)
		bucket := insertBaselineTestBucket(t, db, "nonce-confirmation")
		dataSet := insertBaselineTestDataSet(t, db, bucket, "101", 0, 1, true)
		insertAttempt := func(attemptID, status string, transactionID, statusURL, confirmedTransactionID *string) error {
			content := insertBaselineTestContent(t, db, bucket, attemptID)
			_, err := db.ExecContext(ctx, `INSERT INTO storage_commit_attempts
				(attempt_id, content_id, storage_data_set_id, status, extra_data_hex, transaction_id, status_url,
				 confirmed_transaction_id, attempted_at, resolved_at, created_at, updated_at)
				VALUES (?, ?, ?, ?, 'abcd', ?, ?, ?, CURRENT_TIMESTAMP,
				 CASE WHEN ? = 'confirmed' THEN CURRENT_TIMESTAMP END, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
				attemptID, content, dataSet, status, transactionID, statusURL, confirmedTransactionID, status)
			return err
		}
		tx, url := new("0xsubmitted"), new("https://provider.example/status")
		for _, row := range []struct {
			id, status       string
			tx, url, confirm *string
		}{
			{id: "provider-confirmed", status: "confirmed", tx: tx, url: url, confirm: tx},
			{id: "unacknowledged", status: "attempted"},
		} {
			if err := insertAttempt(row.id, row.status, row.tx, row.url, row.confirm); err != nil {
				t.Fatalf("insert %s attempt: %v", row.id, err)
			}
		}
		if err := insertAttempt("nonce-before", "confirmed", nil, nil, nil); err == nil {
			t.Fatal("the baseline accepted a confirmed attempt without a confirmed transaction")
		}
		before := commitAttemptRows(t, db)

		migrateToLevel(t, db, len(Migrations.Sorted()))
		if got := commitAttemptRows(t, db); got != before {
			t.Fatalf("attempts after the migration:\n%s\nwant:\n%s", got, before)
		}
		for _, row := range []struct {
			id      string
			tx, url *string
		}{
			{id: "nonce-unacknowledged"},
			{id: "nonce-acknowledged", tx: new("0xreplaced"), url: new("https://provider.example/status/replaced")},
		} {
			if err := insertAttempt(row.id, "confirmed", row.tx, row.url, nil); err != nil {
				t.Fatalf("insert %s attempt: %v", row.id, err)
			}
		}
		if err := insertAttempt("confirmed-without-submission", "confirmed", nil, nil, tx); err == nil {
			t.Fatal("a confirmed transaction was accepted without the submission it answered")
		}

		portable := db.Dialect().Name() == dialect.PG
		schema, err := describeSchema(ctx, db, portable)
		if err != nil {
			t.Fatal(err)
		}
		rows := commitAttemptRows(t, db)
		if err := up2026100101CommitNonceConfirmation(ctx, db); err != nil {
			t.Fatalf("rerun on the complete post-state: %v", err)
		}
		rerun, err := describeSchema(ctx, db, portable)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(rerun, schema) {
			t.Fatalf("schema after the rerun:\n%v\nwant:\n%v", rerun, schema)
		}
		if got := commitAttemptRows(t, db); got != rows {
			t.Fatalf("attempts after the rerun:\n%s\nwant:\n%s", got, rows)
		}
		if err := ValidateCurrentSchema(ctx, db); err != nil {
			t.Fatalf("validate current schema: %v", err)
		}
	})
}

func commitAttemptRows(t *testing.T, db *bun.DB) string {
	t.Helper()
	var rows []storageCommitAttempt2026100101
	if err := db.NewSelect().Model(&rows).OrderExpr("attempt_id").Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	var out string
	for _, row := range rows {
		out += fmt.Sprintf("%s %s %v %v %v %v %v\n", row.AttemptID, row.Status, deref(row.TransactionID),
			deref(row.StatusURL), deref(row.ConfirmedTransactionID), row.AttemptedAt != nil, row.ResolvedAt != nil)
	}
	return out
}

func deref(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}
