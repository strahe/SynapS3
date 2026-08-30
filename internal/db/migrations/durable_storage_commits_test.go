package migrations

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestDurableStorageCommitsMigration(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		createDurableStorageCommitLegacyTable(t, db)
		if err := runMigrationBody(t.Context(), db, up2026083001DurableStorageCommits); err != nil {
			t.Fatalf("up migration: %v", err)
		}
		for _, column := range durableStorageCommitColumns2026083001 {
			exists, err := columnExists(t.Context(), db, "storage_upload_copies", column)
			if err != nil || !exists {
				t.Fatalf("column %s exists=%v err=%v", column, exists, err)
			}
		}
		for _, index := range durableStorageCommitIndexes2026083001 {
			exists, err := indexExists(t.Context(), db, index)
			if err != nil || !exists {
				t.Fatalf("index %s exists=%v err=%v", index, exists, err)
			}
		}
		if db.Dialect().Name() == dialect.PG {
			want := time.Date(2026, time.August, 30, 17, 45, 12, 123456000, time.FixedZone("UTC+08", 8*60*60))
			if _, err := db.ExecContext(t.Context(), `INSERT INTO storage_upload_copies
				(id, status, commit_ready_at, commit_attempted_at, commit_attention_at)
				VALUES (?, 'piece_ready', ?, ?, ?)`, 1, want, want, want); err != nil {
				t.Fatalf("insert PostgreSQL commit timestamps: %v", err)
			}
			var got struct {
				ReadyAt     time.Time `bun:"commit_ready_at"`
				AttemptedAt time.Time `bun:"commit_attempted_at"`
				AttentionAt time.Time `bun:"commit_attention_at"`
			}
			if err := db.NewRaw(`SELECT commit_ready_at, commit_attempted_at, commit_attention_at
				FROM storage_upload_copies WHERE id = ?`, 1).Scan(t.Context(), &got); err != nil {
				t.Fatalf("scan PostgreSQL commit timestamps: %v", err)
			}
			if !got.ReadyAt.Equal(want) || !got.AttemptedAt.Equal(want) || !got.AttentionAt.Equal(want) {
				t.Fatalf("PostgreSQL timestamp round trip = %s/%s/%s, want instant %s",
					got.ReadyAt, got.AttemptedAt, got.AttentionAt, want)
			}
		}
		if err := runMigrationBody(t.Context(), db, up2026083001DurableStorageCommits); err != nil {
			t.Fatalf("idempotent up migration: %v", err)
		}
	})
}

func TestDurableStorageCommitsMigrationRejectsLegacyCommittingRowsAtomically(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		createDurableStorageCommitLegacyTable(t, db)
		mustExecMigrationTest(t, db, "INSERT INTO storage_upload_copies (id, status) VALUES (1, 'committing')")

		err := runMigrationBody(t.Context(), db, up2026083001DurableStorageCommits)
		if err == nil || !strings.Contains(err.Error(), "legacy committing copies") {
			t.Fatalf("up migration error = %v, want legacy committing refusal", err)
		}
		assertNoDurableStorageCommitDDL(t, db)
	})
}

func TestDurableStorageCommitsMigrationRejectsPartialSchema(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		createDurableStorageCommitLegacyTable(t, db)
		mustExecMigrationTest(t, db, "ALTER TABLE storage_upload_copies ADD COLUMN commit_ready_at TIMESTAMP")

		err := runMigrationBody(t.Context(), db, up2026083001DurableStorageCommits)
		if err == nil || !strings.Contains(err.Error(), "partial schema state") {
			t.Fatalf("up migration error = %v, want partial schema refusal", err)
		}
		exists, checkErr := columnExists(t.Context(), db, "storage_upload_copies", "commit_attempt_id")
		if checkErr != nil || exists {
			t.Fatalf("failed migration added commit_attempt_id=%v err=%v", exists, checkErr)
		}
	})
}

func TestDurableStorageCommitsMigrationRollbackFence(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		createDurableStorageCommitLegacyTable(t, db)
		if err := runMigrationBody(t.Context(), db, up2026083001DurableStorageCommits); err != nil {
			t.Fatalf("up migration: %v", err)
		}
		mustExecMigrationTest(t, db, `INSERT INTO storage_upload_copies
			(id, status, storage_data_set_id, commit_attempt_id, commit_attempted_at)
			VALUES (1, 'committing', 9, 'attempt-1', CURRENT_TIMESTAMP)`)
		if err := runMigrationBody(t.Context(), db, down2026083001DurableStorageCommits); err == nil || !strings.Contains(err.Error(), "durable commit attempts") {
			t.Fatalf("down migration error = %v, want active attempt refusal", err)
		}
		mustExecMigrationTest(t, db, "DELETE FROM storage_upload_copies")
		mustExecMigrationTest(t, db, `INSERT INTO storage_upload_copies
			(id, status, commit_submission_json)
			VALUES (2, 'committing', '{"version":1,"submission":{}}')`)
		if err := runMigrationBody(t.Context(), db, down2026083001DurableStorageCommits); err == nil || !strings.Contains(err.Error(), "durable commit attempts") {
			t.Fatalf("down migration error = %v, want durable submission refusal", err)
		}
		mustExecMigrationTest(t, db, "DELETE FROM storage_upload_copies")
		if err := runMigrationBody(t.Context(), db, down2026083001DurableStorageCommits); err != nil {
			t.Fatalf("down migration after drain: %v", err)
		}
		assertNoDurableStorageCommitDDL(t, db)
	})
}

func createDurableStorageCommitLegacyTable(t *testing.T, db *bun.DB) {
	t.Helper()
	idColumn := "INTEGER PRIMARY KEY"
	if db.Dialect().Name() == dialect.PG {
		idColumn = "BIGINT PRIMARY KEY"
	}
	mustExecMigrationTest(t, db, `CREATE TABLE storage_upload_copies (
		id `+idColumn+`,
		status TEXT NOT NULL,
		storage_data_set_id BIGINT,
		commit_extra_data_hex TEXT,
		commit_transaction_id TEXT
	)`)
}

func assertNoDurableStorageCommitDDL(t *testing.T, db *bun.DB) {
	t.Helper()
	for _, column := range durableStorageCommitColumns2026083001 {
		exists, err := columnExists(context.Background(), db, "storage_upload_copies", column)
		if err != nil {
			t.Fatalf("check column %s: %v", column, err)
		}
		if exists {
			t.Fatalf("column %s remains", column)
		}
	}
	for _, index := range durableStorageCommitIndexes2026083001 {
		exists, err := indexExists(context.Background(), db, index)
		if err != nil {
			t.Fatalf("check index %s: %v", index, err)
		}
		if exists {
			t.Fatalf("index %s remains", index)
		}
	}
}
