package migrations

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"
)

func TestCommitStatusURLMigrationRebuildsEmptyLedger(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatal(err)
		}
		if err := runMigrationBody(t.Context(), db, up2026092101CommitStatusURL); err != nil {
			t.Fatal(err)
		}
		for _, column := range []string{"status_url", "submission_json"} {
			exists, err := columnExists(t.Context(), db, "storage_commit_attempts", column)
			if err != nil || exists != (column == "status_url") {
				t.Fatalf("column %s exists=%t err=%v", column, exists, err)
			}
		}
		for _, index := range []string{
			"idx_storage_commit_attempts_unresolved_copy",
			"idx_storage_commit_attempts_unresolved_data_set",
			"idx_storage_commit_attempts_copy_history",
		} {
			exists, err := indexExists(t.Context(), db, index)
			if err != nil || !exists {
				t.Fatalf("index %s exists=%t err=%v", index, exists, err)
			}
		}
		constraints := semanticConstraintLines(t, db, applicationSchemaTables(t, db))
		for _, name := range []string{
			"chk_storage_commit_attempts_evidence_shape",
			"chk_storage_commit_attempts_resolution",
			"chk_storage_commit_attempts_submission",
			"uq_storage_commit_attempts_status",
		} {
			if !slices.Contains(constraints, "constraint|storage_commit_attempts|"+name) {
				t.Fatalf("commit ledger constraint %s was not restored", name)
			}
		}
		foreignKeys := semanticForeignKeyLines(t, db, applicationSchemaTables(t, db))
		for _, column := range []string{"confirmed_attempt_id", "confirmed_attempt_status"} {
			found := false
			for _, line := range foreignKeys {
				if strings.HasPrefix(line, "foreign-key|storage_copies|"+column+"|storage_commit_attempts|") {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("storage copy foreign key column %s was not restored", column)
			}
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO storage_commit_attempts
			(attempt_id, content_id, storage_data_set_id, status, extra_data_hex, attempted_at, status_url, created_at, updated_at)
			VALUES ('invalid-status', 1, 1, 'attempted', 'abcd', CURRENT_TIMESTAMP,
			'https://provider.example/status', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`); err == nil {
			t.Fatal("status URL without transaction was accepted")
		}
	})
}

func TestCommitStatusURLMigrationRejectsNonemptyLedgers(t *testing.T) {
	for _, populated := range []string{"attempt", "copy"} {
		t.Run(populated, func(t *testing.T) {
			testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
				if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
					t.Fatal(err)
				}
				if populated == "attempt" {
					if _, err := db.ExecContext(t.Context(), `INSERT INTO storage_commit_attempts
						(attempt_id, content_id, storage_data_set_id, created_at, updated_at)
						VALUES ('existing', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`); err != nil {
						t.Fatal(err)
					}
				} else {
					bucketID := insertBaselineTestBucketWithSlots(t, db, "migration-copy", 1)
					contentID := insertBaselineTestContent(t, db, bucketID, "migration-copy")
					dataSetID := insertBaselineTestDataSet(t, db, bucketID, "provider", 0, 1, true)
					insertBaselineTestCopy(t, db, contentID, bucketID, dataSetID, 0, "provider", "ingress")
				}
				err := runMigrationBody(t.Context(), db, up2026092101CommitStatusURL)
				if !errors.Is(err, ErrIncompatibleDatabase) || !strings.Contains(err.Error(), "new empty database") {
					t.Fatalf("migration error = %v, want explicit empty-database refusal", err)
				}
				exists, err := columnExists(t.Context(), db, "storage_commit_attempts", "submission_json")
				if err != nil || !exists {
					t.Fatalf("rejected migration changed original ledger: exists=%t err=%v", exists, err)
				}
			})
		})
	}
}
