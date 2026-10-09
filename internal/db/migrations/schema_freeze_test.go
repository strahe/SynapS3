package migrations

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestPullDataSetMigrationPreservesLedgerAndEnforcesTarget(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateThrough(t, db, "2026100804")
		bucket := insertBaselineTestBucket(t, db, "pull-ledger")
		content := insertBaselineTestContent(t, db, bucket, "pull-ledger")
		dataSet := insertBaselineTestDataSet(t, db, bucket, "101", 0, 1, true)
		insertFreezePullAttempt(t, db, content, dataSet)
		var before, after storagePullAttempt2026100902
		if err := db.NewSelect().Model(&before).Scan(t.Context()); err != nil {
			t.Fatal(err)
		}
		oldIndexes, err := indexLines(t.Context(), db, []string{"storage_pull_attempts"}, false)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := up2026100902PullDataSetForeignKey(t.Context(), db); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.NewSelect().Model(&after).Scan(t.Context()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("ledger changed: before=%+v after=%+v", before, after)
		}
		newIndexes, err := indexLines(t.Context(), db, []string{"storage_pull_attempts"}, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, index := range oldIndexes {
			if !slices.Contains(newIndexes, index) {
				t.Errorf("lost index %s", index)
			}
		}
		mustRejectStatement(t, db, `UPDATE storage_pull_attempts SET storage_data_set_id = -1`)
		mustRejectStatement(t, db, `DELETE FROM storage_data_sets WHERE id = ?`, dataSet)
		mustRejectStatement(t, db, `UPDATE storage_data_sets SET id = -1 WHERE id = ?`, dataSet)
		mustRejectStatement(t, db, `UPDATE storage_pull_attempts SET status = 'unknown'`)
		mustRejectStatement(t, db, `UPDATE storage_pull_attempts SET extra_data_hex = ''`)
		if _, err := db.ExecContext(t.Context(), `DELETE FROM storage_contents WHERE id = ?`, content); err != nil {
			t.Fatalf("content cleanup should retain ledger: %v", err)
		}
		if err := db.NewSelect().Model(&after).Scan(t.Context()); err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("cleanup changed ledger: %+v, %v", after, err)
		}
		migrateToLevel(t, db, len(Migrations.Sorted()))
		if err := validateCurrentSchema(t.Context(), db); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPullDataSetMigrationRejectsOrphansAndPartialState(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "orphan"
		if partial {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
				migrateThrough(t, db, "2026100804")
				insertFreezePullAttempt(t, db, 101, 999)
				if partial {
					if _, err := db.ExecContext(t.Context(), pullDataSetIndex2026100902); err != nil {
						t.Fatal(err)
					}
				}
				before, err := describeSchema(t.Context(), db, false)
				if err != nil {
					t.Fatal(err)
				}
				if err := up2026100902PullDataSetForeignKey(t.Context(), db); err == nil {
					t.Fatal("invalid upgrade accepted")
				}
				after, err := describeSchema(t.Context(), db, false)
				if err != nil || !slices.Equal(before, after) {
					t.Fatalf("failed upgrade changed schema: %v", err)
				}
				var count int
				if err := db.NewRaw(`SELECT COUNT(*) FROM storage_pull_attempts WHERE storage_data_set_id = 999 AND content_id = 101`).Scan(t.Context(), &count); err != nil || count != 1 {
					t.Fatalf("failed upgrade lost ledger: count=%d, err=%v", count, err)
				}
			})
		})
	}
}

func insertFreezePullAttempt(t *testing.T, db *bun.DB, content, dataSet int64) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `INSERT INTO storage_pull_attempts
		(attempt_id, content_id, storage_data_set_id, status, source_provider_id, source_data_set_id,
		source_piece_id, source_piece_cid, source_retrieval_url, extra_data_hex, attempted_at, resolved_at, created_at, updated_at)
		VALUES ('freeze-attempt', ?, ?, 'attempted', '101', '1001', '1', 'piece', 'https://example.com/piece', 'ab',
		CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, content, dataSet); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryIndexMigrationContractAndRepeat(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateThrough(t, db, "2026100902")
		for range 2 {
			if err := up2026100903TaskHistoryIndexes(t.Context(), db); err != nil {
				t.Fatal(err)
			}
		}
		lines, err := indexLines(t.Context(), db, []string{"task_history"}, false)
		if err != nil {
			t.Fatal(err)
		}
		columns := []string{"subject_type", "subject_key", "status", "superseded_at", "task_id"}
		predicate := ""
		if db.Dialect().Name() == dialect.PG {
			columns = []string{"subject_type", "subject_key", "superseded_at", "task_id"}
			predicate = "status = 'failed' AND superseded_at IS NULL"
		}
		for _, want := range []string{
			indexLine("task_history", "idx_task_history_status_id", false, []string{"status", "task_id"}, "", true),
			indexLine("task_history", "idx_task_history_current_failed_subject", false, columns, predicate, true),
		} {
			if !slices.Contains(lines, want) {
				t.Errorf("missing index contract %s in %v", want, lines)
			}
		}
		// Four original ordinary/partial indexes plus two new ones. Indexes
		// backing constraints, including the PK, are described separately.
		if len(lines) != 6 {
			t.Fatalf("unexpected non-constraint history indexes: %v", lines)
		}
		if _, err := db.ExecContext(t.Context(), `DROP INDEX idx_task_history_status_id`); err != nil {
			t.Fatal(err)
		}
		if err := up2026100903TaskHistoryIndexes(t.Context(), db); err == nil {
			t.Fatal("partial index migration accepted")
		}
		if exists, err := indexExists(t.Context(), db, "idx_task_history_status_id"); err != nil || exists {
			t.Fatalf("partial state was modified: %v, %v", exists, err)
		}
	})
}

func TestPostgresKeyCollationUpgrade(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "upgrade"
		if blocked {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			testPostgresMigrationDialect(t, func(t *testing.T, db *bun.DB) {
				migrateThrough(t, db, "2026100804")
				bucket := insertBaselineTestBucket(t, db, "Mixed-ä-中")
				if _, err := db.ExecContext(t.Context(), `INSERT INTO objects (bucket_id, key, created_at, updated_at) VALUES (?, 'Mixed-ä-中', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucket); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(t.Context(), `CREATE COLLATION freeze_icu (provider = icu, locale = 'en-US', deterministic = true)`); err != nil {
					t.Fatal(err)
				}
				for _, column := range []struct{ table, name string }{{"buckets", "name"}, {"objects", "key"}, {"object_versions", "key"}, {"multipart_uploads", "key"}, {"object_deletions", "key"}} {
					if _, err := db.ExecContext(t.Context(), `ALTER TABLE `+column.table+` ALTER COLUMN `+column.name+` TYPE text COLLATE freeze_icu`); err != nil {
						t.Fatal(err)
					}
				}
				if blocked {
					if _, err := db.ExecContext(t.Context(), `CREATE VIEW freeze_blocker AS SELECT key FROM object_deletions`); err != nil {
						t.Fatal(err)
					}
				}
				before := freezePostgresForeignKeys(t, db)
				err := up2026100901KeyCollation(t.Context(), db)
				if blocked && err == nil || !blocked && err != nil {
					t.Fatalf("blocked=%v, migration error=%v", blocked, err)
				}
				if !slices.Equal(before, freezePostgresForeignKeys(t, db)) {
					t.Fatal("foreign-key definitions or validation changed")
				}
				var collations []string
				if err := db.NewRaw(`SELECT c.relname || '.' || a.attname || '=' || co.collname
					FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_collation co ON co.oid = a.attcollation
					WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
					AND ((c.relname = 'buckets' AND a.attname = 'name') OR (c.relname IN ('objects','object_versions','multipart_uploads','object_deletions') AND a.attname = 'key'))
					ORDER BY c.relname`).Scan(t.Context(), &collations); err != nil {
					t.Fatal(err)
				}
				want := "=C"
				if blocked {
					want = "=freeze_icu"
				}
				if len(collations) != 5 {
					t.Fatalf("missing key columns: %v", collations)
				}
				for _, value := range collations {
					if !strings.HasSuffix(value, want) {
						t.Errorf("collation %s, want %s", value, want)
					}
				}
				if companion, err := indexExists(t.Context(), db, "idx_objects_bucket_key_c"); err != nil || companion != blocked {
					t.Fatalf("companion=%v, err=%v", companion, err)
				}
				var key string
				if err := db.NewRaw(`SELECT key FROM objects WHERE bucket_id = ?`, bucket).Scan(t.Context(), &key); err != nil || key != "Mixed-ä-中" {
					t.Fatalf("object key changed: %s, %v", key, err)
				}
				if !blocked {
					if err := up2026100901KeyCollation(t.Context(), db); err != nil {
						t.Fatalf("repeat: %v", err)
					}
					if _, err := db.ExecContext(t.Context(), `ALTER TABLE buckets ALTER COLUMN name TYPE text COLLATE freeze_icu`); err != nil {
						t.Fatal(err)
					}
					if err := up2026100901KeyCollation(t.Context(), db); err == nil {
						t.Fatal("partial collation state accepted")
					}
				}
			})
		})
	}
}

func TestHistoryIndexMigrationRollsBackBothIndexes(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateThrough(t, db, "2026100902")
		if _, err := db.ExecContext(t.Context(), `CREATE TABLE idx_task_history_current_failed_subject (id INTEGER)`); err != nil {
			t.Fatal(err)
		}
		if err := up2026100903TaskHistoryIndexes(t.Context(), db); err == nil {
			t.Fatal("index name collision accepted")
		}
		for _, name := range []string{"idx_task_history_status_id", "idx_task_history_current_failed_subject"} {
			if exists, err := indexExists(t.Context(), db, name); err != nil || exists {
				t.Errorf("failed migration left index %s: %v, %v", name, exists, err)
			}
		}
	})
}

func freezePostgresForeignKeys(t *testing.T, db *bun.DB) []string {
	t.Helper()
	var definitions []string
	if err := db.NewRaw(`SELECT conrelid::regclass::text || '|' || conname || '|' || pg_get_constraintdef(oid) || '|' || convalidated::text
		FROM pg_constraint WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND contype = 'f'
		ORDER BY conrelid::regclass::text, conname`).Scan(t.Context(), &definitions); err != nil {
		t.Fatal(err)
	}
	return definitions
}
