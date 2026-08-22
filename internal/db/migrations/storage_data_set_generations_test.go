package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	_ "modernc.org/sqlite"
)

func TestStorageDataSetGenerationsMigrationPreservesExistingSlots(t *testing.T) {
	ctx := context.Background()
	db := newGenerationsTestDB(t, "generations_preserve")
	seedLegacyDataSet(t, db, 1, 1, 0, "101")

	if err := up2026082101StorageDataSetGenerations(ctx, db); err != nil {
		t.Fatalf("up migration: %v", err)
	}

	for _, column := range []string{"is_current", "generation"} {
		if !sqliteColumnExists(t, db, "storage_data_sets", column) {
			t.Fatalf("storage_data_sets.%s column missing", column)
		}
	}
	var isCurrent bool
	var generation int
	if err := db.QueryRow("SELECT is_current, generation FROM storage_data_sets WHERE id = 1").
		Scan(&isCurrent, &generation); err != nil {
		t.Fatalf("select migrated data set: %v", err)
	}
	if !isCurrent || generation != 1 {
		t.Fatalf("migrated data set is_current=%v generation=%d, want true/1", isCurrent, generation)
	}

	for _, index := range []string{
		"idx_storage_data_sets_bucket_copy_index",
		"idx_storage_data_sets_bucket_provider",
		"idx_storage_upload_copies_upload_index",
	} {
		if sqliteIndexExists(t, db, index) {
			t.Fatalf("index %s should have been replaced", index)
		}
	}
	for _, table := range []string{"storage_replacements", "storage_replacement_items"} {
		if !sqliteTableExists(t, db, table) {
			t.Fatalf("table %s missing", table)
		}
	}
	for _, column := range []string{
		"client_request_id", "failure_reason", "abandoned_termination_tx_hash",
		"abandoned_termination_epoch", "abandoned_termination_observed_at",
	} {
		if !sqliteColumnExists(t, db, "storage_replacements", column) {
			t.Fatalf("storage_replacements.%s column missing", column)
		}
	}
	if !sqliteIndexExists(t, db, "idx_storage_replacements_bucket_request") {
		t.Fatal("bucket-scoped replacement idempotency index missing")
	}
	for _, index := range []string{
		"idx_storage_uploads_bucket_id",
		"idx_storage_replacement_items_upload_id",
	} {
		if !sqliteIndexExists(t, db, index) {
			t.Fatalf("persistent query index %s missing", index)
		}
	}
}

func TestStorageDataSetGenerationsMigrationEnforcesSlotInvariants(t *testing.T) {
	ctx := context.Background()
	db := newGenerationsTestDB(t, "generations_invariants")
	seedLegacyDataSet(t, db, 1, 1, 0, "101")
	if err := up2026082101StorageDataSetGenerations(ctx, db); err != nil {
		t.Fatalf("up migration: %v", err)
	}

	// A slot keeps exactly one writable generation...
	if err := insertDataSet(db, 2, 1, 0, "202", true, 2); err == nil {
		t.Fatal("second current generation for one slot was accepted, want unique violation")
	}
	// ...but may retain historical ones.
	if err := insertDataSet(db, 2, 1, 0, "202", false, 2); err != nil {
		t.Fatalf("historical generation rejected: %v", err)
	}
	if err := insertDataSet(db, 3, 1, 0, "303", false, 2); err == nil {
		t.Fatal("duplicate generation number for one slot was accepted, want unique violation")
	}

	// A provider that only holds a historical generation can be selected again,
	// which is what lets an operator reuse a previously used provider.
	mustExecMigrationTest(t, db, "UPDATE storage_data_sets SET is_current = 0 WHERE id = 1")
	if err := insertDataSet(db, 4, 1, 0, "101", true, 3); err != nil {
		t.Fatalf("reusing a historical provider rejected: %v", err)
	}
	// Provider 101 now serves slot 0, so it cannot also take slot 1.
	if err := insertDataSet(db, 5, 1, 1, "101", true, 1); err == nil {
		t.Fatal("provider bound to two current slots was accepted, want unique violation")
	}
}

func TestStorageDataSetGenerationsMigrationClosesUnboundCopyHole(t *testing.T) {
	ctx := context.Background()
	db := newGenerationsTestDB(t, "generations_copy_hole")
	seedLegacyDataSet(t, db, 1, 1, 0, "101")
	mustExecMigrationTest(t, db, "INSERT INTO storage_uploads (id, bucket_id) VALUES (1, 1)")
	if err := up2026082101StorageDataSetGenerations(ctx, db); err != nil {
		t.Fatalf("up migration: %v", err)
	}

	mustExecMigrationTest(t, db,
		"INSERT INTO storage_upload_copies (id, upload_id, copy_index, storage_data_set_id) VALUES (1, 1, 0, 1)")
	if err := insertUploadCopy(db, 2, 1, 0, sql.NullInt64{Int64: 1, Valid: true}); err == nil {
		t.Fatal("duplicate copy for one data set was accepted, want unique violation")
	}

	// NULL compares distinct in a unique index, so unbound copies need their own
	// partial index or a slot could accumulate duplicates.
	mustExecMigrationTest(t, db,
		"INSERT INTO storage_upload_copies (id, upload_id, copy_index, storage_data_set_id) VALUES (3, 1, 1, NULL)")
	if err := insertUploadCopy(db, 4, 1, 1, sql.NullInt64{}); err == nil {
		t.Fatal("duplicate unbound copy for one slot was accepted, want unique violation")
	}
}

func TestStorageDataSetGenerationsMigrationLimitsActiveReplacements(t *testing.T) {
	ctx := context.Background()
	db := newGenerationsTestDB(t, "generations_active_replacement")
	seedLegacyDataSet(t, db, 1, 1, 0, "101")
	if err := up2026082101StorageDataSetGenerations(ctx, db); err != nil {
		t.Fatalf("up migration: %v", err)
	}
	mustExecMigrationTest(t, db, "UPDATE storage_data_sets SET is_current = 0 WHERE id = 1")
	if err := insertDataSet(db, 2, 1, 0, "202", true, 2); err != nil {
		t.Fatalf("seed target generation: %v", err)
	}

	if err := insertReplacement(db, 1, 1, 2, "migrating"); err != nil {
		t.Fatalf("first replacement rejected: %v", err)
	}
	if err := insertReplacement(db, 2, 1, 2, "preparing_target"); err == nil {
		t.Fatal("second active replacement for one source was accepted, want unique violation")
	}
	// A terminal replacement releases its source for a later confirmation.
	mustExecMigrationTest(t, db, "UPDATE storage_replacements SET status = 'superseded' WHERE id = 1")
	if err := insertReplacement(db, 2, 1, 2, "preparing_target"); err != nil {
		t.Fatalf("replacement after supersede rejected: %v", err)
	}
	if err := insertReplacement(db, 3, 1, 1, "preparing_target"); err == nil {
		t.Fatal("replacement onto itself was accepted, want check violation")
	}
}

func TestStorageDataSetGenerationsMigrationRejectsIncompatibleData(t *testing.T) {
	ctx := context.Background()
	db := newGenerationsTestDB(t, "generations_incompatible")
	seedLegacyDataSet(t, db, 1, 1, 0, "101")
	mustExecMigrationTest(t, db, "INSERT INTO storage_uploads (id, bucket_id) VALUES (1, 1)")
	// The old schema never enforced this, so a database damaged by an earlier
	// bug must fail loudly instead of part way through the index rebuild.
	mustExecMigrationTest(t, db, "DROP INDEX idx_storage_upload_copies_upload_index")
	mustExecMigrationTest(t, db,
		"INSERT INTO storage_upload_copies (id, upload_id, copy_index, storage_data_set_id) VALUES (1, 1, 0, 1), (2, 1, 1, 1)")

	err := up2026082101StorageDataSetGenerations(ctx, db)
	if err == nil {
		t.Fatal("migration accepted duplicate copies for one data set, want failure")
	}
	if !strings.Contains(err.Error(), "sharing one data set") {
		t.Fatalf("error = %v, want it to name the offending invariant", err)
	}
	if sqliteColumnExists(t, db, "storage_data_sets", "is_current") {
		t.Fatal("failed migration left is_current behind, want a rolled back transaction")
	}
}

func TestStorageDataSetGenerationsMigrationRejectsPartialSchema(t *testing.T) {
	ctx := context.Background()
	db := newGenerationsTestDB(t, "generations_partial_schema")
	mustExecMigrationTest(t, db, "ALTER TABLE storage_data_sets ADD COLUMN generation INTEGER NOT NULL DEFAULT 1")

	err := up2026082101StorageDataSetGenerations(ctx, db)
	if err == nil {
		t.Fatal("migration accepted a partial schema")
	}
	if !strings.Contains(err.Error(), "partial schema state") {
		t.Fatalf("partial schema error = %v", err)
	}
	if sqliteColumnExists(t, db, "storage_data_sets", "is_current") {
		t.Fatal("partial schema failure continued applying DDL")
	}
}

func TestStorageDataSetGenerationsMigrationDown(t *testing.T) {
	ctx := context.Background()
	db := newGenerationsTestDB(t, "generations_down")
	seedLegacyDataSet(t, db, 1, 1, 0, "101")
	if err := up2026082101StorageDataSetGenerations(ctx, db); err != nil {
		t.Fatalf("up migration: %v", err)
	}
	if err := down2026082101StorageDataSetGenerations(ctx, db); err != nil {
		t.Fatalf("down migration: %v", err)
	}
	for _, column := range []string{"is_current", "generation"} {
		if sqliteColumnExists(t, db, "storage_data_sets", column) {
			t.Fatalf("storage_data_sets.%s survived rollback", column)
		}
	}
	for _, index := range []string{
		"idx_storage_data_sets_bucket_copy_index",
		"idx_storage_data_sets_bucket_provider",
		"idx_storage_upload_copies_upload_index",
	} {
		if !sqliteIndexExists(t, db, index) {
			t.Fatalf("index %s was not restored", index)
		}
	}
	if sqliteTableExists(t, db, "storage_replacements") {
		t.Fatal("storage_replacements survived rollback")
	}
	if sqliteIndexExists(t, db, "idx_storage_uploads_bucket_id") {
		t.Fatal("idx_storage_uploads_bucket_id survived rollback")
	}
}

func TestStorageDataSetGenerationsMigrationDownDiagnosesConflicts(t *testing.T) {
	tests := []struct {
		name  string
		want  string
		setup func(*testing.T, *bun.DB)
	}{
		{
			name: "bucket copy index",
			want: "duplicate (bucket_id, copy_index)",
			setup: func(t *testing.T, db *bun.DB) {
				if err := insertDataSet(db, 2, 1, 0, "202", false, 2); err != nil {
					t.Fatalf("seed second generation: %v", err)
				}
			},
		},
		{
			name: "bucket provider",
			want: "duplicate (bucket_id, provider_id)",
			setup: func(t *testing.T, db *bun.DB) {
				if err := insertDataSet(db, 2, 1, 1, "101", false, 1); err != nil {
					t.Fatalf("seed reused provider: %v", err)
				}
			},
		},
		{
			name: "upload copy index",
			want: "duplicate (upload_id, copy_index)",
			setup: func(t *testing.T, db *bun.DB) {
				if err := insertDataSet(db, 2, 1, 1, "202", true, 1); err != nil {
					t.Fatalf("seed second data set: %v", err)
				}
				mustExecMigrationTest(t, db, "INSERT INTO storage_uploads (id, bucket_id) VALUES (1, 1)")
				mustExecMigrationTest(t, db, `INSERT INTO storage_upload_copies
					(id, upload_id, copy_index, storage_data_set_id)
				 VALUES (1, 1, 0, 1), (2, 1, 0, 2)`)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := newGenerationsTestDB(t, "generations_down_"+strings.ReplaceAll(tt.name, " ", "_"))
			seedLegacyDataSet(t, db, 1, 1, 0, "101")
			if err := up2026082101StorageDataSetGenerations(ctx, db); err != nil {
				t.Fatalf("up migration: %v", err)
			}
			tt.setup(t, db)

			err := down2026082101StorageDataSetGenerations(ctx, db)
			if err == nil {
				t.Fatal("rollback accepted incompatible data, want failure")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("rollback error = %v, want %q", err, tt.want)
			}
			if !sqliteColumnExists(t, db, "storage_data_sets", "generation") {
				t.Fatal("failed rollback dropped generation")
			}
		})
	}
}

func newGenerationsTestDB(t *testing.T, name string) *bun.DB {
	t.Helper()
	sqldb, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	// Stand-ins carrying only what this migration reads or rewrites.
	schema := []string{
		`CREATE TABLE buckets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE storage_uploads (id INTEGER PRIMARY KEY AUTOINCREMENT, bucket_id INTEGER NOT NULL REFERENCES buckets (id))`,
		`CREATE TABLE storage_data_sets (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL REFERENCES buckets (id),
			provider_id TEXT NOT NULL,
			copy_index INTEGER NOT NULL,
			status TEXT NOT NULL DEFAULT 'ready'
		)`,
		`CREATE TABLE storage_upload_copies (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			upload_id INTEGER NOT NULL REFERENCES storage_uploads (id),
			copy_index INTEGER NOT NULL,
			storage_data_set_id INTEGER REFERENCES storage_data_sets (id)
		)`,
		`CREATE UNIQUE INDEX idx_storage_data_sets_bucket_copy_index ON storage_data_sets (bucket_id, copy_index)`,
		`CREATE UNIQUE INDEX idx_storage_data_sets_bucket_provider ON storage_data_sets (bucket_id, provider_id)`,
		`CREATE UNIQUE INDEX idx_storage_upload_copies_upload_index ON storage_upload_copies (upload_id, copy_index)`,
		`INSERT INTO buckets (id, name) VALUES (1, 'replacement-bucket')`,
	}
	for _, query := range schema {
		mustExecMigrationTest(t, db, query)
	}
	return db
}

func seedLegacyDataSet(t *testing.T, db *bun.DB, id, bucketID int64, copyIndex int, providerID string) {
	t.Helper()
	mustExecMigrationTest(t, db, fmt.Sprintf(
		"INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index) VALUES (%d, %d, '%s', %d)",
		id, bucketID, providerID, copyIndex,
	))
}

func insertDataSet(db *bun.DB, id, bucketID int64, copyIndex int, providerID string, isCurrent bool, generation int) error {
	_, err := db.Exec(
		"INSERT INTO storage_data_sets (id, bucket_id, provider_id, copy_index, is_current, generation) VALUES (?, ?, ?, ?, ?, ?)",
		id, bucketID, providerID, copyIndex, isCurrent, generation,
	)
	return err
}

func insertUploadCopy(db *bun.DB, id, uploadID int64, copyIndex int, dataSetID sql.NullInt64) error {
	_, err := db.Exec(
		"INSERT INTO storage_upload_copies (id, upload_id, copy_index, storage_data_set_id) VALUES (?, ?, ?, ?)",
		id, uploadID, copyIndex, dataSetID,
	)
	return err
}

func insertReplacement(db *bun.DB, id, sourceDataSetID, targetDataSetID int64, status string) error {
	_, err := db.Exec(
		`INSERT INTO storage_replacements
			(id, bucket_id, copy_index, source_data_set_id, target_data_set_id, selection_mode, client_request_id, status)
		 VALUES (?, 1, 0, ?, ?, 'automatic', ?, ?)`,
		id, sourceDataSetID, targetDataSetID, fmt.Sprintf("migration-request-%d", id), status,
	)
	return err
}

func mustExecMigrationTest(t *testing.T, db *bun.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}
