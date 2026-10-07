package migrations

import (
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestCreationRejectionMigrationPreservesBindingsAndReplays(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		level := 0
		for _, migration := range Migrations.Sorted() {
			if migration.Name == "2026100601" {
				break
			}
			level++
		}
		if level == len(Migrations.Sorted()) {
			t.Fatal("creation rejection migration is not registered")
		}
		migrateToLevel(t, db, level)
		bucketID := insertBaselineTestBucket(t, db, "existing-creation")
		dataSetID := insertBaselineTestDataSet(t, db, bucketID, "101", 0, 1, true)
		if err := runMigrationBody(t.Context(), db, up2026100601DataSetCreationRejection); err != nil {
			t.Fatal(err)
		}
		var absent bool
		if err := db.NewRaw("SELECT creation_rejection IS NULL FROM storage_data_sets WHERE id = ?", dataSetID).Scan(t.Context(), &absent); err != nil || !absent {
			t.Fatalf("old binding refusal = absent:%v err:%v", absent, err)
		}
		payload := `{"version":99}`
		if _, err := db.ExecContext(t.Context(), "UPDATE storage_data_sets SET creation_rejection = ? WHERE id = ?", payload, dataSetID); err != nil {
			t.Fatal(err)
		}
		if _, err := NewMigrator(db).Migrate(t.Context()); err != nil {
			t.Fatal(err)
		}
		var stored string
		if err := db.NewRaw("SELECT creation_rejection FROM storage_data_sets WHERE id = ?", dataSetID).Scan(t.Context(), &stored); err != nil || stored == "" {
			t.Fatalf("replayed refusal=%q err=%v", stored, err)
		}
		wantType := "text"
		if db.Dialect().Name() == dialect.PG {
			wantType = "jsonb"
		}
		found := false
		for _, column := range appliedTableColumns(t, db, "storage_data_sets") {
			if column.Name == "creation_rejection" {
				found = true
				if column.Type != wantType || column.NotNull {
					t.Fatalf("rejection column = %#v", column)
				}
			}
		}
		if !found {
			t.Fatal("creation rejection column missing")
		}
		for _, invalid := range []string{"{", "[]", "null"} {
			if _, err := db.ExecContext(t.Context(), "UPDATE storage_data_sets SET creation_rejection = ? WHERE id = ?", invalid, dataSetID); err == nil {
				t.Fatalf("invalid rejection JSON %q accepted", invalid)
			}
		}
	})
}
