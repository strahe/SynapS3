package migrations

import (
	"testing"

	"github.com/uptrace/bun"
)

func TestBucketProviderSelectionMigrationPreservesBucketsAndReplays(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		level := 0
		for _, migration := range Migrations.Sorted() {
			if migration.Name == "2026100502" {
				break
			}
			level++
		}
		if level == len(Migrations.Sorted()) {
			t.Fatal("bucket preference migration is not registered")
		}
		migrateToLevel(t, db, level)
		bucketID := insertBaselineTestBucket(t, db, "existing-preference")
		dataSetID := insertBaselineTestDataSet(t, db, bucketID, "101", 0, 1, true)
		if err := runMigrationBody(ctx, db, up2026100502BucketProviderSelection); err != nil {
			t.Fatal(err)
		}
		var strategy string
		if err := db.NewRaw("SELECT provider_selection_strategy FROM buckets WHERE id = ?", bucketID).Scan(ctx, &strategy); err != nil || strategy != "distribution" {
			t.Fatalf("existing preference = %q, err=%v", strategy, err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE buckets SET provider_selection_strategy = 'speed' WHERE id = ?", bucketID); err != nil {
			t.Fatal(err)
		}
		if _, err := NewMigrator(db).Migrate(ctx); err != nil {
			t.Fatalf("replay: %v", err)
		}
		if err := db.NewRaw("SELECT provider_selection_strategy FROM buckets WHERE id = ?", bucketID).Scan(ctx, &strategy); err != nil || strategy != "speed" {
			t.Fatalf("replay preference = %q, err=%v", strategy, err)
		}
		var count int
		if err := db.NewRaw("SELECT COUNT(*) FROM storage_data_sets WHERE id = ? AND bucket_id = ? AND provider_id = '101' AND is_current", dataSetID, bucketID).Scan(ctx, &count); err != nil || count != 1 {
			t.Fatalf("existing binding changed: count=%d err=%v", count, err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE buckets SET provider_selection_strategy = 'unknown' WHERE id = ?", bucketID); err == nil {
			t.Fatal("invalid preference was accepted")
		}
	})
}
