package migrations

import (
	"testing"
	"time"

	"github.com/uptrace/bun"
)

func TestTaskWorkStartMigrationPreservesHistoryAndReplays(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		level := 0
		for _, migration := range Migrations.Sorted() {
			if migration.Name == "2026100501" {
				break
			}
			level++
		}
		if level == len(Migrations.Sorted()) {
			t.Fatal("work start migration is not registered")
		}
		migrateToLevel(t, db, level)
		id := insertBaselineTestTask(t, db, "old-work-start")
		claimed := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
		if _, err := db.ExecContext(ctx, "UPDATE tasks SET started_at = ? WHERE id = ?", claimed, id); err != nil {
			t.Fatal(err)
		}
		if err := runMigrationBody(ctx, db, up2026100501TaskWorkStart); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := db.NewRaw("SELECT COUNT(*) FROM tasks WHERE id = ? AND started_at = ? AND work_started_at IS NULL", id, claimed).Scan(ctx, &count); err != nil || count != 1 {
			t.Fatalf("old timing was changed or guessed: count=%d err=%v", count, err)
		}
		actual := claimed.Add(30 * time.Minute)
		if _, err := db.ExecContext(ctx, "UPDATE tasks SET work_started_at = ? WHERE id = ?", actual, id); err != nil {
			t.Fatal(err)
		}
		if err := ValidateTarget(ctx, db); err != nil {
			t.Fatalf("committed schema without migration marker: %v", err)
		}
		if _, err := NewMigrator(db).Migrate(ctx); err != nil {
			t.Fatalf("replay migration: %v", err)
		}
		if err := db.NewRaw("SELECT COUNT(*) FROM tasks WHERE id = ? AND work_started_at = ?", id, actual).Scan(ctx, &count); err != nil || count != 1 {
			t.Fatalf("replay lost timing: count=%d err=%v", count, err)
		}
	})
}
