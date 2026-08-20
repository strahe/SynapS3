package migrations

import (
	"context"
	"database/sql"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	_ "modernc.org/sqlite"
)

func TestBucketMinimumDurableCopiesMigrationAddsNullableBoundedColumn(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:bucket_minimum_durable_copies?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
		t.Fatalf("create buckets: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO buckets (id, name) VALUES (1, 'existing')`); err != nil {
		t.Fatalf("insert existing bucket: %v", err)
	}
	if err := up2026081901BucketMinimumDurableCopies(ctx, db); err != nil {
		t.Fatalf("up migration: %v", err)
	}
	if !sqliteColumnExists(t, db, "buckets", "minimum_durable_copies") {
		t.Fatal("buckets.minimum_durable_copies column missing")
	}
	var minimum sql.NullInt64
	if err := db.NewRaw(`SELECT minimum_durable_copies FROM buckets WHERE id = 1`).Scan(ctx, &minimum); err != nil {
		t.Fatalf("select existing bucket minimum: %v", err)
	}
	if minimum.Valid {
		t.Fatalf("existing bucket minimum = %d, want NULL", minimum.Int64)
	}
	for _, invalid := range []int{0, 9} {
		if _, err := db.ExecContext(ctx, `UPDATE buckets SET minimum_durable_copies = ? WHERE id = 1`, invalid); err == nil {
			t.Fatalf("minimum_durable_copies=%d accepted, want check failure", invalid)
		}
	}
	if _, err := db.ExecContext(ctx, `UPDATE buckets SET minimum_durable_copies = 2 WHERE id = 1`); err != nil {
		t.Fatalf("set valid minimum: %v", err)
	}
	if err := down2026081901BucketMinimumDurableCopies(ctx, db); err != nil {
		t.Fatalf("down migration: %v", err)
	}
	if sqliteColumnExists(t, db, "buckets", "minimum_durable_copies") {
		t.Fatal("buckets.minimum_durable_copies still exists after down migration")
	}
}
