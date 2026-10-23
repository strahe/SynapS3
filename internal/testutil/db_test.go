package testutil

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestNewTestDBCreatesIsolatedDatabases(t *testing.T) {
	assertTestDBsAreIsolated(t, NewTestDB)
}

func TestNewTestDBKeepsCatalogAndSequenceChangesIsolated(t *testing.T) {
	var firstName string
	t.Run("mutate one clone", func(t *testing.T) {
		first := NewTestDB(t)
		second := NewTestDB(t)
		if err := first.QueryRowContext(t.Context(), "SELECT current_database()").Scan(&firstName); err != nil {
			t.Fatal(err)
		}
		connection, err := first.DB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := connection.Close(); err != nil {
				t.Error(err)
			}
		}()
		if _, err := connection.ExecContext(t.Context(), "CREATE TABLE clone_probe (id bigserial PRIMARY KEY, value text CHECK (value = 'allowed'))"); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.ExecContext(t.Context(), "INSERT INTO clone_probe (value) VALUES ('allowed')"); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.ExecContext(t.Context(), "ALTER TABLE clone_probe DROP CONSTRAINT clone_probe_value_check"); err != nil {
			t.Fatal(err)
		}
		var secondName string
		var probeExists bool
		if err := second.QueryRowContext(t.Context(), "SELECT current_database(), to_regclass('clone_probe') IS NOT NULL OR to_regclass('clone_probe_id_seq') IS NOT NULL").Scan(&secondName, &probeExists); err != nil {
			t.Fatal(err)
		}
		if secondName == firstName || probeExists {
			t.Fatalf("peer clone contains another catalog: same database=%v, probe exists=%v", secondName == firstName, probeExists)
		}
	})
	fresh := NewTestDB(t)
	var name string
	var probeExists bool
	if err := fresh.QueryRowContext(t.Context(), "SELECT current_database(), to_regclass('clone_probe') IS NOT NULL OR to_regclass('clone_probe_id_seq') IS NOT NULL").Scan(&name, &probeExists); err != nil {
		t.Fatal(err)
	}
	if name == firstName || probeExists {
		t.Fatalf("fresh clone contains an earlier catalog: same database=%v, probe exists=%v", name == firstName, probeExists)
	}
}

func assertTestDBsAreIsolated(t *testing.T, newDB func(*testing.T) *bun.DB) {
	t.Helper()
	ctx := context.Background()
	first := newDB(t)
	second := newDB(t)

	bucket := &model.Bucket{Name: "isolated-db-bucket", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
	if _, err := first.NewInsert().Model(bucket).Exec(ctx); err != nil {
		t.Fatalf("insert bucket into first db: %v", err)
	}

	count, err := second.NewSelect().Model((*model.Bucket)(nil)).Where("name = ?", bucket.Name).Count(ctx)
	if err != nil {
		t.Fatalf("count bucket in second db: %v", err)
	}
	if count != 0 {
		t.Fatalf("second test db bucket count = %d, want 0", count)
	}
}
