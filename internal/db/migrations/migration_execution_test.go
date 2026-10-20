package migrations

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

func TestMigrationUpFailureRollsBackAndDoesNotMark(t *testing.T) {
	db := newPostgresMigrationDB(t)
	ctx := t.Context()
	registry, err := discoverMigrations(fstest.MapFS{
		"2026999999_failure.tx.up.sql": {Data: []byte(`CREATE TABLE transaction_failure_probe (id INTEGER PRIMARY KEY);
--bun:split
SELECT 1 / 0;`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	migrator := newMigrator(db, registry)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("initialize migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err == nil {
		t.Fatal("migration succeeded, want injected failure")
	}
	if exists, err := tableExists(ctx, db, "transaction_failure_probe"); err != nil || exists {
		t.Fatalf("failed up migration left DDL behind: exists=%t err=%v", exists, err)
	}
	assertAppliedMigrationCount(t, ctx, migrator, 0)
}

func TestDiscoveredSQLMigrationsFollowBaseline(t *testing.T) {
	db := newPostgresMigrationDB(t)
	ctx := t.Context()
	registry, err := discoverMigrations(fstest.MapFS{
		"2026090301_discovery_seed.tx.up.sql":  {Data: []byte(`INSERT INTO discovery_probe (id) VALUES (1);`)},
		"2026090201_discovery_probe.tx.up.sql": {Data: []byte(`CREATE TABLE discovery_probe (id INTEGER PRIMARY KEY, task_id BIGINT REFERENCES tasks(id));`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry.Add(Migrations.Sorted()[0])
	if err := validateTarget(ctx, db, registry); err != nil {
		t.Fatal(err)
	}
	migrator := newMigrator(db, registry)
	if err := migrator.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var names []string
	if err := db.NewRaw("SELECT name FROM bun_migrations ORDER BY id").Scan(ctx, &names); err != nil ||
		!slices.Equal(names, []string{InitialSchemaName, "2026090201", "2026090301"}) {
		t.Fatalf("applied migrations=%v err=%v", names, err)
	}
	var ids []int
	if err := db.NewRaw("SELECT id FROM discovery_probe").Scan(ctx, &ids); err != nil || !slices.Equal(ids, []int{1}) {
		t.Fatalf("discovered migration rows=%v err=%v", ids, err)
	}
}

func TestMigrationsPreserveDataOnRepeatAndMarkerRecovery(t *testing.T) {
	withMigratedDB(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		id := insertSchemaTestTask(t, db, "marker-survivor")
		signed := []byte{0x02, 0x00, 0xff}
		if _, err := db.ExecContext(ctx, `INSERT INTO wallet_operations
			(type, client_request_id, amount, status, tx_hash, signed_transaction, broadcast_attempted_at, submitted_at, created_at, updated_at)
			VALUES ('fund', 'signed-survivor', '1', 'submitted', 'tx-survivor', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, signed); err != nil {
			t.Fatal(err)
		}
		before := appliedSchemaState(t, db)
		migrator := NewMigrator(db)
		for _, repairMarker := range []bool{false, true} {
			if repairMarker {
				registered := Migrations.Sorted()
				if _, err := db.ExecContext(ctx, "DELETE FROM bun_migrations WHERE name = ?", registered[len(registered)-1].Name); err != nil {
					t.Fatalf("remove last migration marker: %v", err)
				}
			}
			if err := ValidateTarget(ctx, db); err != nil {
				t.Fatalf("validate target before repeat migration: %v", err)
			}
			group, err := migrator.Migrate(ctx)
			if err != nil {
				t.Fatalf("repeat migration: %v", err)
			}
			wantApplied := 0
			if repairMarker {
				wantApplied = 1
			}
			if len(group.Migrations) != wantApplied {
				t.Fatalf("repeat applied %d migrations, want %d", len(group.Migrations), wantApplied)
			}
			assertAppliedMigrationCount(t, ctx, migrator, len(Migrations.Sorted()))
			var key string
			if err := db.NewRaw("SELECT subject_key FROM tasks WHERE id = ?", id).Scan(ctx, &key); err != nil {
				t.Fatalf("read task after repeat migration: %v", err)
			}
			if key != "marker-survivor" {
				t.Fatalf("repeat migration changed task key to %q", key)
			}
			var persisted []byte
			if err := db.NewRaw(`SELECT signed_transaction FROM wallet_operations WHERE client_request_id = 'signed-survivor'`).Scan(ctx, &persisted); err != nil || !bytes.Equal(persisted, signed) {
				t.Fatalf("repeat migration changed signed transaction: %v", err)
			}
			if after := appliedSchemaState(t, db); !slices.Equal(before, after) {
				t.Fatalf("repeat migration changed the application schema (repairMarker=%t)", repairMarker)
			}
		}
	})
}

// A partial baseline with an empty marker is refused unchanged, both before
// startup migrates and by the baseline itself, so migration execution cannot alter it.
func TestPartialBaselineWithEmptyMarkerIsRefusedUnchanged(t *testing.T) {
	db := newPostgresMigrationDB(t)
	ctx := t.Context()
	migrator := NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("initialize migrator: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE s3_accounts (access_key TEXT PRIMARY KEY)"); err != nil {
		t.Fatalf("create partial baseline: %v", err)
	}
	if err := ValidateTarget(ctx, db); !errors.Is(err, ErrIncompatibleDatabase) {
		t.Fatalf("ValidateTarget = %v, want ErrIncompatibleDatabase", err)
	}
	if _, err := migrator.Migrate(ctx); !errors.Is(err, ErrIncompatibleDatabase) {
		t.Fatalf("Migrate = %v, want ErrIncompatibleDatabase", err)
	}
	assertAppliedMigrationCount(t, ctx, migrator, 0)
	if tables := applicationSchemaTables(t, db); !slices.Equal(tables, []string{"s3_accounts"}) {
		t.Fatalf("partial baseline tables after refusal = %v", tables)
	}
	if columns := appliedTableColumns(t, db, "s3_accounts"); len(columns) != 1 || columns[0].Name != "access_key" {
		t.Fatalf("partial baseline columns after refusal = %v", columns)
	}
}

func TestCompleteBaselineWithoutMetadataIsRefusedUnchanged(t *testing.T) {
	withMigratedDB(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		id := insertSchemaTestTask(t, db, "unowned-baseline")
		before := appliedSchemaState(t, db)
		if _, err := db.ExecContext(ctx, "DROP TABLE bun_migrations"); err != nil {
			t.Fatal(err)
		}
		if err := ValidateTarget(ctx, db); !errors.Is(err, ErrIncompatibleDatabase) {
			t.Fatalf("ValidateTarget=%v, want incompatible database", err)
		}
		if exists, err := tableExists(ctx, db, "bun_migrations"); err != nil || exists {
			t.Fatalf("metadata after refusal: exists=%t err=%v", exists, err)
		}
		var key string
		if err := db.NewRaw("SELECT subject_key FROM tasks WHERE id = ?", id).Scan(ctx, &key); err != nil || key != "unowned-baseline" {
			t.Fatalf("task after refusal=%q err=%v", key, err)
		}
		if after := appliedSchemaState(t, db); !slices.Equal(before, after) {
			t.Fatal("target validation changed the application schema")
		}
	})
}

func TestBaselineRollbackIsRefusedUnchanged(t *testing.T) {
	withMigratedDB(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		id := insertSchemaTestTask(t, db, "rollback-survivor")
		before := appliedSchemaState(t, db)
		migrator := NewMigrator(db)
		if _, err := migrator.Rollback(ctx); err == nil || !strings.Contains(err.Error(), InitialSchemaName+": down: the initial schema cannot be rolled back") {
			t.Fatalf("baseline rollback error = %v", err)
		}
		assertAppliedMigrationCount(t, ctx, migrator, len(Migrations.Sorted()))
		var key string
		if err := db.NewRaw("SELECT subject_key FROM tasks WHERE id = ?", id).Scan(ctx, &key); err != nil || key != "rollback-survivor" {
			t.Fatalf("task after rollback=%q err=%v", key, err)
		}
		if after := appliedSchemaState(t, db); !slices.Equal(before, after) {
			t.Fatal("refused rollback changed the application schema")
		}
	})
}

func assertAppliedMigrationCount(t *testing.T, ctx context.Context, migrator *migrate.Migrator, want int) {
	t.Helper()
	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read applied migrations: %v", err)
	}
	if len(applied) != want {
		t.Fatalf("applied migration count = %d, want %d", len(applied), want)
	}
}
