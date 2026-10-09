package migrations

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

func TestMigrationUpFailureRollsBackAndDoesNotMark(t *testing.T) {
	testMigrationDialects(t, testMigrationUpFailureRollsBackAndDoesNotMark)
}

func testMigrationUpFailureRollsBackAndDoesNotMark(t *testing.T, db *bun.DB) {
	ctx := context.Background()
	registry := migrate.NewMigrations()
	registry.MustRegister(
		transactionalMigration(func(ctx context.Context, db bun.IDB) error {
			if _, err := db.ExecContext(ctx, "CREATE TABLE transaction_failure_probe (id INTEGER PRIMARY KEY)"); err != nil {
				return err
			}
			return errors.New("injected up failure")
		}),
		transactionalMigration(func(context.Context, bun.IDB) error { return nil }),
	)
	migrator := newMigrator(db, registry)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("initialize migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err == nil {
		t.Fatal("migration succeeded, want injected failure")
	}
	if exists, err := tableExists(ctx, db, "transaction_failure_probe"); err != nil {
		t.Fatalf("inspect failed migration table: %v", err)
	} else if exists {
		t.Fatal("failed up migration left DDL behind")
	}
	assertAppliedMigrationCount(t, ctx, migrator, 0)
}

func TestMigrationsPreserveDataOnRepeatAndMarkerRecovery(t *testing.T) {
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		id := insertSchemaTestTask(t, db, "marker-survivor")
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
			if err := db.NewRaw("SELECT idempotency_key FROM tasks WHERE id = ?", id).Scan(ctx, &key); err != nil {
				t.Fatalf("read task after repeat migration: %v", err)
			}
			if key != "marker-survivor" {
				t.Fatalf("repeat migration changed task key to %q", key)
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
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
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
	})
}

func testMigrationDialects(t *testing.T, test func(*testing.T, *bun.DB)) {
	t.Helper()
	t.Run("SQLite", func(t *testing.T) {
		test(t, newSQLiteMigrationDB(t, strings.ReplaceAll(t.Name(), "/", "_")))
	})
	testPostgresMigrationDialect(t, test)
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
