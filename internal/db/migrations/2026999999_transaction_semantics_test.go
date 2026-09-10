package migrations

import (
	"context"
	"errors"
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

func TestMigrationDownFailureRollsBackAndKeepsMarker(t *testing.T) {
	testMigrationDialects(t, testMigrationDownFailureRollsBackAndKeepsMarker)
}

func testMigrationDownFailureRollsBackAndKeepsMarker(t *testing.T, db *bun.DB) {
	ctx := context.Background()
	registry := migrate.NewMigrations()
	registry.MustRegister(
		transactionalMigration(func(ctx context.Context, db bun.IDB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE transaction_failure_probe (id INTEGER PRIMARY KEY)")
			return err
		}),
		transactionalMigration(func(ctx context.Context, db bun.IDB) error {
			if _, err := db.ExecContext(ctx, "DROP TABLE transaction_failure_probe"); err != nil {
				return err
			}
			return errors.New("injected down failure")
		}),
	)
	migrator := newMigrator(db, registry)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("initialize migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := migrator.Rollback(ctx); err == nil {
		t.Fatal("rollback succeeded, want injected failure")
	}
	if exists, err := tableExists(ctx, db, "transaction_failure_probe"); err != nil {
		t.Fatalf("inspect rolled back table: %v", err)
	} else if !exists {
		t.Fatal("failed down migration committed DDL")
	}
	assertAppliedMigrationCount(t, ctx, migrator, 1)
}

func TestMigrationRepairsMarkerForCompletePostState(t *testing.T) {
	testMigrationDialects(t, testMigrationRepairsMarkerForCompletePostState)
}

func TestInitialBaselineRepairsMissingMarkerOnlyForCompletePostState(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		if err := runMigrationBody(ctx, db, up2026090101InitialSchema); err != nil {
			t.Fatalf("simulate committed baseline without marker: %v", err)
		}
		migrator := NewMigrator(db)
		if err := migrator.Init(ctx); err != nil {
			t.Fatalf("initialize migrator: %v", err)
		}
		if err := ValidateTarget(ctx, db); err != nil {
			t.Fatalf("validate complete baseline without marker: %v", err)
		}
		if _, err := migrator.Migrate(ctx); err != nil {
			t.Fatalf("repair baseline marker: %v", err)
		}
		assertAppliedMigrationCount(t, ctx, migrator, 1)
	})
}

func TestInitialBaselineRejectsPartialPostStateWithEmptyMarker(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		migrator := NewMigrator(db)
		if err := migrator.Init(ctx); err != nil {
			t.Fatalf("initialize migrator: %v", err)
		}
		if _, err := db.ExecContext(ctx, "CREATE TABLE tasks (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatalf("create partial baseline: %v", err)
		}
		if err := ValidateTarget(ctx, db); !errors.Is(err, ErrIncompatibleDatabase) {
			t.Fatalf("validate partial baseline = %v, want ErrIncompatibleDatabase", err)
		}
		assertAppliedMigrationCount(t, ctx, migrator, 0)
	})
}

func testMigrationRepairsMarkerForCompletePostState(t *testing.T, db *bun.DB) {
	ctx := context.Background()
	executions := 0
	body := migrationBody(func(ctx context.Context, db bun.IDB) error {
		exists, err := tableExists(ctx, db, "transaction_marker_probe")
		if err != nil || exists {
			return err
		}
		executions++
		_, err = db.ExecContext(ctx, "CREATE TABLE transaction_marker_probe (id INTEGER PRIMARY KEY)")
		return err
	})
	if err := runMigrationBody(ctx, db, body); err != nil {
		t.Fatalf("simulate committed DDL without marker: %v", err)
	}

	registry := migrate.NewMigrations()
	registry.MustRegister(
		transactionalMigration(body),
		transactionalMigration(func(context.Context, bun.IDB) error { return nil }),
	)
	migrator := newMigrator(db, registry)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("initialize migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("repair migration marker: %v", err)
	}
	if executions != 1 {
		t.Fatalf("migration body executed %d times, want once", executions)
	}
	assertAppliedMigrationCount(t, ctx, migrator, 1)
}

func testMigrationDialects(t *testing.T, test func(*testing.T, *bun.DB)) {
	t.Helper()
	t.Run("SQLite", func(t *testing.T) {
		test(t, newSQLiteMigrationDB(t, strings.ReplaceAll(t.Name(), "/", "_")))
	})
	t.Run("Postgres", func(t *testing.T) {
		test(t, newPostgresMigrationDB(t))
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
