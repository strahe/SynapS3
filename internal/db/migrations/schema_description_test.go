package migrations

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

func applicationSchemaTables(t *testing.T, db *bun.DB) []string {
	t.Helper()
	tables, err := applicationTableNames(t.Context(), db)
	if err != nil {
		t.Fatalf("read application schema tables: %v", err)
	}
	return tables
}

func appliedTableColumns(t *testing.T, db *bun.DB, table string) []appliedColumn {
	t.Helper()
	columns, err := tableColumns(t.Context(), db, table)
	if err != nil {
		t.Fatal(err)
	}
	return columns
}

// migrateToLevel runs the first level registered migrations that have not run.
func migrateToLevel(t *testing.T, db *bun.DB, level int) {
	t.Helper()
	registry := migrate.NewMigrations()
	for _, migration := range Migrations.Sorted()[:level] {
		registry.Add(migration)
	}
	migrator := newMigrator(db, registry)
	if err := migrator.Init(t.Context()); err != nil {
		t.Fatalf("initialize migrator: %v", err)
	}
	if _, err := migrator.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate to level %d: %v", level, err)
	}
}

// The schema check expects exactly what every migration builds, and nothing
// that changes which writes succeed.
func TestValidateCurrentSchemaRejectsDrift(t *testing.T) {
	for name, tc := range map[string]struct {
		changes []string
		names   string
	}{
		"account name index missing": {changes: []string{`DROP INDEX uq_s3_accounts_name`}, names: "uq_s3_accounts_name"},
		"account name column missing": {changes: []string{
			`DROP INDEX uq_s3_accounts_name`, `ALTER TABLE s3_accounts DROP COLUMN name`,
		}},
		"extra column":       {changes: []string{`ALTER TABLE tasks ADD COLUMN operator_note TEXT`}},
		"extra index":        {changes: []string{`CREATE INDEX operator_tasks_type ON tasks (type)`}},
		"extra unique index": {changes: []string{`CREATE UNIQUE INDEX operator_tasks_key ON tasks (type, idempotency_key, id)`}},
	} {
		t.Run(name, func(t *testing.T) {
			testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
				migrateToLevel(t, db, len(Migrations.Sorted()))
				if err := ValidateCurrentSchema(t.Context(), db); err != nil {
					t.Fatalf("ValidateCurrentSchema before the change = %v", err)
				}
				for _, change := range tc.changes {
					if _, err := db.ExecContext(t.Context(), change); err != nil {
						t.Fatalf("%s: %v", change, err)
					}
				}
				err := ValidateCurrentSchema(t.Context(), db)
				if !errors.Is(err, ErrIncompatibleDatabase) {
					t.Fatalf("ValidateCurrentSchema = %v, want ErrIncompatibleDatabase", err)
				}
				if tc.names != "" && !strings.Contains(err.Error(), tc.names) {
					t.Fatalf("ValidateCurrentSchema = %v, want it to name %s", err, tc.names)
				}
			})
		})
	}
}

// SQLite compares indexed text by the index's collation, so an account-name
// index rebuilt with NOCASE would quietly make names case-insensitive.
func TestValidateCurrentSchemaRejectsSQLiteIndexCollationDrift(t *testing.T) {
	db := newSQLiteMigrationDB(t, "index_collation_drift")
	migrateToLevel(t, db, len(Migrations.Sorted()))
	for _, change := range []string{
		`DROP INDEX uq_s3_accounts_name`,
		// Everything but the collation matches the baseline's own DDL.
		`CREATE UNIQUE INDEX "uq_s3_accounts_name" ON "s3_accounts" (name COLLATE NOCASE) WHERE (name <> '')`,
	} {
		if _, err := db.ExecContext(t.Context(), change); err != nil {
			t.Fatalf("%s: %v", change, err)
		}
	}
	err := ValidateCurrentSchema(t.Context(), db)
	if !errors.Is(err, ErrIncompatibleDatabase) || !strings.Contains(err.Error(), "uq_s3_accounts_name") {
		t.Fatalf("ValidateCurrentSchema = %v, want ErrIncompatibleDatabase naming uq_s3_accounts_name", err)
	}
}

func TestValidateCurrentSchemaRequiresEveryMigration(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := ValidateCurrentSchema(t.Context(), db); !errors.Is(err, ErrIncompatibleDatabase) {
			t.Fatalf("ValidateCurrentSchema before migrating = %v, want ErrIncompatibleDatabase", err)
		}
		migrateToLevel(t, db, len(Migrations.Sorted()))
		if err := ValidateCurrentSchema(t.Context(), db); err != nil {
			t.Fatalf("ValidateCurrentSchema after every migration = %v", err)
		}
	})
}

// A schema that is not what its recorded migrations build is refused before a
// pending migration writes to it.
func TestValidateTargetRejectsDriftBeforePendingMigrations(t *testing.T) {
	registry := migrate.NewMigrations()
	registry.Add(Migrations.Sorted()[0])
	registry.Add(migrate.Migration{Name: "2026999996"})
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		migrateToLevel(t, db, 1)
		if err := validateTarget(t.Context(), db, registry); err != nil {
			t.Fatalf("validateTarget before the change = %v", err)
		}
		if _, err := db.ExecContext(t.Context(), `DROP INDEX idx_tasks_type_id`); err != nil {
			t.Fatal(err)
		}
		if err := validateTarget(t.Context(), db, registry); !errors.Is(err, ErrIncompatibleDatabase) {
			t.Fatalf("validateTarget = %v, want ErrIncompatibleDatabase", err)
		}
	})
}

// Editing a migration in place can change a table without renaming anything.
// SQLite keeps the DDL the migration wrote, so CHECK expressions, grouping
// included, and declared column types are compared as written.
func TestValidateCurrentSchemaComparesSQLiteDDL(t *testing.T) {
	for name, tc := range map[string]struct{ table, from, to string }{
		"CHECK bound": {"tasks", "claim_generation >= 0", "claim_generation >= -1"},
		"CHECK grouping": {
			"tasks",
			"retry_count >= 0 AND (retry_limit IS NULL OR (retry_limit >= 0 AND retry_count <= retry_limit))",
			"retry_count >= 0 AND retry_limit IS NULL OR retry_limit >= 0 AND retry_count <= retry_limit",
		},
		"JSON column type": {"task_payloads", `"input_json" text`, `"input_json" blob`},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			db := newSQLiteMigrationDB(t, "ddl_drift_"+strings.ReplaceAll(name, " ", "_"))
			migrateToLevel(t, db, len(Migrations.Sorted()))
			if err := ValidateCurrentSchema(ctx, db); err != nil {
				t.Fatalf("ValidateCurrentSchema before the change = %v", err)
			}
			ddl := currentTableDDL(t, db, tc.table)
			changed := strings.Replace(ddl, tc.from, tc.to, 1)
			if changed == ddl {
				t.Fatalf("%s DDL has no %q to change", tc.table, tc.from)
			}
			if err := rebuildSQLiteTables(ctx, db, sqliteTableRebuild{
				table: tc.table,
				create: func(ctx context.Context, tx bun.Tx, name string) error {
					_, err := tx.ExecContext(ctx, "CREATE TABLE "+quoteSQLiteName(name)+" "+changed[strings.Index(changed, "("):])
					return err
				},
			}); err != nil {
				t.Fatal(err)
			}
			if err := ValidateCurrentSchema(ctx, db); !errors.Is(err, ErrIncompatibleDatabase) {
				t.Fatalf("ValidateCurrentSchema after the change = %v, want ErrIncompatibleDatabase", err)
			}
		})
	}
}
