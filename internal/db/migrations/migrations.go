package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/migrate"
)

// Bun stores the numeric migration name and derives "initial_schema" as its
// human-readable comment from the filename.
const InitialSchemaName = "2026090101"

var ErrIncompatibleDatabase = errors.New("database contains an incompatible SynapS3 schema")

// Migrations is the global registry of database migrations.
var Migrations = migrate.NewMigrations()

type migrationBody func(context.Context, bun.IDB) error

// NewMigrator is the only supported migrator constructor. Migration markers
// are written only after the corresponding body succeeds.
func NewMigrator(db *bun.DB) *migrate.Migrator {
	return newMigrator(db, Migrations)
}

// ValidateTarget refuses, before anything is written, a database that is not
// this application's: its recorded migrations must be an ordered prefix of the
// registry, and with no recorded migration it may hold no application tables
// other than exactly the baseline's. Whether migrations build the intended
// schema is verified by tests, not here.
func ValidateTarget(ctx context.Context, db bun.IDB) error {
	return validateTarget(ctx, db, Migrations)
}

func validateTarget(ctx context.Context, db bun.IDB, registry *migrate.Migrations) error {
	if !validMigrationRegistry(registry) {
		return incompatibleDatabaseError()
	}
	// The marker table and the table count come from one snapshot, so a runner
	// migrating at the same time cannot make them disagree.
	options := &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
	return db.RunInTx(ctx, options, func(ctx context.Context, tx bun.Tx) error {
		return validateTargetSnapshot(ctx, tx, registry)
	})
}

func validateTargetSnapshot(ctx context.Context, db bun.IDB, registry *migrate.Migrations) error {
	markerExists, err := tableExists(ctx, db, "bun_migrations")
	if err != nil {
		return fmt.Errorf("checking migration metadata: %w", err)
	}
	if markerExists {
		var names []string
		if err := db.NewRaw("SELECT name FROM bun_migrations ORDER BY id").Scan(ctx, &names); err != nil {
			return fmt.Errorf("reading migration metadata: %w", err)
		}
		if !appliedMigrationPrefix(names, registry) {
			return incompatibleDatabaseError()
		}
		if len(names) > 0 {
			return nil
		}
		// Bun records the baseline only after its DDL commits, so a database
		// with no recorded migration may already hold the complete baseline.
		complete, err := initialSchemaPostStateComplete(ctx, db)
		if err != nil {
			return fmt.Errorf("checking database contents: %w", err)
		}
		if complete {
			return nil
		}
	}
	count, err := applicationTableCount(ctx, db)
	if err != nil {
		return fmt.Errorf("checking database contents: %w", err)
	}
	if count != 0 {
		return incompatibleDatabaseError()
	}
	return nil
}

func appliedMigrationPrefix(names []string, registry *migrate.Migrations) bool {
	registered := registry.Sorted()
	if len(names) > len(registered) {
		return false
	}
	for i, migration := range registered {
		if i < len(names) && names[i] != migration.Name {
			return false
		}
	}
	return true
}

func validMigrationRegistry(registry *migrate.Migrations) bool {
	registered := registry.Sorted()
	if len(registered) == 0 || registered[0].Name != InitialSchemaName {
		return false
	}
	for i := 1; i < len(registered); i++ {
		if registered[i-1].Name >= registered[i].Name {
			return false
		}
	}
	return true
}

func applicationTableCount(ctx context.Context, db bun.IDB) (int, error) {
	names, err := applicationTableNames(ctx, db)
	return len(names), err
}

func applicationTableNames(ctx context.Context, db bun.IDB) ([]string, error) {
	query := `SELECT name FROM sqlite_schema
		WHERE type = 'table'
		  AND name NOT LIKE 'sqlite_%'
		  AND name NOT IN ('bun_migrations', 'bun_migration_locks')
		ORDER BY name`
	if db.Dialect().Name() == dialect.PG {
		query = `SELECT table_name FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_type = 'BASE TABLE'
			  AND table_name NOT IN ('bun_migrations', 'bun_migration_locks')
			ORDER BY table_name`
	}
	var names []string
	if err := db.NewRaw(query).Scan(ctx, &names); err != nil {
		return nil, err
	}
	return names, nil
}

// initialSchemaPostStateComplete reports whether the database holds exactly
// the tables the baseline builds, which lets it repair a marker Bun lost after
// its DDL committed.
func initialSchemaPostStateComplete(ctx context.Context, db bun.IDB) (bool, error) {
	names, err := applicationTableNames(ctx, db)
	if err != nil || len(names) == 0 {
		return false, err
	}
	slices.Sort(names)
	return slices.Equal(names, initialSchemaTables2026090101), nil
}

func incompatibleDatabaseError() error {
	return fmt.Errorf("%w; keep the existing database as a read-only backup and configure a new empty database", ErrIncompatibleDatabase)
}

func newMigrator(db *bun.DB, registry *migrate.Migrations) *migrate.Migrator {
	return migrate.NewMigrator(db, registry, migrate.WithMarkAppliedOnSuccess(true))
}

// transactionalMigration keeps every migration body atomic. The frozen
// baseline separately recognizes its complete post-state so Bun can repair a
// marker lost after the DDL transaction committed.
func transactionalMigration(body migrationBody) migrate.MigrationFunc {
	return func(ctx context.Context, db *bun.DB) error {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return body(ctx, tx)
		})
	}
}

func tableExists(ctx context.Context, db bun.IDB, table string) (bool, error) {
	query := `SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?`
	if db.Dialect().Name() == dialect.PG {
		query = `SELECT COUNT(*) FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = ?`
	}
	return queryExists(ctx, db, query, table)
}

func columnExists(ctx context.Context, db bun.IDB, table, column string) (bool, error) {
	if db.Dialect().Name() == dialect.PG {
		return queryExists(ctx, db, `SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?`, table, column)
	}
	return queryExists(ctx, db, "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?", table, column)
}

func indexExists(ctx context.Context, db bun.IDB, name string) (bool, error) {
	query := `SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name = ?`
	if db.Dialect().Name() == dialect.PG {
		query = `SELECT COUNT(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?`
	}
	return queryExists(ctx, db, query, name)
}

func queryExists(ctx context.Context, db bun.IDB, query string, args ...any) (bool, error) {
	var count int
	if err := db.NewRaw(query, args...).Scan(ctx, &count); err != nil {
		return false, err
	}
	return count > 0, nil
}
