package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"

	_ "modernc.org/sqlite"
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
// registry and its schema what they build, and without the marker table it
// must hold no application tables.
func ValidateTarget(ctx context.Context, db bun.IDB) error {
	return validateTarget(ctx, db, Migrations)
}

// ValidateCurrentSchema requires exactly the schema the registered migrations
// build, so the process never runs against a schema it was not built for.
func ValidateCurrentSchema(ctx context.Context, db bun.IDB) error {
	return validateSchema(ctx, db, Migrations, len(Migrations.Sorted()))
}

func validateTarget(ctx context.Context, db bun.IDB, registry *migrate.Migrations) error {
	if !validMigrationRegistry(registry) {
		return incompatibleDatabaseError()
	}
	// The markers and the schema come from one snapshot, so a runner migrating
	// at the same time cannot make them disagree.
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
		err := validateSchema(ctx, db, registry, len(names))
		// Bun records a migration only after it commits, so a crash in between
		// leaves the next migration's schema; that migration reruns as a no-op.
		if errors.Is(err, ErrIncompatibleDatabase) && len(names) < len(registry.Sorted()) &&
			validateSchema(ctx, db, registry, len(names)+1) == nil {
			return nil
		}
		return err
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

// validateSchema requires exactly the schema the first level registered
// migrations build. The expected schema comes from running them on a private
// in-memory SQLite database; PostgreSQL is compared on what the two dialects
// share.
func validateSchema(ctx context.Context, db bun.IDB, registry *migrate.Migrations, level int) error {
	portable := db.Dialect().Name() == dialect.PG
	got, err := describeSchema(ctx, db, portable)
	if err != nil {
		return fmt.Errorf("describing database schema: %w", err)
	}
	want, err := referenceSchema(ctx, registry, level, portable)
	if err != nil {
		return err
	}
	if !slices.Equal(got, want) {
		return incompatibleSchemaError(want, got)
	}
	return nil
}

// referenceSchema describes the schema the first level registered migrations
// build on a new private in-memory SQLite database.
func referenceSchema(ctx context.Context, registry *migrate.Migrations, level int, portable bool) ([]string, error) {
	if level == 0 {
		return nil, nil
	}
	sqldb, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("opening schema reference: %w", err)
	}
	sqldb.SetMaxOpenConns(1)
	reference := bun.NewDB(sqldb, sqlitedialect.New())
	defer func() { _ = reference.Close() }()
	applied := migrate.NewMigrations()
	for _, migration := range registry.Sorted()[:level] {
		applied.Add(migration)
	}
	migrator := newMigrator(reference, applied)
	if err := migrator.Init(ctx); err != nil {
		return nil, fmt.Errorf("building schema reference: %w", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("building schema reference: %w", err)
	}
	schema, err := describeSchema(ctx, reference, portable)
	if err != nil {
		return nil, fmt.Errorf("describing schema reference: %w", err)
	}
	return schema, nil
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
// the schema the baseline builds, which lets it repair a marker Bun lost after
// its DDL committed.
func initialSchemaPostStateComplete(ctx context.Context, db bun.IDB) (bool, error) {
	count, err := applicationTableCount(ctx, db)
	if err != nil || count == 0 {
		return false, err
	}
	err = validateSchema(ctx, db, Migrations, 1)
	if errors.Is(err, ErrIncompatibleDatabase) {
		return false, nil
	}
	return err == nil, err
}

func incompatibleDatabaseError() error {
	return fmt.Errorf("%w; keep the existing database as a read-only backup and configure a new empty database", ErrIncompatibleDatabase)
}

func incompatibleSchemaError(want, got []string) error {
	return fmt.Errorf("%w: the schema differs from what its recorded migrations build (missing: %s; unexpected: %s); keep the existing database as a read-only backup and configure a new empty database",
		ErrIncompatibleDatabase, schemaLinesSummary(want, got), schemaLinesSummary(got, want))
}

// schemaLinesSummary names the first lines of lines that others lacks.
func schemaLinesSummary(lines, others []string) string {
	const shown = 5
	var absent []string
	for _, line := range lines {
		if !slices.Contains(others, line) {
			absent = append(absent, line)
		}
	}
	switch {
	case len(absent) == 0:
		return "none"
	case len(absent) > shown:
		return fmt.Sprintf("%s and %d more", strings.Join(absent[:shown], ", "), len(absent)-shown)
	default:
		return strings.Join(absent, ", ")
	}
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
