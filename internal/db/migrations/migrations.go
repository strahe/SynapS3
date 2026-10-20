package migrations

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

// Bun stores the numeric migration name and derives "initial_schema" as its
// human-readable comment from the filename.
const InitialSchemaName = "2026090101"

var ErrIncompatibleDatabase = errors.New("database contains an incompatible SynapS3 schema")

//go:embed *.sql
var sqlMigrations embed.FS

// Migrations is the global registry of database migrations.
var Migrations = func() *migrate.Migrations {
	registry, err := discoverMigrations(sqlMigrations)
	if err != nil {
		panic(err)
	}
	return registry
}()

// The baseline's table inventory recognizes a committed schema whose migration
// marker was lost. Later migrations have their own recovery contracts.
var initialSchemaTables2026090101 = []string{
	"admin_sessions",
	"bucket_replica_slots",
	"buckets",
	"deployment_identity",
	"multipart_parts",
	"multipart_uploads",
	"object_cache",
	"object_deletions",
	"object_versions",
	"objects",
	"observability_collection_states",
	"observability_data_set_states",
	"observability_provider_states",
	"provider_profiles",
	"provider_tier_snapshots",
	"provider_upload_speed_tests",
	"s3_accounts",
	"storage_cleanup_copies",
	"storage_commit_request_pieces",
	"storage_commit_requests",
	"storage_contents",
	"storage_copies",
	"storage_data_set_termination_attempts",
	"storage_data_set_terminations",
	"storage_data_sets",
	"storage_pull_attempts",
	"storage_replacement_items",
	"storage_replacements",
	"task_events",
	"task_schedules",
	"tasks",
	"wallet_operations",
}

func discoverMigrations(fsys fs.FS) (*migrate.Migrations, error) {
	discovered := migrate.NewMigrations()
	if err := discovered.Discover(fsys); err != nil {
		return nil, err
	}
	registry := migrate.NewMigrations()
	for _, migration := range discovered.Sorted() {
		if migration.Name == InitialSchemaName {
			up := migration.Up
			if up == nil {
				return nil, fmt.Errorf("baseline migration %s has no up migration", InitialSchemaName)
			}
			migration.Up = func(ctx context.Context, migrator *migrate.Migrator, current *migrate.Migration) error {
				complete, err := validateInitialSchemaTarget(ctx, migrator.DB())
				if err != nil {
					return fmt.Errorf("checking initial schema target: %w", err)
				}
				if complete {
					return nil
				}
				return up(ctx, migrator, current)
			}
			migration.Down = func(context.Context, *migrate.Migrator, *migrate.Migration) error {
				return errors.New("the initial schema cannot be rolled back; create a new empty database")
			}
		}
		registry.Add(migration)
	}
	return registry, nil
}

// NewMigrator is the only supported migrator constructor. Migration markers
// are written only after the corresponding body succeeds.
func NewMigrator(db *bun.DB) *migrate.Migrator {
	return newMigrator(db, Migrations)
}

// ValidateTarget refuses, before anything is written, a database that is not
// this application's: its recorded migrations must be an ordered prefix of the
// registry. Without a recorded migration, only an empty target or a complete
// baseline with initialized migration metadata is accepted. Whether migrations
// build the intended schema is verified by tests, not here.
func ValidateTarget(ctx context.Context, db bun.IDB) error {
	return validateTarget(ctx, db, Migrations)
}

func validateTarget(ctx context.Context, db bun.IDB, registry *migrate.Migrations) error {
	if !validMigrationRegistry(registry) {
		return incompatibleDatabaseError()
	}
	// The marker table and application tables come from one snapshot, so a runner
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
	}
	complete, err := validateInitialSchemaTarget(ctx, db)
	if err != nil {
		return fmt.Errorf("checking database contents: %w", err)
	}
	if complete && !markerExists {
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

func applicationTableNames(ctx context.Context, db bun.IDB) ([]string, error) {
	query := `SELECT table_name FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_type = 'BASE TABLE'
		  AND table_name NOT IN ('bun_migrations', 'bun_migration_locks')
		ORDER BY table_name`
	var names []string
	if err := db.NewRaw(query).Scan(ctx, &names); err != nil {
		return nil, err
	}
	return names, nil
}

// validateInitialSchemaTarget accepts an empty schema or exactly the baseline's
// tables, returning true for the latter so a lost marker can be repaired.
func validateInitialSchemaTarget(ctx context.Context, db bun.IDB) (bool, error) {
	names, err := applicationTableNames(ctx, db)
	if err != nil || len(names) == 0 {
		return false, err
	}
	slices.Sort(names)
	if !slices.Equal(names, initialSchemaTables2026090101) {
		return false, incompatibleDatabaseError()
	}
	return true, nil
}

func incompatibleDatabaseError() error {
	return fmt.Errorf("%w; preserve the database and matching cache, verify the configured database, and follow the Backup and Recovery guide", ErrIncompatibleDatabase)
}

func newMigrator(db *bun.DB, registry *migrate.Migrations) *migrate.Migrator {
	return migrate.NewMigrator(db, registry, migrate.WithMarkAppliedOnSuccess(true))
}

func tableExists(ctx context.Context, db bun.IDB, table string) (bool, error) {
	query := `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = ?`
	return queryExists(ctx, db, query, table)
}

func queryExists(ctx context.Context, db bun.IDB, query string, args ...any) (bool, error) {
	var count int
	if err := db.NewRaw(query, args...).Scan(ctx, &count); err != nil {
		return false, err
	}
	return count > 0, nil
}
