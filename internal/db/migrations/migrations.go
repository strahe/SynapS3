package migrations

import (
	"context"
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

var initialSchemaTableNames = []string{
	"bucket_replica_slots",
	"buckets",
	"multipart_parts",
	"multipart_uploads",
	"object_cache",
	"object_deletions",
	"object_versions",
	"objects",
	"observability_collection_states",
	"observability_data_set_states",
	"observability_provider_states",
	"provider_upload_speed_tests",
	"s3_accounts",
	"storage_cleanup_copies",
	"storage_commit_attempts",
	"storage_contents",
	"storage_copies",
	"storage_data_set_terminations",
	"storage_data_sets",
	"storage_pull_attempts",
	"storage_replacement_items",
	"storage_replacements",
	"task_payloads",
	"tasks",
	"wallet_operations",
}

type migrationBody func(context.Context, bun.IDB) error

// NewMigrator is the only supported migrator constructor. Migration markers
// are written only after the corresponding body succeeds.
func NewMigrator(db *bun.DB) *migrate.Migrator {
	return newMigrator(db, Migrations)
}

// ValidateTarget accepts an empty application database or a database whose
// applied migrations and schema match the current unreleased baseline.
func ValidateTarget(ctx context.Context, db bun.IDB) error {
	if err := validateTarget(ctx, db, Migrations); err != nil {
		return err
	}
	markerExists, err := tableExists(ctx, db, "bun_migrations")
	if err != nil || !markerExists {
		return err
	}
	var names []string
	if err := db.NewRaw("SELECT name FROM bun_migrations ORDER BY id").Scan(ctx, &names); err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}
	statusURLExists, err := columnExists(ctx, db, "storage_commit_attempts", "status_url")
	if err != nil {
		return fmt.Errorf("checking commit status URL column: %w", err)
	}
	oldJSONExists, err := columnExists(ctx, db, "storage_commit_attempts", "submission_json")
	if err != nil {
		return fmt.Errorf("checking obsolete commit submission column: %w", err)
	}
	if !statusURLExists || oldJSONExists {
		return incompatibleDatabaseError()
	}
	benchmarkExists, err := tableExists(ctx, db, "provider_upload_speed_tests")
	if err != nil {
		return fmt.Errorf("checking provider upload speed tests: %w", err)
	}
	if !benchmarkExists {
		return incompatibleDatabaseError()
	}
	return nil
}

func validateTarget(ctx context.Context, db bun.IDB, registry *migrate.Migrations) error {
	if !validMigrationRegistry(registry) {
		return incompatibleDatabaseError()
	}
	markerExists, err := tableExists(ctx, db, "bun_migrations")
	if err != nil {
		return fmt.Errorf("checking migration metadata: %w", err)
	}
	if markerExists {
		var names []string
		if err := db.NewRaw("SELECT name FROM bun_migrations ORDER BY id").Scan(ctx, &names); err != nil {
			return fmt.Errorf("reading migration metadata: %w", err)
		}
		if len(names) == 0 {
			complete, err := initialSchemaPostStateComplete(ctx, db)
			if err != nil {
				return fmt.Errorf("checking initial schema post-state: %w", err)
			}
			if complete {
				return nil
			}
		} else if appliedMigrationPrefix(names, registry) {
			return nil
		} else {
			return incompatibleDatabaseError()
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

func initialSchemaPostStateComplete(ctx context.Context, db bun.IDB) (bool, error) {
	tables, err := applicationTableNames(ctx, db)
	if err != nil || !slices.Equal(tables, initialSchemaTableNames) {
		return false, err
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{"multipart_uploads", "upload_id"},
		{"storage_contents", "checksum"},
		{"storage_copies", "content_id"},
		{"storage_copies", "storage_data_set_id"},
		{"storage_commit_attempts", "attempt_id"},
		{"storage_commit_attempts", "status_url"},
		{"storage_replacement_items", "target_data_set_id"},
		{"storage_cleanup_copies", "bucket_id"},
		{"storage_cleanup_copies", "checksum"},
		{"object_versions", "content_id"},
		{"object_cache", "content_id"},
	} {
		exists, err := columnExists(ctx, db, column.table, column.name)
		if err != nil || !exists {
			return false, err
		}
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{"multipart_uploads", "id"},
		{"storage_copies", "commit_attempt_id"},
		{"storage_copies", "commit_transaction_id"},
		{"storage_commit_attempts", "submission_json"},
		{"storage_copies", "upload_id"},
		{"storage_replacement_items", "target_copy_id"},
		{"object_versions", "state"},
		{"object_versions", "storage_upload_id"},
		{"storage_data_sets", "repair_task_id"},
	} {
		exists, err := columnExists(ctx, db, column.table, column.name)
		if err != nil || exists {
			return false, err
		}
	}
	for _, index := range []string{
		"idx_storage_commit_attempts_unresolved_copy",
		"idx_storage_data_sets_bucket_provider_active",
		"idx_observability_data_set_states_provider_status",
	} {
		exists, err := indexExists(ctx, db, index)
		if err != nil || !exists {
			return false, err
		}
	}
	return true, nil
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
