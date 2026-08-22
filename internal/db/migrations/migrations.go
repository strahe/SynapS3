package migrations

import (
	"context"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/migrate"
)

// Migrations is the global registry of database migrations.
var Migrations = migrate.NewMigrations()

type migrationBody func(context.Context, bun.IDB) error

// NewMigrator is the only supported migrator constructor. Migration markers
// are written only after the corresponding body succeeds.
func NewMigrator(db *bun.DB) *migrate.Migrator {
	return newMigrator(db, Migrations)
}

func newMigrator(db *bun.DB, registry *migrate.Migrations) *migrate.Migrator {
	return migrate.NewMigrator(db, registry, migrate.WithMarkAppliedOnSuccess(true))
}

// transactionalMigration keeps every migration body atomic while still
// allowing the migrator to repair a missing marker after a committed DDL body.
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
