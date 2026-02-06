package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// New creates a Bun database connection based on the provided configuration.
func New(cfg config.DatabaseConfig) (*bun.DB, error) {
	var (
		sqldb *sql.DB
		err   error
		db    *bun.DB
	)

	switch cfg.Driver {
	case "postgres":
		sqldb, err = sql.Open("pgx", cfg.DSN)
		if err != nil {
			return nil, fmt.Errorf("opening postgres connection: %w", err)
		}
		db = bun.NewDB(sqldb, pgdialect.New())

	case "sqlite":
		sqldb, err = sql.Open("sqlite", cfg.DSN)
		if err != nil {
			return nil, fmt.Errorf("opening sqlite connection: %w", err)
		}
		db = bun.NewDB(sqldb, sqlitedialect.New())

	default:
		return nil, fmt.Errorf("unsupported database driver: %s", cfg.Driver)
	}

	sqldb.SetMaxOpenConns(cfg.MaxOpenConns)
	sqldb.SetMaxIdleConns(cfg.MaxIdleConns)

	return db, nil
}

// RunMigrations initialises the Bun migrator and applies all pending migrations.
func RunMigrations(ctx context.Context, db *bun.DB) error {
	migrator := migrate.NewMigrator(db, migrations.Migrations)

	if err := migrator.Init(ctx); err != nil {
		return fmt.Errorf("initialising migrator: %w", err)
	}

	group, err := migrator.Migrate(ctx)
	if err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	if group != nil && group.ID > 0 {
		slog.Info("applied migrations", "group", group.ID, "count", len(group.Migrations))
	} else {
		slog.Info("no new migrations to apply")
	}

	return nil
}

// Ping verifies the database connection is alive.
func Ping(ctx context.Context, db *bun.DB) error {
	return db.PingContext(ctx)
}
