package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const migrationUnlockTimeout = 5 * time.Second

// New creates a Bun database connection based on the provided configuration.
func New(cfg config.DatabaseConfig) (*bun.DB, error) {
	// A blank DSN would make pgx fall back to libpq environment defaults.
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, errors.New("database.dsn is not set; set it to a PostgreSQL connection URL in the config file or SYNAPS3_DATABASE_DSN")
	}
	if cfg.Driver != "postgres" {
		return nil, fmt.Errorf("unsupported database driver: %s", cfg.Driver)
	}
	sqldb, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("opening postgres connection: %w", err)
	}
	sqldb.SetMaxOpenConns(cfg.MaxOpenConns)
	sqldb.SetMaxIdleConns(cfg.MaxIdleConns)
	return bun.NewDB(sqldb, pgdialect.New()), nil
}

// RunMigrations initialises the Bun migrator and applies all pending migrations.
func RunMigrations(ctx context.Context, db *bun.DB) (retErr error) {
	if err := migrations.ValidateTarget(ctx, db); err != nil {
		return err
	}
	migrator := migrations.NewMigrator(db)

	if err := migrator.Init(ctx); err != nil {
		return fmt.Errorf("initializing migrator: %w", err)
	}
	if err := migrator.Lock(ctx); err != nil {
		return fmt.Errorf("locking migrator: %w; if no migration is running, "+
			"clear a lock left by a killed run with `synaps3 migrate --force-unlock`", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), migrationUnlockTimeout)
		defer cancel()
		if err := migrator.Unlock(unlockCtx); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("unlocking migrator: %w", err))
		}
	}()

	group, err := migrator.Migrate(ctx)
	if err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	if group != nil && group.ID > 0 {
		slog.Info("applied migrations", "group", group.ID, "count", len(group.Migrations))
	} else {
		slog.Info("no new migrations to apply")
	}
	// Statistics are an operational concern, not part of the frozen DDL, and the
	// initial migration returns early on an already-migrated database. Refreshing
	// them here means a partial index such as idx_tasks_pending is costed against
	// what the table actually holds rather than against a default guess.
	if err := analyzeSchema(ctx, db); err != nil {
		slog.Warn("refreshing planner statistics failed (non-fatal)", "error", err)
	}

	return nil
}

// analyzeSchema refreshes query planner statistics for the whole database.
func analyzeSchema(ctx context.Context, db *bun.DB) error {
	if _, err := db.ExecContext(ctx, "ANALYZE"); err != nil {
		return fmt.Errorf("analyzing schema: %w", err)
	}
	return nil
}

// ForceUnlockMigrations releases a migration lock left by a killed run.
func ForceUnlockMigrations(ctx context.Context, db *bun.DB) error {
	if err := migrations.ValidateTarget(ctx, db); err != nil {
		return err
	}
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		return fmt.Errorf("initializing migrator: %w", err)
	}
	if err := migrator.Unlock(ctx); err != nil {
		return fmt.Errorf("releasing migration lock: %w", err)
	}
	return nil
}

// Ping verifies the database connection is alive.
func Ping(ctx context.Context, db *bun.DB) error {
	return db.PingContext(ctx)
}
