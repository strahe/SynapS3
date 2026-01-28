package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"

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

// Migrate creates the database tables if they do not exist.
func Migrate(ctx context.Context, db *bun.DB) error {
	models := []interface{}{
		(*model.Bucket)(nil),
		(*model.Object)(nil),
		(*model.Task)(nil),
	}

	for _, m := range models {
		if _, err := db.NewCreateTable().Model(m).IfNotExists().Exec(ctx); err != nil {
			return fmt.Errorf("creating table for %T: %w", m, err)
		}
	}

	// Composite unique index: one active object per (bucket_id, key).
	if _, err := db.NewCreateIndex().
		Model((*model.Object)(nil)).
		Index("idx_objects_bucket_key").
		Column("bucket_id", "key").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating unique index on objects: %w", err)
	}

	// Index for task polling: status + scheduled_at.
	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_status_scheduled").
		Column("status", "scheduled_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating index on tasks: %w", err)
	}

	slog.Info("database migration completed")
	return nil
}
