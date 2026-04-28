package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

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
		if err := ensureSQLiteDir(cfg.DSN); err != nil {
			return nil, err
		}
		sqldb, err = sql.Open("sqlite", ensureSQLitePragmas(cfg.DSN))
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

func ensureSQLiteDir(dsn string) error {
	path, ok, err := sqliteFilePath(dsn)
	if err != nil {
		return fmt.Errorf("resolving sqlite database path: %w", err)
	}
	if !ok {
		return nil
	}

	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating sqlite database directory %s: %w", dir, err)
	}
	return nil
}

func sqliteFilePath(dsn string) (string, bool, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" || dsn == ":memory:" {
		return "", false, nil
	}

	if strings.HasPrefix(strings.ToLower(dsn), "file:") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", false, err
		}
		if strings.EqualFold(u.Query().Get("mode"), "memory") {
			return "", false, nil
		}

		path := u.Path
		if u.Opaque != "" {
			path = u.Opaque
		}
		if path == "" || path == ":memory:" {
			return "", false, nil
		}
		if u.Host != "" && u.Host != "localhost" {
			return "", false, nil
		}
		return filepath.FromSlash(normalizeFileURLPath(path)), true, nil
	}

	path, rawQuery, hasQuery := strings.Cut(dsn, "?")
	if hasQuery {
		values, err := url.ParseQuery(rawQuery)
		if err != nil {
			return "", false, err
		}
		if strings.EqualFold(values.Get("mode"), "memory") {
			return "", false, nil
		}
	}
	if path == "" || path == ":memory:" {
		return "", false, nil
	}
	return path, true, nil
}

func normalizeFileURLPath(path string) string {
	if runtime.GOOS == "windows" && len(path) >= 4 && path[0] == '/' && path[2] == ':' {
		return path[1:]
	}
	return path
}

func ensureSQLitePragmas(dsn string) string {
	dsn = ensureSQLitePragma(dsn, "foreign_keys(1)", "foreign_keys=")
	return ensureSQLitePragma(dsn, "busy_timeout(5000)", "busy_timeout=")
}

func ensureSQLitePragma(dsn, pragma, assignment string) string {
	lower := strings.ToLower(dsn)
	name := strings.TrimSuffix(strings.SplitN(pragma, "(", 2)[0], "(")
	if strings.Contains(lower, name+"(") || strings.Contains(lower, assignment) {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&_pragma=" + pragma
	}
	return dsn + "?_pragma=" + pragma
}
