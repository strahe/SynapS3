package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

const migrationUnlockTimeout = 5 * time.Second

const sqlitePreflightTimeout = 5 * time.Second

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
		if err := validateExistingSQLiteTarget(cfg.DSN); err != nil {
			return nil, err
		}
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

// validateExistingSQLiteTarget runs before the normal connection can apply a
// persistent journal-mode pragma. An incompatible database is therefore
// rejected without changing the preserved file.
func validateExistingSQLiteTarget(dsn string) (retErr error) {
	path, ok, err := sqliteFilePath(dsn)
	if err != nil {
		return fmt.Errorf("resolving sqlite database path: %w", err)
	}
	if !ok {
		return nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspecting sqlite database: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("sqlite database path is a directory: %s", path)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolving sqlite database absolute path: %w", err)
	}
	readOnlyURL := url.URL{Scheme: "file", Path: filepath.ToSlash(absolutePath)}
	query := readOnlyURL.Query()
	query.Set("mode", "ro")
	query.Add("_pragma", "busy_timeout(5000)")
	readOnlyURL.RawQuery = query.Encode()

	sqldb, err := sql.Open("sqlite", readOnlyURL.String())
	if err != nil {
		return fmt.Errorf("opening sqlite database for compatibility check: %w", err)
	}
	readOnlyDB := bun.NewDB(sqldb, sqlitedialect.New())
	defer func() {
		if err := readOnlyDB.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("closing sqlite compatibility check: %w", err))
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), sqlitePreflightTimeout)
	defer cancel()
	if err := migrations.ValidateTarget(ctx, readOnlyDB); err != nil {
		return fmt.Errorf("validating existing sqlite database: %w", err)
	}
	return nil
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
	if _, ok, _ := sqliteFilePath(dsn); ok {
		dsn = ensureSQLitePragma(dsn, "journal_mode", "journal_mode(WAL)")
	}
	dsn = ensureSQLitePragma(dsn, "foreign_keys", "foreign_keys(1)")
	return ensureSQLitePragma(dsn, "busy_timeout", "busy_timeout(5000)")
}

func ensureSQLitePragma(dsn, name, pragma string) string {
	if sqliteDSNHasPragma(dsn, name) {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&_pragma=" + pragma
	}
	return dsn + "?_pragma=" + pragma
}

func sqliteDSNHasPragma(dsn, name string) bool {
	_, rawQuery, ok := strings.Cut(dsn, "?")
	if !ok {
		return false
	}

	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false
	}
	for key, entries := range values {
		if strings.EqualFold(key, name) {
			return true
		}
		if !strings.EqualFold(key, "_pragma") {
			continue
		}
		for _, entry := range entries {
			if strings.EqualFold(sqlitePragmaName(entry), name) {
				return true
			}
		}
	}
	return false
}

func sqlitePragmaName(entry string) string {
	entry = strings.TrimSpace(entry)
	entry = strings.TrimPrefix(strings.ToLower(entry), "pragma ")
	if before, _, ok := strings.Cut(entry, "("); ok {
		return strings.TrimSpace(before)
	}
	if before, _, ok := strings.Cut(entry, "="); ok {
		return strings.TrimSpace(before)
	}
	return strings.ToLower(entry)
}
