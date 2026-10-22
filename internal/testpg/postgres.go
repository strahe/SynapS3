// Package testpg provides isolated PostgreSQL schemas for tests and the
// system-test server.
package testpg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const cleanupTimeout = 30 * time.Second

var (
	startOnce sync.Once
	baseDSN   string
	startErr  error
	container *postgres.PostgresContainer
	closeOnce sync.Once
	closeErr  error
)

// Main runs a package's tests and then terminates the container this process
// started. Call it from TestMain.
func Main(m *testing.M) {
	code := m.Run()
	if err := Close(); err != nil {
		fmt.Fprintf(os.Stderr, "clean up PostgreSQL test container: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

// DSN returns the external test DSN or starts one PostgreSQL container per process.
func DSN(t *testing.T) string {
	t.Helper()
	dsn, err := BaseDSN()
	if err != nil {
		t.Fatalf("initialize PostgreSQL: %v", err)
	}
	return dsn
}

// BaseDSN returns SYNAPS3_POSTGRES_TEST_DSN when set; otherwise it starts one
// PostgreSQL container per process.
func BaseDSN() (string, error) {
	startOnce.Do(initialize)
	return baseDSN, startErr
}

func initialize() {
	defer func() {
		if cause := recover(); cause != nil {
			startErr = fmt.Errorf("start PostgreSQL: %v", cause)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	baseDSN = os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if baseDSN == "" {
		container, startErr = postgres.Run(ctx, "postgres:18",
			postgres.WithDatabase("synaps3_test"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			postgres.BasicWaitStrategies(),
			testcontainers.WithCmd("postgres", "-c", "fsync=on"),
		)
		if startErr != nil {
			return
		}
		baseDSN, startErr = container.ConnectionString(ctx, "sslmode=disable")
		if startErr != nil {
			return
		}
	}
	config, err := pgx.ParseConfig(baseDSN)
	if err != nil {
		startErr = err
		return
	}
	db := stdlib.OpenDB(*config)
	defer func() {
		if err := db.Close(); err != nil {
			startErr = errors.Join(startErr, fmt.Errorf("close PostgreSQL startup connection: %w", err))
		}
	}()
	startErr = db.PingContext(ctx)
}

// SchemaDSN creates an empty private schema and sets search_path on every connection.
// Register connection cleanup after calling it so connections close before the schema.
func SchemaDSN(t *testing.T) string {
	t.Helper()
	base := DSN(t)
	ctx, cancel := context.WithTimeout(t.Context(), cleanupTimeout)
	defer cancel()
	dsn, drop, err := CreateSchema(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := drop(ctx); err != nil {
			t.Error(err)
		}
	})
	return dsn
}

// CreateSchema creates an empty private schema in the database named by dsn
// and returns a DSN whose connections use it. Close every connection to the
// returned DSN before calling drop.
func CreateSchema(ctx context.Context, dsn string) (string, func(context.Context) error, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", nil, fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	admin := stdlib.OpenDB(*config)
	admin.SetMaxOpenConns(1)
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", nil, errors.Join(fmt.Errorf("generate PostgreSQL schema name: %w", err), admin.Close())
	}
	schema := "test_" + hex.EncodeToString(random[:])
	name := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+name); err != nil {
		return "", nil, errors.Join(fmt.Errorf("create PostgreSQL test schema: %w", err), admin.Close())
	}
	drop := func(ctx context.Context) error {
		var dropErr error
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+name+" CASCADE"); err != nil {
			dropErr = fmt.Errorf("drop PostgreSQL test schema: %w", err)
		}
		if err := admin.Close(); err != nil {
			dropErr = errors.Join(dropErr, fmt.Errorf("close PostgreSQL schema connection: %w", err))
		}
		return dropErr
	}
	return withSchema(dsn, schema), drop, nil
}

func withSchema(dsn, schema string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, _ := url.Parse(dsn) // DSN has already passed pgx.ParseConfig.
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		return u.String()
	}
	return dsn + " search_path=" + schema
}

// Close terminates only the container created by this process. Call after m.Run.
func Close() error {
	closeOnce.Do(func() {
		if container != nil {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			closeErr = container.Terminate(ctx)
		}
	})
	return closeErr
}
