//go:build postgres

// Package testpg provides isolated PostgreSQL schemas for tagged tests.
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

// DSN returns the external test DSN or starts one PostgreSQL container per process.
func DSN(t *testing.T) string {
	t.Helper()
	startOnce.Do(initialize)
	if startErr != nil {
		t.Fatalf("initialize PostgreSQL: %v", startErr)
	}
	return baseDSN
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
	dsn := DSN(t)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	admin := stdlib.OpenDB(*config)
	admin.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close PostgreSQL schema connection: %v", err)
		}
	})
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatalf("generate PostgreSQL schema name: %v", err)
	}
	schema := "test_" + hex.EncodeToString(random[:])
	name := pgx.Identifier{schema}.Sanitize()
	ctx, cancel := context.WithTimeout(t.Context(), cleanupTimeout)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatalf("create PostgreSQL test schema: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+name+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL test schema: %v", err)
		}
	})
	return withSchema(dsn, schema)
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
