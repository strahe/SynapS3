package testpg

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// RunTokenEnv identifies resources owned by one test-command invocation.
const RunTokenEnv = "SYNAPS3_TEST_RUN_TOKEN"

var resourceOwner = sync.OnceValues(func() (string, error) {
	token := os.Getenv(RunTokenEnv)
	if token == "" {
		var err error
		token, err = NewRunToken()
		if err != nil {
			return "", err
		}
	}
	if !validHex(token, 32) {
		return "", fmt.Errorf("invalid %s", RunTokenEnv)
	}
	owner, err := randomHex(4)
	return token + "_" + owner, err
})

var resourceRegistry = struct {
	sync.Mutex
	cleanup map[string]func(context.Context) error
}{cleanup: make(map[string]func(context.Context) error)}

// NewRunToken returns an unpredictable namespace for one test invocation.
func NewRunToken() (string, error) { return randomHex(16) }

func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func validHex(value string, size int) bool {
	if len(value) != size {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func parseTestConfig(dsn string) (*pgx.ConnConfig, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL test DSN")
	}
	return config, nil
}

func newResourceName(kind byte) (string, error) {
	owner, err := resourceOwner()
	if err != nil {
		return "", err
	}
	suffix, err := randomHex(6)
	if err != nil {
		return "", err
	}
	return "s3t_" + owner + "_" + string(kind) + "_" + suffix, nil
}

func resourceKind(name, token string) byte {
	parts := strings.Split(name, "_")
	if len(parts) != 5 || parts[0] != "s3t" || parts[1] != token ||
		!validHex(parts[1], 32) || !validHex(parts[2], 8) || !validHex(parts[4], 12) || len(parts[3]) != 1 {
		return 0
	}
	switch parts[3][0] {
	case 's', 't', 'c':
		return parts[3][0]
	default:
		return 0
	}
}

func registerResource(name string, cleanup func(context.Context) error) func(context.Context) error {
	var mu sync.Mutex
	done := false
	remove := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if done {
			return nil
		}
		err := cleanup(ctx)
		if err == nil {
			done = true
			resourceRegistry.Lock()
			delete(resourceRegistry.cleanup, name)
			resourceRegistry.Unlock()
		}
		return err
	}
	resourceRegistry.Lock()
	resourceRegistry.cleanup[name] = remove
	resourceRegistry.Unlock()
	return remove
}

func closeResources(ctx context.Context) error {
	resourceRegistry.Lock()
	names := make([]string, 0, len(resourceRegistry.cleanup))
	for name := range resourceRegistry.cleanup {
		names = append(names, name)
	}
	resourceRegistry.Unlock()
	slices.SortFunc(names, func(a, b string) int {
		return strings.Compare(resourceSortKey(a), resourceSortKey(b))
	})
	var result error
	for _, name := range names {
		resourceRegistry.Lock()
		cleanup := resourceRegistry.cleanup[name]
		resourceRegistry.Unlock()
		if cleanup != nil {
			result = errors.Join(result, cleanup(ctx))
		}
	}
	return result
}

func resourceSortKey(name string) string {
	parts := strings.Split(name, "_")
	if len(parts) == 5 && parts[3] == "t" {
		return "1" + name
	}
	return "0" + name
}

// CleanupRun reclaims only resources in token's namespace owned by the DSN role.
// The database named by dsn is a maintenance connection and is never removed.
func CleanupRun(ctx context.Context, dsn, token string) (result error) {
	if !validHex(token, 32) {
		return errors.New("invalid PostgreSQL test run token")
	}
	config, err := parseTestConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse PostgreSQL maintenance DSN: %w", err)
	}
	admin := stdlib.OpenDB(*config)
	admin.SetMaxOpenConns(1)
	defer func() { result = errors.Join(result, admin.Close()) }()
	prefix := "s3t_" + token + "_"
	names, err := resourceNames(ctx, admin, `SELECT nspname FROM pg_namespace
		WHERE left(nspname::text, length($1)) = $1
		AND nspowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)`, prefix)
	if err != nil {
		return err
	}
	for _, name := range names {
		if resourceKind(name, token) == 's' {
			_, err := admin.ExecContext(ctx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE")
			result = errors.Join(result, err)
		}
	}
	names, err = resourceNames(ctx, admin, `SELECT datname FROM pg_database
		WHERE left(datname::text, length($1)) = $1
		AND datdba = (SELECT oid FROM pg_roles WHERE rolname = current_user)`, prefix)
	if err != nil {
		return errors.Join(result, err)
	}
	slices.SortFunc(names, func(a, b string) int {
		return strings.Compare(resourceSortKey(a), resourceSortKey(b))
	})
	for _, name := range names {
		kind := resourceKind(name, token)
		if kind == 'c' || kind == 't' {
			result = errors.Join(result, dropDatabase(ctx, admin, config.Database, name))
		}
	}
	return result
}

func resourceNames(ctx context.Context, admin *sql.DB, query, prefix string) (names []string, result error) {
	rows, err := admin.QueryContext(ctx, query, prefix)
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, rows.Close()) }()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func dropDatabase(ctx context.Context, admin *sql.DB, maintenance, name string) error {
	if name == maintenance || name == "postgres" || name == "template0" || name == "template1" {
		return errors.New("refuse to drop a PostgreSQL maintenance database")
	}
	clients, err := waitForDatabaseClients(ctx, admin, name)
	if err != nil {
		return err
	}
	var leak error
	if clients != 0 {
		leak = fmt.Errorf("PostgreSQL test database has %d unclosed connections", clients)
	}
	identifier := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+identifier+" WITH (FORCE)")
	return errors.Join(leak, err)
}

func waitForDatabaseClients(ctx context.Context, admin *sql.DB, name string) (int, error) {
	// PostgreSQL processes Terminate asynchronously after the client closes.
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		var clients int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = $1 AND backend_type = 'client backend'`, name).Scan(&clients); err != nil {
			return 0, err
		}
		remaining := time.Until(deadline)
		if clients == 0 || remaining <= 0 {
			return clients, nil
		}
		timer := time.NewTimer(min(remaining, 10*time.Millisecond))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		}
	}
}
