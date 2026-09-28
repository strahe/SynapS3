//go:build postgres

package testpg

import (
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestSchemaDSNIsolationAndCleanup(t *testing.T) {
	admin := openTestDB(t, DSN(t))
	var fsync string
	if err := admin.QueryRowContext(t.Context(), "SHOW fsync").Scan(&fsync); err != nil || fsync != "on" {
		t.Fatalf("PostgreSQL fsync=%q, want on: %v", fsync, err)
	}
	t.Logf("PostgreSQL fsync=%s", fsync)
	var schemas [2]string
	t.Run("isolated schemas", func(t *testing.T) {
		first := openTestDB(t, SchemaDSN(t))
		second := openTestDB(t, SchemaDSN(t))
		if err := first.QueryRowContext(t.Context(), "SELECT current_schema()").Scan(&schemas[0]); err != nil {
			t.Fatal(err)
		}
		if err := second.QueryRowContext(t.Context(), "SELECT current_schema()").Scan(&schemas[1]); err != nil {
			t.Fatal(err)
		}
		if schemas[0] == schemas[1] || schemas[0] == "public" || schemas[1] == "public" {
			t.Fatalf("schemas are not isolated: %v", schemas)
		}
		if _, err := first.ExecContext(t.Context(), "CREATE TABLE isolated_value (value integer); INSERT INTO isolated_value VALUES (42)"); err != nil {
			t.Fatal(err)
		}
		var visible bool
		if err := second.QueryRowContext(t.Context(), "SELECT to_regclass('isolated_value') IS NOT NULL").Scan(&visible); err != nil || visible {
			t.Fatalf("table leaked to second schema: visible=%v err=%v", visible, err)
		}
		if _, err := second.ExecContext(t.Context(), "CREATE TABLE isolated_value (value integer); INSERT INTO isolated_value VALUES (7)"); err != nil {
			t.Fatal(err)
		}
		// Hold both connections so the pool must open a second physical connection.
		for i := range 2 {
			conn, err := first.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := conn.Close(); err != nil {
					t.Errorf("close test connection: %v", err)
				}
			})
			var schema string
			var value int
			if err := conn.QueryRowContext(t.Context(), "SELECT current_schema(), value FROM isolated_value").Scan(&schema, &value); err != nil {
				t.Fatal(err)
			}
			if schema != schemas[0] || value != 42 {
				t.Fatalf("connection %d: schema=%q value=%d", i, schema, value)
			}
		}
		var value int
		if err := second.QueryRowContext(t.Context(), "SELECT value FROM isolated_value").Scan(&value); err != nil || value != 7 {
			t.Fatalf("second schema value=%d err=%v", value, err)
		}
	})
	for _, schema := range schemas {
		var exists bool
		if err := admin.QueryRowContext(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema).Scan(&exists); err != nil || exists {
			t.Errorf("schema %q remains after subtest cleanup: exists=%v err=%v", schema, exists, err)
		}
	}
}

func TestSchemaDSNPreservesConnectionParameters(t *testing.T) {
	for name, dsn := range map[string]string{
		"URL":     "postgresql://tester:password@localhost:6543/testing?sslmode=disable&application_name=schema-test&connect_timeout=5&search_path=public&options=-c%20statement_timeout%3D10000",
		"keyword": "host=localhost port=6543 user=tester password='pass word' dbname=testing sslmode=disable application_name=schema-test connect_timeout=5 search_path=public options='-c statement_timeout=10000'",
	} {
		t.Run(name, func(t *testing.T) {
			config, err := pgx.ParseConfig(withSchema(dsn, "test_private"))
			if err != nil {
				t.Fatal(err)
			}
			if config.Host != "localhost" || config.Port != 6543 || config.User != "tester" || config.Database != "testing" || config.TLSConfig != nil || config.ConnectTimeout.Seconds() != 5 {
				t.Fatal("connection settings changed")
			}
			for key, want := range map[string]string{
				"search_path":      "test_private",
				"application_name": "schema-test",
				"options":          "-c statement_timeout=10000",
			} {
				if got := config.RuntimeParams[key]; got != want {
					t.Errorf("%s=%q, want %q", key, got, want)
				}
			}
			original, err := pgx.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if config.Password != original.Password {
				t.Error("password changed")
			}
		})
	}
}

func openTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})
	return db
}
