package migrations

import (
	"database/sql"
	"errors"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"
	_ "modernc.org/sqlite"
)

func TestMigrationFilesDoNotImportRuntimePackages(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob migration files: %v", err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", file, err)
			}
			if strings.HasPrefix(path, "github.com/strahe/synaps3/internal/") {
				t.Errorf("%s imports runtime package %q", file, path)
			}
		}
	}
}

func TestValidateTargetRejectsLegacyDatabaseWithoutModification(t *testing.T) {
	db := newSQLiteMigrationDB(t, "legacy_rejection")
	if _, err := db.Exec(`CREATE TABLE tasks (id INTEGER PRIMARY KEY, status TEXT)`); err != nil {
		t.Fatalf("seed legacy table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (id, status) VALUES (7, 'running')`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	err := ValidateTarget(t.Context(), db)
	if !errors.Is(err, ErrIncompatibleDatabase) {
		t.Fatalf("ValidateTarget error = %v, want ErrIncompatibleDatabase", err)
	}
	var status string
	if err := db.NewRaw(`SELECT status FROM tasks WHERE id = 7`).Scan(t.Context(), &status); err != nil {
		t.Fatalf("read legacy row after rejection: %v", err)
	}
	if status != "running" {
		t.Fatalf("legacy row status = %q, want unchanged", status)
	}
	if exists, err := tableExists(t.Context(), db, "bun_migrations"); err != nil || exists {
		t.Fatalf("migration marker exists=%t err=%v after rejection", exists, err)
	}
}

func TestValidateTargetAcceptsOnlyAppliedMigrationPrefixes(t *testing.T) {
	registry := migrate.NewMigrations()
	registry.Add(Migrations.Sorted()[0])
	registry.Add(migrate.Migration{Name: "2026090201"})
	registry.Add(migrate.Migration{Name: "2026090301"})

	tests := []struct {
		name    string
		applied []string
		wantErr bool
	}{
		{name: "metadata only"},
		{name: "baseline", applied: []string{InitialSchemaName}},
		{name: "longer prefix", applied: []string{InitialSchemaName, "2026090201"}},
		{name: "full registry", applied: []string{InitialSchemaName, "2026090201", "2026090301"}},
		{name: "legacy marker", applied: []string{"2026040501"}, wantErr: true},
		{name: "unknown marker", applied: []string{InitialSchemaName, "2026090250"}, wantErr: true},
		{name: "duplicate marker", applied: []string{InitialSchemaName, InitialSchemaName}, wantErr: true},
		{name: "out of order", applied: []string{"2026090201", InitialSchemaName}, wantErr: true},
		{name: "gap", applied: []string{InitialSchemaName, "2026090301"}, wantErr: true},
		{name: "longer than registry", applied: []string{InitialSchemaName, "2026090201", "2026090301", "2026090401"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newSQLiteMigrationDB(t, "migration_prefix_"+strings.ReplaceAll(tt.name, " ", "_"))
			if err := newMigrator(db, registry).Init(t.Context()); err != nil {
				t.Fatalf("initialize migration metadata: %v", err)
			}
			for _, name := range tt.applied {
				if _, err := db.Exec(`INSERT INTO bun_migrations (name, group_id) VALUES (?, 1)`, name); err != nil {
					t.Fatalf("insert migration marker %q: %v", name, err)
				}
			}

			err := validateTarget(t.Context(), db, registry)
			if tt.wantErr {
				if !errors.Is(err, ErrIncompatibleDatabase) {
					t.Fatalf("validateTarget error = %v, want ErrIncompatibleDatabase", err)
				}
			} else if err != nil {
				t.Fatalf("validateTarget error = %v", err)
			}

			var names []string
			if err := db.NewRaw(`SELECT name FROM bun_migrations ORDER BY id`).Scan(t.Context(), &names); err != nil {
				t.Fatalf("read migration markers: %v", err)
			}
			if !slices.Equal(names, tt.applied) {
				t.Fatalf("migration markers after validation = %v, want unchanged %v", names, tt.applied)
			}
		})
	}
}

func TestValidateTargetRejectsInvalidMigrationRegistry(t *testing.T) {
	tests := []struct {
		name     string
		registry *migrate.Migrations
	}{
		{name: "empty", registry: migrate.NewMigrations()},
		{name: "missing baseline", registry: migrationRegistryForTest("2026090201")},
		{name: "duplicate", registry: migrationRegistryForTest(InitialSchemaName, InitialSchemaName)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newSQLiteMigrationDB(t, "invalid_registry_"+strings.ReplaceAll(tt.name, " ", "_"))
			if err := validateTarget(t.Context(), db, tt.registry); !errors.Is(err, ErrIncompatibleDatabase) {
				t.Fatalf("validateTarget error = %v, want ErrIncompatibleDatabase", err)
			}
			if count, err := applicationTableCount(t.Context(), db); err != nil || count != 0 {
				t.Fatalf("application table count after rejection = %d, err=%v", count, err)
			}
		})
	}
}

func migrationRegistryForTest(names ...string) *migrate.Migrations {
	registry := migrate.NewMigrations()
	for _, name := range names {
		registry.Add(migrate.Migration{Name: name})
	}
	return registry
}

func newSQLiteMigrationDB(t *testing.T, name string) *bun.DB {
	t.Helper()
	sqldb, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	return db
}
