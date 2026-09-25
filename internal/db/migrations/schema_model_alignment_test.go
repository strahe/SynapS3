package migrations

import (
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestRuntimeModelsMatchAppliedBaseline(t *testing.T) {
	models := runtimePersistentModels()
	runtimeTablesFromAST := runtimePersistentModelTablesFromAST(t)

	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		registeredTables := make([]string, 0, len(models))
		for _, runtimeModel := range models {
			typ := reflect.TypeOf(runtimeModel)
			if typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}
			registeredTables = append(registeredTables, db.Table(typ).Name)
		}
		slices.Sort(registeredTables)
		if !slices.Equal(registeredTables, runtimeTablesFromAST) {
			t.Fatalf("runtime model registry tables = %v, AST-discovered tables = %v", registeredTables, runtimeTablesFromAST)
		}

		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		if err := runMigrationBody(t.Context(), db, up2026092401S3AccountName); err != nil {
			t.Fatalf("add S3 account names: %v", err)
		}
		appliedTables := applicationSchemaTables(t, db)
		if !slices.Equal(appliedTables, registeredTables) {
			t.Fatalf("applied tables = %v, runtime model tables = %v", appliedTables, registeredTables)
		}
		for _, runtimeModel := range models {
			assertRuntimeModelMatchesTable(t, db, runtimeModel)
		}
	})
}

func runtimePersistentModels() []any {
	return []any{
		(*model.Task)(nil),
		(*model.TaskPayload)(nil),
		(*model.S3Account)(nil),
		(*model.Bucket)(nil),
		(*model.BucketReplicaSlot)(nil),
		(*model.Object)(nil),
		(*model.MultipartUpload)(nil),
		(*model.MultipartPart)(nil),
		(*model.StorageContent)(nil),
		(*model.StorageDataSet)(nil),
		(*model.StorageCopy)(nil),
		(*storagecommit.Attempt)(nil),
		(*storagepull.Attempt)(nil),
		(*model.ObjectVersion)(nil),
		(*model.ObjectCache)(nil),
		(*model.ObjectDeletion)(nil),
		(*model.StorageCleanupCopy)(nil),
		(*storagereplacement.Replacement)(nil),
		(*storagereplacement.Termination)(nil),
		(*storagereplacement.Item)(nil),
		(*model.WalletOperation)(nil),
		(*observability.CollectionState)(nil),
		(*observability.ProviderState)(nil),
		(*providerbenchmark.Result)(nil),
		(*observability.DataSetState)(nil),
	}
}

func runtimePersistentModelTablesFromAST(t *testing.T) []string {
	t.Helper()
	internalRoot := filepath.Clean(filepath.Join("..", ".."))
	migrationRoot := filepath.Clean(filepath.Join(internalRoot, "db", "migrations"))
	tables := make(map[string]string)
	err := filepath.WalkDir(internalRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if filepath.Clean(path) == migrationRoot {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			structType, ok := node.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range structType.Fields.List {
				if field.Tag == nil {
					continue
				}
				rawTag, err := strconv.Unquote(field.Tag.Value)
				if err != nil {
					continue
				}
				for option := range strings.SplitSeq(reflect.StructTag(rawTag).Get("bun"), ",") {
					table, found := strings.CutPrefix(option, "table:")
					if !found || table == "" {
						continue
					}
					if previous, exists := tables[table]; exists {
						t.Errorf("persistent table %s is declared in both %s and %s", table, previous, path)
					} else {
						tables[table] = path
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("discover runtime persistent models: %v", err)
	}
	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	slices.Sort(result)
	return result
}

type appliedColumn struct {
	Name       string
	Type       string
	NotNull    bool
	Default    string
	PrimaryKey bool
	Generated  bool
}

func assertRuntimeModelMatchesTable(t *testing.T, db *bun.DB, runtimeModel any) {
	t.Helper()
	typ := reflect.TypeOf(runtimeModel)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	table := db.Table(typ)
	actual := appliedTableColumns(t, db, table.Name)
	if len(actual) != len(table.Fields) {
		t.Errorf("%s column count = %d, runtime fields = %d", table.Name, len(actual), len(table.Fields))
	}
	limit := min(len(actual), len(table.Fields))
	for i := range limit {
		field := table.Fields[i]
		got := actual[i]
		want := appliedColumn{
			Name:       field.Name,
			Type:       normalizedSQLType(field.CreateTableSQLType),
			NotNull:    field.NotNull,
			Default:    got.Default,
			PrimaryKey: field.IsPK,
			Generated:  field.AutoIncrement && field.Identity,
		}
		if db.Dialect().Name() == dialect.SQLite && isInitialJSONColumn(table.Name, field.Name) {
			want.Type = "text"
		}
		// A runtime default changes Bun insert behavior by omitting zero values.
		// It is optional, but when present it must match the frozen DDL default.
		if field.SQLDefault != "" {
			want.Default = normalizedSQLDefault(field.SQLDefault)
		}
		if got != want {
			t.Errorf("%s column %d mismatch\n got: %#v\nwant: %#v", table.Name, i+1, got, want)
		}
	}
}

func appliedTableColumns(t *testing.T, db *bun.DB, table string) []appliedColumn {
	t.Helper()
	if db.Dialect().Name() == dialect.PG {
		return appliedPostgresColumns(t, db, table)
	}
	return appliedSQLiteColumns(t, db, table)
}

func appliedSQLiteColumns(t *testing.T, db *bun.DB, table string) []appliedColumn {
	t.Helper()
	var tableDDL string
	if err := db.NewRaw(`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(t.Context(), &tableDDL); err != nil {
		t.Fatalf("read SQLite table DDL for %s: %v", table, err)
	}
	rows, err := db.Query(`SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		t.Fatalf("read SQLite columns for %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []appliedColumn
	for rows.Next() {
		var name, sqlType string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&name, &sqlType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan SQLite column for %s: %v", table, err)
		}
		columns = append(columns, appliedColumn{
			Name:       name,
			Type:       normalizedSQLType(sqlType),
			NotNull:    notNull != 0,
			Default:    normalizedSQLDefault(defaultValue.String),
			PrimaryKey: primaryKey != 0,
			Generated:  primaryKey != 0 && name == "id" && strings.Contains(strings.ToUpper(tableDDL), "AUTOINCREMENT"),
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SQLite columns for %s: %v", table, err)
	}
	return columns
}

func appliedPostgresColumns(t *testing.T, db *bun.DB, table string) []appliedColumn {
	t.Helper()
	rows, err := db.Query(`SELECT column_info.column_name,
		       column_info.data_type,
		       column_info.is_nullable = 'NO',
		       column_info.column_default,
		       column_info.is_identity = 'YES',
		       EXISTS (
		           SELECT 1
		           FROM information_schema.table_constraints AS table_constraint
		           JOIN information_schema.key_column_usage AS key_column
		             ON key_column.constraint_schema = table_constraint.constraint_schema
		            AND key_column.constraint_name = table_constraint.constraint_name
		           WHERE table_constraint.table_schema = current_schema()
		             AND table_constraint.table_name = column_info.table_name
		             AND table_constraint.constraint_type = 'PRIMARY KEY'
		             AND key_column.column_name = column_info.column_name
		       )
		FROM information_schema.columns AS column_info
		WHERE column_info.table_schema = current_schema() AND column_info.table_name = ?
		ORDER BY column_info.ordinal_position`, table)
	if err != nil {
		t.Fatalf("read PostgreSQL columns for %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []appliedColumn
	for rows.Next() {
		var name, sqlType string
		var notNull, generated, primaryKey bool
		var defaultValue sql.NullString
		if err := rows.Scan(&name, &sqlType, &notNull, &defaultValue, &generated, &primaryKey); err != nil {
			t.Fatalf("scan PostgreSQL column for %s: %v", table, err)
		}
		columns = append(columns, appliedColumn{
			Name:       name,
			Type:       normalizedSQLType(sqlType),
			NotNull:    notNull,
			Default:    normalizedSQLDefault(defaultValue.String),
			PrimaryKey: primaryKey,
			Generated:  generated,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PostgreSQL columns for %s: %v", table, err)
	}
	return columns
}

func normalizedSQLType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "timestamptz", "timestamp with time zone", "timestamp":
		return "timestamp"
	case "bytea", "blob":
		return "binary"
	default:
		return value
	}
}

var postgresDefaultCast = regexp.MustCompile(`::[a-zA-Z0-9_\"]+`)

func normalizedSQLDefault(value string) string {
	value = strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	value = postgresDefaultCast.ReplaceAllString(value, "")
	for len(value) >= 2 && value[0] == '(' && value[len(value)-1] == ')' {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	return value
}
