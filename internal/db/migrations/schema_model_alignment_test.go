package migrations

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
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
		(*storagecommit.Request)(nil),
		(*storagecommit.RequestPiece)(nil),
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
		(*observability.ProviderProfile)(nil),
		(*observability.ProviderTierSnapshot)(nil),
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
		if _, ok := initialJSONColumn(table.Name, field.Name); ok && db.Dialect().Name() == dialect.SQLite {
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
