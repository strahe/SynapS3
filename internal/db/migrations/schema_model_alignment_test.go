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

func TestRuntimeModelsMatchCurrentSchema(t *testing.T) {
	models := runtimePersistentModels()
	runtimeTablesFromAST := runtimePersistentModelTablesFromAST(t)

	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
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
		(*model.TaskHistory)(nil),
		(*model.TaskSchedule)(nil),
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
	byName := make(map[string]appliedColumn, len(actual))
	for _, column := range actual {
		byName[column.Name] = column
	}
	for i, field := range table.Fields {
		got, exists := byName[field.Name]
		if !exists {
			t.Errorf("%s missing runtime column %s", table.Name, field.Name)
			continue
		}
		want := appliedColumn{
			Name:       field.Name,
			Type:       normalizedSQLType(field.CreateTableSQLType),
			NotNull:    field.NotNull,
			Default:    got.Default,
			PrimaryKey: field.IsPK,
			Generated:  field.AutoIncrement && field.Identity,
		}
		if db.Dialect().Name() == dialect.SQLite && strings.EqualFold(field.CreateTableSQLType, "jsonb") {
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

func TestCurrentSchemaColumnContracts(t *testing.T) {
	// A DEFAULT current_timestamp column is written by the database whenever bun
	// omits it, and SQLite renders that as second-granularity local text while
	// bun renders a fractional offset timestamp. One column would then hold two
	// encodings whose lexical order disagrees with time inside the same second.
	testSchemaDialects(t, func(t *testing.T, db *bun.DB) {
		for _, table := range applicationSchemaTables(t, db) {
			for _, column := range appliedTableColumns(t, db, table) {
				if strings.Contains(strings.ToLower(column.Default), "current_timestamp") {
					t.Errorf("%s.%s still defaults to the database clock: %s", table, column.Name, column.Default)
				}
			}
		}
		if db.Dialect().Name() == dialect.PG {
			var collations []string
			if err := db.NewRaw(`SELECT c.relname || '.' || a.attname || '=' || co.collname
				FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_collation co ON co.oid = a.attcollation
				WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
				AND ((c.relname = 'buckets' AND a.attname = 'name') OR (c.relname IN ('objects','object_versions','multipart_uploads','object_deletions') AND a.attname = 'key'))
				ORDER BY c.relname`).Scan(t.Context(), &collations); err != nil {
				t.Fatal(err)
			}
			if len(collations) != 5 {
				t.Fatalf("missing key columns: %v", collations)
			}
			for _, value := range collations {
				if !strings.HasSuffix(value, "=C") {
					t.Errorf("key collation %s, want C", value)
				}
			}
		}
	})
}
