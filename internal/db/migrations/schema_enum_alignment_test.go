package migrations

import (
	"go/ast"
	"go/build"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type closedEnumCheck struct {
	directory  string
	typeName   string
	table      string
	constraint string
}

func TestClosedLifecycleEnumsMatchAppliedChecks(t *testing.T) {
	checks := []closedEnumCheck{
		{directory: "model", typeName: "TaskStatus", table: "tasks", constraint: "chk_tasks_status"},
		{directory: "model", typeName: "TaskResumeMode", table: "tasks", constraint: "chk_tasks_resume_mode"},
		{directory: "model", typeName: "BucketStatus", table: "buckets", constraint: "chk_buckets_status"},
		{directory: "model", typeName: "BucketReplicaSlotStatus", table: "bucket_replica_slots", constraint: "chk_bucket_replica_slots_status"},
		{directory: "model", typeName: "MultipartStatus", table: "multipart_uploads", constraint: "chk_multipart_uploads_status"},
		{directory: "model", typeName: "StorageDataSetStatus", table: "storage_data_sets", constraint: "chk_storage_data_sets_status"},
		{directory: "model", typeName: "StorageCopyTransferMethod", table: "storage_copies", constraint: "chk_storage_copies_transfer_method"},
		{directory: "model", typeName: "StorageCopyStatus", table: "storage_copies", constraint: "chk_storage_copies_status"},
		{directory: "model", typeName: "StorageCleanupCopyStatus", table: "storage_cleanup_copies", constraint: "chk_storage_cleanup_copies_status"},
		{directory: "model", typeName: "WalletOperationType", table: "wallet_operations", constraint: "chk_wallet_operations_type"},
		{directory: "model", typeName: "WalletOperationStatus", table: "wallet_operations", constraint: "chk_wallet_operations_status"},
		{directory: "observability", typeName: "CollectionType", table: "observability_collection_states", constraint: "chk_observability_collection_type"},
		{directory: "observability", typeName: "Status", table: "observability_provider_states", constraint: "chk_observability_provider_status"},
		{directory: "observability", typeName: "Status", table: "observability_data_set_states", constraint: "chk_observability_data_set_status"},
		{directory: "storagecommit", typeName: "AttemptStatus", table: "storage_commit_attempts", constraint: "chk_storage_commit_attempts_status"},
		{directory: "storagepull", typeName: "AttemptStatus", table: "storage_pull_attempts", constraint: "chk_storage_pull_attempts_status"},
		{directory: "storagereplacement", typeName: "SelectionMode", table: "storage_replacements", constraint: "chk_storage_replacements_selection_mode"},
		{directory: "storagereplacement", typeName: "Status", table: "storage_replacements", constraint: "chk_storage_replacements_status"},
		{directory: "storagereplacement", typeName: "TerminationRole", table: "storage_data_set_terminations", constraint: "chk_storage_data_set_terminations_role"},
		{directory: "storagereplacement", typeName: "ItemStatus", table: "storage_replacement_items", constraint: "chk_storage_replacement_items_status"},
	}

	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		for _, check := range checks {
			t.Run(check.table+"/"+check.typeName, func(t *testing.T) {
				goValues := typedStringConstants(t, filepath.Join("..", "..", check.directory), check.typeName)
				ddlValues := appliedCheckStringValues(t, db, check.table, check.constraint)
				if !slices.Equal(goValues, ddlValues) {
					t.Fatalf("%s constants = %v, %s.%s values = %v", check.typeName, goValues, check.table, check.constraint, ddlValues)
				}
			})
		}
	})
}

func typedStringConstants(t *testing.T, directory, typeName string) []string {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read %s: %v", directory, err)
	}
	var packageName string
	var declarations []ast.Decl
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		matches, err := build.Default.MatchFile(directory, entry.Name())
		if err != nil {
			t.Fatalf("match build constraints for %s: %v", entry.Name(), err)
		}
		if !matches {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(directory, entry.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		if packageName == "" {
			packageName = file.Name.Name
		} else if packageName != file.Name.Name {
			t.Fatalf("parse %s: found package %s alongside %s", directory, file.Name.Name, packageName)
		}
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			switch general.Tok {
			case token.TYPE:
				for _, spec := range general.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if ok && typeSpec.Name.Name == typeName {
						declarations = append(declarations, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{typeSpec}})
					}
				}
			case token.CONST:
				if constDeclarationDefinesType(general, typeName) {
					// Keep the complete group so go/types can apply ConstSpec
					// inheritance and iota semantics before we filter by type.
					declarations = append(declarations, general)
				}
			}
		}
	}
	if len(declarations) == 0 {
		t.Fatalf("no declarations found for %s in %s", typeName, directory)
	}
	synthetic := &ast.File{Name: ast.NewIdent(packageName), Decls: declarations}
	typedPackage, err := new(types.Config).Check(packageName, fset, []*ast.File{synthetic}, nil)
	if err != nil {
		t.Fatalf("type-check %s.%s: %v", packageName, typeName, err)
	}
	var values []string
	for _, name := range typedPackage.Scope().Names() {
		object, ok := typedPackage.Scope().Lookup(name).(*types.Const)
		if !ok {
			continue
		}
		named, ok := object.Type().(*types.Named)
		if !ok || named.Obj().Name() != typeName || object.Val().Kind() != constant.String {
			continue
		}
		values = append(values, constant.StringVal(object.Val()))
	}
	slices.Sort(values)
	return slices.Compact(values)
}

func constDeclarationDefinesType(declaration *ast.GenDecl, typeName string) bool {
	for _, spec := range declaration.Specs {
		valueSpec, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		identifier, ok := valueSpec.Type.(*ast.Ident)
		if ok && identifier.Name == typeName {
			return true
		}
	}
	return false
}

var singleQuotedSQLValue = regexp.MustCompile(`'(?:''|[^'])*'`)

func appliedCheckStringValues(t *testing.T, db *bun.DB, table, constraint string) []string {
	t.Helper()
	var definition string
	if db.Dialect().Name() == dialect.PG {
		if err := db.NewRaw(`SELECT pg_get_constraintdef(constraint_info.oid)
			FROM pg_constraint AS constraint_info
			JOIN pg_class AS table_info ON table_info.oid = constraint_info.conrelid
			JOIN pg_namespace AS namespace_info ON namespace_info.oid = table_info.relnamespace
			WHERE namespace_info.nspname = current_schema()
			  AND table_info.relname = ?
			  AND constraint_info.conname = ?`, table, constraint).Scan(t.Context(), &definition); err != nil {
			t.Fatalf("read PostgreSQL check %s.%s: %v", table, constraint, err)
		}
	} else {
		var ddl string
		if err := db.NewRaw(`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(t.Context(), &ddl); err != nil {
			t.Fatalf("read SQLite DDL for %s: %v", table, err)
		}
		definition = namedCheckExpression(t, ddl, constraint)
	}
	values := make([]string, 0)
	for _, quoted := range singleQuotedSQLValue.FindAllString(definition, -1) {
		values = append(values, strings.ReplaceAll(quoted[1:len(quoted)-1], "''", "'"))
	}
	slices.Sort(values)
	return slices.Compact(values)
}

func namedCheckExpression(t *testing.T, ddl, constraint string) string {
	t.Helper()
	lower := strings.ToLower(ddl)
	start := strings.Index(lower, "constraint "+strings.ToLower(constraint))
	if start < 0 {
		t.Fatalf("constraint %s not found in %s", constraint, ddl)
	}
	check := strings.Index(lower[start:], "check")
	if check < 0 {
		t.Fatalf("CHECK keyword for %s not found in %s", constraint, ddl)
	}
	open := strings.Index(ddl[start+check:], "(")
	if open < 0 {
		t.Fatalf("CHECK expression for %s not found in %s", constraint, ddl)
	}
	open += start + check
	depth := 0
	inString := false
	for index := open; index < len(ddl); index++ {
		switch ddl[index] {
		case '\'':
			if inString && index+1 < len(ddl) && ddl[index+1] == '\'' {
				index++
				continue
			}
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString {
				depth--
				if depth == 0 {
					return ddl[open+1 : index]
				}
			}
		}
	}
	t.Fatalf("unterminated CHECK expression for %s in %s", constraint, ddl)
	return ""
}
