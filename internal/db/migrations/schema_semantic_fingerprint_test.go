package migrations

import (
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func portableSchemaFingerprint(t *testing.T, db *bun.DB) (string, string) {
	t.Helper()
	tables := applicationSchemaTables(t, db)
	lines := make([]string, 0, len(tables)*16)
	for _, table := range tables {
		for position, column := range appliedTableColumns(t, db, table) {
			lines = append(lines, fmt.Sprintf(
				"column|%s|%04d|%s|%s|%t|%s|%t|%t",
				table,
				position+1,
				column.Name,
				portableColumnTypeFamily(table, column.Name, column.Type),
				column.NotNull,
				column.Default,
				column.PrimaryKey,
				column.Generated,
			))
		}
	}
	lines = append(lines, semanticConstraintLines(t, db, tables)...)
	lines = append(lines, semanticUniqueConstraintLines(t, db, tables)...)
	lines = append(lines, semanticForeignKeyLines(t, db, tables)...)
	lines = append(lines, semanticIndexLines(t, db)...)
	slices.Sort(lines)
	return normalizedFingerprint(lines)
}

func semanticUniqueConstraintLines(t *testing.T, db *bun.DB, tables []string) []string {
	t.Helper()
	if db.Dialect().Name() == dialect.PG {
		rows, err := db.Query(`SELECT table_info.relname,
		       string_agg(column_info.attname, ',' ORDER BY key_column.position)
		FROM pg_constraint AS constraint_info
		JOIN pg_class AS table_info ON table_info.oid = constraint_info.conrelid
		JOIN pg_namespace AS namespace_info ON namespace_info.oid = table_info.relnamespace
		CROSS JOIN LATERAL unnest(constraint_info.conkey)
		    WITH ORDINALITY AS key_column(attnum, position)
		JOIN pg_attribute AS column_info
		  ON column_info.attrelid = table_info.oid AND column_info.attnum = key_column.attnum
		WHERE constraint_info.contype = 'u' AND namespace_info.nspname = current_schema()
		GROUP BY table_info.relname, constraint_info.conname
		ORDER BY table_info.relname, constraint_info.conname`)
		if err != nil {
			t.Fatalf("read PostgreSQL unique constraints: %v", err)
		}
		defer func() { _ = rows.Close() }()
		var lines []string
		for rows.Next() {
			var table, columns string
			if err := rows.Scan(&table, &columns); err != nil {
				t.Fatalf("scan PostgreSQL unique constraint: %v", err)
			}
			lines = append(lines, "unique|"+table+"|"+columns)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate PostgreSQL unique constraints: %v", err)
		}
		return lines
	}

	var lines []string
	for _, table := range tables {
		rows, err := db.Query(`SELECT name FROM pragma_index_list(?) WHERE origin = 'u' ORDER BY seq`, table)
		if err != nil {
			t.Fatalf("read SQLite unique constraints for %s: %v", table, err)
		}
		var indexes []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				_ = rows.Close()
				t.Fatalf("scan SQLite unique constraint for %s: %v", table, err)
			}
			indexes = append(indexes, name)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close SQLite unique constraints for %s: %v", table, err)
		}
		for _, index := range indexes {
			var columns []string
			if err := db.NewRaw(`SELECT name FROM pragma_index_info(?) ORDER BY seqno`, index).Scan(t.Context(), &columns); err != nil {
				t.Fatalf("read SQLite unique columns for %s: %v", index, err)
			}
			lines = append(lines, "unique|"+table+"|"+strings.Join(columns, ","))
		}
	}
	return lines
}

func applicationSchemaTables(t *testing.T, db *bun.DB) []string {
	t.Helper()
	query := `SELECT name FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		  AND name NOT IN ('bun_migrations', 'bun_migration_locks')
		ORDER BY name`
	if db.Dialect().Name() == dialect.PG {
		query = `SELECT table_name FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
			  AND table_name NOT IN ('bun_migrations', 'bun_migration_locks')
			ORDER BY table_name`
	}
	var tables []string
	if err := db.NewRaw(query).Scan(t.Context(), &tables); err != nil {
		t.Fatalf("read application schema tables: %v", err)
	}
	return tables
}

func semanticConstraintLines(t *testing.T, db *bun.DB, tables []string) []string {
	t.Helper()
	var lines []string
	if db.Dialect().Name() == dialect.PG {
		rows, err := db.Query(`SELECT table_info.relname, constraint_info.conname
			FROM pg_constraint AS constraint_info
			JOIN pg_class AS table_info ON table_info.oid = constraint_info.conrelid
			JOIN pg_namespace AS namespace_info ON namespace_info.oid = table_info.relnamespace
			WHERE namespace_info.nspname = current_schema()
			  AND (constraint_info.conname LIKE 'chk_%' OR constraint_info.conname LIKE 'uq_%' OR constraint_info.conname LIKE 'fk_%')
			ORDER BY table_info.relname, constraint_info.conname`)
		if err != nil {
			t.Fatalf("read PostgreSQL constraints: %v", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var table, name string
			if err := rows.Scan(&table, &name); err != nil {
				t.Fatalf("scan PostgreSQL constraint: %v", err)
			}
			lines = append(lines, "constraint|"+table+"|"+name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate PostgreSQL constraints: %v", err)
		}
		return lines
	}

	constraintPattern := regexp.MustCompile(`(?i)CONSTRAINT\s+([A-Za-z0-9_]+)`)
	for _, table := range tables {
		var ddl string
		if err := db.NewRaw(`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(t.Context(), &ddl); err != nil {
			t.Fatalf("read SQLite DDL for %s: %v", table, err)
		}
		for _, match := range constraintPattern.FindAllStringSubmatch(ddl, -1) {
			lines = append(lines, "constraint|"+table+"|"+match[1])
		}
	}
	return lines
}

func semanticForeignKeyLines(t *testing.T, db *bun.DB, tables []string) []string {
	t.Helper()
	var lines []string
	if db.Dialect().Name() == dialect.PG {
		rows, err := db.Query(`SELECT source_table.relname,
		       source_column.attname,
		       target_table.relname,
		       target_column.attname,
		       CASE constraint_info.confupdtype
		           WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE'
		           WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT'
		       END,
		       CASE constraint_info.confdeltype
		           WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE'
		           WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT'
		       END
		FROM pg_constraint AS constraint_info
		JOIN pg_class AS source_table ON source_table.oid = constraint_info.conrelid
		JOIN pg_namespace AS source_namespace ON source_namespace.oid = source_table.relnamespace
		JOIN pg_class AS target_table ON target_table.oid = constraint_info.confrelid
		CROSS JOIN LATERAL unnest(constraint_info.conkey, constraint_info.confkey)
		    WITH ORDINALITY AS key_pair(source_attnum, target_attnum, position)
		JOIN pg_attribute AS source_column
		  ON source_column.attrelid = source_table.oid AND source_column.attnum = key_pair.source_attnum
		JOIN pg_attribute AS target_column
		  ON target_column.attrelid = target_table.oid AND target_column.attnum = key_pair.target_attnum
		WHERE constraint_info.contype = 'f' AND source_namespace.nspname = current_schema()
		ORDER BY source_table.relname, constraint_info.conname, key_pair.position`)
		if err != nil {
			t.Fatalf("read PostgreSQL foreign keys: %v", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var sourceTable, sourceColumn, targetTable, targetColumn, onUpdate, onDelete string
			if err := rows.Scan(&sourceTable, &sourceColumn, &targetTable, &targetColumn, &onUpdate, &onDelete); err != nil {
				t.Fatalf("scan PostgreSQL foreign key: %v", err)
			}
			lines = append(lines, semanticForeignKeyLine(sourceTable, sourceColumn, targetTable, targetColumn, onUpdate, onDelete))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate PostgreSQL foreign keys: %v", err)
		}
		return lines
	}

	for _, table := range tables {
		rows, err := db.Query(`SELECT "from", "table", "to", on_update, on_delete FROM pragma_foreign_key_list(?) ORDER BY id, seq`, table)
		if err != nil {
			t.Fatalf("read SQLite foreign keys for %s: %v", table, err)
		}
		for rows.Next() {
			var sourceColumn, targetTable, targetColumn, onUpdate, onDelete string
			if err := rows.Scan(&sourceColumn, &targetTable, &targetColumn, &onUpdate, &onDelete); err != nil {
				_ = rows.Close()
				t.Fatalf("scan SQLite foreign key for %s: %v", table, err)
			}
			lines = append(lines, semanticForeignKeyLine(table, sourceColumn, targetTable, targetColumn, onUpdate, onDelete))
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close SQLite foreign keys for %s: %v", table, err)
		}
	}
	return lines
}

func semanticForeignKeyLine(sourceTable, sourceColumn, targetTable, targetColumn, onUpdate, onDelete string) string {
	return fmt.Sprintf("foreign-key|%s|%s|%s|%s|%s|%s", sourceTable, sourceColumn, targetTable, targetColumn, strings.ToUpper(onUpdate), strings.ToUpper(onDelete))
}

func semanticIndexLines(t *testing.T, db *bun.DB) []string {
	t.Helper()
	if db.Dialect().Name() == dialect.PG {
		return semanticPostgresIndexLines(t, db)
	}
	return semanticSQLiteIndexLines(t, db)
}

func semanticPostgresIndexLines(t *testing.T, db *bun.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT table_info.relname,
		       index_info.relname,
		       index_meta.indisunique,
			       COALESCE((
			           SELECT string_agg(
			               pg_get_indexdef(index_meta.indexrelid, position, TRUE) ||
			               CASE WHEN (index_meta.indoption[position - 1] & 1) = 1 THEN ' DESC' ELSE '' END,
			               '|' ORDER BY position
			           )
			           FROM generate_series(1, index_meta.indnkeyatts) AS position
			       ), ''),
		       COALESCE(pg_get_expr(index_meta.indpred, index_meta.indrelid), '')
		FROM pg_index AS index_meta
		JOIN pg_class AS table_info ON table_info.oid = index_meta.indrelid
		JOIN pg_namespace AS namespace_info ON namespace_info.oid = table_info.relnamespace
		JOIN pg_class AS index_info ON index_info.oid = index_meta.indexrelid
		WHERE namespace_info.nspname = current_schema()
		  AND index_info.relname LIKE 'idx_%'
		  AND index_info.relname NOT LIKE '%\_c' ESCAPE '\'
		ORDER BY table_info.relname, index_info.relname`)
	if err != nil {
		t.Fatalf("read PostgreSQL indexes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var table, name, columns, predicate string
		var unique bool
		if err := rows.Scan(&table, &name, &unique, &columns, &predicate); err != nil {
			t.Fatalf("scan PostgreSQL index: %v", err)
		}
		lines = append(lines, semanticIndexLine(table, name, unique, strings.Split(columns, "|"), predicate))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PostgreSQL indexes: %v", err)
	}
	return lines
}

func semanticSQLiteIndexLines(t *testing.T, db *bun.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT tbl_name, name, sql FROM sqlite_schema
		WHERE type = 'index' AND name LIKE 'idx_%'
		ORDER BY tbl_name, name`)
	if err != nil {
		t.Fatalf("read SQLite indexes: %v", err)
	}
	var indexes []struct {
		table string
		name  string
		ddl   string
	}
	for rows.Next() {
		var table, name, ddl string
		if err := rows.Scan(&table, &name, &ddl); err != nil {
			t.Fatalf("scan SQLite index: %v", err)
		}
		indexes = append(indexes, struct {
			table string
			name  string
			ddl   string
		}{table: table, name: name, ddl: ddl})
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close SQLite indexes: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SQLite indexes: %v", err)
	}

	var lines []string
	for _, index := range indexes {
		table, name, ddl := index.table, index.name, index.ddl
		columnRows, err := db.Query(`SELECT name, "desc" FROM pragma_index_xinfo(?) WHERE "key" = 1 ORDER BY seqno`, name)
		if err != nil {
			t.Fatalf("read SQLite index columns for %s: %v", name, err)
		}
		var columns []string
		for columnRows.Next() {
			var column sql.NullString
			var descending bool
			if err := columnRows.Scan(&column, &descending); err != nil {
				_ = columnRows.Close()
				t.Fatalf("scan SQLite index column for %s: %v", name, err)
			}
			value := column.String
			if descending {
				value += " DESC"
			}
			columns = append(columns, value)
		}
		if err := columnRows.Close(); err != nil {
			t.Fatalf("close SQLite index columns for %s: %v", name, err)
		}
		predicate := ""
		if where := strings.Index(strings.ToUpper(ddl), " WHERE "); where >= 0 {
			predicate = ddl[where+len(" WHERE "):]
		}
		unique := strings.HasPrefix(strings.ToUpper(ddl), "CREATE UNIQUE INDEX")
		lines = append(lines, semanticIndexLine(table, name, unique, columns, predicate))
	}
	return lines
}

func semanticIndexLine(table, name string, unique bool, columns []string, predicate string) string {
	for i := range columns {
		columns[i] = normalizedSQLExpression(columns[i])
	}
	return fmt.Sprintf("index|%s|%s|%t|%s|%s", table, name, unique, strings.Join(columns, ","), normalizedSQLExpression(predicate))
}

func normalizedSQLExpression(value string) string {
	value = strings.ToLower(value)
	value = postgresDefaultCast.ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, ` collate "c"`, "")
	value = strings.ReplaceAll(value, "= any (array[", " in ")
	value = strings.ReplaceAll(value, "<> all (array[", " not in ")
	value = strings.NewReplacer(
		"(", " ", ")", " ", "[", " ", "]", " ", ",", " ", `"`, "",
	).Replace(value)
	return strings.Join(strings.Fields(value), " ")
}

func portableSQLTypeFamily(value string) string {
	switch value {
	case "bigint", "integer":
		return "integer"
	case "jsonb", "json":
		return "json"
	default:
		return value
	}
}

func portableColumnTypeFamily(table, column, value string) string {
	if isInitialJSONColumn(table, column) {
		return "json"
	}
	return portableSQLTypeFamily(value)
}
