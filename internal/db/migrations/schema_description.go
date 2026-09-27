package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// appliedColumn is one column as the database reports it.
type appliedColumn struct {
	Name       string
	Type       string
	NotNull    bool
	Default    string
	PrimaryKey bool
	Generated  bool
}

// describeSchema lists the application schema as sorted lines: tables,
// columns, named constraints with the expression of every CHECK, unique
// constraints, foreign keys, and indexes. A portable description keeps only
// what PostgreSQL and SQLite share, so a PostgreSQL schema can be compared with
// the SQLite reference; otherwise SQLite is described as its DDL declares it.
func describeSchema(ctx context.Context, db bun.IDB, portable bool) ([]string, error) {
	tables, err := applicationTableNames(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("reading application tables: %w", err)
	}
	lines := make([]string, 0, len(tables)*16)
	for _, table := range tables {
		columns, err := tableColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		for position, column := range columns {
			columnType := column.Type
			if portable {
				columnType = portableColumnType(db.Dialect().Name(), table, column.Name, column.Type)
			}
			lines = append(lines, fmt.Sprintf(
				"column|%s|%04d|%s|%s|%t|%s|%t|%t",
				table,
				position+1,
				column.Name,
				columnType,
				column.NotNull,
				column.Default,
				column.PrimaryKey,
				column.Generated,
			))
		}
	}
	for _, read := range []func(context.Context, bun.IDB, []string) ([]string, error){
		constraintLines,
		uniqueConstraintLines,
		foreignKeyLines,
	} {
		more, err := read(ctx, db, tables)
		if err != nil {
			return nil, err
		}
		lines = append(lines, more...)
	}
	indexes, err := indexLines(ctx, db, tables, portable)
	if err != nil {
		return nil, err
	}
	lines = append(lines, indexes...)
	if portable {
		// CHECK expressions are written per dialect, and only PostgreSQL has the
		// C-collated companion indexes, whose names end in _c.
		lines = slices.DeleteFunc(lines, func(line string) bool {
			fields := strings.Split(line, "|")
			return fields[0] == "check" || (fields[0] == "index" && strings.HasSuffix(fields[2], "_c"))
		})
	}
	slices.Sort(lines)
	return lines, nil
}

func tableColumns(ctx context.Context, db bun.IDB, table string) ([]appliedColumn, error) {
	if db.Dialect().Name() == dialect.PG {
		return postgresColumns(ctx, db, table)
	}
	return sqliteColumns(ctx, db, table)
}

func sqliteColumns(ctx context.Context, db bun.IDB, table string) ([]appliedColumn, error) {
	ddl, err := sqliteTableDDL(ctx, db, table)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		return nil, fmt.Errorf("reading columns of %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []appliedColumn
	for rows.Next() {
		var name, sqlType string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&name, &sqlType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("reading columns of %s: %w", table, err)
		}
		columns = append(columns, appliedColumn{
			Name:       name,
			Type:       normalizedSQLType(sqlType),
			NotNull:    notNull != 0,
			Default:    normalizedSQLDefault(defaultValue.String),
			PrimaryKey: primaryKey != 0,
			Generated:  primaryKey != 0 && name == "id" && strings.Contains(strings.ToUpper(ddl), "AUTOINCREMENT"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading columns of %s: %w", table, err)
	}
	return columns, nil
}

func postgresColumns(ctx context.Context, db bun.IDB, table string) ([]appliedColumn, error) {
	rows, err := db.QueryContext(ctx, `SELECT column_info.column_name,
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
		return nil, fmt.Errorf("reading columns of %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []appliedColumn
	for rows.Next() {
		var name, sqlType string
		var notNull, generated, primaryKey bool
		var defaultValue sql.NullString
		if err := rows.Scan(&name, &sqlType, &notNull, &defaultValue, &generated, &primaryKey); err != nil {
			return nil, fmt.Errorf("reading columns of %s: %w", table, err)
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
		return nil, fmt.Errorf("reading columns of %s: %w", table, err)
	}
	return columns, nil
}

// constraintLines lists the migration-named constraints and the expression of
// every CHECK. PostgreSQL renders its own CHECK text and SQLite keeps the text
// the migration wrote, so CHECK expressions only compare within one dialect.
func constraintLines(ctx context.Context, db bun.IDB, tables []string) ([]string, error) {
	if db.Dialect().Name() == dialect.PG {
		return postgresConstraintLines(ctx, db, tables)
	}
	return sqliteConstraintLines(ctx, db, tables)
}

func postgresConstraintLines(ctx context.Context, db bun.IDB, tables []string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT table_info.relname,
		       constraint_info.conname,
		       constraint_info.contype = 'c',
		       pg_get_constraintdef(constraint_info.oid, TRUE)
		FROM pg_constraint AS constraint_info
		JOIN pg_class AS table_info ON table_info.oid = constraint_info.conrelid
		JOIN pg_namespace AS namespace_info ON namespace_info.oid = table_info.relnamespace
		WHERE namespace_info.nspname = current_schema()
		ORDER BY table_info.relname, constraint_info.conname`)
	if err != nil {
		return nil, fmt.Errorf("reading constraints: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var table, name, definition string
		var check bool
		if err := rows.Scan(&table, &name, &check, &definition); err != nil {
			return nil, fmt.Errorf("reading constraints: %w", err)
		}
		if !slices.Contains(tables, table) {
			continue
		}
		if migrationConstraintName(name) {
			lines = append(lines, "constraint|"+table+"|"+name)
		}
		if check {
			lines = append(lines, checkLine(table, name, strings.TrimPrefix(definition, "CHECK ")))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading constraints: %w", err)
	}
	return lines, nil
}

var sqliteNamedConstraint = regexp.MustCompile(`(?i)\bCONSTRAINT\s+"?([A-Za-z0-9_]+)"?\s+(CHECK\s*\()?`)

func sqliteConstraintLines(ctx context.Context, db bun.IDB, tables []string) ([]string, error) {
	var lines []string
	for _, table := range tables {
		ddl, err := sqliteTableDDL(ctx, db, table)
		if err != nil {
			return nil, err
		}
		for _, match := range sqliteNamedConstraint.FindAllStringSubmatchIndex(ddl, -1) {
			name := ddl[match[2]:match[3]]
			lines = append(lines, "constraint|"+table+"|"+name)
			if match[4] < 0 {
				continue
			}
			open := match[5] - 1
			end, err := closingParenthesis(ddl, open)
			if err != nil {
				return nil, fmt.Errorf("reading CHECK %s on %s: %w", name, table, err)
			}
			lines = append(lines, checkLine(table, name, ddl[open+1:end]))
		}
	}
	return lines, nil
}

func migrationConstraintName(name string) bool {
	return strings.HasPrefix(name, "chk_") || strings.HasPrefix(name, "uq_") || strings.HasPrefix(name, "fk_")
}

// checkLine keeps the grouping of the expression: CHECK lines are compared only
// within SQLite, where the text is the one the migration wrote.
func checkLine(table, name, expression string) string {
	return "check|" + table + "|" + name + "|" + sqlExpression(expression)
}

// closingParenthesis finds the parenthesis closing the one at open, skipping
// quoted text such as a GLOB pattern.
func closingParenthesis(text string, open int) (int, error) {
	depth := 0
	var quote byte
	for i := open; i < len(text); i++ {
		c := text[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(text) && text[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return 0, errors.New("unbalanced parentheses")
}

func sqliteTableDDL(ctx context.Context, db bun.IDB, table string) (string, error) {
	var ddl string
	if err := db.NewRaw(`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(ctx, &ddl); err != nil {
		return "", fmt.Errorf("reading DDL of %s: %w", table, err)
	}
	return ddl, nil
}

func uniqueConstraintLines(ctx context.Context, db bun.IDB, tables []string) ([]string, error) {
	if db.Dialect().Name() == dialect.PG {
		rows, err := db.QueryContext(ctx, `SELECT table_info.relname,
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
			return nil, fmt.Errorf("reading unique constraints: %w", err)
		}
		defer func() { _ = rows.Close() }()
		var lines []string
		for rows.Next() {
			var table, columns string
			if err := rows.Scan(&table, &columns); err != nil {
				return nil, fmt.Errorf("reading unique constraints: %w", err)
			}
			if slices.Contains(tables, table) {
				lines = append(lines, "unique|"+table+"|"+columns)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("reading unique constraints: %w", err)
		}
		return lines, nil
	}

	var lines []string
	for _, table := range tables {
		var indexes []string
		if err := db.NewRaw(`SELECT name FROM pragma_index_list(?) WHERE origin = 'u' ORDER BY seq`, table).Scan(ctx, &indexes); err != nil {
			return nil, fmt.Errorf("reading unique constraints of %s: %w", table, err)
		}
		for _, index := range indexes {
			var columns []string
			if err := db.NewRaw(`SELECT name FROM pragma_index_info(?) ORDER BY seqno`, index).Scan(ctx, &columns); err != nil {
				return nil, fmt.Errorf("reading unique columns of %s: %w", index, err)
			}
			lines = append(lines, "unique|"+table+"|"+strings.Join(columns, ","))
		}
	}
	return lines, nil
}

func foreignKeyLines(ctx context.Context, db bun.IDB, tables []string) ([]string, error) {
	if db.Dialect().Name() == dialect.PG {
		rows, err := db.QueryContext(ctx, `SELECT source_table.relname,
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
			return nil, fmt.Errorf("reading foreign keys: %w", err)
		}
		defer func() { _ = rows.Close() }()
		var lines []string
		for rows.Next() {
			var sourceTable, sourceColumn, targetTable, targetColumn, onUpdate, onDelete string
			if err := rows.Scan(&sourceTable, &sourceColumn, &targetTable, &targetColumn, &onUpdate, &onDelete); err != nil {
				return nil, fmt.Errorf("reading foreign keys: %w", err)
			}
			if slices.Contains(tables, sourceTable) {
				lines = append(lines, foreignKeyLine(sourceTable, sourceColumn, targetTable, targetColumn, onUpdate, onDelete))
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("reading foreign keys: %w", err)
		}
		return lines, nil
	}

	var lines []string
	for _, table := range tables {
		rows, err := db.QueryContext(ctx, `SELECT "from", "table", "to", on_update, on_delete FROM pragma_foreign_key_list(?) ORDER BY id, seq`, table)
		if err != nil {
			return nil, fmt.Errorf("reading foreign keys of %s: %w", table, err)
		}
		for rows.Next() {
			var sourceColumn, targetTable, targetColumn, onUpdate, onDelete string
			if err := rows.Scan(&sourceColumn, &targetTable, &targetColumn, &onUpdate, &onDelete); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("reading foreign keys of %s: %w", table, err)
			}
			lines = append(lines, foreignKeyLine(table, sourceColumn, targetTable, targetColumn, onUpdate, onDelete))
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("reading foreign keys of %s: %w", table, err)
		}
	}
	return lines, nil
}

func foreignKeyLine(sourceTable, sourceColumn, targetTable, targetColumn, onUpdate, onDelete string) string {
	return fmt.Sprintf("foreign-key|%s|%s|%s|%s|%s|%s", sourceTable, sourceColumn, targetTable, targetColumn, strings.ToUpper(onUpdate), strings.ToUpper(onDelete))
}

func indexLines(ctx context.Context, db bun.IDB, tables []string, portable bool) ([]string, error) {
	if db.Dialect().Name() == dialect.PG {
		return postgresIndexLines(ctx, db, tables)
	}
	return sqliteIndexLines(ctx, db, tables, portable)
}

func postgresIndexLines(ctx context.Context, db bun.IDB, tables []string) ([]string, error) {
	// Indexes backing a primary key or unique constraint are already covered
	// by the columns and unique constraints.
	rows, err := db.QueryContext(ctx, `SELECT table_info.relname,
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
		  AND NOT EXISTS (
		      SELECT 1 FROM pg_constraint AS constraint_info
		      WHERE constraint_info.conindid = index_meta.indexrelid
		        AND constraint_info.contype IN ('p', 'u', 'x')
		  )
		ORDER BY table_info.relname, index_info.relname`)
	if err != nil {
		return nil, fmt.Errorf("reading indexes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var table, name, columns, predicate string
		var unique bool
		if err := rows.Scan(&table, &name, &unique, &columns, &predicate); err != nil {
			return nil, fmt.Errorf("reading indexes: %w", err)
		}
		if slices.Contains(tables, table) {
			lines = append(lines, indexLine(table, name, unique, strings.Split(columns, "|"), predicate, true))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading indexes: %w", err)
	}
	return lines, nil
}

func sqliteIndexLines(ctx context.Context, db bun.IDB, tables []string, portable bool) ([]string, error) {
	// An index without DDL backs a primary key or unique constraint, which the
	// columns and unique constraints already cover.
	rows, err := db.QueryContext(ctx, `SELECT tbl_name, name, sql FROM sqlite_schema
		WHERE type = 'index' AND sql IS NOT NULL
		ORDER BY tbl_name, name`)
	if err != nil {
		return nil, fmt.Errorf("reading indexes: %w", err)
	}
	type sqliteIndex struct{ table, name, ddl string }
	var indexes []sqliteIndex
	for rows.Next() {
		var index sqliteIndex
		if err := rows.Scan(&index.table, &index.name, &index.ddl); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("reading indexes: %w", err)
		}
		indexes = append(indexes, index)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("reading indexes: %w", err)
	}
	var lines []string
	for _, index := range indexes {
		unique := strings.HasPrefix(strings.ToUpper(index.ddl), "CREATE UNIQUE INDEX")
		if !slices.Contains(tables, index.table) {
			continue
		}
		columns, err := sqliteIndexColumns(ctx, db, index.name)
		if err != nil {
			return nil, err
		}
		predicate := ""
		if where := strings.Index(strings.ToUpper(index.ddl), " WHERE "); where >= 0 {
			predicate = index.ddl[where+len(" WHERE "):]
		}
		lines = append(lines, indexLine(index.table, index.name, unique, columns, predicate, portable))
	}
	return lines, nil
}

func sqliteIndexColumns(ctx context.Context, db bun.IDB, index string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name, "desc" FROM pragma_index_xinfo(?) WHERE "key" = 1 ORDER BY seqno`, index)
	if err != nil {
		return nil, fmt.Errorf("reading columns of index %s: %w", index, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []string
	for rows.Next() {
		var column sql.NullString
		var descending bool
		if err := rows.Scan(&column, &descending); err != nil {
			return nil, fmt.Errorf("reading columns of index %s: %w", index, err)
		}
		value := column.String
		if descending {
			value += " DESC"
		}
		columns = append(columns, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading columns of index %s: %w", index, err)
	}
	return columns, nil
}

func indexLine(table, name string, unique bool, columns []string, predicate string, portable bool) string {
	normalize := sqlExpression
	if portable {
		normalize = portableSQLExpression
	}
	for i := range columns {
		columns[i] = normalize(columns[i])
	}
	return fmt.Sprintf("index|%s|%s|%t|%s|%s", table, name, unique, strings.Join(columns, ","), normalize(predicate))
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

// sqlExpression folds case, identifier quotes and spacing, and keeps the
// grouping that decides what the expression means.
func sqlExpression(value string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(strings.ToLower(value), `"`, "")), " ")
}

// portableSQLExpression also drops what only PostgreSQL renders: casts, the C
// collation, ANY(ARRAY[...]) lists, and its added parentheses. Dropping every
// parenthesis loses grouping, so only a PostgreSQL comparison uses it.
func portableSQLExpression(value string) string {
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

// portableColumnType maps a column type to the family both dialects share. A
// JSON column is jsonb or text depending on the dialect, so it joins the json
// family only while it has the type its dialect declares.
func portableColumnType(name dialect.Name, table, column, value string) string {
	if spec, ok := initialJSONColumn(table, column); ok {
		declared := "jsonb"
		if name == dialect.SQLite || spec.text {
			declared = "text"
		}
		if value == declared {
			return "json"
		}
		return value
	}
	switch value {
	case "bigint", "integer":
		return "integer"
	case "jsonb", "json":
		return "json"
	default:
		return value
	}
}
