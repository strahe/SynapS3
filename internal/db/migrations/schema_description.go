package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
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
