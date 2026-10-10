package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"

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

func applicationSchemaTables(t *testing.T, db *bun.DB) []string {
	t.Helper()
	tables, err := applicationTableNames(t.Context(), db)
	if err != nil {
		t.Fatalf("read application schema tables: %v", err)
	}
	return tables
}

func appliedTableColumns(t *testing.T, db *bun.DB, table string) []appliedColumn {
	t.Helper()
	columns, err := tableColumns(t.Context(), db, table)
	if err != nil {
		t.Fatal(err)
	}
	return columns
}

func testSchemaDialects(t *testing.T, test func(*testing.T, *bun.DB)) {
	t.Helper()
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		if err := ValidateTarget(ctx, db); err != nil {
			t.Fatalf("validate empty target: %v", err)
		}
		migrator := NewMigrator(db)
		if err := migrator.Init(ctx); err != nil {
			t.Fatalf("initialize migrator: %v", err)
		}
		if _, err := migrator.Migrate(ctx); err != nil {
			t.Fatalf("apply complete migration chain: %v", err)
		}
		test(t, db)
	})
}

// Compare native definitions within one database; no cross-dialect rendering is needed.
func appliedSchemaState(t *testing.T, db *bun.DB) []string {
	t.Helper()
	query := `SELECT type || '|' || name || '|' || sql FROM sqlite_schema
		WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
		AND tbl_name NOT IN ('bun_migrations', 'bun_migration_locks') ORDER BY 1`
	if db.Dialect().Name() == dialect.PG {
		query = `WITH app_tables AS (
			SELECT c.oid, c.relname, c.relkind FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = current_schema() AND c.relkind IN ('r','p','v','m')
			AND c.relname NOT IN ('bun_migrations', 'bun_migration_locks')
		)
		SELECT 'table|' || t.relname || '|' || t.relkind::text FROM app_tables t
		UNION ALL
		SELECT 'column|' || json_build_array(t.relname, a.attnum, a.attname,
			format_type(a.atttypid, a.atttypmod), a.attnotnull, a.attidentity, a.attgenerated,
			a.attcollation::regcollation::text, pg_get_expr(d.adbin, d.adrelid))::text
		FROM app_tables t JOIN pg_attribute a ON a.attrelid = t.oid
		LEFT JOIN pg_attrdef d ON d.adrelid = t.oid AND d.adnum = a.attnum
		WHERE a.attnum > 0 AND NOT a.attisdropped
		UNION ALL
		SELECT 'constraint|' || t.relname || '|' || c.conname || '|' || pg_get_constraintdef(c.oid)
		FROM app_tables t JOIN pg_constraint c ON c.conrelid = t.oid
		UNION ALL
		SELECT 'index|' || pg_get_indexdef(i.indexrelid) || '|' || i.indisvalid || '|' || i.indisready
		FROM app_tables t JOIN pg_index i ON i.indrelid = t.oid
		UNION ALL
		SELECT 'sequence|' || json_build_array(c.relname, format_type(s.seqtypid, -1),
			s.seqstart, s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle)::text
		FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid
		JOIN pg_depend d ON d.objid = s.seqrelid AND d.classid = 'pg_class'::regclass
			AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
		JOIN app_tables t ON t.oid = d.refobjid
		UNION ALL
		SELECT 'view|' || t.relname || '|' || pg_get_viewdef(t.oid) FROM app_tables t WHERE t.relkind IN ('v','m')
		UNION ALL
		SELECT 'trigger|' || pg_get_triggerdef(g.oid) FROM app_tables t
		JOIN pg_trigger g ON g.tgrelid = t.oid WHERE NOT g.tgisinternal
		ORDER BY 1`
	}
	var state []string
	if err := db.NewRaw(query).Scan(t.Context(), &state); err != nil {
		t.Fatalf("read application schema state: %v", err)
	}
	return state
}
