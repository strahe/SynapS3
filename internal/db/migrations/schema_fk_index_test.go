package migrations

import (
	"database/sql"
	"strconv"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func TestEveryForeignKeyHasLeadingChildIndexCoverage(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		if err := runMigrationBody(t.Context(), db, up2026090101InitialSchema); err != nil {
			t.Fatalf("create initial schema: %v", err)
		}
		for _, table := range applicationSchemaTables(t, db) {
			indexed := appliedLeadingIndexColumns(t, db, table)
			for _, foreignKey := range appliedForeignKeyLeadingColumns(t, db, table) {
				if !indexed[foreignKey.column] {
					t.Errorf("%s foreign key %s begins with unindexed child column %s", table, foreignKey.name, foreignKey.column)
				}
			}
		}
	})
}

type appliedForeignKeyLeadingColumn struct {
	name   string
	column string
}

func appliedForeignKeyLeadingColumns(t *testing.T, db *bun.DB, table string) []appliedForeignKeyLeadingColumn {
	t.Helper()
	if db.Dialect().Name() == dialect.PG {
		rows, err := db.Query(`SELECT constraint_info.conname, column_info.attname
			FROM pg_constraint AS constraint_info
			JOIN pg_class AS table_info ON table_info.oid = constraint_info.conrelid
			JOIN pg_namespace AS namespace_info ON namespace_info.oid = table_info.relnamespace
			CROSS JOIN LATERAL unnest(constraint_info.conkey)
			    WITH ORDINALITY AS key_column(attnum, position)
			JOIN pg_attribute AS column_info
			  ON column_info.attrelid = table_info.oid AND column_info.attnum = key_column.attnum
			WHERE constraint_info.contype = 'f'
			  AND namespace_info.nspname = current_schema()
			  AND table_info.relname = ?
			  AND key_column.position = 1
			ORDER BY constraint_info.conname`, table)
		if err != nil {
			t.Fatalf("read PostgreSQL foreign keys for %s: %v", table, err)
		}
		defer func() { _ = rows.Close() }()
		var result []appliedForeignKeyLeadingColumn
		for rows.Next() {
			var item appliedForeignKeyLeadingColumn
			if err := rows.Scan(&item.name, &item.column); err != nil {
				t.Fatalf("scan PostgreSQL foreign key for %s: %v", table, err)
			}
			result = append(result, item)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate PostgreSQL foreign keys for %s: %v", table, err)
		}
		return result
	}

	rows, err := db.Query(`SELECT id, "from" FROM pragma_foreign_key_list(?) WHERE seq = 0 ORDER BY id`, table)
	if err != nil {
		t.Fatalf("read SQLite foreign keys for %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var result []appliedForeignKeyLeadingColumn
	for rows.Next() {
		var id int
		var column string
		if err := rows.Scan(&id, &column); err != nil {
			t.Fatalf("scan SQLite foreign key for %s: %v", table, err)
		}
		result = append(result, appliedForeignKeyLeadingColumn{name: table + "#" + strconv.Itoa(id), column: column})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SQLite foreign keys for %s: %v", table, err)
	}
	return result
}

func appliedLeadingIndexColumns(t *testing.T, db *bun.DB, table string) map[string]bool {
	t.Helper()
	indexed := make(map[string]bool)
	if db.Dialect().Name() == dialect.PG {
		rows, err := db.Query(`SELECT column_info.attname
			FROM pg_index AS index_info
			JOIN pg_class AS table_info ON table_info.oid = index_info.indrelid
			JOIN pg_namespace AS namespace_info ON namespace_info.oid = table_info.relnamespace
			JOIN pg_attribute AS column_info
			  ON column_info.attrelid = table_info.oid
			 AND column_info.attnum = index_info.indkey[0]
			WHERE namespace_info.nspname = current_schema()
			  AND table_info.relname = ?`, table)
		if err != nil {
			t.Fatalf("read PostgreSQL indexes for %s: %v", table, err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				t.Fatalf("scan PostgreSQL index for %s: %v", table, err)
			}
			indexed[column] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate PostgreSQL indexes for %s: %v", table, err)
		}
		return indexed
	}

	rows, err := db.Query(`SELECT name, pk FROM pragma_table_info(?)`, table)
	if err != nil {
		t.Fatalf("read SQLite primary key for %s: %v", table, err)
	}
	for rows.Next() {
		var column string
		var primaryKey int
		if err := rows.Scan(&column, &primaryKey); err != nil {
			_ = rows.Close()
			t.Fatalf("scan SQLite primary key for %s: %v", table, err)
		}
		if primaryKey == 1 {
			indexed[column] = true
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close SQLite primary key rows for %s: %v", table, err)
	}

	indexRows, err := db.Query(`SELECT name FROM pragma_index_list(?)`, table)
	if err != nil {
		t.Fatalf("read SQLite index list for %s: %v", table, err)
	}
	var indexNames []string
	for indexRows.Next() {
		var name string
		if err := indexRows.Scan(&name); err != nil {
			_ = indexRows.Close()
			t.Fatalf("scan SQLite index list for %s: %v", table, err)
		}
		indexNames = append(indexNames, name)
	}
	if err := indexRows.Close(); err != nil {
		t.Fatalf("close SQLite index list for %s: %v", table, err)
	}
	for _, index := range indexNames {
		var column sql.NullString
		if err := db.NewRaw(`SELECT name FROM pragma_index_xinfo(?) WHERE seqno = 0 AND "key" = 1`, index).Scan(t.Context(), &column); err != nil {
			t.Fatalf("read SQLite leading index column for %s: %v", index, err)
		}
		if column.Valid {
			indexed[column.String] = true
		}
	}
	return indexed
}
