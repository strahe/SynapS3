package migrations

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

const sqliteRebuildRestoreTimeout = 5 * time.Second

// sqliteTableRebuild describes one SQLite table rebuild, the only way SQLite
// changes a CHECK or foreign key on an existing table.
type sqliteTableRebuild struct {
	// table keeps its name once rebuilt.
	table string
	// create creates the table's new shape under the name it is given.
	create func(ctx context.Context, tx bun.Tx, name string) error
	// copy moves the rows into the new table. Without it every column the old
	// and new tables share is copied.
	copy func(ctx context.Context, tx bun.Tx, from, to string) error
	// indexes replace the old table's index DDL when set.
	indexes []string
}

// rebuildSQLiteTables follows SQLite's documented rebuild for each table: the
// new shape is created beside the old table, filled, and renamed into its
// place. Foreign keys must be off while an old table is dropped, or SQLite
// deletes its rows first and cascades into every child. SQLite ignores that
// pragma inside a transaction, so the rebuild runs on its own connection
// outside the migration transaction. Every table is rebuilt in one transaction,
// which keeps the migration atomic and commits only if no row it touched breaks
// a foreign key. Rebuilding to the same shape again changes nothing, so a
// migration using it can rerun after Bun loses its marker.
func rebuildSQLiteTables(ctx context.Context, db *bun.DB, rebuilds ...sqliteTableRebuild) (err error) {
	if db.Dialect().Name() != dialect.SQLite {
		return errors.New("rebuilding tables: only SQLite tables are rebuilt")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("rebuilding tables: %w", err)
	}
	defer func() { err = errors.Join(err, releaseSQLiteRebuildConnection(ctx, conn)) }()
	if err := setSQLiteForeignKeys(ctx, conn, false); err != nil {
		return fmt.Errorf("rebuilding tables: %w", err)
	}
	return conn.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for _, rebuild := range rebuilds {
			if err := rebuildSQLiteTableInTx(ctx, tx, rebuild); err != nil {
				return fmt.Errorf("rebuilding %s: %w", rebuild.table, err)
			}
		}
		// A foreign key between two rebuilt tables holds only once both have
		// their new shape.
		for _, rebuild := range rebuilds {
			if err := checkSQLiteForeignKeys(ctx, tx, rebuild.table); err != nil {
				return fmt.Errorf("rebuilding %s: %w", rebuild.table, err)
			}
		}
		return nil
	})
}

func rebuildSQLiteTableInTx(ctx context.Context, tx bun.Tx, rebuild sqliteTableRebuild) error {
	table := rebuild.table
	var dependents int
	if err := tx.NewRaw(`SELECT COUNT(*) FROM sqlite_schema WHERE (type = 'trigger' AND tbl_name = ?) OR type = 'view'`, table).Scan(ctx, &dependents); err != nil {
		return err
	}
	if dependents > 0 {
		return errors.New("triggers and views are not carried across a rebuild")
	}
	indexes := rebuild.indexes
	if indexes == nil {
		if err := tx.NewRaw(`SELECT sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name`, table).Scan(ctx, &indexes); err != nil {
			return err
		}
	}
	sequence, err := sqliteSequence(ctx, tx, table)
	if err != nil {
		return err
	}
	replacement := table + "__rebuild"
	if err := rebuild.create(ctx, tx, replacement); err != nil {
		return fmt.Errorf("creating the new table: %w", err)
	}
	copyRows := rebuild.copy
	if copyRows == nil {
		copyRows = copySharedSQLiteColumns
	}
	if err := copyRows(ctx, tx, table, replacement); err != nil {
		return fmt.Errorf("copying rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE "+quoteSQLiteName(table)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "ALTER TABLE "+quoteSQLiteName(replacement)+" RENAME TO "+quoteSQLiteName(table)); err != nil {
		return err
	}
	for _, index := range indexes {
		if _, err := tx.ExecContext(ctx, index); err != nil {
			return fmt.Errorf("recreating index: %w", err)
		}
	}
	return restoreSQLiteSequence(ctx, tx, table, sequence)
}

// sqliteSequence reads a table's AUTOINCREMENT high-water mark, which SQLite
// forgets when the table is dropped.
func sqliteSequence(ctx context.Context, tx bun.Tx, table string) (sql.NullInt64, error) {
	var sequence sql.NullInt64
	exists, err := tableExists(ctx, tx, "sqlite_sequence")
	if err != nil || !exists {
		return sequence, err
	}
	err = tx.NewRaw(`SELECT MAX(seq) FROM sqlite_sequence WHERE name = ?`, table).Scan(ctx, &sequence)
	return sequence, err
}

// restoreSQLiteSequence gives the rebuilt table its old high-water mark back.
// The copied rows raise it only to the largest id left, so without it an id
// whose row was deleted would be handed out again.
func restoreSQLiteSequence(ctx context.Context, tx bun.Tx, table string, sequence sql.NullInt64) error {
	if !sequence.Valid {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sqlite_sequence (name, seq) SELECT ?, 0
		WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = ?)`, table, table); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE sqlite_sequence SET seq = MAX(seq, ?) WHERE name = ?`, sequence.Int64, table)
	return err
}

func copySharedSQLiteColumns(ctx context.Context, tx bun.Tx, from, to string) error {
	var fromColumns, toColumns []string
	if err := tx.NewRaw(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, from).Scan(ctx, &fromColumns); err != nil {
		return err
	}
	if err := tx.NewRaw(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, to).Scan(ctx, &toColumns); err != nil {
		return err
	}
	var columns []string
	for _, column := range toColumns {
		if slices.Contains(fromColumns, column) {
			columns = append(columns, quoteSQLiteName(column))
		}
	}
	list := strings.Join(columns, ", ")
	_, err := tx.ExecContext(ctx, "INSERT INTO "+quoteSQLiteName(to)+" ("+list+") SELECT "+list+" FROM "+quoteSQLiteName(from))
	return err
}

// checkSQLiteForeignKeys checks the rebuilt table's own references and every
// table referencing it, which are the only rows a rebuild can orphan.
func checkSQLiteForeignKeys(ctx context.Context, tx bun.Tx, table string) error {
	var children []string
	if err := tx.NewRaw(`SELECT DISTINCT parent.name FROM sqlite_schema AS parent, pragma_foreign_key_list(parent.name) AS reference
		WHERE parent.type = 'table' AND reference."table" = ?`, table).Scan(ctx, &children); err != nil {
		return err
	}
	for _, checked := range append([]string{table}, children...) {
		rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check("+quoteSQLiteName(checked)+")")
		if err != nil {
			return err
		}
		violated := rows.Next()
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		if violated {
			return fmt.Errorf("rows of %s break a foreign key after the rebuild", checked)
		}
	}
	return nil
}

func setSQLiteForeignKeys(ctx context.Context, conn bun.Conn, on bool) error {
	want := 0
	if on {
		want = 1
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA foreign_keys = %d", want)); err != nil {
		return err
	}
	var got int
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&got); err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("foreign_keys = %d, want %d", got, want)
	}
	return nil
}

// releaseSQLiteRebuildConnection returns the connection to the pool only once
// foreign keys are back on; one that cannot be restored is discarded.
func releaseSQLiteRebuildConnection(ctx context.Context, conn bun.Conn) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sqliteRebuildRestoreTimeout)
	defer cancel()
	if err := setSQLiteForeignKeys(ctx, conn, true); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return fmt.Errorf("restoring foreign keys on the rebuild connection: %w", err)
	}
	return conn.Close()
}

func quoteSQLiteName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
