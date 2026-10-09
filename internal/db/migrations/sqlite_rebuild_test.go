package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"
)

// newSQLiteRebuildDB opens a migrated file database with a connection pool, so
// a test can see whether the rebuild connection returned with foreign keys on.
func newSQLiteRebuildDB(t *testing.T) *bun.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rebuild.db")
	sqldb, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	sqldb.SetMaxOpenConns(4)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	migrateToLevel(t, db, len(Migrations.Sorted()))
	return db
}

// withConstraint creates a table's shape as of DDL under a new name, plus one
// constraint, as a migration adding a CHECK would.
func withConstraint(ddl, constraint string) func(context.Context, bun.Tx, string) error {
	return func(ctx context.Context, tx bun.Tx, name string) error {
		columns := strings.TrimSuffix(strings.TrimSpace(ddl[strings.Index(ddl, "("):]), ")")
		_, err := tx.ExecContext(ctx, "CREATE TABLE "+quoteSQLiteName(name)+" "+columns+", "+constraint+")")
		return err
	}
}

func currentTableDDL(t *testing.T, db bun.IDB, table string) string {
	t.Helper()
	ddl, err := sqliteTableDDL(t.Context(), db, table)
	if err != nil {
		t.Fatal(err)
	}
	return ddl
}

// tableSnapshot renders every row and index of a table.
func tableSnapshot(t *testing.T, db *bun.DB, table string) string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+quoteSQLiteName(table)+" ORDER BY rowid")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var snapshot strings.Builder
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(&snapshot, values...)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	var indexes []string
	if err := db.NewRaw(`SELECT sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name`, table).
		Scan(t.Context(), &indexes); err != nil {
		t.Fatal(err)
	}
	return snapshot.String() + strings.Join(indexes, "\n")
}

func assertPoolEnforcesForeignKeys(t *testing.T, db *bun.DB) {
	t.Helper()
	var conns []bun.Conn
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for range 4 {
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		var enabled int
		if err := conn.QueryRowContext(t.Context(), "PRAGMA foreign_keys").Scan(&enabled); err != nil {
			t.Fatal(err)
		}
		if enabled != 1 {
			t.Fatalf("pooled connection %d has foreign_keys = %d", len(conns), enabled)
		}
	}
}

// seedCascadeChildren gives each parent the SQLite rebuild must not empty a
// child that ON DELETE CASCADE would otherwise remove with it.
func seedCascadeChildren(t *testing.T, db *bun.DB) {
	t.Helper()
	for i := range 3 {
		insertBaselineTestTask(t, db, fmt.Sprintf("rebuild-%d", i))
	}
	bucketID := insertBaselineTestBucket(t, db, "rebuild-bucket")
	for i := range 2 {
		uploadID := fmt.Sprintf("upload-%d", i)
		if _, err := db.Exec(`INSERT INTO multipart_uploads (bucket_id, key, upload_id, created_at, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, bucketID, uploadID+".bin", uploadID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO multipart_parts (upload_id, part_number, size, e_tag, created_at)
			VALUES (?, 1, 10, 'part-etag', CURRENT_TIMESTAMP)`, uploadID); err != nil {
			t.Fatal(err)
		}
		dataSetID := insertBaselineTestDataSet(t, db, bucketID, fmt.Sprintf("10%d", i), i, 1, true)
		if _, err := db.Exec(`INSERT INTO observability_data_set_states
			(local_data_set_id, bucket_id, copy_index, provider_id, status, reason_codes, last_checked_at, evidence_json)
			VALUES (?, ?, ?, ?, 'available', '[]', CURRENT_TIMESTAMP, '{}')`, dataSetID, bucketID, i, fmt.Sprintf("10%d", i)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteRebuildKeepsCascadingChildRows(t *testing.T) {
	for parent, child := range map[string]string{
		"multipart_uploads": "multipart_parts",
		"storage_data_sets": "observability_data_set_states",
	} {
		t.Run(parent, func(t *testing.T) {
			db := newSQLiteRebuildDB(t)
			seedCascadeChildren(t, db)
			parentBefore, childBefore := tableSnapshot(t, db, parent), tableSnapshot(t, db, child)
			if strings.Count(childBefore, "\n") < 2 {
				t.Fatalf("%s seeded too few rows:\n%s", child, childBefore)
			}

			if err := rebuildSQLiteTables(t.Context(), db, sqliteTableRebuild{
				table:  parent,
				create: withConstraint(currentTableDDL(t, db, parent), "CONSTRAINT chk_rebuild_probe CHECK (created_at IS NOT NULL)"),
			}); err != nil {
				t.Fatalf("rebuild %s: %v", parent, err)
			}

			if got := tableSnapshot(t, db, parent); got != parentBefore {
				t.Fatalf("%s after rebuild:\n%s\nwant:\n%s", parent, got, parentBefore)
			}
			if got := tableSnapshot(t, db, child); got != childBefore {
				t.Fatalf("%s after rebuilding %s:\n%s\nwant:\n%s", child, parent, got, childBefore)
			}
			if !strings.Contains(currentTableDDL(t, db, parent), "chk_rebuild_probe") {
				t.Fatalf("%s kept its old shape", parent)
			}
			assertPoolEnforcesForeignKeys(t, db)
		})
	}
}

func TestSQLiteRebuildRollsBackWhenRowsDoNotFit(t *testing.T) {
	for name, rebuilds := range map[string]func(tasks, uploads string) []sqliteTableRebuild{
		"rows break the new CHECK": func(tasks, _ string) []sqliteTableRebuild {
			return []sqliteTableRebuild{{table: "tasks", create: withConstraint(tasks, "CONSTRAINT chk_rebuild_probe CHECK (id < 0)")}}
		},
		"copy orphans child rows": func(_, uploads string) []sqliteTableRebuild {
			return []sqliteTableRebuild{{
				table:  "multipart_uploads",
				create: withConstraint(uploads, "CONSTRAINT chk_rebuild_probe CHECK (upload_id <> '')"),
				copy: func(ctx context.Context, tx bun.Tx, from, to string) error {
					_, err := tx.ExecContext(ctx, "INSERT INTO "+quoteSQLiteName(to)+" SELECT * FROM "+quoteSQLiteName(from)+" WHERE upload_id > (SELECT MIN(upload_id) FROM "+quoteSQLiteName(from)+")")
					return err
				},
			}}
		},
		// The tables of one rebuild commit together or not at all.
		"a later table fails": func(tasks, uploads string) []sqliteTableRebuild {
			return []sqliteTableRebuild{
				{table: "multipart_uploads", create: withConstraint(uploads, "CONSTRAINT chk_rebuild_probe CHECK (created_at IS NOT NULL)")},
				{table: "tasks", create: withConstraint(tasks, "CONSTRAINT chk_rebuild_probe CHECK (id < 0)")},
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			db := newSQLiteRebuildDB(t)
			seedCascadeChildren(t, db)
			tasksDDL, uploadsDDL := currentTableDDL(t, db, "tasks"), currentTableDDL(t, db, "multipart_uploads")
			before := make(map[string]string)
			for _, table := range []string{"tasks", "multipart_uploads", "multipart_parts"} {
				before[table] = tableSnapshot(t, db, table)
			}

			if err := rebuildSQLiteTables(t.Context(), db, rebuilds(tasksDDL, uploadsDDL)...); err == nil {
				t.Fatal("rebuild succeeded, want it rolled back")
			}

			if currentTableDDL(t, db, "tasks") != tasksDDL || currentTableDDL(t, db, "multipart_uploads") != uploadsDDL {
				t.Fatal("a failed rebuild changed a table's shape")
			}
			for table, snapshot := range before {
				if tableSnapshot(t, db, table) != snapshot {
					t.Fatalf("a failed rebuild changed the rows of %s", table)
				}
			}
			assertPoolEnforcesForeignKeys(t, db)
		})
	}
}

// SQLite forgets a dropped table's AUTOINCREMENT high-water mark. The rebuild
// keeps it, so an id whose row was deleted is never handed out again.
func TestSQLiteRebuildKeepsAutoincrementHighWaterMark(t *testing.T) {
	ctx := t.Context()
	db := newSQLiteRebuildDB(t)
	const shape = `CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)`
	if _, err := db.ExecContext(ctx, fmt.Sprintf(shape, "sequence_probe")); err != nil {
		t.Fatal(err)
	}
	insert := func() (id int64) {
		t.Helper()
		if err := db.NewRaw(`INSERT INTO sequence_probe (value) VALUES ('row') RETURNING id`).Scan(ctx, &id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	for range 3 {
		insert()
	}
	for _, step := range []struct {
		deleted string
		want    int64
	}{
		{deleted: "id = (SELECT MAX(id) FROM sequence_probe)", want: 4},
		{deleted: "TRUE", want: 5},
	} {
		if _, err := db.ExecContext(ctx, "DELETE FROM sequence_probe WHERE "+step.deleted); err != nil {
			t.Fatal(err)
		}
		if err := rebuildSQLiteTables(ctx, db, sqliteTableRebuild{
			table: "sequence_probe",
			create: func(ctx context.Context, tx bun.Tx, name string) error {
				_, err := tx.ExecContext(ctx, fmt.Sprintf(shape, quoteSQLiteName(name)))
				return err
			},
		}); err != nil {
			t.Fatal(err)
		}
		if got := insert(); got != step.want {
			t.Fatalf("id after deleting rows where %s and rebuilding = %d, want %d", step.deleted, got, step.want)
		}
	}
}

// A migration that rebuilds a table commits before Bun records it; rerunning
// it after the marker is lost reaches the same shape and keeps every row.
func TestSQLiteRebuildMigrationRerunsAfterALostMarker(t *testing.T) {
	ctx := t.Context()
	db := newSQLiteRebuildDB(t)
	seedCascadeChildren(t, db)
	original := currentTableDDL(t, db, "tasks")
	historyBefore := tableSnapshot(t, db, "task_history")

	const name = "2026999998"
	registry := migrate.NewMigrations()
	for _, migration := range Migrations.Sorted() {
		registry.Add(migration)
	}
	registry.Add(migrate.Migration{Name: name, Up: func(ctx context.Context, _ *migrate.Migrator, _ *migrate.Migration) error {
		return rebuildSQLiteTables(ctx, db, sqliteTableRebuild{
			table:  "tasks",
			create: withConstraint(original, "CONSTRAINT chk_rebuild_probe CHECK (id > 0)"),
		})
	}})
	migrator := newMigrator(db, registry)
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("rebuild migration: %v", err)
	}
	rebuilt := currentTableDDL(t, db, "tasks")
	if _, err := db.ExecContext(ctx, `DELETE FROM bun_migrations WHERE name = ?`, name); err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("rerun rebuild migration: %v", err)
	}

	if got := currentTableDDL(t, db, "tasks"); got != rebuilt {
		t.Fatalf("tasks after the rerun:\n%s\nwant:\n%s", got, rebuilt)
	}
	if got := tableSnapshot(t, db, "task_history"); got != historyBefore {
		t.Fatalf("task history after the rerun:\n%s\nwant:\n%s", got, historyBefore)
	}
	assertAppliedMigrationCount(t, ctx, migrator, len(Migrations.Sorted())+1)
	assertPoolEnforcesForeignKeys(t, db)
}
