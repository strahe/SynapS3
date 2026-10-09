//go:build postgres

package migrations

import (
	"errors"
	"testing"
)

// The SQLite reference stores JSON as text, so a PostgreSQL JSON column counts
// as JSON only while it keeps its own declared type.
func TestValidateCurrentSchemaComparesPostgresJSONColumnType(t *testing.T) {
	ctx := t.Context()
	db := newPostgresMigrationDB(t)
	migrateToLevel(t, db, len(Migrations.Sorted()))
	if err := ValidateCurrentSchema(ctx, db); err != nil {
		t.Fatalf("ValidateCurrentSchema before the change = %v", err)
	}
	for _, change := range []string{
		`ALTER TABLE tasks DROP CONSTRAINT chk_tasks_input_json_json`,
		`ALTER TABLE tasks ALTER COLUMN input_json TYPE text USING input_json::text`,
		`ALTER TABLE tasks ADD CONSTRAINT chk_tasks_input_json_json CHECK (jsonb_typeof(input_json::jsonb) = 'object')`,
	} {
		if _, err := db.ExecContext(ctx, change); err != nil {
			t.Fatalf("%s: %v", change, err)
		}
	}
	if err := ValidateCurrentSchema(ctx, db); !errors.Is(err, ErrIncompatibleDatabase) {
		t.Fatalf("ValidateCurrentSchema with a text JSON column = %v, want ErrIncompatibleDatabase", err)
	}
}
