package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type initialTableSpec struct {
	name        string
	model       any
	constraints []string
	foreignKeys []string
	// forwardForeignKeys reference a table this one is created before. SQLite
	// resolves a foreign key parent when rows are written, so the constraint can
	// be declared inline; PostgreSQL requires the parent to exist already, so
	// there the same constraint is added by addForwardForeignKey once both
	// tables are in place.
	forwardForeignKeys []initialForwardForeignKey
	jsonColumns        []initialJSONColumnSpec
}

type initialForwardForeignKey struct {
	name       string
	definition string
}

type initialJSONShape string

const (
	initialJSONAny    initialJSONShape = "any"
	initialJSONObject initialJSONShape = "object"
	initialJSONArray  initialJSONShape = "array"
)

type initialJSONColumnSpec struct {
	name     string
	shape    initialJSONShape
	nullable bool
	// text marks a column already declared as text rather than jsonb. Its shape
	// still has to hold, so PostgreSQL casts before asking.
	text bool
}

func initialJSONColumns(table string) []initialJSONColumnSpec {
	switch table {
	case "task_payloads":
		return []initialJSONColumnSpec{
			{name: "input_json", shape: initialJSONObject},
			{name: "checkpoint_json", shape: initialJSONObject, nullable: true},
		}
	case "multipart_uploads", "object_versions":
		return []initialJSONColumnSpec{{name: "metadata", shape: initialJSONObject}}
	case "observability_provider_states", "observability_data_set_states":
		return []initialJSONColumnSpec{
			{name: "reason_codes", shape: initialJSONArray},
			{name: "evidence_json", shape: initialJSONObject},
		}
	default:
		return nil
	}
}

func isInitialJSONColumn(table, column string) bool {
	for _, candidate := range initialJSONColumns(table) {
		if candidate.name == column {
			return true
		}
	}
	return false
}

func createInitialTable(ctx context.Context, db bun.IDB, spec initialTableSpec) error {
	query := db.NewCreateTable().Model(spec.model)
	for _, constraint := range spec.constraints {
		query.ColumnExpr(constraint)
	}
	for _, foreignKey := range spec.foreignKeys {
		query.ForeignKey(foreignKey)
	}
	if db.Dialect().Name() == dialect.SQLite {
		for _, foreignKey := range spec.forwardForeignKeys {
			query.ColumnExpr("CONSTRAINT " + foreignKey.name + " FOREIGN KEY " + foreignKey.definition)
		}
	}
	for _, column := range spec.jsonColumns {
		query.ColumnExpr(initialJSONConstraint(db.Dialect().Name(), spec.name, column))
	}

	var err error
	if db.Dialect().Name() == dialect.SQLite && len(spec.jsonColumns) > 0 {
		ddl := query.String()
		for _, column := range spec.jsonColumns {
			if column.text {
				continue
			}
			jsonType := fmt.Sprintf(`"%s" jsonb`, column.name)
			if strings.Count(ddl, jsonType) != 1 {
				return fmt.Errorf("rendering SQLite JSON column %s.%s", spec.name, column.name)
			}
			ddl = strings.Replace(ddl, jsonType, fmt.Sprintf(`"%s" text`, column.name), 1)
		}
		_, err = db.ExecContext(ctx, ddl)
	} else {
		_, err = query.Exec(ctx)
	}
	if err != nil {
		return fmt.Errorf("creating table %s: %w", spec.name, err)
	}
	return nil
}

func initialJSONConstraint(dialectName dialect.Name, table string, column initialJSONColumnSpec) string {
	name := fmt.Sprintf("chk_%s_%s_json", table, column.name)
	if dialectName == dialect.PG {
		// text::jsonb is immutable, so a CHECK may cast a text column here.
		value := column.name
		if column.text {
			value = column.name + "::jsonb"
		}
		expression := fmt.Sprintf("jsonb_typeof(%s) IS NOT NULL", value)
		if column.shape != initialJSONAny {
			expression = fmt.Sprintf("jsonb_typeof(%s) = '%s'", value, column.shape)
		}
		if column.nullable {
			expression = fmt.Sprintf("%s IS NULL OR %s", column.name, expression)
		}
		return fmt.Sprintf("CONSTRAINT %s CHECK (%s)", name, expression)
	}

	expression := fmt.Sprintf("json_valid(%s)", column.name)
	if column.shape != initialJSONAny {
		expression = fmt.Sprintf(
			"CASE WHEN json_valid(%s) THEN json_type(%s) = '%s' ELSE FALSE END",
			column.name,
			column.name,
			column.shape,
		)
	}
	if column.nullable {
		expression = fmt.Sprintf("%s IS NULL OR (%s)", column.name, expression)
	}
	return fmt.Sprintf("CONSTRAINT %s CHECK (%s)", name, expression)
}

type initialIndexSpec struct {
	name    string
	table   string
	columns []string
	where   string
	unique  bool
}

func createInitialIndexes(ctx context.Context, db bun.IDB, specs ...initialIndexSpec) error {
	for _, spec := range specs {
		query := db.NewCreateIndex().Index(spec.name).Table(spec.table)
		if spec.unique {
			query.Unique()
		}
		for _, column := range spec.columns {
			query.ColumnExpr(column)
		}
		if spec.where != "" {
			query.Where(spec.where)
		}
		if _, err := query.Exec(ctx); err != nil {
			return fmt.Errorf("creating index %s: %w", spec.name, err)
		}
	}
	return nil
}

// addForwardForeignKey completes a constraint that could not be declared while
// the child table was created because its parent did not exist yet. Only
// PostgreSQL needs it; SQLite already carries the constraint inline.
func addForwardForeignKey(ctx context.Context, db bun.IDB, table string, foreignKey initialForwardForeignKey) error {
	if db.Dialect().Name() != dialect.PG {
		return nil
	}
	statement := fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY %s",
		table, foreignKey.name, foreignKey.definition,
	)
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("adding foreign key %s on %s: %w", foreignKey.name, table, err)
	}
	return nil
}
