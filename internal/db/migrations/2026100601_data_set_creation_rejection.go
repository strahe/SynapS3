package migrations

import (
	"context"
	"errors"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(transactionalMigration(up2026100601DataSetCreationRejection), func(context.Context, *bun.DB) error {
		return errors.New("data set creation rejection migration cannot be rolled back without losing request evidence")
	})
}

func up2026100601DataSetCreationRejection(ctx context.Context, db bun.IDB) error {
	exists, err := columnExists(ctx, db, "storage_data_sets", "creation_rejection")
	if err != nil || exists {
		return err
	}
	column := `creation_rejection TEXT CONSTRAINT chk_storage_data_sets_creation_rejection_json CHECK (
		creation_rejection IS NULL OR CASE WHEN json_valid(creation_rejection) THEN json_type(creation_rejection) = 'object' ELSE FALSE END)`
	if db.Dialect().Name() == dialect.PG {
		column = `creation_rejection JSONB CONSTRAINT chk_storage_data_sets_creation_rejection_json CHECK (
			creation_rejection IS NULL OR jsonb_typeof(creation_rejection) = 'object')`
	}
	_, err = db.NewRaw("ALTER TABLE storage_data_sets ADD COLUMN " + column).Exec(ctx)
	return err
}
