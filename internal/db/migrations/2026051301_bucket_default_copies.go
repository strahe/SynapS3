package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026051301BucketDefaultCopies),
		transactionalMigration(down2026051301BucketDefaultCopies),
	)
}

func up2026051301BucketDefaultCopies(ctx context.Context, db bun.IDB) error {
	exists, err := columnExists(ctx, db, "buckets", "default_copies")
	if err != nil || exists {
		return err
	}
	query := "ALTER TABLE buckets ADD COLUMN default_copies INTEGER CHECK (default_copies IS NULL OR (default_copies >= 1 AND default_copies <= 8))"
	if db.Dialect().Name() == dialect.PG {
		query = "ALTER TABLE buckets ADD COLUMN default_copies INTEGER CONSTRAINT chk_buckets_default_copies CHECK (default_copies IS NULL OR (default_copies >= 1 AND default_copies <= 8))"
	}
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("adding buckets.default_copies: %w", err)
	}
	return nil
}

func down2026051301BucketDefaultCopies(ctx context.Context, db bun.IDB) error {
	exists, err := columnExists(ctx, db, "buckets", "default_copies")
	if err != nil || !exists {
		return err
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE buckets DROP COLUMN default_copies"); err != nil {
		return fmt.Errorf("dropping buckets.default_copies: %w", err)
	}
	return nil
}
