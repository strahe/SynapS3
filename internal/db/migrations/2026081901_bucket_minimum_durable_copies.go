package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026081901BucketMinimumDurableCopies),
		transactionalMigration(down2026081901BucketMinimumDurableCopies),
	)
}

func up2026081901BucketMinimumDurableCopies(ctx context.Context, db bun.IDB) error {
	exists, err := columnExists(ctx, db, "buckets", "minimum_durable_copies")
	if err != nil || exists {
		return err
	}
	query := "ALTER TABLE buckets ADD COLUMN minimum_durable_copies INTEGER CHECK (minimum_durable_copies IS NULL OR (minimum_durable_copies >= 1 AND minimum_durable_copies <= 8))"
	if db.Dialect().Name() == dialect.PG {
		query = "ALTER TABLE buckets ADD COLUMN minimum_durable_copies INTEGER CONSTRAINT chk_buckets_minimum_durable_copies CHECK (minimum_durable_copies IS NULL OR (minimum_durable_copies >= 1 AND minimum_durable_copies <= 8))"
	}
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("adding buckets.minimum_durable_copies: %w", err)
	}
	return nil
}

func down2026081901BucketMinimumDurableCopies(ctx context.Context, db bun.IDB) error {
	exists, err := columnExists(ctx, db, "buckets", "minimum_durable_copies")
	if err != nil || !exists {
		return err
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE buckets DROP COLUMN minimum_durable_copies"); err != nil {
		return fmt.Errorf("dropping buckets.minimum_durable_copies: %w", err)
	}
	return nil
}
