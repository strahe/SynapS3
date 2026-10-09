package migrations

import (
	"context"
	"errors"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up2026100901KeyCollation, func(context.Context, *bun.DB) error {
		return errors.New("key collation migration requires restoring a database backup to roll back")
	})
}

func up2026100901KeyCollation(ctx context.Context, db *bun.DB) error {
	if db.Dialect().Name() != dialect.PG {
		return nil
	}
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		columns := []struct{ table, column string }{
			{"buckets", "name"},
			{"objects", "key"},
			{"object_versions", "key"},
			{"multipart_uploads", "key"},
			{"object_deletions", "key"},
		}
		collated := 0
		for _, column := range columns {
			var isC bool
			if err := tx.NewRaw(`SELECT COALESCE(data_type = 'text' AND collation_name = 'C', FALSE)
				FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?`,
				column.table, column.column).Scan(ctx, &isC); err != nil {
				return fmt.Errorf("reading key collation: %w", err)
			}
			if isC {
				collated++
			}
		}
		companion, err := indexExists(ctx, tx, "idx_objects_bucket_key_c")
		if err != nil {
			return err
		}
		if collated == len(columns) && !companion {
			return nil
		}
		if collated != 0 || !companion {
			return incompatibleDatabaseError()
		}
		for _, column := range columns {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+column.table+` ALTER COLUMN `+column.column+` TYPE text COLLATE "C"`); err != nil {
				return fmt.Errorf("setting %s.%s collation: %w", column.table, column.column, err)
			}
		}
		_, err = tx.ExecContext(ctx, `DROP INDEX idx_objects_bucket_key_c`)
		return err
	})
}
