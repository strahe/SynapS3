package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026072801CacheLRU),
		transactionalMigration(down2026072801CacheLRU),
	)
}

func up2026072801CacheLRU(ctx context.Context, db bun.IDB) error {
	done, err := cacheLRUReached2026072801(ctx, db, true)
	if err != nil || done {
		return err
	}
	columnType := "TIMESTAMP"
	if db.Dialect().Name() == dialect.PG {
		columnType = "TIMESTAMPTZ"
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE object_versions ADD COLUMN cache_accessed_at "+columnType); err != nil {
		return fmt.Errorf("adding object_versions.cache_accessed_at: %w", err)
	}
	if _, err := db.ExecContext(
		ctx,
		"UPDATE object_versions SET cache_accessed_at = created_at WHERE cache_accessed_at IS NULL",
	); err != nil {
		return fmt.Errorf("initializing object version cache access timestamps: %w", err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE INDEX idx_object_versions_cache_lru ON object_versions (in_cache, cache_accessed_at, created_at, version_id)",
	); err != nil {
		return fmt.Errorf("creating object version cache LRU index: %w", err)
	}
	return nil
}

func down2026072801CacheLRU(ctx context.Context, db bun.IDB) error {
	done, err := cacheLRUReached2026072801(ctx, db, false)
	if err != nil || done {
		return err
	}
	if _, err := db.ExecContext(ctx, "DROP INDEX IF EXISTS idx_object_versions_cache_lru"); err != nil {
		return fmt.Errorf("dropping object version cache LRU index: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE object_versions DROP COLUMN cache_accessed_at"); err != nil {
		return fmt.Errorf("dropping object_versions.cache_accessed_at: %w", err)
	}
	return nil
}

func cacheLRUReached2026072801(ctx context.Context, db bun.IDB, wantPresent bool) (bool, error) {
	columnPresent, err := columnExists(ctx, db, "object_versions", "cache_accessed_at")
	if err != nil {
		return false, fmt.Errorf("checking object_versions.cache_accessed_at: %w", err)
	}
	indexPresent, err := indexExists(ctx, db, "idx_object_versions_cache_lru")
	if err != nil {
		return false, fmt.Errorf("checking idx_object_versions_cache_lru: %w", err)
	}
	if columnPresent == wantPresent && indexPresent == wantPresent {
		return true, nil
	}
	if columnPresent != wantPresent && indexPresent != wantPresent {
		return false, nil
	}
	return false, fmt.Errorf(
		"2026072801_cache_lru has partial schema state: cache_accessed_at=%t, index=%t",
		columnPresent,
		indexPresent,
	)
}
