package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up2026072801CacheLRU, down2026072801CacheLRU)
}

func up2026072801CacheLRU(ctx context.Context, db *bun.DB) error {
	columnType := "TIMESTAMP"
	if db.Dialect().Name() == dialect.PG {
		columnType = "TIMESTAMPTZ"
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE object_versions ADD COLUMN cache_accessed_at "+columnType); err != nil {
		return fmt.Errorf("adding object_versions.cache_accessed_at: %w", err)
	}
	if _, err := db.ExecContext(
		ctx,
		"CREATE INDEX idx_object_versions_cache_lru ON object_versions (in_cache, state, cache_accessed_at, created_at, version_id)",
	); err != nil {
		return fmt.Errorf("creating object version cache LRU index: %w", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE tasks
		SET status = 'cancelled',
		    completed_at = CURRENT_TIMESTAMP,
		    claimed_at = NULL,
		    lease_until = NULL,
		    started_at = NULL,
		    wait_reason = NULL,
		    status_message = 'Cancelled while upgrading cache eviction policies'
		WHERE type = 'evict_cache'
		  AND (stage IS NULL OR stage = '')
		  AND status IN ('queued', 'scheduled', 'waiting', 'running')`); err != nil {
		return fmt.Errorf("cancelling legacy cache eviction tasks: %w", err)
	}
	return nil
}

func down2026072801CacheLRU(ctx context.Context, db *bun.DB) error {
	if _, err := db.ExecContext(ctx, "DROP INDEX IF EXISTS idx_object_versions_cache_lru"); err != nil {
		return fmt.Errorf("dropping object version cache LRU index: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE object_versions DROP COLUMN cache_accessed_at"); err != nil {
		return fmt.Errorf("dropping object_versions.cache_accessed_at: %w", err)
	}
	return nil
}
