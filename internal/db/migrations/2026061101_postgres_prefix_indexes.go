package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up2026061101PostgresPrefixIndexes, down2026061101PostgresPrefixIndexes)
}

func up2026061101PostgresPrefixIndexes(ctx context.Context, db *bun.DB) error {
	if db.Dialect().Name() != dialect.PG {
		return nil
	}
	statements := []struct {
		name string
		sql  string
	}{
		{
			name: "current object version prefix index",
			sql:  `CREATE INDEX IF NOT EXISTS idx_object_versions_current_bucket_delete_key_c ON object_versions (bucket_id, is_delete_marker, (key COLLATE "C")) WHERE is_current = TRUE`,
		},
		{
			name: "version history prefix index",
			sql:  `CREATE INDEX IF NOT EXISTS idx_object_versions_bucket_key_created_c ON object_versions (bucket_id, (key COLLATE "C"), created_at DESC, version_id DESC)`,
		},
		{
			name: "multipart upload prefix index",
			sql:  `CREATE INDEX IF NOT EXISTS idx_multipart_uploads_bucket_status_key_upload_c ON multipart_uploads (bucket_id, status, (key COLLATE "C"), upload_id)`,
		},
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt.sql); err != nil {
			return fmt.Errorf("creating %s: %w", stmt.name, err)
		}
	}
	return nil
}

func down2026061101PostgresPrefixIndexes(ctx context.Context, db *bun.DB) error {
	if db.Dialect().Name() != dialect.PG {
		return nil
	}
	for _, name := range []string{
		"idx_multipart_uploads_bucket_status_key_upload_c",
		"idx_object_versions_bucket_key_created_c",
		"idx_object_versions_current_bucket_delete_key_c",
	} {
		if _, err := db.ExecContext(ctx, "DROP INDEX IF EXISTS "+name); err != nil {
			return fmt.Errorf("dropping %s: %w", name, err)
		}
	}
	return nil
}
