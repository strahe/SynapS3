package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(up2026060401MultipartObjectParts, down2026060401MultipartObjectParts)
}

func up2026060401MultipartObjectParts(ctx context.Context, db *bun.DB) error {
	addMultipartUploadID := "ALTER TABLE object_versions ADD COLUMN multipart_upload_id TEXT REFERENCES multipart_uploads(upload_id) ON UPDATE CASCADE ON DELETE RESTRICT"
	if db.Dialect().Name() == dialect.PG {
		addMultipartUploadID = "ALTER TABLE object_versions ADD COLUMN multipart_upload_id TEXT"
	}
	if _, err := db.ExecContext(ctx, addMultipartUploadID); err != nil {
		return fmt.Errorf("adding object_versions.multipart_upload_id: %w", err)
	}
	if db.Dialect().Name() == dialect.PG {
		if _, err := db.ExecContext(ctx, "ALTER TABLE object_versions ADD CONSTRAINT fk_object_versions_multipart_upload_id FOREIGN KEY (multipart_upload_id) REFERENCES multipart_uploads(upload_id) ON UPDATE CASCADE ON DELETE RESTRICT"); err != nil {
			return fmt.Errorf("adding object_versions multipart upload foreign key: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_object_versions_multipart_upload ON object_versions (multipart_upload_id)"); err != nil {
		return fmt.Errorf("creating object_versions multipart upload index: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE multipart_parts ADD COLUMN checksum TEXT"); err != nil {
		return fmt.Errorf("adding multipart_parts.checksum: %w", err)
	}
	return nil
}

func down2026060401MultipartObjectParts(ctx context.Context, db *bun.DB) error {
	if _, err := db.ExecContext(ctx, "DROP INDEX IF EXISTS idx_object_versions_multipart_upload"); err != nil {
		return fmt.Errorf("dropping object_versions multipart upload index: %w", err)
	}
	if db.Dialect().Name() == dialect.PG {
		if _, err := db.ExecContext(ctx, "ALTER TABLE object_versions DROP CONSTRAINT IF EXISTS fk_object_versions_multipart_upload_id"); err != nil {
			return fmt.Errorf("dropping object_versions multipart upload foreign key: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE object_versions DROP COLUMN multipart_upload_id"); err != nil {
		return fmt.Errorf("dropping object_versions.multipart_upload_id: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE multipart_parts DROP COLUMN checksum"); err != nil {
		return fmt.Errorf("dropping multipart_parts.checksum: %w", err)
	}
	return nil
}
