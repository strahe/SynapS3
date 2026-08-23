package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026060401MultipartObjectParts),
		transactionalMigration(down2026060401MultipartObjectParts),
	)
}

func up2026060401MultipartObjectParts(ctx context.Context, db bun.IDB) error {
	done, err := multipartObjectPartsReached2026060401(ctx, db, true)
	if err != nil || done {
		return err
	}
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

func down2026060401MultipartObjectParts(ctx context.Context, db bun.IDB) error {
	done, err := multipartObjectPartsReached2026060401(ctx, db, false)
	if err != nil || done {
		return err
	}
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

func multipartObjectPartsReached2026060401(ctx context.Context, db bun.IDB, wantPresent bool) (bool, error) {
	multipartUploadID, err := columnExists(ctx, db, "object_versions", "multipart_upload_id")
	if err != nil {
		return false, fmt.Errorf("checking object_versions.multipart_upload_id: %w", err)
	}
	checksum, err := columnExists(ctx, db, "multipart_parts", "checksum")
	if err != nil {
		return false, fmt.Errorf("checking multipart_parts.checksum: %w", err)
	}
	index, err := indexExists(ctx, db, "idx_object_versions_multipart_upload")
	if err != nil {
		return false, fmt.Errorf("checking idx_object_versions_multipart_upload: %w", err)
	}
	foreignKey, err := multipartUploadForeignKeyExists2026060401(ctx, db)
	if err != nil {
		return false, fmt.Errorf("checking object_versions multipart upload foreign key: %w", err)
	}
	if multipartUploadID == wantPresent && checksum == wantPresent && index == wantPresent && foreignKey == wantPresent {
		return true, nil
	}
	if multipartUploadID != wantPresent && checksum != wantPresent && index != wantPresent && foreignKey != wantPresent {
		return false, nil
	}
	return false, fmt.Errorf(
		"2026060401_multipart_object_parts has partial schema state: multipart_upload_id=%t, checksum=%t, index=%t, foreign_key=%t",
		multipartUploadID,
		checksum,
		index,
		foreignKey,
	)
}

func multipartUploadForeignKeyExists2026060401(ctx context.Context, db bun.IDB) (bool, error) {
	if db.Dialect().Name() == dialect.PG {
		return queryExists(ctx, db, `SELECT COUNT(*)
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE n.nspname = current_schema()
			  AND t.relname = 'object_versions'
			  AND c.conname = 'fk_object_versions_multipart_upload_id'`)
	}
	return queryExists(ctx, db, `SELECT COUNT(*) FROM pragma_foreign_key_list('object_versions')
		WHERE "table" = 'multipart_uploads'
		  AND "from" = 'multipart_upload_id'
		  AND "to" = 'upload_id'
		  AND on_update = 'CASCADE'
		  AND on_delete = 'RESTRICT'`)
}
