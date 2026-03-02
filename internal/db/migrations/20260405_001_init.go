package migrations

import (
	"context"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func init() {
	Migrations.MustRegister(up001Init, down001Init)
}

func up001Init(ctx context.Context, db *bun.DB) error {
	// Create buckets table.
	if _, err := db.NewCreateTable().
		Model((*model.Bucket)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating buckets table: %w", err)
	}

	// Create objects table.
	if _, err := db.NewCreateTable().
		Model((*model.Object)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating objects table: %w", err)
	}

	// Create tasks table.
	if _, err := db.NewCreateTable().
		Model((*model.Task)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating tasks table: %w", err)
	}

	// Composite unique index: one active object per (bucket_id, key).
	if _, err := db.NewCreateIndex().
		Model((*model.Object)(nil)).
		Index("idx_objects_bucket_key").
		Column("bucket_id", "key").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating unique index on objects: %w", err)
	}

	// Index for task polling: status + scheduled_at.
	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_status_scheduled").
		Column("status", "scheduled_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating index on tasks: %w", err)
	}

	// Index for task lease expiry scanning.
	if _, err := db.NewCreateIndex().
		Model((*model.Task)(nil)).
		Index("idx_tasks_lease_until").
		Column("lease_until").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating index on task leases: %w", err)
	}

	// Index for object state queries (workers scan by state).
	if _, err := db.NewCreateIndex().
		Model((*model.Object)(nil)).
		Index("idx_objects_state").
		Column("state").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating index on object state: %w", err)
	}

	// Create multipart_uploads table.
	if _, err := db.NewCreateTable().
		Model((*model.MultipartUpload)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart_uploads table: %w", err)
	}

	// Create multipart_parts table.
	if _, err := db.NewCreateTable().
		Model((*model.MultipartPart)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating multipart_parts table: %w", err)
	}

	// Index for listing uploads by bucket and status.
	if _, err := db.NewCreateIndex().
		Model((*model.MultipartUpload)(nil)).
		Index("idx_multipart_uploads_bucket_status").
		Column("bucket_id", "status").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating index on multipart_uploads: %w", err)
	}

	// Composite unique index: one part per (upload_id, part_number).
	if _, err := db.NewCreateIndex().
		Model((*model.MultipartPart)(nil)).
		Index("idx_multipart_parts_upload_part").
		Column("upload_id", "part_number").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating unique index on multipart_parts: %w", err)
	}

	return nil
}

func down001Init(ctx context.Context, db *bun.DB) error {
	for _, m := range []interface{}{
		(*model.MultipartPart)(nil),
		(*model.MultipartUpload)(nil),
		(*model.Task)(nil),
		(*model.Object)(nil),
		(*model.Bucket)(nil),
	} {
		if _, err := db.NewDropTable().Model(m).IfExists().Exec(ctx); err != nil {
			return fmt.Errorf("dropping table %T: %w", m, err)
		}
	}
	return nil
}
