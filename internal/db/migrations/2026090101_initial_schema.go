package migrations

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/uptrace/bun"
)

// initialSchemaSQL is the complete baseline schema.
//
//go:embed 2026090101_initial_schema.sql
var initialSchemaSQL string

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026090101InitialSchema),
		func(context.Context, *bun.DB) error {
			return errors.New("the initial schema cannot be rolled back; create a new empty database")
		},
	)
}

// initialSchemaTables2026090101 lists, sorted, the tables the baseline creates.
var initialSchemaTables2026090101 = []string{
	"bucket_replica_slots",
	"buckets",
	"multipart_parts",
	"multipart_uploads",
	"object_cache",
	"object_deletions",
	"object_versions",
	"objects",
	"observability_collection_states",
	"observability_data_set_states",
	"observability_provider_states",
	"provider_profiles",
	"provider_tier_snapshots",
	"provider_upload_speed_tests",
	"s3_accounts",
	"storage_cleanup_copies",
	"storage_commit_request_pieces",
	"storage_commit_requests",
	"storage_contents",
	"storage_copies",
	"storage_data_set_terminations",
	"storage_data_sets",
	"storage_pull_attempts",
	"storage_replacement_items",
	"storage_replacements",
	"task_history",
	"task_schedules",
	"tasks",
	"wallet_operations",
}

func up2026090101InitialSchema(ctx context.Context, db bun.IDB) error {
	complete, err := initialSchemaPostStateComplete(ctx, db)
	if err != nil {
		return fmt.Errorf("checking initial schema post-state: %w", err)
	}
	if complete {
		return nil
	}
	count, err := applicationTableCount(ctx, db)
	if err != nil {
		return fmt.Errorf("checking initial schema pre-state: %w", err)
	}
	if count != 0 {
		return incompatibleDatabaseError()
	}
	if _, err := db.ExecContext(ctx, initialSchemaSQL); err != nil {
		return fmt.Errorf("creating initial schema: %w", err)
	}
	return nil
}
