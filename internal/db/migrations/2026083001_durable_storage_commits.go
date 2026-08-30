package migrations

import (
	"context"
	"fmt"
	"slices"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

const durableStorageCommitMigration2026083001 = "2026083001_durable_storage_commits"

var durableStorageCommitColumns2026083001 = []string{
	"commit_ready_at",
	"commit_attempt_id",
	"commit_attempted_at",
	"commit_submission_json",
	"commit_confirmed_transaction_id",
	"commit_attention_code",
	"commit_attention_at",
}

var durableStorageCommitIndexes2026083001 = []string{
	"idx_storage_upload_copies_commit_attempt_data_set",
	"idx_storage_upload_copies_commit_ready",
}

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026083001DurableStorageCommits),
		transactionalMigration(down2026083001DurableStorageCommits),
	)
}

func up2026083001DurableStorageCommits(ctx context.Context, db bun.IDB) error {
	columns, indexes, err := durableStorageCommitSchemaCounts2026083001(ctx, db)
	if err != nil {
		return err
	}
	if err := assertDurableStorageCommitUpgradePreconditions2026083001(ctx, db); err != nil {
		return err
	}
	if columns == len(durableStorageCommitColumns2026083001) && indexes == len(durableStorageCommitIndexes2026083001) {
		return nil
	}
	if columns != 0 || indexes != 0 {
		return fmt.Errorf(
			"%s has partial schema state: columns=%d/%d indexes=%d/%d",
			durableStorageCommitMigration2026083001,
			columns,
			len(durableStorageCommitColumns2026083001),
			indexes,
			len(durableStorageCommitIndexes2026083001),
		)
	}
	timestampType := "TIMESTAMP"
	if db.Dialect().Name() == dialect.PG {
		timestampType = "TIMESTAMPTZ"
	}
	statements := []string{
		"ALTER TABLE storage_upload_copies ADD COLUMN commit_ready_at " + timestampType,
		"ALTER TABLE storage_upload_copies ADD COLUMN commit_attempt_id TEXT",
		"ALTER TABLE storage_upload_copies ADD COLUMN commit_attempted_at " + timestampType,
		"ALTER TABLE storage_upload_copies ADD COLUMN commit_submission_json TEXT",
		"ALTER TABLE storage_upload_copies ADD COLUMN commit_confirmed_transaction_id TEXT",
		"ALTER TABLE storage_upload_copies ADD COLUMN commit_attention_code TEXT",
		"ALTER TABLE storage_upload_copies ADD COLUMN commit_attention_at " + timestampType,
		`CREATE INDEX idx_storage_upload_copies_commit_attempt_data_set
			ON storage_upload_copies (storage_data_set_id, commit_attempt_id)
			WHERE commit_attempt_id IS NOT NULL`,
		`CREATE INDEX idx_storage_upload_copies_commit_ready
			ON storage_upload_copies (storage_data_set_id, commit_ready_at, id)
			WHERE status = 'piece_ready' AND commit_attempt_id IS NULL AND commit_ready_at IS NOT NULL`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("applying %s: %w", durableStorageCommitMigration2026083001, err)
		}
	}
	return nil
}

func down2026083001DurableStorageCommits(ctx context.Context, db bun.IDB) error {
	columns, indexes, err := durableStorageCommitSchemaCounts2026083001(ctx, db)
	if err != nil {
		return err
	}
	if columns == 0 && indexes == 0 {
		return nil
	}
	if columns != len(durableStorageCommitColumns2026083001) || indexes != len(durableStorageCommitIndexes2026083001) {
		return fmt.Errorf(
			"%s has partial schema state: columns=%d/%d indexes=%d/%d",
			durableStorageCommitMigration2026083001,
			columns,
			len(durableStorageCommitColumns2026083001),
			indexes,
			len(durableStorageCommitIndexes2026083001),
		)
	}
	var active int
	if err := db.NewRaw(`SELECT COUNT(*) FROM storage_upload_copies
		WHERE commit_attempt_id IS NOT NULL OR commit_submission_json IS NOT NULL`).Scan(ctx, &active); err != nil {
		return fmt.Errorf("checking %s rollback preconditions: %w", durableStorageCommitMigration2026083001, err)
	}
	if active > 0 {
		return fmt.Errorf(
			"cannot rollback %s while %d durable commit attempts or submissions remain",
			durableStorageCommitMigration2026083001,
			active,
		)
	}
	for _, index := range slices.Backward(durableStorageCommitIndexes2026083001) {
		if _, err := db.ExecContext(ctx, "DROP INDEX IF EXISTS "+index); err != nil {
			return fmt.Errorf("dropping %s index %s: %w", durableStorageCommitMigration2026083001, index, err)
		}
	}
	for _, column := range slices.Backward(durableStorageCommitColumns2026083001) {
		if _, err := db.ExecContext(ctx, "ALTER TABLE storage_upload_copies DROP COLUMN "+column); err != nil {
			return fmt.Errorf("dropping %s column %s: %w", durableStorageCommitMigration2026083001, column, err)
		}
	}
	return nil
}

func assertDurableStorageCommitUpgradePreconditions2026083001(ctx context.Context, db bun.IDB) error {
	var committing int
	query := "SELECT COUNT(*) FROM storage_upload_copies WHERE status = 'committing'"
	attemptColumnExists, err := columnExists(ctx, db, "storage_upload_copies", "commit_attempt_id")
	if err != nil {
		return fmt.Errorf("checking durable attempt column before %s: %w", durableStorageCommitMigration2026083001, err)
	}
	if attemptColumnExists {
		query += " AND (commit_attempt_id IS NULL OR commit_attempt_id = '')"
	}
	if err := db.NewRaw(query).Scan(ctx, &committing); err != nil {
		return fmt.Errorf("checking existing committing copies before %s: %w", durableStorageCommitMigration2026083001, err)
	}
	if committing > 0 {
		return fmt.Errorf(
			"cannot apply %s while %d legacy committing copies remain; drain storage commit work before upgrading",
			durableStorageCommitMigration2026083001,
			committing,
		)
	}
	return nil
}

func durableStorageCommitSchemaCounts2026083001(ctx context.Context, db bun.IDB) (int, int, error) {
	columns := 0
	for _, column := range durableStorageCommitColumns2026083001 {
		exists, err := columnExists(ctx, db, "storage_upload_copies", column)
		if err != nil {
			return 0, 0, fmt.Errorf("checking %s column %s: %w", durableStorageCommitMigration2026083001, column, err)
		}
		if exists {
			columns++
		}
	}
	indexes := 0
	for _, index := range durableStorageCommitIndexes2026083001 {
		exists, err := indexExists(ctx, db, index)
		if err != nil {
			return 0, 0, fmt.Errorf("checking %s index %s: %w", durableStorageCommitMigration2026083001, index, err)
		}
		if exists {
			indexes++
		}
	}
	return columns, indexes, nil
}
