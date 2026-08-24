package migrations

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026082101StorageDataSetGenerations),
		transactionalMigration(down2026082101StorageDataSetGenerations),
	)
}

// A replica slot may now own several data set generations so an operator can
// replace a provider while the previous generation stays readable.
func up2026082101StorageDataSetGenerations(ctx context.Context, db bun.IDB) error {
	done, err := storageDataSetGenerationsReached2026082101(ctx, db, true)
	if done {
		return nil
	}
	if err != nil {
		if dataErr := assertStorageGenerationPreconditions2026082101(ctx, db); dataErr != nil {
			return dataErr
		}
		return err
	}
	if err := assertStorageGenerationPreconditions2026082101(ctx, db); err != nil {
		return err
	}
	pg := db.Dialect().Name() == dialect.PG

	isCurrentColumn := "INTEGER NOT NULL DEFAULT 1"
	if pg {
		isCurrentColumn = "BOOLEAN NOT NULL DEFAULT TRUE"
	}
	// Neither column carries a CHECK: SQLite cannot add one later and a
	// column-level CHECK blocks DROP COLUMN on rollback. The partial unique
	// indexes below enforce the invariants on both dialects instead.
	statements := []string{
		"ALTER TABLE storage_data_sets ADD COLUMN generation INTEGER NOT NULL DEFAULT 1",
		"ALTER TABLE storage_data_sets ADD COLUMN is_current " + isCurrentColumn,
		"DROP INDEX IF EXISTS idx_storage_data_sets_bucket_copy_index",
		"DROP INDEX IF EXISTS idx_storage_data_sets_bucket_provider",
		"DROP INDEX IF EXISTS idx_storage_upload_copies_upload_index",

		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_data_sets_bucket_copy_current
				ON storage_data_sets (bucket_id, copy_index) WHERE is_current`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_data_sets_bucket_copy_generation
				ON storage_data_sets (bucket_id, copy_index, generation)`,
		// A historical generation never blocks reusing its provider; only a
		// live slot does.
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_data_sets_bucket_provider_current
				ON storage_data_sets (bucket_id, provider_id) WHERE is_current`,

		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_upload_copies_upload_data_set
				ON storage_upload_copies (upload_id, storage_data_set_id) WHERE storage_data_set_id IS NOT NULL`,
		// NULLs compare distinct in unique indexes on both dialects, so the
		// index above would let unbound duplicates accumulate per slot.
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_upload_copies_upload_slot_unbound
				ON storage_upload_copies (upload_id, copy_index) WHERE storage_data_set_id IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_storage_upload_copies_upload_slot
				ON storage_upload_copies (upload_id, copy_index)`,
		`CREATE INDEX IF NOT EXISTS idx_storage_uploads_bucket_id
				ON storage_uploads (bucket_id, id)`,

		storageReplacementsTableSQL2026082101(pg),
		storageReplacementItemsTableSQL2026082101(pg),

		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_replacements_active_source
				ON storage_replacements (source_data_set_id) WHERE status NOT IN ('completed', 'superseded')`,
		`CREATE INDEX IF NOT EXISTS idx_storage_replacements_bucket_slot
				ON storage_replacements (bucket_id, copy_index, id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_replacements_bucket_request
				ON storage_replacements (bucket_id, client_request_id)`,
		`CREATE INDEX IF NOT EXISTS idx_storage_replacement_items_due
				ON storage_replacement_items (replacement_id, scheduled_at, id)
				WHERE status IN ('pending', 'retrying', 'waiting_source') AND claimed_at IS NULL AND max_retries IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_storage_replacement_items_lease
				ON storage_replacement_items (replacement_id, lease_until, id)
				WHERE status = 'running' AND max_retries IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_storage_replacement_items_state
				ON storage_replacement_items (replacement_id, status, id)`,
		`CREATE INDEX IF NOT EXISTS idx_storage_replacement_items_upload_id
				ON storage_replacement_items (upload_id)`,
		`CREATE INDEX IF NOT EXISTS idx_storage_replacements_dispatch
				ON storage_replacements (last_dispatched_at, id) WHERE status = 'migrating'`,
	}
	for _, query := range statements {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("adding storage data set generations: %w", err)
		}
	}
	return nil
}

// Rollback only succeeds while every slot still owns a single generation. Once
// a replacement has produced a second generation the original unique indexes
// cannot be restored, and refusing is the correct outcome.
func down2026082101StorageDataSetGenerations(ctx context.Context, db bun.IDB) error {
	done, err := storageDataSetGenerationsReached2026082101(ctx, db, false)
	if err != nil || done {
		return err
	}
	if err := assertStorageGenerationRollbackPreconditions2026082101(ctx, db); err != nil {
		return err
	}
	statements := []string{
		"DROP INDEX IF EXISTS idx_storage_replacements_dispatch",
		"DROP INDEX IF EXISTS idx_storage_replacement_items_lease",
		"DROP INDEX IF EXISTS idx_storage_replacement_items_due",
		"DROP INDEX IF EXISTS idx_storage_replacement_items_state",
		"DROP INDEX IF EXISTS idx_storage_replacement_items_upload_id",
		"DROP INDEX IF EXISTS idx_storage_uploads_bucket_id",
		"DROP TABLE IF EXISTS storage_replacement_items",
		"DROP TABLE IF EXISTS storage_replacements",

		// SQLite refuses to drop a column named by an index or by a partial
		// index predicate, so the indexes go first.
		"DROP INDEX IF EXISTS idx_storage_upload_copies_upload_slot",
		"DROP INDEX IF EXISTS idx_storage_upload_copies_upload_slot_unbound",
		"DROP INDEX IF EXISTS idx_storage_upload_copies_upload_data_set",
		"DROP INDEX IF EXISTS idx_storage_data_sets_bucket_provider_current",
		"DROP INDEX IF EXISTS idx_storage_data_sets_bucket_copy_generation",
		"DROP INDEX IF EXISTS idx_storage_data_sets_bucket_copy_current",

		"ALTER TABLE storage_data_sets DROP COLUMN generation",
		"ALTER TABLE storage_data_sets DROP COLUMN is_current",

		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_data_sets_bucket_copy_index
				ON storage_data_sets (bucket_id, copy_index)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_data_sets_bucket_provider
				ON storage_data_sets (bucket_id, provider_id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_upload_copies_upload_index
				ON storage_upload_copies (upload_id, copy_index)`,
	}
	for _, query := range statements {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("removing storage data set generations: %w", err)
		}
	}
	return nil
}

// The new unique indexes would fail mid-migration on a database that violated
// an invariant the old schema never enforced. Reporting the offending rows up
// front keeps the failure actionable.
func assertStorageGenerationPreconditions2026082101(ctx context.Context, db bun.IDB) error {
	checks := []struct {
		description string
		query       string
	}{
		{
			description: "storage upload copies sharing one data set",
			query: `SELECT COUNT(*) FROM (
				SELECT upload_id, storage_data_set_id FROM storage_upload_copies
				WHERE storage_data_set_id IS NOT NULL
				GROUP BY upload_id, storage_data_set_id HAVING COUNT(*) > 1
			) AS duplicates`,
		},
		{
			description: "unbound storage upload copies sharing one replica slot",
			query: `SELECT COUNT(*) FROM (
				SELECT upload_id, copy_index FROM storage_upload_copies
				WHERE storage_data_set_id IS NULL
				GROUP BY upload_id, copy_index HAVING COUNT(*) > 1
			) AS duplicates`,
		},
		{
			description: "storage data sets sharing one replica slot",
			query: `SELECT COUNT(*) FROM (
				SELECT bucket_id, copy_index FROM storage_data_sets
				GROUP BY bucket_id, copy_index HAVING COUNT(*) > 1
			) AS duplicates`,
		},
	}
	for _, check := range checks {
		var count int
		if err := db.NewRaw(check.query).Scan(ctx, &count); err != nil {
			return fmt.Errorf("checking %s: %w", check.description, err)
		}
		if count > 0 {
			return fmt.Errorf("cannot add storage data set generations: found %d %s", count, check.description)
		}
	}
	return nil
}

func assertStorageGenerationRollbackPreconditions2026082101(ctx context.Context, db bun.IDB) error {
	checks := []struct {
		description string
		query       string
	}{
		{
			description: "duplicate (bucket_id, copy_index) groups in storage_data_sets",
			query: `SELECT COUNT(*) FROM (
				SELECT bucket_id, copy_index FROM storage_data_sets
				GROUP BY bucket_id, copy_index HAVING COUNT(*) > 1
			) AS duplicates`,
		},
		{
			description: "duplicate (bucket_id, provider_id) groups in storage_data_sets",
			query: `SELECT COUNT(*) FROM (
				SELECT bucket_id, provider_id FROM storage_data_sets
				GROUP BY bucket_id, provider_id HAVING COUNT(*) > 1
			) AS duplicates`,
		},
		{
			description: "duplicate (upload_id, copy_index) groups in storage_upload_copies",
			query: `SELECT COUNT(*) FROM (
				SELECT upload_id, copy_index FROM storage_upload_copies
				GROUP BY upload_id, copy_index HAVING COUNT(*) > 1
			) AS duplicates`,
		},
	}
	for _, check := range checks {
		var count int
		if err := db.NewRaw(check.query).Scan(ctx, &count); err != nil {
			return fmt.Errorf("checking %s: %w", check.description, err)
		}
		if count > 0 {
			return fmt.Errorf("cannot remove storage data set generations: found %d %s", count, check.description)
		}
	}
	return nil
}

func storageDataSetGenerationsReached2026082101(
	ctx context.Context,
	db bun.IDB,
	wantMigrated bool,
) (bool, error) {
	generation, err := columnExists(ctx, db, "storage_data_sets", "generation")
	if err != nil {
		return false, fmt.Errorf("checking storage_data_sets.generation: %w", err)
	}
	isCurrent, err := columnExists(ctx, db, "storage_data_sets", "is_current")
	if err != nil {
		return false, fmt.Errorf("checking storage_data_sets.is_current: %w", err)
	}
	legacyIndexes := []string{
		"idx_storage_data_sets_bucket_copy_index",
		"idx_storage_data_sets_bucket_provider",
		"idx_storage_upload_copies_upload_index",
	}
	migratedIndexes := []string{
		"idx_storage_data_sets_bucket_copy_current",
		"idx_storage_data_sets_bucket_copy_generation",
		"idx_storage_data_sets_bucket_provider_current",
		"idx_storage_upload_copies_upload_data_set",
		"idx_storage_upload_copies_upload_slot_unbound",
		"idx_storage_upload_copies_upload_slot",
		"idx_storage_uploads_bucket_id",
		"idx_storage_replacements_active_source",
		"idx_storage_replacements_bucket_slot",
		"idx_storage_replacements_bucket_request",
		"idx_storage_replacement_items_due",
		"idx_storage_replacement_items_lease",
		"idx_storage_replacement_items_state",
		"idx_storage_replacement_items_upload_id",
		"idx_storage_replacements_dispatch",
	}
	legacyIndexCount, err := existingIndexes2026082101(ctx, db, legacyIndexes)
	if err != nil {
		return false, err
	}
	migratedIndexCount, err := existingIndexes2026082101(ctx, db, migratedIndexes)
	if err != nil {
		return false, err
	}
	replacementTables := 0
	for _, table := range []string{"storage_replacements", "storage_replacement_items"} {
		exists, err := tableExists(ctx, db, table)
		if err != nil {
			return false, fmt.Errorf("checking %s: %w", table, err)
		}
		if exists {
			replacementTables++
		}
	}
	replacementColumns := []struct {
		table  string
		column string
	}{
		{"storage_replacements", "state_version"},
		{"storage_replacements", "last_dispatched_at"},
		{"storage_replacement_items", "scheduled_at"},
		{"storage_replacement_items", "retry_count"},
		{"storage_replacement_items", "max_retries"},
		{"storage_replacement_items", "claimed_at"},
		{"storage_replacement_items", "lease_until"},
	}
	replacementColumnCount := 0
	for _, item := range replacementColumns {
		exists, err := columnExists(ctx, db, item.table, item.column)
		if err != nil {
			return false, fmt.Errorf("checking %s.%s: %w", item.table, item.column, err)
		}
		if exists {
			replacementColumnCount++
		}
	}

	legacy := !generation && !isCurrent && legacyIndexCount == len(legacyIndexes) && migratedIndexCount == 0 && replacementTables == 0 && replacementColumnCount == 0
	migrated := generation && isCurrent && legacyIndexCount == 0 && migratedIndexCount == len(migratedIndexes) && replacementTables == 2 && replacementColumnCount == len(replacementColumns)
	if (wantMigrated && migrated) || (!wantMigrated && legacy) {
		return true, nil
	}
	if (wantMigrated && legacy) || (!wantMigrated && migrated) {
		return false, nil
	}
	return false, fmt.Errorf(
		"2026082101_storage_data_set_generations has partial schema state: generation=%t, is_current=%t, legacy_indexes=%d/%d, migrated_indexes=%d/%d, replacement_tables=%d/2, replacement_columns=%d/%d",
		generation,
		isCurrent,
		legacyIndexCount,
		len(legacyIndexes),
		migratedIndexCount,
		len(migratedIndexes),
		replacementTables,
		replacementColumnCount,
		len(replacementColumns),
	)
}

func existingIndexes2026082101(ctx context.Context, db bun.IDB, names []string) (int, error) {
	count := 0
	for _, name := range names {
		exists, err := indexExists(ctx, db, name)
		if err != nil {
			return 0, fmt.Errorf("checking %s: %w", name, err)
		}
		if exists {
			count++
		}
	}
	return count, nil
}

func storageReplacementsTableSQL2026082101(pg bool) string {
	identity := "id INTEGER PRIMARY KEY AUTOINCREMENT"
	reference := "INTEGER"
	timestamp := "TIMESTAMP"
	boolean := "INTEGER NOT NULL DEFAULT 0"
	if pg {
		identity = "id BIGSERIAL PRIMARY KEY"
		reference = "BIGINT"
		timestamp = "TIMESTAMPTZ"
		boolean = "BOOLEAN NOT NULL DEFAULT FALSE"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS storage_replacements (
		%[1]s,
		bucket_id %[2]s NOT NULL REFERENCES buckets (id) ON UPDATE CASCADE ON DELETE RESTRICT,
		copy_index INTEGER NOT NULL,
		source_data_set_id %[2]s NOT NULL REFERENCES storage_data_sets (id) ON UPDATE CASCADE ON DELETE RESTRICT,
		target_data_set_id %[2]s NOT NULL REFERENCES storage_data_sets (id) ON UPDATE CASCADE ON DELETE RESTRICT,
		selection_mode TEXT NOT NULL,
		requested_provider_id TEXT,
		client_request_id TEXT NOT NULL,
		status TEXT NOT NULL,
		wait_reason TEXT,
		failure_reason TEXT,
		last_error TEXT,
		items_total INTEGER NOT NULL DEFAULT 0,
		items_copied INTEGER NOT NULL DEFAULT 0,
		seed_cursor_upload_id %[2]s NOT NULL DEFAULT 0,
		seeding_complete %[4]s,
		state_version %[2]s NOT NULL DEFAULT 1,
		last_dispatched_at %[3]s,
		termination_tx_hash TEXT,
		termination_epoch %[2]s,
		termination_observed_at %[3]s,
		abandoned_termination_tx_hash TEXT,
		abandoned_termination_epoch %[2]s,
		abandoned_termination_observed_at %[3]s,
		superseded_by_id %[2]s REFERENCES storage_replacements (id) ON UPDATE CASCADE ON DELETE SET NULL,
		confirmed_at %[3]s NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at %[3]s NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at %[3]s NOT NULL DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT chk_storage_replacements_copy_index CHECK (copy_index >= 0),
		CONSTRAINT chk_storage_replacements_selection_mode CHECK (selection_mode IN ('automatic', 'manual')),
		CONSTRAINT chk_storage_replacements_status CHECK (status IN ('preparing_target', 'migrating', 'waiting', 'retiring', 'cleanup_attention', 'failed', 'completed', 'superseded')),
		CONSTRAINT chk_storage_replacements_wait_reason CHECK (wait_reason IS NULL OR wait_reason IN ('readable_source', 'target', 'target_creating', 'target_writable', 'funding', 'provider', 'termination_epoch', 'source_writes', 'coverage')),
		CONSTRAINT chk_storage_replacements_failure_reason CHECK (failure_reason IS NULL OR failure_reason IN ('target_in_use')),
		CONSTRAINT chk_storage_replacements_client_request_id CHECK (length(client_request_id) BETWEEN 1 AND 128),
		CONSTRAINT chk_storage_replacements_distinct_data_sets CHECK (source_data_set_id <> target_data_set_id),
		CONSTRAINT chk_storage_replacements_items CHECK (items_total >= 0 AND items_copied >= 0 AND items_copied <= items_total)
	)`, identity, reference, timestamp, boolean)
}

func storageReplacementItemsTableSQL2026082101(pg bool) string {
	identity := "id INTEGER PRIMARY KEY AUTOINCREMENT"
	reference := "INTEGER"
	timestamp := "TIMESTAMP"
	if pg {
		identity = "id BIGSERIAL PRIMARY KEY"
		reference = "BIGINT"
		timestamp = "TIMESTAMPTZ"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS storage_replacement_items (
		%[1]s,
		replacement_id %[2]s NOT NULL REFERENCES storage_replacements (id) ON UPDATE CASCADE ON DELETE CASCADE,
		upload_id %[2]s NOT NULL REFERENCES storage_uploads (id) ON UPDATE CASCADE ON DELETE RESTRICT,
		target_copy_id %[2]s REFERENCES storage_upload_copies (id) ON UPDATE CASCADE ON DELETE SET NULL,
		status TEXT NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		scheduled_at %[3]s NOT NULL DEFAULT CURRENT_TIMESTAMP,
		retry_count INTEGER NOT NULL DEFAULT 0,
		max_retries INTEGER,
		claimed_at %[3]s,
		lease_until %[3]s,
		last_error TEXT,
		created_at %[3]s NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at %[3]s NOT NULL DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT chk_storage_replacement_items_status CHECK (status IN ('pending', 'running', 'retrying', 'waiting_source', 'copied', 'cancelled', 'failed')),
		CONSTRAINT chk_storage_replacement_items_attempts CHECK (attempts >= 0),
		CONSTRAINT chk_storage_replacement_items_retry CHECK (retry_count >= 0 AND (max_retries IS NULL OR max_retries >= 0)),
		CONSTRAINT chk_storage_replacement_items_claim CHECK (
			(status = 'running' AND claimed_at IS NOT NULL AND lease_until IS NOT NULL)
			OR (status <> 'running' AND claimed_at IS NULL AND lease_until IS NULL)
		),
		CONSTRAINT uq_storage_replacement_items_upload UNIQUE (replacement_id, upload_id)
	)`, identity, reference, timestamp)
}
