package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026092101CommitStatusURL),
		func(context.Context, *bun.DB) error {
			return errors.New("commit status URL schema cannot be rolled back; create a new empty database")
		},
	)
}

// Both ledgers must be empty because this development migration deliberately
// does not convert persisted submissions or their confirmed projections.
func up2026092101CommitStatusURL(ctx context.Context, db bun.IDB) error {
	for _, table := range []string{"storage_commit_attempts", "storage_copies"} {
		count, err := db.NewSelect().Table(table).Count(ctx)
		if err != nil {
			return fmt.Errorf("checking %s before commit ledger rebuild: %w", table, err)
		}
		if count != 0 {
			return fmt.Errorf("%s is not empty: %w", table, incompatibleDatabaseError())
		}
	}
	if db.Dialect().Name() == dialect.PG {
		if _, err := db.ExecContext(ctx, `ALTER TABLE storage_copies DROP CONSTRAINT fk_storage_copies_confirmed_attempt`); err != nil {
			return fmt.Errorf("dropping commit projection foreign key: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE storage_commit_attempts`); err != nil {
		return fmt.Errorf("dropping empty commit ledger: %w", err)
	}
	spec := storageCommitAttemptTable2026090101()
	spec.model = (*storageCommitAttempt2026092101)(nil)
	spec.jsonColumns = nil
	for i, constraint := range spec.constraints {
		spec.constraints[i] = strings.ReplaceAll(constraint, "submission_json", "status_url")
	}
	if err := createInitialTable(ctx, db, spec); err != nil {
		return err
	}
	for _, index := range storageIndexes2026090101() {
		if index.table != "storage_commit_attempts" {
			continue
		}
		if err := createInitialIndexes(ctx, db, index); err != nil {
			return err
		}
	}
	return addForwardForeignKey(ctx, db, "storage_copies", storageCopyConfirmedAttemptForeignKey2026090101())
}

type storageCommitAttempt2026092101 struct {
	bun.BaseModel `bun:"table:storage_commit_attempts"`

	AttemptID              string  `bun:"type:text,pk"`
	ContentID              int64   `bun:",notnull"`
	StorageDataSetID       int64   `bun:",notnull"`
	Status                 string  `bun:"type:text,notnull,default:'reserved'"`
	ExtraDataHex           *string `bun:"type:text"`
	TransactionID          *string `bun:"type:text"`
	StatusURL              *string `bun:"type:text"`
	ConfirmedTransactionID *string `bun:"type:text"`
	AttentionCode          *string `bun:"type:text"`
	AttentionAt            *time.Time
	ReleaseReason          *string `bun:"type:text"`
	LastError              *string `bun:"type:text"`
	AttemptedAt            *time.Time
	ResolvedAt             *time.Time
	CreatedAt              time.Time `bun:",notnull"`
	UpdatedAt              time.Time `bun:",notnull"`
}
