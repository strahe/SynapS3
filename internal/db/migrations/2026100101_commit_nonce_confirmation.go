package migrations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		up2026100101CommitNonceConfirmation,
		func(context.Context, *bun.DB) error {
			return errors.New("commit attempts confirmed by their nonce cannot be rolled back")
		},
	)
}

// up2026100101CommitNonceConfirmation lets a confirmed commit attempt carry no
// confirmed transaction. The FWSS nonce record proves the pieces were added
// even when no provider reported the transaction that included them, and the
// attempt's extra data names that nonce for anyone checking again.
func up2026100101CommitNonceConfirmation(ctx context.Context, db *bun.DB) error {
	if db.Dialect().Name() == dialect.SQLite {
		return rebuildSQLiteTables(ctx, db, sqliteTableRebuild{
			table:  "storage_commit_attempts",
			create: createStorageCommitAttempts2026100101,
		})
	}
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.ExecContext(ctx, "ALTER TABLE storage_commit_attempts DROP CONSTRAINT chk_storage_commit_attempts_evidence_shape, ADD "+
			storageCommitAttemptEvidenceShape2026100101)
		if err != nil {
			return fmt.Errorf("replacing storage commit attempt evidence shape: %w", err)
		}
		return nil
	})
}

type storageCommitAttempt2026100101 struct {
	bun.BaseModel `bun:"table:storage_commit_attempts"`

	AttemptID              string  `bun:"type:text,pk"`
	ContentID              int64   `bun:",notnull"`
	StorageDataSetID       int64   `bun:",notnull"`
	Status                 string  `bun:"type:text,notnull,default:'reserved'"`
	ExtraDataHex           *string `bun:"type:text"`
	TransactionID          *string `bun:"type:text"`
	StatusURL              *string `bun:"type:text"`
	SubmitError            *string `bun:"type:text"`
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

// A confirmed attempt names its confirmed transaction only when a provider
// reported one, and only alongside the submission it answered.
const storageCommitAttemptEvidenceShape2026100101 = `CONSTRAINT chk_storage_commit_attempts_evidence_shape CHECK (
	(status = 'reserved' AND attempted_at IS NULL AND extra_data_hex IS NULL AND transaction_id IS NULL AND status_url IS NULL AND confirmed_transaction_id IS NULL AND attention_code IS NULL AND attention_at IS NULL AND last_error IS NULL)
	OR (status = 'attempted' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND confirmed_transaction_id IS NULL AND last_error IS NULL)
	OR (status = 'confirmed' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND last_error IS NULL AND (confirmed_transaction_id IS NULL OR transaction_id IS NOT NULL))
	OR (status = 'released' AND confirmed_transaction_id IS NULL AND last_error IS NULL AND ((attempted_at IS NULL AND extra_data_hex IS NULL AND transaction_id IS NULL AND status_url IS NULL AND attention_code IS NULL AND attention_at IS NULL) OR (attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL)))
	OR (status = 'rejected' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND confirmed_transaction_id IS NULL AND last_error IS NOT NULL AND last_error <> '')
)`

// createStorageCommitAttempts2026100101 creates the SQLite table under name.
// Every constraint other than the evidence shape is the baseline's.
func createStorageCommitAttempts2026100101(ctx context.Context, tx bun.Tx, name string) error {
	query := tx.NewCreateTable().
		Model((*storageCommitAttempt2026100101)(nil)).
		ModelTableExpr("?", bun.Ident(name))
	for _, constraint := range []string{
		"CONSTRAINT chk_storage_commit_attempts_identity CHECK (attempt_id <> '' AND (extra_data_hex IS NULL OR extra_data_hex <> '') AND (transaction_id IS NULL OR transaction_id <> '') AND (status_url IS NULL OR status_url <> '') AND (submit_error IS NULL OR submit_error <> '') AND (confirmed_transaction_id IS NULL OR confirmed_transaction_id <> '') AND (attention_code IS NULL OR attention_code <> '') AND (release_reason IS NULL OR release_reason <> ''))",
		"CONSTRAINT chk_storage_commit_attempts_status CHECK (status IN ('reserved', 'attempted', 'confirmed', 'released', 'rejected'))",
		"CONSTRAINT uq_storage_commit_attempts_projection UNIQUE (attempt_id, status, content_id, storage_data_set_id)",
		"CONSTRAINT chk_storage_commit_attempts_resolution CHECK ((status IN ('reserved', 'attempted') AND resolved_at IS NULL) OR (status IN ('confirmed', 'released', 'rejected') AND resolved_at IS NOT NULL))",
		storageCommitAttemptEvidenceShape2026100101,
		"CONSTRAINT chk_storage_commit_attempts_submission_evidence CHECK ((transaction_id IS NULL AND status_url IS NULL) OR (transaction_id IS NOT NULL AND status_url IS NOT NULL))",
		"CONSTRAINT chk_storage_commit_attempts_submit_error CHECK (submit_error IS NULL OR attempted_at IS NOT NULL)",
		"CONSTRAINT chk_storage_commit_attempts_attention CHECK ((attention_code IS NULL AND attention_at IS NULL) OR (attention_code IS NOT NULL AND attention_at IS NOT NULL AND attempted_at IS NOT NULL))",
		"CONSTRAINT chk_storage_commit_attempts_release CHECK ((status = 'released' AND release_reason IS NOT NULL) OR (status <> 'released' AND release_reason IS NULL))",
	} {
		query.ColumnExpr(constraint)
	}
	if _, err := query.Exec(ctx); err != nil {
		return fmt.Errorf("creating table %s: %w", name, err)
	}
	return nil
}
