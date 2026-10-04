package migrations

import (
	"context"
	"errors"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

const commitSealIntentCheck2026100401 = "CONSTRAINT chk_storage_commit_requests_seal_intent CHECK (seal_requested_at IS NULL OR status = 'collecting')"

func init() {
	Migrations.MustRegister(up2026100401CommitBatchSealing, func(context.Context, *bun.DB) error {
		return errors.New("batch sealing migration cannot be rolled back while seal requests may exist")
	})
}

type storageCommitRequest2026100401 struct {
	bun.BaseModel          `bun:"table:storage_commit_requests"`
	RequestID              string `bun:"type:text,pk"`
	StorageDataSetID       int64  `bun:",notnull"`
	Status                 string `bun:"type:text,notnull"`
	TaskID                 *int64
	PieceCount             int     `bun:"type:integer,notnull"`
	ExtraDataHex           *string `bun:"type:text"`
	SealedAt               *time.Time
	FirstSentAt            *time.Time
	Sends                  int `bun:"type:integer,notnull"`
	SubmittedAt            *time.Time
	LastSentAt             *time.Time
	TransactionID          *string `bun:"type:text"`
	StatusURL              *string `bun:"type:text"`
	SubmitError            *string `bun:"type:text"`
	Refusals               int     `bun:"type:integer,notnull"`
	RetryAt                *time.Time
	FirstPieceID           *string `bun:"type:text"`
	ConfirmedTransactionID *string `bun:"type:text"`
	ConfirmedAt            *time.Time
	LastError              *string `bun:"type:text"`
	AttentionCode          *string `bun:"type:text"`
	AttentionAt            *time.Time
	CreatedAt              time.Time `bun:",notnull"`
	UpdatedAt              time.Time `bun:",notnull"`
	SealRequestedAt        *time.Time
}

func commitBatchIndexes2026100401() []initialIndexSpec {
	return []initialIndexSpec{
		{name: "idx_storage_commit_requests_created", table: "storage_commit_requests", columns: []string{"created_at", "request_id"}},
		{name: "idx_storage_commit_requests_status_created", table: "storage_commit_requests", columns: []string{"status", "created_at", "request_id"}},
	}
}

func up2026100401CommitBatchSealing(ctx context.Context, db *bun.DB) error {
	exists, err := columnExists(ctx, db, "storage_commit_requests", "seal_requested_at")
	if err != nil {
		return err
	}
	if exists {
		for _, index := range commitBatchIndexes2026100401() {
			ok, err := indexExists(ctx, db, index.name)
			if err != nil {
				return err
			}
			if !ok {
				return incompatibleDatabaseError()
			}
		}
		return nil
	}
	if db.Dialect().Name() == dialect.PG {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			if _, err := tx.NewAddColumn().Model((*storageCommitRequest2026100401)(nil)).ColumnExpr("seal_requested_at TIMESTAMPTZ").Exec(ctx); err != nil {
				return err
			}
			if _, err := tx.NewRaw("ALTER TABLE storage_commit_requests ADD " + commitSealIntentCheck2026100401).Exec(ctx); err != nil {
				return err
			}
			return createInitialIndexes(ctx, tx, commitBatchIndexes2026100401()...)
		})
	}
	var indexes []string
	for _, index := range append(storageIndexes2026090101(), commitBatchIndexes2026100401()...) {
		if index.table != "storage_commit_requests" {
			continue
		}
		query := db.NewCreateIndex().Index(index.name).Table(index.table)
		if index.unique {
			query.Unique()
		}
		for _, column := range index.columns {
			query.ColumnExpr(column)
		}
		if index.where != "" {
			query.Where(index.where)
		}
		ddl, err := query.AppendQuery(db.QueryGen(), nil)
		if err != nil {
			return err
		}
		indexes = append(indexes, string(ddl))
	}
	return rebuildSQLiteTables(ctx, db, sqliteTableRebuild{
		table: "storage_commit_requests", indexes: indexes,
		create: func(ctx context.Context, tx bun.Tx, name string) error {
			spec := storageCommitRequestTable2026090101()
			query := tx.NewCreateTable().Model((*storageCommitRequest2026100401)(nil)).ModelTableExpr("?", bun.Ident(name))
			for _, constraint := range spec.constraints {
				query.ColumnExpr(constraint)
			}
			query.ColumnExpr(commitSealIntentCheck2026100401)
			for _, foreignKey := range spec.foreignKeys {
				query.ForeignKey(foreignKey)
			}
			_, err := query.Exec(ctx)
			return err
		},
	})
}
