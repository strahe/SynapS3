package migrations

import (
	"context"
	"errors"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type storagePullAttempt2026100902 struct {
	bun.BaseModel `bun:"table:storage_pull_attempts"`

	AttemptID          string    `bun:"type:text,pk"`
	ContentID          int64     `bun:",notnull"`
	StorageDataSetID   int64     `bun:",notnull"`
	Status             string    `bun:"type:text,notnull"`
	SourceProviderID   string    `bun:"type:text,notnull"`
	SourceDataSetID    string    `bun:"type:text,notnull"`
	SourcePieceID      string    `bun:"type:text,notnull"`
	SourcePieceCID     string    `bun:"type:text,notnull"`
	SourceRetrievalURL string    `bun:"type:text,notnull"`
	ExtraDataHex       string    `bun:"type:text,notnull"`
	LastError          *string   `bun:"type:text"`
	AttemptedAt        time.Time `bun:",notnull"`
	ResolvedAt         *time.Time
	CreatedAt          time.Time `bun:",notnull"`
	UpdatedAt          time.Time `bun:",notnull"`
}

const pullDataSetIndex2026100902 = "CREATE INDEX idx_storage_pull_attempts_data_set ON storage_pull_attempts (storage_data_set_id)"

func init() {
	Migrations.MustRegister(up2026100902PullDataSetForeignKey, func(context.Context, *bun.DB) error {
		return errors.New("pull data-set foreign key migration requires restoring a database backup to roll back")
	})
}

func pullDataSetForeignKey2026100902(ctx context.Context, db bun.IDB) (bool, error) {
	if db.Dialect().Name() == dialect.PG {
		return queryExists(ctx, db, `SELECT COUNT(*) FROM pg_constraint
			WHERE conrelid = 'storage_pull_attempts'::regclass AND contype = 'f'
			AND conname = 'fk_storage_pull_attempts_data_set'
			AND confrelid = 'storage_data_sets'::regclass AND convalidated
			AND confupdtype = 'r' AND confdeltype = 'r'
			AND conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = conrelid AND attname = 'storage_data_set_id')]::smallint[]
			AND confkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = confrelid AND attname = 'id')]::smallint[]`)
	}
	return queryExists(ctx, db, `SELECT COUNT(*) FROM pragma_foreign_key_list('storage_pull_attempts')
		WHERE "table" = 'storage_data_sets' AND "from" = 'storage_data_set_id' AND "to" = 'id'
		AND on_update = 'RESTRICT' AND on_delete = 'RESTRICT'`)
}

func up2026100902PullDataSetForeignKey(ctx context.Context, db *bun.DB) error {
	foreignKey, err := pullDataSetForeignKey2026100902(ctx, db)
	if err != nil {
		return err
	}
	index, err := indexExists(ctx, db, "idx_storage_pull_attempts_data_set")
	if err != nil {
		return err
	}
	if foreignKey && index {
		return nil
	}
	if foreignKey || index {
		return incompatibleDatabaseError()
	}
	if db.Dialect().Name() == dialect.PG {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE storage_pull_attempts ADD CONSTRAINT fk_storage_pull_attempts_data_set
				FOREIGN KEY (storage_data_set_id) REFERENCES storage_data_sets (id) ON UPDATE RESTRICT ON DELETE RESTRICT`); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, pullDataSetIndex2026100902)
			return err
		})
	}
	var indexes []string
	if err := db.NewRaw(`SELECT sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'storage_pull_attempts' AND sql IS NOT NULL ORDER BY name`).Scan(ctx, &indexes); err != nil {
		return err
	}
	indexes = append(indexes, pullDataSetIndex2026100902)
	return rebuildSQLiteTables(ctx, db, sqliteTableRebuild{
		table: "storage_pull_attempts", indexes: indexes,
		create: func(ctx context.Context, tx bun.Tx, name string) error {
			_, err := tx.NewCreateTable().Model((*storagePullAttempt2026100902)(nil)).ModelTableExpr("?", bun.Ident(name)).
				ColumnExpr("CONSTRAINT chk_storage_pull_attempts_identity CHECK (attempt_id <> '' AND source_provider_id <> '' AND source_data_set_id <> '' AND source_piece_id <> '' AND source_piece_cid <> '' AND source_retrieval_url <> '')").
				ColumnExpr("CONSTRAINT chk_storage_pull_attempts_authorization CHECK (extra_data_hex <> '')").
				ColumnExpr("CONSTRAINT chk_storage_pull_attempts_status CHECK (status IN ('attempted', 'abandoned'))").
				ColumnExpr("CONSTRAINT chk_storage_pull_attempts_error CHECK (last_error IS NULL OR (status = 'abandoned' AND last_error <> ''))").
				ColumnExpr("CONSTRAINT chk_storage_pull_attempts_abandoned CHECK (status <> 'abandoned' OR resolved_at IS NOT NULL)").
				ColumnExpr("CONSTRAINT fk_storage_pull_attempts_data_set FOREIGN KEY (storage_data_set_id) REFERENCES storage_data_sets (id) ON UPDATE RESTRICT ON DELETE RESTRICT").Exec(ctx)
			return err
		},
	})
}
