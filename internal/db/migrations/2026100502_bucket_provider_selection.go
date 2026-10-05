package migrations

import (
	"context"
	"errors"

	"github.com/uptrace/bun"
)

type bucketProviderSelection2026100502 struct {
	bun.BaseModel             `bun:"table:buckets"`
	ProviderSelectionStrategy string
}

func init() {
	Migrations.MustRegister(transactionalMigration(up2026100502BucketProviderSelection), func(context.Context, *bun.DB) error {
		return errors.New("bucket provider selection migration cannot be rolled back without losing bucket preferences")
	})
}

func up2026100502BucketProviderSelection(ctx context.Context, db bun.IDB) error {
	exists, err := columnExists(ctx, db, "buckets", "provider_selection_strategy")
	if err != nil || exists {
		return err
	}
	_, err = db.NewAddColumn().Model((*bucketProviderSelection2026100502)(nil)).
		ColumnExpr("provider_selection_strategy TEXT NOT NULL DEFAULT 'distribution' CHECK (provider_selection_strategy IN ('distribution', 'speed'))").Exec(ctx)
	return err
}
