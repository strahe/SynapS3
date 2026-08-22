package repository

import (
	"context"

	"github.com/uptrace/bun"
)

// runMaybeTx runs fn in a transaction when db is a connection pool, and inline
// when it is already a transaction. Repository methods use it so they compose
// under WithTx without nesting transactions.
func runMaybeTx(ctx context.Context, db bun.IDB, fn func(bun.IDB) error) error {
	if pool, ok := db.(*bun.DB); ok {
		return pool.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(tx)
		})
	}
	return fn(db)
}
