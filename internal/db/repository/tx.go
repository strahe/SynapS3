package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// runMaybeTx runs fn in a transaction when db is a connection pool, and inline
// when it is already a transaction. Repository methods use it so they compose
// under WithTx without nesting transactions.
func runMaybeTx(ctx context.Context, db bun.IDB, fn func(bun.IDB) error) error {
	if pool, ok := db.(*bun.DB); ok {
		return runRepositoryTx(ctx, pool, func(ctx context.Context, tx bun.Tx) error {
			return fn(tx)
		})
	}
	return fn(db)
}

func runRepositoryTx(ctx context.Context, db *bun.DB, fn func(context.Context, bun.Tx) error) error {
	var options *sql.TxOptions
	if db.Dialect().Name() == dialect.PG {
		options = &sql.TxOptions{Isolation: sql.LevelReadCommitted}
	}
	for attempt := 0; ; attempt++ {
		err := db.RunInTx(ctx, options, fn)
		if err == nil || !shouldRetryRepositoryTx(err) {
			return err
		}
		if attempt >= 19 {
			return fmt.Errorf("%w: %w", ErrRepositoryContended, err)
		}
		delay := min(time.Duration(attempt+1)*25*time.Millisecond, 200*time.Millisecond)
		delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
