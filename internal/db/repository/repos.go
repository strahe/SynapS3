package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// Repositories aggregates all repository implementations, providing a single
// dependency for service/backend layers.  The WithTx helper executes a callback
// inside a database transaction with a clone of Repositories backed by the tx.
type Repositories struct {
	Buckets    BucketRepository
	S3Accounts S3AccountRepository
	Objects    ObjectRepository
	Tasks      TaskRepository
	Multiparts MultipartUploadRepository

	db bun.IDB
}

// NewRepositories constructs a Repositories with concrete Bun-backed implementations.
func NewRepositories(db bun.IDB) *Repositories {
	return &Repositories{
		Buckets:    &BunBucketRepo{db: db},
		S3Accounts: &BunS3AccountRepo{db: db},
		Objects:    &BunObjectRepo{db: db},
		Tasks:      &BunTaskRepo{db: db},
		Multiparts: &BunMultipartRepo{db: db},
		db:         db,
	}
}

// WithTx runs fn inside a database transaction.  The callback receives a
// *Repositories whose repository implementations are all backed by the same tx.
// If fn returns nil the transaction is committed; otherwise it is rolled back.
func (r *Repositories) WithTx(ctx context.Context, fn func(txRepos *Repositories) error) error {
	bunDB, ok := r.db.(*bun.DB)
	if !ok {
		return fmt.Errorf("WithTx requires *bun.DB, got %T", r.db)
	}

	for attempt := 0; ; attempt++ {
		err := bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			txRepos := NewRepositories(tx)
			return fn(txRepos)
		})
		if err == nil {
			return nil
		}
		if !shouldRetryRepositoryTx(err) || attempt >= 19 {
			return err
		}
		delay := time.Duration(attempt+1) * 25 * time.Millisecond
		if delay > 200*time.Millisecond {
			delay = 200 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
