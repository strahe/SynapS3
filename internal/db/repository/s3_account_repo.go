package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// BunS3AccountRepo implements S3AccountRepository using Bun ORM.
type BunS3AccountRepo struct {
	db bun.IDB
}

var _ S3AccountRepository = (*BunS3AccountRepo)(nil)

func (r *BunS3AccountRepo) Create(ctx context.Context, account *model.S3Account) error {
	_, err := r.db.NewInsert().Model(account).Exec(ctx)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("inserting S3 account %q: %w", account.AccessKey, ErrAlreadyExists)
		}
		return fmt.Errorf("inserting S3 account: %w", err)
	}
	return nil
}

func (r *BunS3AccountRepo) GetByAccessKey(ctx context.Context, accessKey string) (*model.S3Account, error) {
	account := new(model.S3Account)
	err := r.db.NewSelect().
		Model(account).
		Where("access_key = ?", accessKey).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting S3 account by access key: %w", err)
	}
	return account, nil
}

func (r *BunS3AccountRepo) GetRoot(ctx context.Context) (*model.S3Account, error) {
	account := new(model.S3Account)
	err := r.db.NewSelect().
		Model(account).
		Where("is_root = ?", true).
		OrderExpr("created_at ASC, access_key ASC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting root S3 account: %w", err)
	}
	return account, nil
}

func (r *BunS3AccountRepo) ListNonRoot(ctx context.Context) ([]model.S3Account, error) {
	var accounts []model.S3Account
	err := r.db.NewSelect().
		Model(&accounts).
		Where("is_root = ?", false).
		OrderExpr("access_key ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing S3 accounts: %w", err)
	}
	return accounts, nil
}

func (r *BunS3AccountRepo) Update(ctx context.Context, accessKey string, update S3AccountUpdate) error {
	query := r.db.NewUpdate().
		Model((*model.S3Account)(nil)).
		Set("updated_at = ?", time.Now().UTC()).
		Where("access_key = ?", accessKey)
	if update.SecretKey != nil {
		query.Set("secret_key = ?", *update.SecretKey)
	}
	if update.Role != "" {
		query.Set("role = ?", update.Role)
	}
	res, err := query.Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating S3 account: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("updating S3 account %q: %w", accessKey, ErrNotFound)
	}
	return nil
}

func (r *BunS3AccountRepo) Delete(ctx context.Context, accessKey string) error {
	res, err := r.db.NewDelete().
		Model((*model.S3Account)(nil)).
		Where("access_key = ?", accessKey).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting S3 account: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("deleting S3 account %q: %w", accessKey, ErrNotFound)
	}
	return nil
}

func (r *BunS3AccountRepo) LockByAccessKey(ctx context.Context, accessKey string) (*model.S3Account, error) {
	// Use a no-op UPDATE as a portable pessimistic lock for the surrounding transaction.
	// SQLite takes a write lock, and PostgreSQL holds the row update lock until commit.
	res, err := r.db.NewUpdate().
		Model((*model.S3Account)(nil)).
		Set("access_key = access_key").
		Where("access_key = ?", accessKey).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("locking S3 account: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return nil, nil
	}
	return r.GetByAccessKey(ctx, accessKey)
}
