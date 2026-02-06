package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// BunBucketRepo implements BucketRepository using Bun ORM.
type BunBucketRepo struct {
	db bun.IDB
}

var _ BucketRepository = (*BunBucketRepo)(nil)

func (r *BunBucketRepo) Create(ctx context.Context, bucket *model.Bucket) error {
	_, err := r.db.NewInsert().Model(bucket).Exec(ctx)
	if err != nil {
		return fmt.Errorf("inserting bucket: %w", err)
	}
	return nil
}

func (r *BunBucketRepo) GetByName(ctx context.Context, name string) (*model.Bucket, error) {
	bucket := new(model.Bucket)
	err := r.db.NewSelect().
		Model(bucket).
		Where("name = ?", name).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting bucket by name: %w", err)
	}
	return bucket, nil
}

func (r *BunBucketRepo) GetByID(ctx context.Context, id int64) (*model.Bucket, error) {
	bucket := new(model.Bucket)
	err := r.db.NewSelect().
		Model(bucket).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting bucket by id: %w", err)
	}
	return bucket, nil
}

func (r *BunBucketRepo) ListActive(ctx context.Context) ([]model.Bucket, error) {
	var buckets []model.Bucket
	err := r.db.NewSelect().
		Model(&buckets).
		Where("status != ?", model.BucketStatusDeleted).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing active buckets: %w", err)
	}
	return buckets, nil
}

func (r *BunBucketRepo) SoftDelete(ctx context.Context, id int64) error {
	_, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("status = ?", model.BucketStatusDeleted).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("soft-deleting bucket: %w", err)
	}
	return nil
}
