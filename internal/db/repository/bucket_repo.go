package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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
		if isUniqueViolation(err) {
			return fmt.Errorf("inserting bucket %q: %w", bucket.Name, ErrAlreadyExists)
		}
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
		Where("status IN (?)", bun.List([]model.BucketStatus{
			model.BucketStatusActive,
			model.BucketStatusCreating,
			model.BucketStatusDeleting,
		})).
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

func (r *BunBucketRepo) UpdateStatus(ctx context.Context, id int64, from, to model.BucketStatus) error {
	res, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("status = ?", to).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND status = ?", id, from).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating bucket status: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("bucket status transition %s→%s failed: bucket %d not in expected state", from, to, id)
	}
	return nil
}

func (r *BunBucketRepo) SetProofSetID(ctx context.Context, id int64, proofSetID string) error {
	res, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("proof_set_id = ?", proofSetID).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("setting proof set ID: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("setting proof set ID: bucket %d not found", id)
	}
	return nil
}

func (r *BunBucketRepo) HardDelete(ctx context.Context, id int64) error {
	_, err := r.db.NewDelete().
		Model((*model.Bucket)(nil)).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("hard-deleting bucket: %w", err)
	}
	return nil
}

func (r *BunBucketRepo) List(ctx context.Context) ([]model.Bucket, error) {
	var buckets []model.Bucket
	err := r.db.NewSelect().
		Model(&buckets).
		OrderExpr("name ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing all buckets: %w", err)
	}
	return buckets, nil
}

func (r *BunBucketRepo) CountByStatus(ctx context.Context) ([]BucketStatusCount, error) {
	var counts []BucketStatusCount
	err := r.db.NewSelect().
		TableExpr("buckets").
		ColumnExpr("status, COUNT(*) AS count").
		GroupExpr("status").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting buckets by status: %w", err)
	}
	return counts, nil
}
