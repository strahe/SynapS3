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
		Where("status = ?", model.BucketStatusActive).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing active buckets: %w", err)
	}
	return buckets, nil
}

func (r *BunBucketRepo) SoftDelete(ctx context.Context, id int64) error {
	_, err := r.db.NewDelete().
		Model((*model.Bucket)(nil)).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting bucket: %w", err)
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

func (r *BunBucketRepo) SetACL(ctx context.Context, name string, acl []byte) error {
	res, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("acl = ?", acl).
		Set("updated_at = ?", time.Now().UTC()).
		Where("name = ?", name).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("setting bucket ACL: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("setting bucket ACL: bucket %q not found", name)
	}
	return nil
}

func (r *BunBucketRepo) SetOwnerAndACL(ctx context.Context, name string, ownerAccessKey *string, acl []byte) error {
	res, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("owner_access_key = ?", ownerAccessKey).
		Set("acl = ?", acl).
		Set("updated_at = ?", time.Now().UTC()).
		Where("name = ?", name).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("setting bucket owner and ACL: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("setting bucket owner and ACL: bucket %q not found", name)
	}
	return nil
}

func (r *BunBucketRepo) SetDefaultCopies(ctx context.Context, name string, copies *int) error {
	res, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("default_copies = ?", copies).
		Set("updated_at = ?", time.Now().UTC()).
		Where("name = ?", name).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("setting bucket default copies: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("setting bucket default copies: bucket %q not found", name)
	}
	return nil
}

func (r *BunBucketRepo) CountByOwner(ctx context.Context, ownerAccessKey string) (int, error) {
	count, err := r.db.NewSelect().
		Model((*model.Bucket)(nil)).
		Where("owner_access_key = ?", ownerAccessKey).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting buckets by owner: %w", err)
	}
	return count, nil
}

func (r *BunBucketRepo) AggregateCountsByOwner(ctx context.Context) (map[string]int, error) {
	var rows []struct {
		OwnerAccessKey string `bun:"owner_access_key"`
		Count          int    `bun:"count"`
	}
	err := r.db.NewSelect().
		TableExpr("buckets").
		ColumnExpr("owner_access_key, COUNT(*) AS count").
		Where("owner_access_key IS NOT NULL").
		Where("owner_access_key <> ''").
		GroupExpr("owner_access_key").
		Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("aggregating bucket counts by owner: %w", err)
	}
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[row.OwnerAccessKey] = row.Count
	}
	return counts, nil
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

func (r *BunBucketRepo) ListACLs(ctx context.Context) ([]BucketACLSnapshot, error) {
	var buckets []BucketACLSnapshot
	err := r.db.NewSelect().
		TableExpr("buckets").
		Column("name", "status", "acl").
		OrderExpr("name ASC").
		Scan(ctx, &buckets)
	if err != nil {
		return nil, fmt.Errorf("listing bucket ACL snapshots: %w", err)
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

func (r *BunBucketRepo) CountStorageDataSets(ctx context.Context) (int, error) {
	count, err := r.db.NewSelect().
		Model((*model.StorageDataSet)(nil)).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting storage data sets: %w", err)
	}
	return count, nil
}
