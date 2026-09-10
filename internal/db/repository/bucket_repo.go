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

// Create inserts a bucket together with the replica slots its durability policy
// asks for, so a bucket never exists without the slots its data sets bind to.
func (r *BunBucketRepo) Create(ctx context.Context, bucket *model.Bucket) error {
	if !model.ValidStorageCopies(bucket.DefaultCopies) || !model.ValidStorageCopies(bucket.MinimumDurableCopies) ||
		bucket.MinimumDurableCopies > bucket.DefaultCopies {
		return fmt.Errorf("inserting bucket %q: %w", bucket.Name, ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if _, err := db.NewInsert().Model(bucket).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("inserting bucket %q: %w", bucket.Name, ErrAlreadyExists)
			}
			return fmt.Errorf("inserting bucket: %w", err)
		}
		return openBucketReplicaSlots(ctx, db, bucket.ID, bucket.DefaultCopies)
	})
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

func (r *BunBucketRepo) GetNamesByIDs(ctx context.Context, ids []int64) (map[int64]string, error) {
	out := make(map[int64]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	var buckets []model.Bucket
	if err := r.db.NewSelect().
		Model(&buckets).
		Column("id", "name").
		Where("id IN (?)", bun.List(ids)).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("selecting bucket names: %w", err)
	}
	for i := range buckets {
		out[buckets[i].ID] = buckets[i].Name
	}
	return out, nil
}

func (r *BunBucketRepo) ListActive(ctx context.Context) ([]model.Bucket, error) {
	var buckets []model.Bucket
	err := r.db.NewSelect().
		Model(&buckets).
		Where("status IN (?, ?)", model.BucketStatusProvisioning, model.BucketStatusReady).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing active buckets: %w", err)
	}
	return buckets, nil
}

func (r *BunBucketRepo) SoftDelete(ctx context.Context, id int64) error {
	return deleteBucketRow(ctx, r.db, id, "deleting bucket")
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

func (r *BunBucketRepo) PromoteReadyIfProvisioned(ctx context.Context, id int64, requiredDataSets int) (bool, error) {
	if id < 1 || !model.ValidStorageCopies(requiredDataSets) {
		return false, fmt.Errorf("promoting bucket storage readiness: %w", ErrInvalidInput)
	}
	result, err := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("status = ?", model.BucketStatusReady).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND status = ?", id, model.BucketStatusProvisioning).
		Where(`? = (SELECT COUNT(*) FROM storage_data_sets
			WHERE bucket_id = ? AND is_current = TRUE AND status = ? AND copy_index >= 0 AND copy_index < ?)`,
			requiredDataSets, id, model.StorageDataSetStatusReady, requiredDataSets).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("promoting bucket storage readiness: %w", err)
	}
	rows, _ := result.RowsAffected()
	return rows == 1, nil
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

func (r *BunBucketRepo) UpdateCopyPolicy(ctx context.Context, input UpdateBucketCopyPolicyInput) (*model.Bucket, error) {
	bucket, err := lockBucketByName(ctx, r.db, input.Name)
	if err != nil || bucket == nil {
		return bucket, err
	}

	update := r.db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", bucket.ID)
	if input.SetDefaultCopies && input.DefaultCopies != nil {
		// Lowering the target would leave the replicas above it running and
		// billed with nothing to retire them, so it is refused until slot
		// retirement exists. An explicit null resolves to the configured
		// default before it reaches here, so a reset that would lower is
		// refused by this same rule.
		if *input.DefaultCopies < bucket.DefaultCopies {
			return nil, fmt.Errorf("replica target %d -> %d: %w",
				bucket.DefaultCopies, *input.DefaultCopies, ErrReplicaTargetLowered)
		}
		bucket.DefaultCopies = *input.DefaultCopies
		update = update.Set("default_copies = ?", *input.DefaultCopies)
	}
	if input.SetMinimumDurableCopies {
		// A nil minimum means "every replica", which is now stored as the
		// replica target itself instead of a null standing for it.
		minimum := bucket.DefaultCopies
		if input.MinimumDurableCopies != nil {
			minimum = *input.MinimumDurableCopies
		}
		bucket.MinimumDurableCopies = minimum
		update = update.Set("minimum_durable_copies = ?", minimum)
	}
	if bucket.MinimumDurableCopies > bucket.DefaultCopies {
		return nil, fmt.Errorf("minimum durable copies exceeds explicit default copies: %w", ErrInvalidInput)
	}
	if _, err := update.Exec(ctx); err != nil {
		return nil, fmt.Errorf("updating bucket copy policy: %w", err)
	}
	if err := openBucketReplicaSlots(ctx, r.db, bucket.ID, bucket.DefaultCopies); err != nil {
		return nil, err
	}
	return bucket, nil
}

// ActiveReplicaSlots lists the copy indexes still open for new writes.
func (r *BunBucketRepo) ActiveReplicaSlots(ctx context.Context, bucketID int64) ([]int, error) {
	var indexes []int
	if err := r.db.NewSelect().
		Model((*model.BucketReplicaSlot)(nil)).
		Column("copy_index").
		Where("bucket_id = ? AND status = ?", bucketID, model.BucketReplicaSlotStatusActive).
		Order("copy_index ASC").
		Scan(ctx, &indexes); err != nil {
		return nil, fmt.Errorf("listing active bucket replica slots: %w", err)
	}
	return indexes, nil
}

// openBucketReplicaSlots makes slots 0..copies-1 active. Nothing closes a slot
// yet: the replica target can only grow, because lowering it would strand a paid
// storage service with no way to retire it. Closing slots belongs with that
// retirement path.
func openBucketReplicaSlots(ctx context.Context, db bun.IDB, bucketID int64, copies int) error {
	slots := make([]model.BucketReplicaSlot, 0, copies)
	now := time.Now().UTC()
	for copyIndex := range copies {
		slots = append(slots, model.BucketReplicaSlot{
			BucketID: bucketID, CopyIndex: copyIndex,
			Status: model.BucketReplicaSlotStatusActive, CreatedAt: now, UpdatedAt: now,
		})
	}
	if len(slots) > 0 {
		if _, err := db.NewInsert().
			Model(&slots).
			On("CONFLICT (bucket_id, copy_index) DO UPDATE").
			Set("status = ?", model.BucketReplicaSlotStatusActive).
			Set("updated_at = ?", now).
			Exec(ctx); err != nil {
			return fmt.Errorf("opening bucket replica slots: %w", err)
		}
	}
	return nil
}

func (r *BunBucketRepo) SetDefaultCopies(ctx context.Context, name string, copies *int) error {
	bucket, err := r.UpdateCopyPolicy(ctx, UpdateBucketCopyPolicyInput{
		Name:             name,
		SetDefaultCopies: true,
		DefaultCopies:    copies,
	})
	if err != nil {
		return err
	}
	if bucket == nil {
		return fmt.Errorf("setting bucket default copies: bucket %q not found", name)
	}
	return nil
}

func lockBucketByName(ctx context.Context, db bun.IDB, name string) (*model.Bucket, error) {
	lockResult, err := db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("updated_at = updated_at").
		Where("name = ?", name).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("locking bucket copy policy: %w", err)
	}
	rows, _ := lockResult.RowsAffected()
	if rows == 0 {
		return nil, nil
	}

	bucket := new(model.Bucket)
	if err := db.NewSelect().
		Model(bucket).
		Where("name = ?", name).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("locking bucket copy policy: %w", err)
	}
	return bucket, nil
}

func lockBucketByID(ctx context.Context, db bun.IDB, id int64) (*model.Bucket, error) {
	lockResult, err := db.NewUpdate().
		Model((*model.Bucket)(nil)).
		Set("updated_at = updated_at").
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("locking bucket %d: %w", id, err)
	}
	rows, _ := lockResult.RowsAffected()
	if rows == 0 {
		return nil, nil
	}
	bucket := new(model.Bucket)
	if err := db.NewSelect().Model(bucket).Where("id = ?", id).Scan(ctx); err != nil {
		return nil, fmt.Errorf("loading locked bucket %d: %w", id, err)
	}
	return bucket, nil
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
	return deleteBucketRow(ctx, r.db, id, "hard-deleting bucket")
}

// deleteBucketRow removes a bucket together with the replica slots it owns.
// The slots are deleted explicitly rather than by cascade, so a slot a data set
// still references blocks the delete instead of silently disappearing.
func deleteBucketRow(ctx context.Context, db bun.IDB, id int64, action string) error {
	return runMaybeTx(ctx, db, func(db bun.IDB) error {
		if _, err := db.NewDelete().
			Model((*model.BucketReplicaSlot)(nil)).
			Where("bucket_id = ?", id).
			Exec(ctx); err != nil {
			return fmt.Errorf("%s replica slots: %w", action, err)
		}
		if _, err := db.NewDelete().
			Model((*model.Bucket)(nil)).
			Where("id = ?", id).
			Exec(ctx); err != nil {
			return fmt.Errorf("%s: %w", action, err)
		}
		return nil
	})
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
