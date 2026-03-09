package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// BunObjectRepo implements ObjectRepository using Bun ORM.
// The Bun soft_delete tag on model.Object automatically adds
// "WHERE deleted_at IS NULL" to SELECT queries.
type BunObjectRepo struct {
	db bun.IDB
}

var _ ObjectRepository = (*BunObjectRepo)(nil)

func (r *BunObjectRepo) UpsertAndBumpGeneration(ctx context.Context, obj *model.Object) (int64, int64, error) {
	// Attempt to find an existing row (including soft-deleted) for this bucket+key.
	// We use AllWithDeleted() to bypass the soft_delete filter so that re-uploads
	// after deletion can reclaim the same row.
	existing := new(model.Object)
	err := r.db.NewSelect().
		Model(existing).
		Where("bucket_id = ? AND key = ?", obj.BucketID, obj.Key).
		WhereAllWithDeleted().
		Scan(ctx)

	if err != nil && err != sql.ErrNoRows {
		return 0, 0, fmt.Errorf("checking existing object: %w", err)
	}

	if err == sql.ErrNoRows {
		// New object — generation starts at 1.
		obj.Generation = 1
		obj.DeletedAt = nil
		_, insertErr := r.db.NewInsert().Model(obj).Exec(ctx)
		if insertErr != nil {
			return 0, 0, fmt.Errorf("inserting new object: %w", insertErr)
		}
		return obj.ID, obj.Generation, nil
	}

	// Existing row — bump generation, clear soft-delete, update all mutable fields.
	newGen := existing.Generation + 1
	_, updateErr := r.db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("generation = ?", newGen).
		Set("size = ?", obj.Size).
		Set("e_tag = ?", obj.ETag).
		Set("checksum = ?", obj.Checksum).
		Set("content_type = ?", obj.ContentType).
		Set("metadata = ?", obj.Metadata).
		Set("cache_path = ?", obj.CachePath).
		Set("piece_cid = NULL").
		Set("state = ?", model.ObjectStateCached).
		Set("retry_count = 0").
		Set("max_retries = ?", obj.MaxRetries).
		Set("last_error = NULL").
		Set("failed_at_state = NULL").
		Set("deleted_at = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ?", existing.ID).
		WhereAllWithDeleted().
		Exec(ctx)
	if updateErr != nil {
		return 0, 0, fmt.Errorf("updating existing object: %w", updateErr)
	}

	return existing.ID, newGen, nil
}

func (r *BunObjectRepo) GetByID(ctx context.Context, id int64) (*model.Object, error) {
	obj := new(model.Object)
	err := r.db.NewSelect().
		Model(obj).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting object by ID: %w", err)
	}
	return obj, nil
}

func (r *BunObjectRepo) GetByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.Object, error) {
	obj := new(model.Object)
	err := r.db.NewSelect().
		Model(obj).
		Where("bucket_id = ? AND key = ?", bucketID, key).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting object by bucket+key: %w", err)
	}
	return obj, nil
}

func (r *BunObjectRepo) ListByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.Object, error) {
	var objects []model.Object
	q := r.db.NewSelect().
		Model(&objects).
		Where("bucket_id = ?", bucketID).
		OrderExpr("key ASC")

	if prefix != "" {
		escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
		q = q.Where("key LIKE ? ESCAPE '\\'", escaped+"%")
	}
	if afterKey != "" {
		q = q.Where("key > ?", afterKey)
	}
	if maxKeys > 0 {
		q = q.Limit(maxKeys)
	}

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}
	return objects, nil
}

func (r *BunObjectRepo) SoftDelete(ctx context.Context, id int64) error {
	_, err := r.db.NewDelete().
		Model((*model.Object)(nil)).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("soft-deleting object: %w", err)
	}
	return nil
}

func (r *BunObjectRepo) UpdateState(ctx context.Context, id int64, generation int64, from, to model.ObjectState) error {
	q := r.db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("state = ?", to).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND generation = ? AND state = ?", id, generation, from)

	// Clear FailedAtState when retrying from failed state.
	if from == model.ObjectStateFailed {
		q = q.Set("failed_at_state = NULL")
	}

	res, err := q.Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating object state: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("state transition %s→%s failed: object %d gen %d not in expected state", from, to, id, generation)
	}
	return nil
}

func (r *BunObjectRepo) UpdateStateToFailed(ctx context.Context, id int64, generation int64, from model.ObjectState, lastError string) error {
	res, err := r.db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("state = ?", model.ObjectStateFailed).
		Set("failed_at_state = ?", from).
		Set("last_error = ?", lastError).
		Set("retry_count = retry_count + 1").
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND generation = ? AND state = ?", id, generation, from).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating object state to failed: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("state transition %s→failed failed: object %d gen %d not in expected state", from, id, generation)
	}
	return nil
}

func (r *BunObjectRepo) SetPieceCIDAndTransition(ctx context.Context, id int64, generation int64, pieceCID string, from, to model.ObjectState) error {
	res, err := r.db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("piece_cid = ?", pieceCID).
		Set("state = ?", to).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND generation = ? AND state = ?", id, generation, from).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("setting piece CID and transitioning state: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("SetPieceCIDAndTransition %s→%s failed: object %d gen %d not in expected state", from, to, id, generation)
	}
	return nil
}

func (r *BunObjectRepo) ListByState(ctx context.Context, state model.ObjectState, limit int) ([]model.Object, error) {
	var objects []model.Object
	q := r.db.NewSelect().
		Model(&objects).
		Where("state = ?", state).
		OrderExpr("updated_at ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing objects by state: %w", err)
	}
	return objects, nil
}

func (r *BunObjectRepo) ResetStaleStates(ctx context.Context, fromState, toState model.ObjectState, staleBefore time.Time) (int, error) {
	res, err := r.db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("state = ?", toState).
		Set("updated_at = ?", time.Now()).
		Where("state = ? AND updated_at < ?", fromState, staleBefore).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("resetting stale %s states: %w", fromState, err)
	}
	rows, _ := res.RowsAffected()
	return int(rows), nil
}
