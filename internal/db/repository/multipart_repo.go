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

// BunMultipartRepo implements MultipartUploadRepository using Bun ORM.
type BunMultipartRepo struct {
	db bun.IDB
}

var _ MultipartUploadRepository = (*BunMultipartRepo)(nil)

func (r *BunMultipartRepo) Create(ctx context.Context, upload *model.MultipartUpload) error {
	_, err := r.db.NewInsert().Model(upload).Exec(ctx)
	if err != nil {
		return fmt.Errorf("inserting multipart upload: %w", err)
	}
	return nil
}

func (r *BunMultipartRepo) GetByUploadID(ctx context.Context, uploadID string) (*model.MultipartUpload, error) {
	upload := new(model.MultipartUpload)
	err := r.db.NewSelect().
		Model(upload).
		Where("upload_id = ?", uploadID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting multipart upload: %w", err)
	}
	return upload, nil
}

func (r *BunMultipartRepo) ListByBucket(
	ctx context.Context,
	bucketID int64,
	prefix, keyMarker, uploadIDMarker string,
	maxUploads int,
) ([]model.MultipartUpload, error) {
	var uploads []model.MultipartUpload
	q := r.db.NewSelect().
		Model(&uploads).
		Where("bucket_id = ?", bucketID).
		Where("status = ?", model.MultipartStatusInitiated).
		OrderExpr("key ASC, upload_id ASC")

	if prefix != "" {
		escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
		q = q.Where("key LIKE ? ESCAPE '\\'", escaped+"%")
	}
	if keyMarker != "" {
		if uploadIDMarker != "" {
			q = q.Where("(key > ? OR (key = ? AND upload_id > ?))", keyMarker, keyMarker, uploadIDMarker)
		} else {
			q = q.Where("key > ?", keyMarker)
		}
	}
	if maxUploads > 0 {
		q = q.Limit(maxUploads)
	}

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing multipart uploads: %w", err)
	}
	return uploads, nil
}

func (r *BunMultipartRepo) CountActiveByBucket(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		Model((*model.MultipartUpload)(nil)).
		Where("bucket_id = ?", bucketID).
		Where("status IN (?)", bun.List([]model.MultipartStatus{
			model.MultipartStatusInitiated,
			model.MultipartStatusCompleting,
		})).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting active multipart uploads by bucket: %w", err)
	}
	return int64(count), nil
}

// SetStatus atomically transitions a multipart upload from one status to another (CAS).
// Returns an error if the upload is not in the expected 'from' status.
func (r *BunMultipartRepo) SetStatus(ctx context.Context, uploadID string, from, to model.MultipartStatus) error {
	res, err := r.db.NewUpdate().
		Model((*model.MultipartUpload)(nil)).
		Set("status = ?", to).
		Set("updated_at = ?", time.Now()).
		Where("upload_id = ? AND status = ?", uploadID, from).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating multipart upload status: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("status transition %s→%s failed: upload %s not in expected state", from, to, uploadID)
	}
	return nil
}

func (r *BunMultipartRepo) Delete(ctx context.Context, uploadID string) error {
	_, err := r.db.NewDelete().
		Model((*model.MultipartUpload)(nil)).
		Where("upload_id = ?", uploadID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting multipart upload: %w", err)
	}
	return nil
}

// CreatePart upserts a part record. If the same (upload_id, part_number) exists, it is replaced.
func (r *BunMultipartRepo) CreatePart(ctx context.Context, part *model.MultipartPart) error {
	_, err := r.db.NewInsert().
		Model(part).
		On("CONFLICT (upload_id, part_number) DO UPDATE").
		Set("size = EXCLUDED.size").
		Set("e_tag = EXCLUDED.e_tag").
		Set("created_at = EXCLUDED.created_at").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upserting multipart part: %w", err)
	}
	return nil
}

func (r *BunMultipartRepo) GetParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]model.MultipartPart, error) {
	var parts []model.MultipartPart
	q := r.db.NewSelect().
		Model(&parts).
		Where("upload_id = ?", uploadID).
		OrderExpr("part_number ASC")

	if partNumberMarker > 0 {
		q = q.Where("part_number > ?", partNumberMarker)
	}
	if maxParts > 0 {
		q = q.Limit(maxParts)
	}

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing multipart parts: %w", err)
	}
	return parts, nil
}

func (r *BunMultipartRepo) GetPartsByNumbers(ctx context.Context, uploadID string, numbers []int) ([]model.MultipartPart, error) {
	var parts []model.MultipartPart
	err := r.db.NewSelect().
		Model(&parts).
		Where("upload_id = ?", uploadID).
		Where("part_number IN (?)", bun.List(numbers)).
		OrderExpr("part_number ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("selecting parts by numbers: %w", err)
	}
	return parts, nil
}

func (r *BunMultipartRepo) DeleteParts(ctx context.Context, uploadID string) error {
	_, err := r.db.NewDelete().
		Model((*model.MultipartPart)(nil)).
		Where("upload_id = ?", uploadID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting multipart parts: %w", err)
	}
	return nil
}
