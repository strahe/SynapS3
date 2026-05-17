package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/availability"
	"github.com/uptrace/bun"
)

const (
	defaultAvailabilityListLimit = 100
	maxAvailabilityListLimit     = 500
)

type BunAvailabilityRepo struct {
	db bun.IDB
}

func (r *BunAvailabilityRepo) ReplaceProviderSnapshots(ctx context.Context, snapshots []availability.ProviderSnapshot) error {
	return r.withTx(ctx, func(ctx context.Context, db bun.IDB) error {
		now := time.Now().UTC()
		for i := range snapshots {
			prepareProviderSnapshot(&snapshots[i], now)
		}
		if _, err := db.NewDelete().Model((*availability.ProviderSnapshot)(nil)).Where("1 = 1").Exec(ctx); err != nil {
			return err
		}
		if len(snapshots) > 0 {
			_, err := db.NewInsert().
				Model(&snapshots).
				Exec(ctx)
			return err
		}
		return nil
	})
}

func (r *BunAvailabilityRepo) ListProviderSnapshots(ctx context.Context, opts availability.ListOptions) (availability.ProviderSnapshotPage, error) {
	limit, offset := normalizeAvailabilityPagination(opts)
	var rows []availability.ProviderSnapshot
	if err := applyProviderAvailabilityFilters(r.db.NewSelect().Model(&rows), opts).
		OrderExpr("length(provider_id) ASC, provider_id ASC").
		Limit(limit).
		Offset(offset).
		Scan(ctx); err != nil {
		return availability.ProviderSnapshotPage{}, err
	}

	aggregate, err := r.providerSnapshotAggregate(ctx, opts)
	if err != nil {
		return availability.ProviderSnapshotPage{}, err
	}
	return availability.ProviderSnapshotPage{
		Items:         rows,
		Summary:       aggregate.summary(),
		LastCheckedAt: aggregate.lastCheckedAt(),
		Total:         aggregate.Total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (r *BunAvailabilityRepo) ReplaceDataSetSnapshots(ctx context.Context, snapshots []availability.DataSetSnapshot) error {
	return r.withTx(ctx, func(ctx context.Context, db bun.IDB) error {
		now := time.Now().UTC()
		for i := range snapshots {
			prepareDataSetSnapshot(&snapshots[i], now)
		}
		if _, err := db.NewDelete().Model((*availability.DataSetSnapshot)(nil)).Where("1 = 1").Exec(ctx); err != nil {
			return err
		}
		if len(snapshots) > 0 {
			_, err := db.NewInsert().
				Model(&snapshots).
				Exec(ctx)
			return err
		}
		return nil
	})
}

func (r *BunAvailabilityRepo) ListDataSetSnapshots(ctx context.Context, opts availability.ListOptions) (availability.DataSetSnapshotPage, error) {
	limit, offset := normalizeAvailabilityPagination(opts)
	var rows []availability.DataSetSnapshot
	if err := applyDataSetAvailabilityFilters(r.db.NewSelect().Model(&rows), opts).
		OrderExpr("bucket_name ASC, local_data_set_id ASC").
		Limit(limit).
		Offset(offset).
		Scan(ctx); err != nil {
		return availability.DataSetSnapshotPage{}, err
	}

	aggregate, err := r.dataSetSnapshotAggregate(ctx, opts)
	if err != nil {
		return availability.DataSetSnapshotPage{}, err
	}
	return availability.DataSetSnapshotPage{
		Items:         rows,
		Summary:       aggregate.summary(),
		LastCheckedAt: aggregate.lastCheckedAt(),
		Total:         aggregate.Total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (r *BunAvailabilityRepo) GetDataSetSnapshotsByLocalIDs(ctx context.Context, localIDs []int64) (map[int64]availability.DataSetSnapshot, error) {
	out := make(map[int64]availability.DataSetSnapshot)
	if len(localIDs) == 0 {
		return out, nil
	}
	var rows []availability.DataSetSnapshot
	if err := r.db.NewSelect().
		Model(&rows).
		Where("local_data_set_id IN (?)", bun.List(localIDs)).
		Scan(ctx); err != nil {
		return nil, err
	}
	for _, row := range rows {
		out[row.LocalDataSetID] = row
	}
	return out, nil
}

func (r *BunAvailabilityRepo) withTx(ctx context.Context, fn func(context.Context, bun.IDB) error) error {
	if db, ok := r.db.(*bun.DB); ok {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(ctx, tx)
		})
	}
	return fn(ctx, r.db)
}

func prepareProviderSnapshot(snapshot *availability.ProviderSnapshot, now time.Time) {
	if snapshot.ReasonCodes == nil {
		snapshot.ReasonCodes = []availability.ReasonCode{}
	}
	if snapshot.Evidence == nil {
		snapshot.Evidence = map[string]any{}
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = now
	}
	snapshot.UpdatedAt = now
}

func prepareDataSetSnapshot(snapshot *availability.DataSetSnapshot, now time.Time) {
	if snapshot.ReasonCodes == nil {
		snapshot.ReasonCodes = []availability.ReasonCode{}
	}
	if snapshot.Evidence == nil {
		snapshot.Evidence = map[string]any{}
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = now
	}
	snapshot.UpdatedAt = now
}

func normalizeAvailabilityPagination(opts availability.ListOptions) (int, int) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultAvailabilityListLimit
	}
	if limit > maxAvailabilityListLimit {
		limit = maxAvailabilityListLimit
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func applyProviderAvailabilityFilters(q *bun.SelectQuery, opts availability.ListOptions) *bun.SelectQuery {
	if opts.Status != "" {
		q.Where("status = ?", opts.Status)
	}
	if opts.ProviderID != nil {
		q.Where("provider_id = ?", opts.ProviderID.String())
	}
	return q
}

func applyDataSetAvailabilityFilters(q *bun.SelectQuery, opts availability.ListOptions) *bun.SelectQuery {
	if opts.Status != "" {
		q.Where("status = ?", opts.Status)
	}
	if opts.BucketID > 0 {
		q.Where("bucket_id = ?", opts.BucketID)
	}
	if opts.ProviderID != nil {
		q.Where("provider_id = ?", opts.ProviderID.String())
	}
	return q
}

type availabilitySnapshotAggregate struct {
	Total         int
	Available     int
	Degraded      int
	Unavailable   int
	Unknown       int
	LastCheckedAt *time.Time `bun:"last_checked_at"`
}

func (r *BunAvailabilityRepo) providerSnapshotAggregate(ctx context.Context, opts availability.ListOptions) (availabilitySnapshotAggregate, error) {
	var aggregate availabilitySnapshotAggregate
	err := applyProviderAvailabilityFilters(r.db.NewSelect().Model((*availability.ProviderSnapshot)(nil)), opts).
		ColumnExpr("COUNT(*) AS total").
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS available", availability.StatusAvailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS degraded", availability.StatusDegraded).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unavailable", availability.StatusUnavailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unknown", availability.StatusUnknown).
		ColumnExpr("MAX(last_checked_at) AS last_checked_at").
		Scan(ctx, &aggregate)
	return aggregate, err
}

func (r *BunAvailabilityRepo) dataSetSnapshotAggregate(ctx context.Context, opts availability.ListOptions) (availabilitySnapshotAggregate, error) {
	var aggregate availabilitySnapshotAggregate
	err := applyDataSetAvailabilityFilters(r.db.NewSelect().Model((*availability.DataSetSnapshot)(nil)), opts).
		ColumnExpr("COUNT(*) AS total").
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS available", availability.StatusAvailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS degraded", availability.StatusDegraded).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unavailable", availability.StatusUnavailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unknown", availability.StatusUnknown).
		ColumnExpr("MAX(last_checked_at) AS last_checked_at").
		Scan(ctx, &aggregate)
	return aggregate, err
}

func (a availabilitySnapshotAggregate) summary() availability.Summary {
	return availability.Summary{
		Total:       a.Total,
		Available:   a.Available,
		Degraded:    a.Degraded,
		Unavailable: a.Unavailable,
		Unknown:     a.Unknown,
	}
}

func (a availabilitySnapshotAggregate) lastCheckedAt() *time.Time {
	if a.LastCheckedAt == nil || a.LastCheckedAt.IsZero() {
		return nil
	}
	return a.LastCheckedAt
}
