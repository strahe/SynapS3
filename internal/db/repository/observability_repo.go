package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/observability"
	"github.com/uptrace/bun"
)

const (
	defaultObservabilityListLimit = 100
	maxObservabilityListLimit     = 500
)

type BunObservabilityRepo struct {
	db bun.IDB
}

func (r *BunObservabilityRepo) ReplaceProviderStates(ctx context.Context, states []observability.ProviderState) error {
	return r.withTx(ctx, func(ctx context.Context, db bun.IDB) error {
		now := time.Now().UTC()
		for i := range states {
			prepareProviderState(&states[i], now)
		}
		if _, err := db.NewDelete().Model((*observability.ProviderState)(nil)).Where("1 = 1").Exec(ctx); err != nil {
			return err
		}
		if len(states) > 0 {
			_, err := db.NewInsert().
				Model(&states).
				Exec(ctx)
			return err
		}
		return nil
	})
}

func (r *BunObservabilityRepo) ListProviderStates(ctx context.Context, opts observability.ListOptions) (observability.ProviderStatePage, error) {
	limit, offset := normalizeObservabilityPagination(opts)
	var rows []observability.ProviderState
	if err := applyProviderObservabilityFilters(r.db.NewSelect().Model(&rows), opts).
		OrderExpr("length(provider_id) ASC, provider_id ASC").
		Limit(limit).
		Offset(offset).
		Scan(ctx); err != nil {
		return observability.ProviderStatePage{}, err
	}

	aggregate, err := r.providerStateAggregate(ctx, opts)
	if err != nil {
		return observability.ProviderStatePage{}, err
	}
	return observability.ProviderStatePage{
		Items:         rows,
		Summary:       aggregate.summary(),
		LastCheckedAt: aggregate.lastCheckedAt(),
		Total:         aggregate.Total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (r *BunObservabilityRepo) ReplaceDataSetStates(ctx context.Context, states []observability.DataSetState) error {
	return r.withTx(ctx, func(ctx context.Context, db bun.IDB) error {
		now := time.Now().UTC()
		for i := range states {
			prepareDataSetState(&states[i], now)
		}
		if _, err := db.NewDelete().Model((*observability.DataSetState)(nil)).Where("1 = 1").Exec(ctx); err != nil {
			return err
		}
		if len(states) > 0 {
			_, err := db.NewInsert().
				Model(&states).
				Exec(ctx)
			return err
		}
		return nil
	})
}

func (r *BunObservabilityRepo) ListDataSetStates(ctx context.Context, opts observability.ListOptions) (observability.DataSetStatePage, error) {
	limit, offset := normalizeObservabilityPagination(opts)
	var rows []observability.DataSetState
	if err := applyDataSetObservabilityFilters(r.db.NewSelect().Model(&rows), opts).
		OrderExpr("bucket_name ASC, local_data_set_id ASC").
		Limit(limit).
		Offset(offset).
		Scan(ctx); err != nil {
		return observability.DataSetStatePage{}, err
	}

	aggregate, err := r.dataSetStateAggregate(ctx, opts)
	if err != nil {
		return observability.DataSetStatePage{}, err
	}
	return observability.DataSetStatePage{
		Items:         rows,
		Summary:       aggregate.summary(),
		LastCheckedAt: aggregate.lastCheckedAt(),
		Total:         aggregate.Total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (r *BunObservabilityRepo) GetDataSetStatesByLocalIDs(ctx context.Context, localIDs []int64) (map[int64]observability.DataSetState, error) {
	out := make(map[int64]observability.DataSetState)
	if len(localIDs) == 0 {
		return out, nil
	}
	var rows []observability.DataSetState
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

func (r *BunObservabilityRepo) withTx(ctx context.Context, fn func(context.Context, bun.IDB) error) error {
	if db, ok := r.db.(*bun.DB); ok {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(ctx, tx)
		})
	}
	return fn(ctx, r.db)
}

func prepareProviderState(state *observability.ProviderState, now time.Time) {
	if state.ReasonCodes == nil {
		state.ReasonCodes = []observability.ReasonCode{}
	}
	if state.Evidence == nil {
		state.Evidence = map[string]any{}
	}
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now
}

func prepareDataSetState(state *observability.DataSetState, now time.Time) {
	if state.ReasonCodes == nil {
		state.ReasonCodes = []observability.ReasonCode{}
	}
	if state.Evidence == nil {
		state.Evidence = map[string]any{}
	}
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now
}

func normalizeObservabilityPagination(opts observability.ListOptions) (int, int) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultObservabilityListLimit
	}
	if limit > maxObservabilityListLimit {
		limit = maxObservabilityListLimit
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func applyProviderObservabilityFilters(q *bun.SelectQuery, opts observability.ListOptions) *bun.SelectQuery {
	if opts.Status != "" {
		q.Where("status = ?", opts.Status)
	}
	if opts.ProviderID != nil {
		q.Where("provider_id = ?", opts.ProviderID.String())
	}
	return q
}

func applyDataSetObservabilityFilters(q *bun.SelectQuery, opts observability.ListOptions) *bun.SelectQuery {
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

type observabilityStateAggregate struct {
	Total         int
	Available     int
	Degraded      int
	Unavailable   int
	Unknown       int
	LastCheckedAt *time.Time `bun:"last_checked_at"`
}

func (r *BunObservabilityRepo) providerStateAggregate(ctx context.Context, opts observability.ListOptions) (observabilityStateAggregate, error) {
	var aggregate observabilityStateAggregate
	err := applyProviderObservabilityFilters(r.db.NewSelect().Model((*observability.ProviderState)(nil)), opts).
		ColumnExpr("COUNT(*) AS total").
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS available", observability.StatusAvailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS degraded", observability.StatusDegraded).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unavailable", observability.StatusUnavailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unknown", observability.StatusUnknown).
		ColumnExpr("MAX(last_checked_at) AS last_checked_at").
		Scan(ctx, &aggregate)
	return aggregate, err
}

func (r *BunObservabilityRepo) dataSetStateAggregate(ctx context.Context, opts observability.ListOptions) (observabilityStateAggregate, error) {
	var aggregate observabilityStateAggregate
	err := applyDataSetObservabilityFilters(r.db.NewSelect().Model((*observability.DataSetState)(nil)), opts).
		ColumnExpr("COUNT(*) AS total").
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS available", observability.StatusAvailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS degraded", observability.StatusDegraded).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unavailable", observability.StatusUnavailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unknown", observability.StatusUnknown).
		ColumnExpr("MAX(last_checked_at) AS last_checked_at").
		Scan(ctx, &aggregate)
	return aggregate, err
}

func (a observabilityStateAggregate) summary() observability.Summary {
	return observability.Summary{
		Total:       a.Total,
		Available:   a.Available,
		Degraded:    a.Degraded,
		Unavailable: a.Unavailable,
		Unknown:     a.Unknown,
	}
}

func (a observabilityStateAggregate) lastCheckedAt() *time.Time {
	if a.LastCheckedAt == nil || a.LastCheckedAt.IsZero() {
		return nil
	}
	return a.LastCheckedAt
}
