package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// OverviewStorageStates scopes health to local dependencies without changing global observations.
func (r *BunObservabilityRepo) OverviewStorageStates(ctx context.Context) ([]model.StorageDataSet, []observability.ProviderState, []observability.DataSetState, *time.Time, *time.Time, error) {
	var dataSets []model.StorageDataSet
	err := r.db.NewSelect().Model(&dataSets).Where(overviewDataSetDependencySQL("storage_data_set")).Scan(ctx)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	var providers []observability.ProviderState
	var observedDataSets []observability.DataSetState
	if err := r.db.NewSelect().Model(&providers).ModelTableExpr("observability_provider_states AS provider_state").
		Where(`EXISTS (SELECT 1 FROM storage_data_sets AS scoped_data_set
			WHERE scoped_data_set.provider_id = provider_state.provider_id AND ` + overviewDataSetDependencySQL("scoped_data_set") + `)`).Scan(ctx); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := r.db.NewSelect().Model(&observedDataSets).
		Where(`EXISTS (SELECT 1 FROM storage_data_sets AS scoped_data_set
			WHERE scoped_data_set.id = observability_data_set_state.local_data_set_id AND ` + overviewDataSetDependencySQL("scoped_data_set") + `)`).Scan(ctx); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	providerCheckedAt, err := r.collectionLastCheckedAt(ctx, observability.CollectionProviders)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	dataSetCheckedAt, err := r.collectionLastCheckedAt(ctx, observability.CollectionDataSets)
	return dataSets, providers, observedDataSets, providerCheckedAt, dataSetCheckedAt, err
}

func overviewDataSetDependencySQL(alias string) string {
	return fmt.Sprintf(`((%[1]s.is_current AND %[1]s.status <> 'retired')
		OR EXISTS (SELECT 1 FROM storage_replacements AS replacement
			WHERE replacement.status NOT IN ('completed', 'superseded')
			AND (replacement.source_data_set_id = %[1]s.id OR replacement.target_data_set_id = %[1]s.id))
		OR (%[1]s.status IN ('ready', 'draining') AND EXISTS (
			SELECT 1 FROM storage_copies AS copy
			JOIN object_versions AS version ON version.content_id = copy.content_id
			WHERE copy.storage_data_set_id = %[1]s.id AND %[2]s)))`,
		alias, readableCommittedCopyPredicateSQL("copy", alias))
}

const (
	defaultObservabilityListLimit = 100
	maxObservabilityListLimit     = 500
	observabilityStateInsertBatch = 500
)

type BunObservabilityRepo struct {
	db bun.IDB
}

func (r *BunObservabilityRepo) ReplaceProviderStates(ctx context.Context, checkedAt time.Time, states []observability.ProviderState) error {
	for i := range states {
		if err := validProviderStateStatus(states[i]); err != nil {
			return err
		}
	}
	return r.withTx(ctx, func(ctx context.Context, db bun.IDB) error {
		checkedAt = normalizeCheckedAt(checkedAt, time.Now())
		claimed, err := claimObservabilityCollection(ctx, db, observability.CollectionProviders, checkedAt)
		if err != nil || !claimed {
			return err
		}
		for i := range states {
			prepareProviderState(&states[i], checkedAt)
			if err := upsertProviderProfile(ctx, db, states[i].Profile, checkedAt); err != nil {
				return err
			}
			if err := upsertProviderState(ctx, db, &states[i]); err != nil {
				return err
			}
		}
		ids := make([]string, 0, len(states))
		for _, state := range states {
			ids = append(ids, state.ProviderID.String())
		}
		deleteQuery := db.NewDelete().Model((*observability.ProviderState)(nil)).
			Where("(last_attempt_at < ? OR last_attempt_at > ?)", checkedAt, latestPlausibleObservationTime())
		if len(ids) > 0 {
			deleteQuery = deleteQuery.Where("provider_id NOT IN (?)", bun.List(ids))
		}
		_, err = deleteQuery.Exec(ctx)
		return err
	})
}

func (r *BunObservabilityRepo) UpsertProviderObservation(ctx context.Context, checkedAt time.Time, state observability.ProviderState) error {
	if err := validProviderStateStatus(state); err != nil {
		return err
	}
	return r.withTx(ctx, func(ctx context.Context, db bun.IDB) error {
		checkedAt = normalizeCheckedAt(checkedAt, time.Now().UTC())
		prepareProviderState(&state, checkedAt)
		if err := upsertProviderProfile(ctx, db, state.Profile, checkedAt); err != nil {
			return err
		}
		return upsertProviderState(ctx, db, &state)
	})
}

func upsertProviderProfile(ctx context.Context, db bun.IDB, profile *observability.ProviderProfile, checkedAt time.Time) error {
	if profile == nil {
		return nil
	}
	profile.LastSuccessAt = checkedAt
	guard, latest := upsertTimestampGuard(db, "provider_profile", "last_success_at")
	_, err := db.NewInsert().Model(profile).On("CONFLICT (provider_id) DO UPDATE").
		Set("name = EXCLUDED.name").Set("description = EXCLUDED.description").
		Set("service_provider_address = EXCLUDED.service_provider_address").
		Set("payee_address = EXCLUDED.payee_address").Set("active = EXCLUDED.active").
		Set("service_url = EXCLUDED.service_url").Set("registry_snapshot_json = EXCLUDED.registry_snapshot_json").
		Set("last_success_at = EXCLUDED.last_success_at").
		Where(guard, latest).Exec(ctx)
	return err
}

func upsertProviderState(ctx context.Context, db bun.IDB, state *observability.ProviderState) error {
	guard, latest := upsertTimestampGuard(db, "provider_state", "last_attempt_at")
	if state.Status == observability.StatusUnknown && state.LastError != nil {
		_, err := db.NewInsert().Model(state).On("CONFLICT (provider_id) DO UPDATE").
			Set("last_attempt_at = EXCLUDED.last_attempt_at").Set("last_error = EXCLUDED.last_error").
			Where(guard, latest).Exec(ctx)
		return err
	}
	_, err := db.NewInsert().Model(state).On("CONFLICT (provider_id) DO UPDATE").
		Set("status = EXCLUDED.status").Set("reason_codes = EXCLUDED.reason_codes").
		Set("active = EXCLUDED.active").Set("has_pdp = EXCLUDED.has_pdp").
		Set("service_url = EXCLUDED.service_url").Set("health_status = EXCLUDED.health_status").
		Set("last_checked_at = EXCLUDED.last_checked_at").Set("last_attempt_at = EXCLUDED.last_attempt_at").Set("last_error = EXCLUDED.last_error").
		Set("evidence_json = EXCLUDED.evidence_json").
		Where(guard, latest).Exec(ctx)
	return err
}

// observationClockSkew is how far ahead of this process's clock a stored
// observation time can be without the clock having moved back.
const observationClockSkew = time.Minute

// latestPlausibleObservationTime is the latest observation time this clock
// could have written. A later stored time was written before the clock moved
// back; newer observations replace it instead of waiting for the clock to
// catch up.
func latestPlausibleObservationTime() time.Time {
	return time.Now().UTC().Add(observationClockSkew)
}

// upsertTimestampGuard lets an observation replace one taken at the same or an
// earlier time. The returned time is the guard's only argument.
func upsertTimestampGuard(db bun.IDB, modelAlias, column string) (string, time.Time) {
	return observationTimeGuard(db, modelAlias, column, "<=")
}

// snapshotTimestampGuard orders complete snapshots strictly. A snapshot taken
// at the stored time is the same observation replayed, so the first commit
// stands and a late equal or older read changes nothing. The returned time is
// the guard's only argument.
func snapshotTimestampGuard(db bun.IDB, modelAlias, column string) (string, time.Time) {
	return observationTimeGuard(db, modelAlias, column, "<")
}

func observationTimeGuard(db bun.IDB, modelAlias, column, op string) (string, time.Time) {
	stored := column
	if db.Dialect().Name() == dialect.PG {
		stored = modelAlias + "." + column
	}
	return "(" + stored + " " + op + " EXCLUDED." + column + " OR " + stored + " > ?)", latestPlausibleObservationTime()
}

func (r *BunObservabilityRepo) ProviderProfiles(ctx context.Context, ids []types.OnChainID) (map[string]observability.ProviderProfile, error) {
	out := make(map[string]observability.ProviderProfile)
	if len(ids) == 0 {
		return out, nil
	}
	var rows []observability.ProviderProfile
	if err := r.db.NewSelect().Model(&rows).Where("provider_id IN (?)", bun.List(ids)).Scan(ctx); err != nil {
		return nil, err
	}
	var tiers []observability.ProviderTierSnapshot
	if err := r.db.NewSelect().Model(&tiers).Scan(ctx); err != nil {
		return nil, err
	}
	for _, tier := range tiers {
		var providerIDs []string
		if err := json.Unmarshal(tier.ProviderIDs, &providerIDs); err != nil {
			return nil, fmt.Errorf("decoding %s provider tier: %w", tier.Tier, err)
		}
		members := make(map[string]struct{}, len(providerIDs))
		for _, id := range providerIDs {
			members[id] = struct{}{}
		}
		for i := range rows {
			_, member := members[rows[i].ProviderID.String()]
			checkedAt := tier.CheckedAt
			switch tier.Tier {
			case "approved":
				rows[i].Approved = member
				rows[i].ApprovedCheckedAt = &checkedAt
			case "endorsed":
				rows[i].Endorsed = member
				rows[i].EndorsedCheckedAt = &checkedAt
			}
		}
	}
	for _, row := range rows {
		out[row.ProviderID.String()] = row
	}
	return out, nil
}

// RecordApprovedProviders stores a complete approved-tier read and returns the
// collection time the stored snapshot carries, which is a newer read's time
// when that read already committed.
func (r *BunObservabilityRepo) RecordApprovedProviders(ctx context.Context, startedAt time.Time, ids []types.OnChainID) (time.Time, error) {
	return r.recordProviderTier(ctx, startedAt, ids, "approved")
}

// RecordEndorsedProviders is RecordApprovedProviders for the endorsed tier.
func (r *BunObservabilityRepo) RecordEndorsedProviders(ctx context.Context, startedAt time.Time, ids []types.OnChainID) (time.Time, error) {
	return r.recordProviderTier(ctx, startedAt, ids, "endorsed")
}

func (r *BunObservabilityRepo) recordProviderTier(ctx context.Context, startedAt time.Time, ids []types.OnChainID, tier string) (time.Time, error) {
	if startedAt.IsZero() {
		return time.Time{}, errors.New("provider tier check time is required")
	}
	providerIDs := make([]string, len(ids))
	for i, id := range ids {
		if id.IsZero() {
			return time.Time{}, errors.New("invalid provider tier ID")
		}
		providerIDs[i] = id.String()
	}
	encoded, err := json.Marshal(providerIDs)
	if err != nil {
		return time.Time{}, err
	}
	snapshot := &observability.ProviderTierSnapshot{Tier: tier, ProviderIDs: encoded, CheckedAt: normalizeCheckedAt(startedAt, startedAt)}
	result, err := r.db.NewInsert().Model(snapshot).On("CONFLICT (tier) DO UPDATE").
		Set("provider_ids_json = EXCLUDED.provider_ids_json").Set("checked_at = EXCLUDED.checked_at").
		Where(snapshotTimestampGuard(r.db, "provider_tier_snapshot", "checked_at")).Exec(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if rows, err := result.RowsAffected(); err != nil {
		return time.Time{}, err
	} else if rows == 1 {
		return snapshot.CheckedAt, nil
	}
	var stored observability.ProviderTierSnapshot
	if err := r.db.NewSelect().Model(&stored).Column("checked_at").Where("tier = ?", tier).Scan(ctx); err != nil {
		return time.Time{}, fmt.Errorf("reading %s provider tier time: %w", tier, err)
	}
	return stored.CheckedAt.UTC(), nil
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
	lastCheckedAt, err := r.collectionLastCheckedAt(ctx, observability.CollectionProviders)
	if err != nil {
		return observability.ProviderStatePage{}, err
	}
	return observability.ProviderStatePage{
		Items:         rows,
		Summary:       aggregate.summary(),
		LastCheckedAt: lastCheckedAt,
		Total:         aggregate.Total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (r *BunObservabilityRepo) ReplaceDataSetStates(ctx context.Context, checkedAt time.Time, states []observability.DataSetState) error {
	for i := range states {
		if !states[i].Status.Valid() {
			return fmt.Errorf("data set %d observation status %q: %w", states[i].LocalDataSetID, states[i].Status, ErrInvalidInput)
		}
	}
	return r.withTx(ctx, func(ctx context.Context, db bun.IDB) error {
		checkedAt = normalizeCheckedAt(checkedAt, time.Now())
		claimed, err := claimObservabilityCollection(ctx, db, observability.CollectionDataSets, checkedAt)
		if err != nil || !claimed {
			return err
		}
		for i := range states {
			prepareDataSetState(&states[i], checkedAt)
		}
		if _, err := db.NewDelete().Model((*observability.DataSetState)(nil)).Where("1 = 1").Exec(ctx); err != nil {
			return err
		}
		return insertDataSetStateRows(ctx, db, states)
	})
}

func (r *BunObservabilityRepo) ListDataSetStates(ctx context.Context, opts observability.ListOptions) (observability.DataSetStatePage, error) {
	limit, offset := normalizeObservabilityPagination(opts)
	var rows []observability.DataSetState
	if err := applyDataSetObservabilityFilters(withDataSetStateJoins(r.db.NewSelect().Model(&rows)), opts).
		OrderExpr("observed_bucket.name ASC, observability_data_set_state.local_data_set_id ASC").
		Limit(limit).
		Offset(offset).
		Scan(ctx); err != nil {
		return observability.DataSetStatePage{}, err
	}

	aggregate, err := r.dataSetStateAggregate(ctx, opts)
	if err != nil {
		return observability.DataSetStatePage{}, err
	}
	lastCheckedAt, err := r.collectionLastCheckedAt(ctx, observability.CollectionDataSets)
	if err != nil {
		return observability.DataSetStatePage{}, err
	}
	return observability.DataSetStatePage{
		Items:         rows,
		Summary:       aggregate.summary(),
		LastCheckedAt: lastCheckedAt,
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
	if err := withDataSetStateJoins(r.db.NewSelect().Model(&rows)).
		Where("observability_data_set_state.local_data_set_id IN (?)", bun.List(localIDs)).
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

func validProviderStateStatus(state observability.ProviderState) error {
	if !state.Status.Valid() {
		return fmt.Errorf("provider %s observation status %q: %w", state.ProviderID.String(), state.Status, ErrInvalidInput)
	}
	return nil
}

func prepareProviderState(state *observability.ProviderState, checkedAt time.Time) {
	if state.ReasonCodes == nil {
		state.ReasonCodes = []observability.ReasonCode{}
	}
	if state.Evidence == nil {
		state.Evidence = map[string]any{}
	}
	if state.LastCheckedAt.IsZero() {
		state.LastCheckedAt = checkedAt
	}
	state.LastAttemptAt = checkedAt
}

func prepareDataSetState(state *observability.DataSetState, checkedAt time.Time) {
	if state.ReasonCodes == nil {
		state.ReasonCodes = []observability.ReasonCode{}
	}
	if state.Evidence == nil {
		state.Evidence = map[string]any{}
	}
	if state.LastCheckedAt.IsZero() {
		state.LastCheckedAt = checkedAt
	}
}

func insertDataSetStateRows(ctx context.Context, db bun.IDB, states []observability.DataSetState) error {
	for start := 0; start < len(states); start += observabilityStateInsertBatch {
		end := min(start+observabilityStateInsertBatch, len(states))
		batch := states[start:end]
		if _, err := db.NewInsert().Model(&batch).Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// normalizeCheckedAt matches the stored precision, so the time a guard compares
// is the time the row keeps.
func normalizeCheckedAt(checkedAt time.Time, fallback time.Time) time.Time {
	if checkedAt.IsZero() {
		checkedAt = fallback
	}
	return checkedAt.UTC().Truncate(time.Microsecond)
}

// claimObservabilityCollection records the time of a complete snapshot before
// any of its rows are written. It reports false when a snapshot taken at the
// same or a later time already committed; the caller then leaves the rows
// alone. A stored time ahead of the clock does not count as later. On
// PostgreSQL the conflicting row stays locked for the transaction, so
// concurrent complete snapshots of one collection commit one at a time.
func claimObservabilityCollection(ctx context.Context, db bun.IDB, collectionType observability.CollectionType, checkedAt time.Time) (bool, error) {
	row := observability.CollectionState{CollectionType: collectionType, LastCheckedAt: checkedAt}
	result, err := db.NewInsert().
		Model(&row).
		On("CONFLICT (collection_type) DO UPDATE").
		Set("last_checked_at = EXCLUDED.last_checked_at").
		Where(snapshotTimestampGuard(db, "collection_state", "last_checked_at")).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("recording %s collection time: %w", collectionType, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("recording %s collection time: %w", collectionType, err)
	}
	return rows == 1, nil
}

func normalizeObservabilityPagination(opts observability.ListOptions) (int, int) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultObservabilityListLimit
	}
	if limit > maxObservabilityListLimit {
		limit = maxObservabilityListLimit
	}
	offset := max(opts.Offset, 0)
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
		q.Where("observability_data_set_state.status = ?", opts.Status)
	}
	if opts.BucketID > 0 {
		q.Where("observability_data_set_state.bucket_id = ?", opts.BucketID)
	}
	if opts.ProviderID != nil {
		q.Where("observability_data_set_state.provider_id = ?", opts.ProviderID.String())
	}
	return q
}

// withDataSetStateJoins reads the bucket name and local status from the rows
// that own them instead of from a copy taken when the check ran.
func withDataSetStateJoins(q *bun.SelectQuery) *bun.SelectQuery {
	return q.
		ColumnExpr("observability_data_set_state.*").
		ColumnExpr("observed_bucket.name AS bucket_name").
		ColumnExpr("observed_data_set.status AS local_status").
		Join("JOIN storage_data_sets AS observed_data_set ON observed_data_set.id = observability_data_set_state.local_data_set_id").
		Join("JOIN buckets AS observed_bucket ON observed_bucket.id = observability_data_set_state.bucket_id")
}

type observabilityStateAggregate struct {
	Total       int
	Available   int
	Degraded    int
	Unavailable int
	// Unknown also counts a status this version does not know, so the summary
	// never reads healthier than the rows behind it.
	Unknown int
}

func (r *BunObservabilityRepo) providerStateAggregate(ctx context.Context, opts observability.ListOptions) (observabilityStateAggregate, error) {
	var aggregate observabilityStateAggregate
	err := applyProviderObservabilityFilters(r.db.NewSelect().Model((*observability.ProviderState)(nil)), opts).
		ColumnExpr("COUNT(*) AS total").
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS available", observability.StatusAvailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS degraded", observability.StatusDegraded).
		ColumnExpr("COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) AS unavailable", observability.StatusUnavailable).
		ColumnExpr("COALESCE(SUM(CASE WHEN status NOT IN (?, ?, ?) THEN 1 ELSE 0 END), 0) AS unknown",
			observability.StatusAvailable, observability.StatusDegraded, observability.StatusUnavailable).
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
		ColumnExpr("COALESCE(SUM(CASE WHEN status NOT IN (?, ?, ?) THEN 1 ELSE 0 END), 0) AS unknown",
			observability.StatusAvailable, observability.StatusDegraded, observability.StatusUnavailable).
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

func (r *BunObservabilityRepo) collectionLastCheckedAt(ctx context.Context, collectionType observability.CollectionType) (*time.Time, error) {
	var row observability.CollectionState
	err := r.db.NewSelect().
		Model(&row).
		Where("collection_type = ?", collectionType).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if row.LastCheckedAt.IsZero() {
		return nil, nil
	}
	last := row.LastCheckedAt
	return &last, nil
}

func (r *BunObservabilityRepo) GetProviderTierSnapshot(ctx context.Context, tier string) (*observability.ProviderTierSnapshot, error) {
	row := new(observability.ProviderTierSnapshot)
	err := r.db.NewSelect().Model(row).Where("tier = ?", tier).Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return row, err
}
