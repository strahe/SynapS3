package repository

import (
	"context"
	"fmt"
)

type objectAggregateRow struct {
	State          string `bun:"state"`
	Count          int64  `bun:"count"`
	TotalSize      int64  `bun:"total_size"`
	NeedsAttention int64  `bun:"needs_attention"`
	Unavailable    int64  `bun:"unavailable"`
}

func (r *BunObjectRepo) OverviewSnapshot(ctx context.Context) (ObjectOverviewSnapshot, error) {
	rows, err := r.aggregateCurrentObjects(ctx, true)
	if err != nil {
		return ObjectOverviewSnapshot{}, err
	}
	snapshot := ObjectOverviewSnapshot{States: make([]ObjectStateAggregate, 0, len(rows))}
	for _, row := range rows {
		snapshot.States = append(snapshot.States, ObjectStateAggregate{State: row.State, Count: row.Count, TotalSize: row.TotalSize})
		snapshot.Attention.NeedsAttention += row.NeedsAttention
		snapshot.Attention.Unavailable += row.Unavailable
	}
	return snapshot, nil
}

func (r *BunObjectRepo) aggregateCurrentObjects(ctx context.Context, sizes bool) ([]objectAggregateRow, error) {
	totalSize := "0::bigint"
	if sizes {
		totalSize = "COALESCE(SUM(current_version.size), 0)"
	}
	// Materialize per-content facts so shared objects do not repeat copy scans.
	query := `WITH current_versions AS MATERIALIZED (
		SELECT version.content_id, version.size
		FROM objects AS object
		JOIN object_versions AS version ON version.version_id = object.current_version_id
		WHERE NOT version.is_delete_marker
	), content_states AS MATERIALIZED (
		SELECT storage_content.id, ` + contentPipelineStateFromFactsSQL("facts.readable_slots", "facts.copy_count > 0", "facts.live_count = 0", "facts.committing") + ` AS state,
		COALESCE(cache_entry.in_cache, FALSE) AS in_cache, facts.readable_slots
		FROM (SELECT DISTINCT content_id FROM current_versions) AS wanted
		JOIN storage_contents AS storage_content ON storage_content.id = wanted.content_id
		LEFT JOIN object_cache AS cache_entry ON cache_entry.content_id = storage_content.id
		CROSS JOIN LATERAL (
			SELECT COUNT(*) AS copy_count,
			COUNT(*) FILTER (WHERE copy.status <> 'failed') AS live_count,
			COALESCE(BOOL_OR(copy.status IN ('piece_ready', 'committing')), FALSE) AS committing,
			COUNT(DISTINCT data_set.copy_index) FILTER (WHERE ` + readableCommittedCopyPredicateSQL("copy", "data_set") + `) AS readable_slots
			FROM storage_copies AS copy
			JOIN storage_data_sets AS data_set ON data_set.id = copy.storage_data_set_id
			WHERE copy.content_id = storage_content.id
		) AS facts
	)
	SELECT state, COUNT(*) AS count, ` + totalSize + ` AS total_size,
	COUNT(*) FILTER (WHERE state = 'failed') AS needs_attention,
	COUNT(*) FILTER (WHERE NOT in_cache AND readable_slots = 0) AS unavailable
	FROM current_versions AS current_version
	JOIN content_states ON content_states.id = current_version.content_id
	GROUP BY state`
	var rows []objectAggregateRow
	if err := r.db.NewRaw(query).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("aggregating current objects: %w", err)
	}
	return rows, nil
}
