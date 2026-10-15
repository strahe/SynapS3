package repository

import (
	"context"

	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

// ProviderRankingSpeeds reads measurements matching each provider's profile URL.
func (r *Repositories) ProviderRankingSpeeds(ctx context.Context, ids []types.OnChainID) (map[string]int64, error) {
	return providerRankingSpeeds(ctx, r.db, ids)
}

func providerRankingSpeeds(ctx context.Context, db bun.IDB, ids []types.OnChainID) (map[string]int64, error) {
	speeds := make(map[string]int64, len(ids))
	if len(ids) == 0 {
		return speeds, nil
	}
	var profiles []observability.ProviderProfile
	if err := db.NewSelect().Model(&profiles).Column("provider_id", "service_url").
		Where("provider_id IN (?)", bun.List(ids)).Scan(ctx); err != nil {
		return nil, err
	}
	rawIDs := make([]string, len(ids))
	for i, id := range ids {
		rawIDs[i] = id.String()
	}
	results, err := (&BunProviderUploadSpeedRepo{db: db}).ListByProviderIDs(ctx, rawIDs)
	if err != nil {
		return nil, err
	}
	for _, profile := range profiles {
		if row, ok := results[profile.ProviderID.String()]; ok {
			speeds[profile.ProviderID.String()], _ = row.Outcome(profile.ServiceURL)
		}
	}
	return speeds, nil
}
