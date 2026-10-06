package repository

import (
	"context"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/types"
)

func (r *Repositories) EnrichProviderCandidates(ctx context.Context, in *providerselect.Inventory) error {
	ids := make([]types.OnChainID, len(in.Candidates))
	raw := make([]string, len(ids))
	for i, c := range in.Candidates {
		ids[i], raw[i] = c.ID, c.ID.String()
	}
	loads, err := r.Contents.ProviderDataSetLoads(ctx, ids)
	if err != nil {
		return err
	}
	speeds, err := r.ProviderUploadSpeed.ListByProviderIDs(ctx, raw)
	if err != nil {
		return err
	}
	for i := range in.Candidates {
		c := &in.Candidates[i]
		c.Load = loads[c.ID.String()]
		c.BytesPerSecond = 0
		row, ok := speeds[c.ID.String()]
		if ok && row.State == providerbenchmark.StateSucceeded && row.BytesPerSecond != nil && *row.BytesPerSecond > 0 && row.ServiceURLHash == providerbenchmark.URLHash(c.ServiceURL) {
			c.BytesPerSecond = *row.BytesPerSecond
		}
	}
	return nil
}

// ProviderSelectionState uses the existing binding and replacement ledgers.
// Failed replacements remain reservations because their accepted target can retry.
func (r *Repositories) ProviderSelectionState(ctx context.Context, bucketID, sourceID int64, admission providerselect.Admission) (map[string]bool, bool, error) {
	bindings, err := r.Contents.ListDataSetBindings(ctx, bucketID)
	if err != nil {
		return nil, false, err
	}
	replacements, err := r.Replacements.ListForBucket(ctx, bucketID, 0)
	if err != nil {
		return nil, false, err
	}
	blocked := map[int64]bool{sourceID: true}
	for _, row := range replacements {
		if row.Status != storagereplacement.StatusCompleted && row.Status != storagereplacement.StatusSuperseded {
			blocked[row.SourceDataSetID], blocked[row.TargetDataSetID] = true, true
		}
	}
	excluded := make(map[string]bool)
	hasTrusted := admission.Tier == providerselect.TierNone
	for _, binding := range bindings {
		if binding.Status == model.StorageDataSetStatusRetired {
			continue
		}
		excluded[binding.ProviderID.String()] = true
		if binding.IsCurrent && !blocked[binding.ID] && admission.Trusted(binding.ProviderID) {
			hasTrusted = true
		}
	}
	return excluded, hasTrusted, nil
}

// ValidateProviderSelection runs inside the caller's bucket-locked transaction.
func (r *Repositories) ValidateProviderSelection(ctx context.Context, bucketID, sourceID int64, admission providerselect.Admission, targets []types.OnChainID) error {
	if !admission.Tier.Valid() {
		return ErrInvalidInput
	}
	if admission.Tier == providerselect.TierNone {
		return nil
	}
	_, hasTrusted, err := r.ProviderSelectionState(ctx, bucketID, sourceID, admission)
	if err != nil {
		return err
	}
	if !admission.Satisfied(hasTrusted, targets...) {
		return providerselect.ErrNoTrustedProvider
	}
	return nil
}
