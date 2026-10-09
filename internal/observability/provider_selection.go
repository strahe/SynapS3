package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"

	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/types"
)

type providerSelectionStore interface {
	ProviderProfiles(context.Context, []types.OnChainID) (map[string]ProviderProfile, error)
	GetProviderTierSnapshot(context.Context, string) (*ProviderTierSnapshot, error)
}

// SelectionInventory reads health and a fresh, independent tier snapshot.
func (s *Service) SelectionInventory(ctx context.Context, tier providerselect.Tier) (providerselect.Inventory, error) {
	in := providerselect.Inventory{Admission: providerselect.Admission{Tier: tier}}
	if s == nil || !tier.Valid() {
		return in, providerselect.ErrInventoryUnavailable
	}
	store, ok := s.store.(providerSelectionStore)
	if !ok {
		return in, providerselect.ErrInventoryUnavailable
	}
	var observations []ProviderObservation
	for offset := 0; ; {
		page, err := s.ListProviderObservations(ctx, ListOptions{Limit: 200, Offset: offset})
		if err != nil {
			return in, err
		}
		// Rows and totals can observe different sides of a concurrent refresh.
		if offset == 0 && len(page.Items) == 0 && s.checker != nil {
			if err := s.RefreshProviderStates(ctx); err != nil {
				return in, err
			}
			page, err = s.ListProviderObservations(ctx, ListOptions{Limit: 200})
			if err != nil {
				return in, err
			}
		}
		observations = append(observations, page.Items...)
		offset += len(page.Items)
		if len(page.Items) == 0 || offset >= page.Total {
			break
		}
	}
	ids := make([]types.OnChainID, 0, len(observations))
	for _, o := range observations {
		ids = append(ids, o.Facts.ProviderID)
	}
	profiles, err := store.ProviderProfiles(ctx, ids)
	if err != nil {
		return in, err
	}
	in.Admission, err = s.SelectionAdmission(ctx, tier)
	if err != nil {
		return in, err
	}

	for _, o := range observations {
		p, exists := profiles[o.Facts.ProviderID.String()]
		c := providerselect.Candidate{
			ID: o.Facts.ProviderID, HasProfile: exists, ProfileURL: p.ServiceURL,
			Active: exists && p.Active && o.Facts.Active != nil && *o.Facts.Active,
			HasPDP: o.Facts.HasPDP != nil && *o.Facts.HasPDP, Healthy: o.Signal.Status == StatusAvailable,
			Fresh: !o.Signal.Freshness.Stale, TieBreak: rand.Uint64(),
		}
		if o.Facts.ServiceURL != nil {
			c.ServiceURL = *o.Facts.ServiceURL
		}
		in.Candidates = append(in.Candidates, c)
	}
	return in, nil
}

func (s *Service) SelectionAdmission(ctx context.Context, tier providerselect.Tier) (providerselect.Admission, error) {
	in := providerselect.Inventory{Admission: providerselect.Admission{Tier: tier}}
	if s == nil || !tier.Valid() {
		return in.Admission, providerselect.ErrInventoryUnavailable
	}
	if tier == providerselect.TierNone {
		return in.Admission, nil
	}
	store, ok := s.store.(providerSelectionStore)
	if !ok {
		return in.Admission, providerselect.ErrInventoryUnavailable
	}
	if tier != providerselect.TierNone {
		snapshot, err := store.GetProviderTierSnapshot(ctx, string(tier))
		if err != nil {
			return in.Admission, err
		}
		if snapshot == nil || snapshot.CheckedAt.IsZero() || s.checkedAt().Sub(snapshot.CheckedAt) > 2*s.RefreshInterval() {
			if tier == providerselect.TierApproved {
				_, _, err = s.RefreshApprovedProviders(ctx)
			} else {
				_, _, err = s.RefreshEndorsedProviders(ctx)
			}
			if err != nil {
				return in.Admission, fmt.Errorf("refreshing provider tier: %w", err)
			}
			snapshot, err = store.GetProviderTierSnapshot(ctx, string(tier))
			if err != nil {
				return in.Admission, err
			}
		}
		if snapshot == nil || snapshot.CheckedAt.IsZero() || s.checkedAt().Sub(snapshot.CheckedAt) > 2*s.RefreshInterval() {
			return in.Admission, providerselect.ErrInventoryUnavailable
		}
		var raw []string
		if err := json.Unmarshal(snapshot.ProviderIDs, &raw); err != nil {
			return in.Admission, err
		}
		for _, rawID := range raw {
			id, err := types.ParseOnChainID("provider_id", rawID)
			if err != nil {
				return in.Admission, err
			}
			in.Admission.TrustedIDs = append(in.Admission.TrustedIDs, id)
		}
	}
	return in.Admission, nil
}
