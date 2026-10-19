package admin

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/types"
)

type taskProviderSelectionView struct {
	Tier            providerselect.Tier         `json:"tier"`
	TierCheckedAt   *time.Time                  `json:"tier_checked_at,omitempty"`
	HasTrusted      bool                        `json:"has_trusted"`
	RequestedCount  int                         `json:"requested_count"`
	EligibleCount   int                         `json:"eligible_count"`
	RejectedTotal   int                         `json:"rejected_total"`
	RejectedOmitted int                         `json:"rejected_omitted"`
	Rejections      []taskProviderRejectionView `json:"rejections"`
}

type taskProviderRejectionView struct {
	ProviderID           types.OnChainID           `json:"provider_id"`
	ProviderIdentity     *providerIdentityResponse `json:"provider_identity,omitempty"`
	Reason               string                    `json:"reason"`
	ObservationCheckedAt *time.Time                `json:"observation_checked_at,omitempty"`
}

func (s *Server) attachTaskProviderIdentities(ctx context.Context, items []taskListItem) {
	var rawIDs []string
	for _, item := range items {
		if item.SubjectType == "provider" {
			rawIDs = append(rawIDs, item.SubjectKey)
		}
		if item.StorageConfirmation != nil {
			rawIDs = append(rawIDs, item.StorageConfirmation.ProviderID)
		}
	}
	providerIDs := providerIdentityIDs(rawIDs)
	if len(providerIDs) == 0 {
		return
	}
	identities := s.providerIdentities(ctx, providerIDs)
	for i := range items {
		if items[i].SubjectType == "provider" {
			items[i].ProviderIdentity = identities[items[i].SubjectKey]
		}
		if confirmation := items[i].StorageConfirmation; confirmation != nil {
			confirmation.ProviderIdentity = identities[confirmation.ProviderID]
		}
	}
}

func (s *Server) attachTaskEventProviderIdentities(ctx context.Context, events []taskEventView) {
	var rawIDs []string
	for _, event := range events {
		if event.ProviderSelection != nil {
			for _, rejection := range event.ProviderSelection.Rejections {
				rawIDs = append(rawIDs, rejection.ProviderID.String())
			}
		}
	}
	providerIDs := providerIdentityIDs(rawIDs)
	if len(providerIDs) == 0 {
		return
	}
	identities := s.providerIdentities(ctx, providerIDs)
	for _, event := range events {
		if event.ProviderSelection != nil {
			for i := range event.ProviderSelection.Rejections {
				rejection := &event.ProviderSelection.Rejections[i]
				rejection.ProviderIdentity = identities[rejection.ProviderID.String()]
			}
		}
	}
}

func providerIdentityIDs(rawIDs []string) []types.OnChainID {
	ids := make([]types.OnChainID, 0, len(rawIDs))
	seen := make(map[string]bool, len(rawIDs))
	for _, raw := range rawIDs {
		id, err := types.ParseOnChainID("provider_id", raw)
		if err != nil || seen[id.String()] {
			continue
		}
		seen[id.String()] = true
		ids = append(ids, id)
	}
	return ids
}
