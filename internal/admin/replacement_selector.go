package admin

import (
	"context"
	"slices"
	"time"

	"github.com/strahe/synaps3/internal/observability"
	idtypes "github.com/strahe/synaps3/internal/types"
)

// replacementProviderListTimeout bounds the inventory read so a confirmation
// dialog cannot hang.
const replacementProviderListTimeout = 15 * time.Second

// replacementProviderPageSize caps one inventory read. The registry holds tens
// of providers, not thousands.
const replacementProviderPageSize = 200

// ProviderRegistry lists the storage providers a replacement can move to. It is
// an interface so provider selection stays testable without live observability.
type ProviderRegistry interface {
	ListProviderObservations(ctx context.Context, opts observability.ListOptions) (observability.ProviderObservationPage, error)
}

// StorageProviderSelector reports which storage providers a replacement can
// use. It resolves candidates only; the paid service itself is created later by
// the worker, after the operator has confirmed.
type StorageProviderSelector struct {
	providers ProviderRegistry
}

// NewStorageProviderSelector builds the provider inventory used by replacement.
func NewStorageProviderSelector(providers ProviderRegistry) *StorageProviderSelector {
	return &StorageProviderSelector{providers: providers}
}

// ListReplacementProviders returns the providers currently reachable and able
// to take a data set, lowest registry ID first so the same inventory always
// yields the same choice. Which of them a given replica can take is decided by
// replacementProviderCandidates, which owns that rule alone.
//
// It reads the same provider observations the storage topology reports and
// applies the same availability filter, so the two views cannot disagree: a
// provider an operator sees listed as available there is offered here, and one
// that is unreachable is offered in neither. Reading the registry directly
// would drop the health signal and offer providers that cannot answer.
//
// The inventory is deliberately not narrowed to the storage service's
// approved-provider list. That list governs the SDK's automatic selection only
// -- naming a provider outright bypasses it -- so filtering on it here would
// hide providers replacement can in fact use, and it is small enough for one
// bucket to exhaust.
func (s *StorageProviderSelector) ListReplacementProviders(ctx context.Context) ([]idtypes.OnChainID, error) {
	if s == nil || s.providers == nil {
		return nil, errReplacementUnavailable
	}
	listCtx, cancel := context.WithTimeout(ctx, replacementProviderListTimeout)
	defer cancel()

	providers := make([]idtypes.OnChainID, 0, replacementProviderPageSize)
	seen := make(map[string]struct{}, replacementProviderPageSize)
	for offset := 0; ; {
		page, err := s.providers.ListProviderObservations(listCtx, observability.ListOptions{
			Status: observability.StatusAvailable,
			Limit:  replacementProviderPageSize,
			Offset: offset,
		})
		if err != nil {
			return nil, err
		}
		added := 0
		for _, item := range page.Items {
			facts := item.Facts
			key := facts.ProviderID.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			added++
			// A provider without an active PDP offering cannot hold a data set.
			if facts.ProviderID.IsZero() || facts.Active == nil || !*facts.Active || facts.HasPDP == nil || !*facts.HasPDP {
				continue
			}
			providers = append(providers, facts.ProviderID)
		}
		next := offset + len(page.Items)
		if len(page.Items) == 0 || len(page.Items) < replacementProviderPageSize ||
			(page.Total > 0 && next >= page.Total) || next <= offset || added == 0 {
			break
		}
		offset = next
	}
	slices.SortFunc(providers, func(a, b idtypes.OnChainID) int {
		return a.SDK().Cmp(b.SDK())
	})
	return providers, nil
}
