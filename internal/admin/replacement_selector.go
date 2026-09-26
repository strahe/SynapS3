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

// ListReplacementProviderObservations reads the same observed inventory shown
// by storage topology; eligibility is decided by the caller.
func (s *StorageProviderSelector) ListReplacementProviderObservations(ctx context.Context, providerID *idtypes.OnChainID) ([]observability.ProviderObservation, error) {
	if s == nil || s.providers == nil {
		return nil, errReplacementUnavailable
	}
	listCtx, cancel := context.WithTimeout(ctx, replacementProviderListTimeout)
	defer cancel()

	providers := make([]observability.ProviderObservation, 0, replacementProviderPageSize)
	seen := make(map[string]struct{}, replacementProviderPageSize)
	for offset := 0; ; {
		page, err := s.providers.ListProviderObservations(listCtx, observability.ListOptions{
			Limit:      replacementProviderPageSize,
			Offset:     offset,
			ProviderID: providerID,
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
			if facts.ProviderID.IsZero() {
				continue
			}
			providers = append(providers, item)
		}
		next := offset + len(page.Items)
		if len(page.Items) == 0 || len(page.Items) < replacementProviderPageSize ||
			(page.Total > 0 && next >= page.Total) || next <= offset || added == 0 {
			break
		}
		offset = next
	}
	slices.SortFunc(providers, func(a, b observability.ProviderObservation) int {
		return a.Facts.ProviderID.SDK().Cmp(b.Facts.ProviderID.SDK())
	})
	return providers, nil
}
