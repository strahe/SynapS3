package observability

import (
	"context"
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
)

type ApprovedProviderSource interface {
	ReadApprovedProviderIDs(context.Context) ([]types.OnChainID, error)
}

type EndorsedProviderSource interface {
	GetEndorsedProviderIDs(context.Context) ([]sdktypes.BigInt, error)
}

func (s *Service) ProviderTiersAvailable() bool {
	if s == nil || s.approvedProviders == nil || s.endorsedProviders == nil {
		return false
	}
	_, ok := s.store.(interface {
		RecordApprovedProviders(context.Context, time.Time, []types.OnChainID) (time.Time, error)
		RecordEndorsedProviders(context.Context, time.Time, []types.OnChainID) (time.Time, error)
	})
	return ok
}

// RefreshApprovedProviders reads the approved tier and returns when the read
// started and the collection time of the snapshot stored afterwards. The two
// differ when a newer read committed first; the newer snapshot is kept.
func (s *Service) RefreshApprovedProviders(ctx context.Context) (time.Time, time.Time, error) {
	return s.approvedRefresh.DoAt(ctx, s.RefreshTimeout(), s.checkedAt, func(refreshCtx context.Context, startedAt time.Time) (time.Time, error) {
		if s.approvedProviders == nil {
			return time.Time{}, errors.New("approved provider source unavailable")
		}
		store, ok := s.store.(interface {
			RecordApprovedProviders(context.Context, time.Time, []types.OnChainID) (time.Time, error)
		})
		if !ok {
			return time.Time{}, errors.New("approved provider store unavailable")
		}
		ids, err := s.readApprovedProviders(refreshCtx)
		if err != nil {
			return time.Time{}, err
		}
		return store.RecordApprovedProviders(refreshCtx, startedAt, ids)
	})
}

// RefreshEndorsedProviders is RefreshApprovedProviders for the endorsed tier.
func (s *Service) RefreshEndorsedProviders(ctx context.Context) (time.Time, time.Time, error) {
	return s.endorsedRefresh.DoAt(ctx, s.RefreshTimeout(), s.checkedAt, func(refreshCtx context.Context, startedAt time.Time) (time.Time, error) {
		if s.endorsedProviders == nil {
			return time.Time{}, errors.New("endorsed provider source unavailable")
		}
		store, ok := s.store.(interface {
			RecordEndorsedProviders(context.Context, time.Time, []types.OnChainID) (time.Time, error)
		})
		if !ok {
			return time.Time{}, errors.New("endorsed provider store unavailable")
		}
		raw, err := s.endorsedProviders.GetEndorsedProviderIDs(refreshCtx)
		if err != nil {
			return time.Time{}, err
		}
		ids := make([]types.OnChainID, 0, len(raw))
		seen := make(map[string]struct{}, len(raw))
		for _, value := range raw {
			id := types.OnChainIDFromSDK(value)
			if id.IsZero() {
				return time.Time{}, errors.New("invalid endorsed provider ID")
			}
			if _, exists := seen[id.String()]; exists {
				return time.Time{}, errors.New("duplicate endorsed provider ID")
			}
			seen[id.String()] = struct{}{}
			ids = append(ids, id)
		}
		return store.RecordEndorsedProviders(refreshCtx, startedAt, ids)
	})
}

func (s *Service) readApprovedProviders(ctx context.Context) ([]types.OnChainID, error) {
	ids, err := s.approvedProviders.ReadApprovedProviderIDs(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id.IsZero() {
			return nil, errors.New("invalid approved provider ID")
		}
		if _, exists := seen[id.String()]; exists {
			return nil, errors.New("duplicate approved provider ID")
		}
		seen[id.String()] = struct{}{}
	}
	return ids, nil
}
