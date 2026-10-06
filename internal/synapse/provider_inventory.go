package synapse

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/provider"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
)

type ProviderInventory struct {
	Selection providerselect.Inventory
	Providers map[string]storage.Provider
}

func (c *sdkReadinessClient) ProviderInventory(ctx context.Context, cfg ReadinessConfig) (ProviderInventory, error) {
	in := ProviderInventory{Selection: providerselect.Inventory{Admission: providerselect.Admission{Tier: providerselect.Tier(cfg.AnchorProviderTier)}}, Providers: make(map[string]storage.Provider)}
	healthClient := NewProviderHTTPClient(cfg.ProviderTimeout, cfg.AllowPrivateNetworks)
	defer healthClient.CloseIdleConnections()
	checker := observability.NewChecker(observability.CheckerOptions{
		ProviderSource: observability.NewRegistryProviderSource(provider.NewRegistryService(c.client.SPRegistry())),
		ProviderHealth: provider.NewHealthChecker(healthClient).Probe,
		Timeout:        cfg.ProviderTimeout, Concurrency: cfg.ProviderConcurrency,
	})
	states, err := checker.CheckProviders(ctx, time.Now().UTC(), nil)
	if err != nil {
		return in, err
	}
	for _, state := range states {
		p := state.Profile
		candidate := providerselect.Candidate{
			ID: state.ProviderID, Active: state.Active != nil && *state.Active,
			HasPDP: state.HasPDP != nil && *state.HasPDP, HasProfile: p != nil, Fresh: true, Healthy: state.Status == observability.StatusAvailable, TieBreak: rand.Uint64(),
		}
		if state.ServiceURL != nil {
			candidate.ServiceURL = *state.ServiceURL
		}
		if p != nil {
			candidate.ProfileURL = p.ServiceURL
			in.Providers[state.ProviderID.String()] = storage.Provider{ID: state.ProviderID.SDK(), ServiceURL: p.ServiceURL, ServiceProvider: common.HexToAddress(p.ServiceProviderAddress), Payee: common.HexToAddress(p.PayeeAddress)}
		}
		in.Selection.Candidates = append(in.Selection.Candidates, candidate)
	}
	switch in.Selection.Admission.Tier {
	case providerselect.TierNone:
		return in, nil
	case providerselect.TierApproved:
		chain, err := ethclient.DialContext(ctx, cfg.RPCURL)
		if err != nil {
			return in, err
		}
		defer chain.Close()
		catalog, err := NewApprovedProviderCatalog(chain, c.client.WarmStorage().ViewAddress())
		if err != nil {
			return in, err
		}
		in.Selection.Admission.TrustedIDs, err = catalog.ReadApprovedProviderIDs(ctx)
		return in, err
	case providerselect.TierEndorsed:
		ids, err := c.client.SPRegistry().GetEndorsedProviderIDs(ctx)
		if err != nil {
			return in, err
		}
		for _, id := range ids {
			in.Selection.Admission.TrustedIDs = append(in.Selection.Admission.TrustedIDs, types.OnChainIDFromSDK(id))
		}
		return in, nil
	default:
		return in, providerselect.ErrInventoryUnavailable
	}
}

func normalizedReadinessConfig(cfg ReadinessConfig) ReadinessConfig {
	if cfg.AnchorProviderTier == "" {
		cfg.AnchorProviderTier = string(providerselect.TierApproved)
	}
	if cfg.ProviderTimeout <= 0 {
		cfg.ProviderTimeout = 5 * time.Second
	}
	if cfg.ProviderConcurrency <= 0 {
		cfg.ProviderConcurrency = 8
	}
	return cfg
}
