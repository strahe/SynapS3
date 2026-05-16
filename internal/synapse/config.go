package synapse

import "github.com/strahe/synaps3/internal/config"

func ReadinessConfigFromFilecoinConfig(cfg config.FilecoinConfig) ReadinessConfig {
	return ReadinessConfig{
		Network:              cfg.Network,
		RPCURL:               cfg.RPCURL,
		PrivateKey:           cfg.PrivateKey,
		Source:               cfg.Source,
		WithCDN:              cfg.WithCDN,
		AllowPrivateNetworks: cfg.AllowPrivateNetworks,
		DefaultCopies:        cfg.DefaultCopies,
	}
}
