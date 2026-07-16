package synapse

import (
	"context"
	"fmt"
	"log/slog"

	sdk "github.com/strahe/synapse-go"
)

const dataSetSource = "synaps3"

// ClientConfig contains SynapS3 runtime settings forwarded into synapse-go.
type ClientConfig struct {
	PrivateKey           string
	RPCURL               string
	WithCDN              bool
	AllowPrivateNetworks bool
	Logger               *slog.Logger
}

// NewClient creates a synapse-go Client from config fields.
func NewClient(ctx context.Context, cfg ClientConfig) (*sdk.Client, error) {
	opts := []sdk.ClientOption{
		sdk.WithPrivateKeyHex(cfg.PrivateKey),
		sdk.WithRPCURL(cfg.RPCURL),
		sdk.WithSource(dataSetSource),
		sdk.WithCDN(cfg.WithCDN),
		sdk.WithAllowPrivateNetworks(cfg.AllowPrivateNetworks),
	}
	if cfg.Logger != nil {
		opts = append(opts, sdk.WithLogger(cfg.Logger))
	}

	client, err := sdk.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating synapse client: %w", err)
	}
	return client, nil
}
