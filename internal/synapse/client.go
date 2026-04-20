package synapse

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/strahe/synapse-go"
)

// NewClient creates a synapse.Client from config fields.
func NewClient(ctx context.Context, privateKey, rpcURL, source string, logger *slog.Logger) (*synapse.Client, error) {
	opts := []synapse.ClientOption{
		synapse.WithPrivateKeyHex(privateKey),
		synapse.WithRPCURL(rpcURL),
	}
	if source != "" {
		opts = append(opts, synapse.WithSource(source))
	}
	if logger != nil {
		opts = append(opts, synapse.WithLogger(logger))
	}

	client, err := synapse.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating synapse client: %w", err)
	}
	return client, nil
}
