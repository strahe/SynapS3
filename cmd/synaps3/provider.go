package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"text/tabwriter"
	"time"

	"github.com/data-preservation-programs/go-synapse/constants"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/provider"
	"github.com/urfave/cli/v3"
)

func providerCommand() *cli.Command {
	return &cli.Command{
		Name:  "provider",
		Usage: "manage and inspect network providers",
		Commands: []*cli.Command{
			providerListCommand(),
		},
	}
}

func providerListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list PDP providers registered on the Filecoin network",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "json",
				Usage: "output as JSON",
			},
			&cli.BoolFlag{
				Name:  "active",
				Usage: "only show active providers",
			},
			&cli.StringFlag{
				Name:  "rpc-url",
				Usage: "override RPC endpoint URL",
			},
			&cli.StringFlag{
				Name:  "network",
				Usage: "target network (calibration | mainnet)",
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Value: 5 * time.Second,
				Usage: "health check timeout per provider",
			},
			&cli.BoolFlag{
				Name:  "no-health",
				Usage: "skip health checks",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return runProviderList(ctx, cmd)
		},
	}
}

func runProviderList(ctx context.Context, cmd *cli.Command) error {
	rpcURL, network, err := resolveRPCAndNetwork(cmd)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stderr, "Querying %s network (%s)...\n", network, maskRPCURL(rpcURL))

	reg, closer, err := provider.NewRegistryService(ctx, rpcURL, network)
	if err != nil {
		return fmt.Errorf("connecting to registry: %w", err)
	}
	defer closer()

	opts := provider.ListOptions{
		ActiveOnly: cmd.Bool("active"),
	}

	healthCheck := !cmd.Bool("no-health")
	healthTimeout := cmd.Duration("timeout")

	providers, err := provider.ListProviders(ctx, reg, opts)
	if err != nil {
		return fmt.Errorf("listing providers: %w", err)
	}

	if healthCheck {
		_, _ = fmt.Fprintf(os.Stderr, "Running health checks on %d providers...\n", len(providers))
		provider.CheckHealthBatch(ctx, providers, healthTimeout)
	}

	if cmd.Bool("json") {
		return outputJSON(providers)
	}
	return outputTable(providers)
}

// resolveRPCAndNetwork determines the RPC URL and network from CLI flags and config.
// Priority: CLI flag > config file > built-in defaults.
func resolveRPCAndNetwork(cmd *cli.Command) (rpcURL, network string, err error) {
	// Try loading config (optional — command works without it).
	var cfg *config.Config
	configPath := cmd.Root().String("config")
	if loaded, err := config.Load(configPath); err != nil {
		// Distinguish file-not-found (OK) from parse errors (warn).
		if _, statErr := os.Stat(configPath); statErr == nil {
			_, _ = fmt.Fprintf(os.Stderr, "Warning: failed to load config %s: %v, using defaults\n", configPath, err)
		}
	} else {
		cfg = loaded
	}

	// Resolve network.
	if cmd.IsSet("network") {
		network = cmd.String("network")
	} else if cfg != nil && cfg.Filecoin.Network != "" {
		network = cfg.Filecoin.Network
	} else {
		network = "calibration"
	}

	// Validate network.
	net := constants.Network(network)
	if net != constants.NetworkCalibration && net != constants.NetworkMainnet {
		return "", "", fmt.Errorf("unsupported network %q (supported: calibration, mainnet)", network)
	}

	// Resolve RPC URL.
	if cmd.IsSet("rpc-url") {
		rpcURL = cmd.String("rpc-url")
	} else if cfg != nil && cfg.Filecoin.RPCURL != "" {
		rpcURL = cfg.Filecoin.RPCURL
	} else if defaultURL, ok := constants.RPCURLs[net]; ok {
		rpcURL = defaultURL
	} else {
		return "", "", fmt.Errorf("no RPC URL available for network %q; use --rpc-url", network)
	}

	return rpcURL, network, nil
}

func outputJSON(providers []provider.ProviderDetail) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(providers)
}

func outputTable(providers []provider.ProviderDetail) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tName\tActive\tService URL\tLocation\tHealth\tMin Piece\tMax Piece")

	for _, p := range providers {
		active := "✗"
		if p.Active {
			active = "✓"
		}

		serviceURL := "—"
		location := "—"
		health := "—"
		minPiece := "—"
		maxPiece := "—"

		if p.HasPDP {
			if p.ServiceURL != "" {
				serviceURL = p.ServiceURL
			}
			if p.Location != "" {
				location = p.Location
			}
			minPiece = provider.FormatSize(p.MinPieceSize)
			maxPiece = provider.FormatSize(p.MaxPieceSize)
		}

		if p.HealthStatus != "skipped" {
			health = p.HealthStatus
		}

		_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			p.ID, p.Name, active, serviceURL, location, health, minPiece, maxPiece)
	}

	if err := w.Flush(); err != nil {
		return err
	}

	// Summary line.
	pdpCount := 0
	reachableCount := 0
	healthChecked := false
	for _, p := range providers {
		if p.HasPDP {
			pdpCount++
		}
		if p.HealthStatus == "reachable" {
			reachableCount++
		}
		if p.HealthStatus != "skipped" {
			healthChecked = true
		}
	}
	if healthChecked {
		_, _ = fmt.Fprintf(os.Stdout, "\nTotal: %d providers (%d with PDP, %d reachable)\n",
			len(providers), pdpCount, reachableCount)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "\nTotal: %d providers (%d with PDP, health skipped)\n",
			len(providers), pdpCount)
	}
	return nil
}

// maskRPCURL returns only the scheme and host of a URL to avoid leaking API keys
// embedded in the URL path or query parameters.
func maskRPCURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<invalid-url>"
	}
	return u.Scheme + "://" + u.Host
}
