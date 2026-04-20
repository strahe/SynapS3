package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/knadh/koanf/parsers/yaml"
	kfile "github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/provider"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/spregistry"
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

	c, err := parseChain(network)
	if err != nil {
		return err
	}

	ethClient, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("connecting to RPC: %w", err)
	}
	defer ethClient.Close()

	addrs := c.Addresses()
	svc, err := spregistry.New(spregistry.Options{
		Client:  ethClient,
		Address: addrs.SPRegistry,
	})
	if err != nil {
		return fmt.Errorf("creating registry service: %w", err)
	}

	reg := provider.NewRegistryService(svc)

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
	network = normalizeNetwork(network)

	// Validate network.
	if _, parseErr := parseChain(network); parseErr != nil {
		return "", "", parseErr
	}

	// Resolve RPC URL.
	if cmd.IsSet("rpc-url") {
		rpcURL = cmd.String("rpc-url")
	} else if envRPCURL, ok := os.LookupEnv("SYNAPS3_FILECOIN_RPC_URL"); ok && strings.TrimSpace(envRPCURL) != "" {
		rpcURL = strings.TrimSpace(envRPCURL)
	} else if cfg != nil && configFileSetsRPCURL(configPath) {
		rpcURL = cfg.Filecoin.RPCURL
	} else {
		// Use well-known default RPC URLs.
		defaults := map[string]string{
			"calibration": "https://api.calibration.node.glif.io/rpc/v1",
			"mainnet":     "https://api.node.glif.io/rpc/v1",
		}
		if defaultURL, ok := defaults[network]; ok {
			rpcURL = defaultURL
		} else {
			return "", "", fmt.Errorf("no RPC URL available for network %q; use --rpc-url", network)
		}
	}

	return rpcURL, network, nil
}

func normalizeNetwork(network string) string {
	return strings.ToLower(strings.TrimSpace(network))
}

func configFileSetsRPCURL(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	if _, err := os.Stat(path); err != nil {
		return false
	}

	k := koanf.New(".")
	if err := k.Load(kfile.Provider(path), yaml.Parser()); err != nil {
		return false
	}
	return strings.TrimSpace(k.String("filecoin.rpc_url")) != ""
}

// parseChain converts a network name string to a chain.Chain constant.
func parseChain(network string) (chain.Chain, error) {
	switch network {
	case "calibration":
		return chain.Calibration, nil
	case "mainnet":
		return chain.Mainnet, nil
	default:
		return 0, fmt.Errorf("unsupported network %q (supported: calibration, mainnet)", network)
	}
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
