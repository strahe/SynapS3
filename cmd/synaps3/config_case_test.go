package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/config"
	"github.com/urfave/cli/v3"
)

func TestIsAutoEvictEnabled_CaseInsensitive(t *testing.T) {
	tests := []struct {
		policy string
		want   bool
	}{
		{policy: "lru", want: true},
		{policy: "LRU", want: true},
		{policy: " LrU ", want: true},
		{policy: "manual", want: false},
		{policy: "none", want: false},
	}

	for _, tt := range tests {
		if got := isAutoEvictEnabled(tt.policy); got != tt.want {
			t.Fatalf("isAutoEvictEnabled(%q) = %v, want %v", tt.policy, got, tt.want)
		}
	}
}

func TestResolveRPCAndNetwork_NormalizesConfigNetwork(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfgTOML := "[filecoin]\nnetwork = \"Mainnet\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgTOML), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var (
		gotRPC     string
		gotNetwork string
	)
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			var err error
			gotRPC, gotNetwork, err = resolveRPCAndNetwork(cmd)
			return err
		},
	}

	if err := cmd.Run(context.Background(), []string{"synaps3", "--config", cfgPath}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if gotNetwork != "mainnet" {
		t.Fatalf("network = %q, want mainnet", gotNetwork)
	}
	if gotRPC != "https://api.node.glif.io/rpc/v1" {
		t.Fatalf("rpcURL = %q, want mainnet default RPC", gotRPC)
	}
}

func TestResolveRPCAndNetwork_UsesExplicitConfigRPCURL(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfgTOML := "[filecoin]\nnetwork = \"Mainnet\"\nrpc_url = \"https://rpc.example.test\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgTOML), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var (
		gotRPC     string
		gotNetwork string
	)
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			var err error
			gotRPC, gotNetwork, err = resolveRPCAndNetwork(cmd)
			return err
		},
	}

	if err := cmd.Run(context.Background(), []string{"synaps3", "--config", cfgPath}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if gotNetwork != "mainnet" {
		t.Fatalf("network = %q, want mainnet", gotNetwork)
	}
	if gotRPC != "https://rpc.example.test" {
		t.Fatalf("rpcURL = %q, want explicit config RPC", gotRPC)
	}
}

func TestResolveRPCAndNetwork_UsesDefaultRPCWhenEnvOverridesConfigNetwork(t *testing.T) {
	t.Setenv("SYNAPS3_FILECOIN_NETWORK", "calibration")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfgTOML := "[filecoin]\nnetwork = \"mainnet\"\nrpc_url = \"https://api.node.glif.io/rpc/v1\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgTOML), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var (
		gotRPC     string
		gotNetwork string
	)
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			var err error
			gotRPC, gotNetwork, err = resolveRPCAndNetwork(cmd)
			return err
		},
	}

	if err := cmd.Run(context.Background(), []string{"synaps3", "--config", cfgPath}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if gotNetwork != "calibration" {
		t.Fatalf("network = %q, want calibration", gotNetwork)
	}
	if gotRPC != "https://api.calibration.node.glif.io/rpc/v1" {
		t.Fatalf("rpcURL = %q, want calibration default RPC", gotRPC)
	}
}

func TestResolveRPCAndNetwork_UsesFallbackAppDataConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(t.TempDir())

	cfgDir := filepath.Join(home, ".synaps3")
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	cfgPath := filepath.Join(cfgDir, "config.toml")
	cfgTOML := "[filecoin]\nnetwork = \"mainnet\"\nrpc_url = \"https://fallback.example.test/rpc\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgTOML), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var (
		gotRPC     string
		gotNetwork string
	)
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Value: "config.toml"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			var err error
			gotRPC, gotNetwork, err = resolveRPCAndNetwork(cmd)
			return err
		},
	}

	if err := cmd.Run(context.Background(), []string{"synaps3"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if gotNetwork != "mainnet" {
		t.Fatalf("network = %q, want mainnet", gotNetwork)
	}
	if gotRPC != "https://fallback.example.test/rpc" {
		t.Fatalf("rpcURL = %q, want fallback app data RPC", gotRPC)
	}
}

func TestConfigSourceFromCommand_DefaultIgnoresWorkingDirectoryConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cwd := t.TempDir()
	t.Chdir(cwd)

	if err := os.WriteFile("config.toml", []byte("[filecoin]\nnetwork = \"mainnet\"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile cwd config: %v", err)
	}

	var got config.Source
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Value: "config.toml"},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			var err error
			got, err = configSourceFromCommand(cmd)
			return err
		},
	}

	if err := cmd.Run(context.Background(), []string{"synaps3"}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := filepath.Join(home, ".synaps3", "config.toml")
	if got.Path != want {
		t.Fatalf("source path = %q, want %q", got.Path, want)
	}
	if got.Exists {
		t.Fatal("Exists = true, want false for ignored cwd config")
	}
	if !got.GeneratedDefault {
		t.Fatal("GeneratedDefault = false, want true")
	}
}

func TestConfigSourceFromCommand_ExplicitRelativeConfigBecomesAbsolute(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)

	var got config.Source
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Value: "config.toml"},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			var err error
			got, err = configSourceFromCommand(cmd)
			return err
		},
	}

	if err := cmd.Run(context.Background(), []string{"synaps3", "--config", "config.toml"}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := filepath.Join(cwd, "config.toml")
	if got.Path != want {
		t.Fatalf("source path = %q, want %q", got.Path, want)
	}
	if !got.Explicit {
		t.Fatal("Explicit = false, want true")
	}
}

func TestS3ServerOptions_TLS(t *testing.T) {
	cfg := config.ServerConfig{
		Port:           ":8080",
		MaxConnections: 1,
		MaxRequests:    1,
		TLS: config.TLSConfig{
			Enabled:  true,
			CertFile: filepath.Join(t.TempDir(), "missing-cert.pem"),
			KeyFile:  filepath.Join(t.TempDir(), "missing-key.pem"),
		},
	}

	_, err := s3ServerOptions(cfg)
	if err == nil {
		t.Fatal("expected TLS certificate loading error")
	}
	if !strings.Contains(err.Error(), "TLS certificate") {
		t.Fatalf("error = %v, want TLS certificate context", err)
	}
}

func TestS3AccessLoggerFromConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.LoggingS3AccessConfig{Enabled: true, Level: "debug"}
	if got := s3AccessLogger(logger, cfg); got == nil {
		t.Fatal("s3AccessLogger(enabled) = nil, want logger")
	}

	cfg.Enabled = false
	if got := s3AccessLogger(logger, cfg); got != nil {
		t.Fatalf("s3AccessLogger(disabled) = %#v, want nil", got)
	}
}
