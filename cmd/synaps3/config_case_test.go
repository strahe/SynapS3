package main

import (
	"context"
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
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgYAML := "filecoin:\n  network: Mainnet\n"
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o644); err != nil {
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
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgYAML := "filecoin:\n  network: Mainnet\n  rpc_url: https://rpc.example.test\n"
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o644); err != nil {
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

func TestResolveRPCAndNetwork_UsesFallbackAppDataConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(t.TempDir())

	cfgDir := filepath.Join(home, ".synaps3")
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	cfgPath := filepath.Join(cfgDir, "config.yaml")
	cfgYAML := "filecoin:\n  network: mainnet\n  rpc_url: https://fallback.example.test/rpc\n"
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var (
		gotRPC     string
		gotNetwork string
	)
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Value: "config.yaml"},
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
