package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
