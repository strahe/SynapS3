package app

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/walletoperation"
)

func TestNewRuntimeReportsAllMissingDependencies(t *testing.T) {
	t.Parallel()
	_, err := NewRuntime(context.Background(), RuntimeOptions{})
	if err == nil {
		t.Fatal("NewRuntime succeeded without dependencies")
	}
	for _, dependency := range []string{
		"config", "database", "settings", "logger", "filecoin storage", "filecoin wallet query",
		"filecoin wallet operator", "filecoin receipts", "filecoin readiness", "filecoin observability", "filecoin removal chain",
	} {
		if !strings.Contains(err.Error(), dependency) {
			t.Errorf("NewRuntime error = %q, missing dependency %q", err, dependency)
		}
	}
}

type identityStorage struct {
	synapse.StorageClient
	verifier common.Address
}

func (s identityStorage) PDPVerifierAddress() common.Address { return s.verifier }

type identityWallet struct {
	synapse.WalletOperator
	identity walletoperation.ExecutionIdentity
}

func (w identityWallet) WalletIdentity() walletoperation.ExecutionIdentity { return w.identity }

type identityWalletQuery struct {
	synapse.WalletQuerier
	identity walletoperation.ExecutionIdentity
}

func (w identityWalletQuery) WalletIdentity() walletoperation.ExecutionIdentity { return w.identity }

func TestRuntimeRejectsInconsistentSigningServices(t *testing.T) {
	for _, scenario := range []string{"valid", "declared network", "probe network", "record keeper", "PDP verifier", "payment contracts"} {
		t.Run(scenario, func(t *testing.T) {
			walletIdentity := testutil.DefaultWalletExecutionIdentity()
			queryIdentity := testutil.DefaultWalletExecutionIdentity()
			storage := identityStorage{StorageClient: &testutil.MockStorageClient{}, verifier: testutil.DefaultPDPVerifierAddress}
			services := FilecoinServices{ChainID: uint64(testutil.DefaultContextIdentity.ChainID), Storage: storage}
			network := "calibration"
			switch scenario {
			case "declared network":
				network = "mainnet"
			case "probe network":
				services.ChainID = 314
			case "record keeper":
				walletIdentity.Identity.RecordKeeper = common.HexToAddress("0x00000000000000000000000000000000000000ee")
				queryIdentity.Identity.RecordKeeper = walletIdentity.Identity.RecordKeeper
			case "PDP verifier":
				storage.verifier = common.Address{}
				services.Storage = storage
			case "payment contracts":
				address := common.HexToAddress("0x00000000000000000000000000000000000000ee")
				queryIdentity.Payments = &address
			}
			services.Wallet = identityWallet{identity: walletIdentity}
			services.WalletQuery = identityWalletQuery{identity: queryIdentity}
			err := validateFilecoinIdentity(network, services)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("runtime identity check = %v", err)
			}
		})
	}
}

func TestRuntimeConfigurationHelpers(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		input string
		want  cache.EvictionPolicy
	}{
		{input: "lru", want: cache.EvictionPolicyLRU},
		{input: "LRU", want: cache.EvictionPolicyLRU},
		{input: " LrU ", want: cache.EvictionPolicyLRU},
		{input: "after_upload", want: cache.EvictionPolicyAfterUpload},
		{input: "none", want: cache.EvictionPolicyNone},
	} {
		got, ok := cache.ParseEvictionPolicy(test.input)
		if !ok || got != test.want {
			t.Errorf("ParseEvictionPolicy(%q) = %q/%v, want %q/true", test.input, got, ok, test.want)
		}
	}

	serverConfig := config.ServerConfig{
		Port: ":8080", MaxConnections: 1, MaxRequests: 1,
		TLS: config.TLSConfig{
			Enabled: true, CertFile: filepath.Join(t.TempDir(), "missing-cert.pem"),
			KeyFile: filepath.Join(t.TempDir(), "missing-key.pem"),
		},
	}
	if _, err := s3ServerOptions(serverConfig); err == nil || !strings.Contains(err.Error(), "TLS certificate") {
		t.Fatalf("s3ServerOptions error = %v, want TLS certificate context", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	accessConfig := config.LoggingS3AccessConfig{Enabled: true, Level: "debug"}
	if got := s3AccessLogger(logger, accessConfig); got == nil {
		t.Fatal("s3AccessLogger(enabled) = nil, want logger")
	}
	accessConfig.Enabled = false
	if got := s3AccessLogger(logger, accessConfig); got != nil {
		t.Fatalf("s3AccessLogger(disabled) = %#v, want nil", got)
	}
}
