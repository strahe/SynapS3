package synapse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/objectlimits"
	sdk "github.com/strahe/synapse-go"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/spregistry"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestReadinessCheckerBlocksMissingConfig(t *testing.T) {
	checker := NewReadinessChecker(ReadinessConfig{}, nil)

	got := checker.CheckRuntime(context.Background())

	if got.Mode != ReadinessModeRuntime {
		t.Fatalf("Mode = %q, want %q", got.Mode, ReadinessModeRuntime)
	}
	requireResultStatus(t, got, ReadinessStatusBlocked)
	requireChecksStatus(t, got.Checks, ReadinessStatusBlocked,
		"config_private_key",
		"config_rpc_url",
		"config_network",
		"config_source",
		"config_default_copies",
	)
	requireCheckMessage(t, got.Checks, "config_private_key", "Filecoin private key is missing.")
	requireCheckMessage(t, got.Checks, "config_rpc_url", "Filecoin RPC URL is missing or invalid.")
	requireCheckMessage(t, got.Checks, "config_network", "Filecoin network is unsupported.")
	requireCheckMessage(t, got.Checks, "config_source", "Filecoin source namespace is missing.")
	requireCheckMessage(t, got.Checks, "config_default_copies", "Default Filecoin copy count is invalid.")
}

func TestReadinessCheckerBlocksNetworkMismatch(t *testing.T) {
	cfg := readyReadinessConfig()
	cfg.Network = "mainnet"
	client := readyReadinessClient(1)
	client.chain = chain.Calibration
	checker := NewReadinessChecker(cfg, client)

	got := checker.CheckRuntime(context.Background())

	requireResultStatus(t, got, ReadinessStatusBlocked)
	requireCheckStatus(t, got.Checks, "network_match", ReadinessStatusBlocked)
}

func TestReadinessCheckerUsesApprovedProviderInventoryAndCostEstimate(t *testing.T) {
	cfg := readyReadinessConfig()
	cfg.DefaultCopies = 2
	client := readyReadinessClient(2)
	client.payments.account = readinessAccountWithRunway(1_000, 248)
	checker := NewReadinessChecker(cfg, client)

	got := checker.CheckRuntime(context.Background())

	requireResultStatus(t, got, ReadinessStatusReady)
	if len(client.storage.costRefs) != cfg.DefaultCopies {
		t.Fatalf("cost refs = %d, want %d", len(client.storage.costRefs), cfg.DefaultCopies)
	}
	if client.storage.costDataSize != uint64(objectlimits.MinFOCUploadSize) {
		t.Fatalf("cost data size = %d, want %d", client.storage.costDataSize, objectlimits.MinFOCUploadSize)
	}
	if client.storage.costPayer != client.address {
		t.Fatalf("cost payer = %s, want %s", client.storage.costPayer.Hex(), client.address.Hex())
	}
	requireChecksStatus(t, got.Checks, ReadinessStatusReady, "payment_account", "payment_runway", "providers", "storage_cost")
}

func TestReadinessCheckerReportsFundingAndApprovalGaps(t *testing.T) {
	for _, tc := range []struct {
		name          string
		depositNeeded int64
		needsApproval bool
		want          ReadinessStatus
		wantFunding   ReadinessStatus
		wantApproval  ReadinessStatus
		wantRequired  string
	}{
		{
			name:          "deposit needed without approval gap",
			depositNeeded: 10,
			want:          ReadinessStatusBlocked,
			wantFunding:   ReadinessStatusBlocked,
			wantApproval:  ReadinessStatusReady,
			wantRequired:  "10",
		},
		{
			name:         "funding ready without approval gap",
			want:         ReadinessStatusReady,
			wantFunding:  ReadinessStatusReady,
			wantApproval: ReadinessStatusReady,
		},
		{
			name:          "approval gap",
			needsApproval: true,
			want:          ReadinessStatusBlocked,
			wantFunding:   ReadinessStatusReady,
			wantApproval:  ReadinessStatusBlocked,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := readyReadinessClient(1)
			client.storage.costs = readinessCosts(tc.depositNeeded, tc.needsApproval, false)
			checker := NewReadinessChecker(readyReadinessConfig(), client)

			got := checker.CheckRuntime(context.Background())

			requireResultStatus(t, got, tc.want)
			funding := requireCheckStatus(t, got.Checks, "payment_funding", tc.wantFunding)
			if funding.RequiredUSDFC != tc.wantRequired {
				t.Fatalf("payment_funding required_usdfc = %q, want %q", funding.RequiredUSDFC, tc.wantRequired)
			}
			requireCheckStatus(t, got.Checks, "fwss_approval", tc.wantApproval)
		})
	}
}

func TestReadinessCheckerChecksPaymentRunway(t *testing.T) {
	for _, tc := range []struct {
		name string
		acct *payments.AccountState
		want ReadinessStatus
	}{
		{
			name: "under threshold",
			acct: readinessAccountWithRunway(1_000, 20),
			want: ReadinessStatusWarning,
		},
		{
			name: "expired",
			acct: readinessAccountWithRunway(1_000, 0),
			want: ReadinessStatusBlocked,
		},
		{
			name: "unknown",
			acct: readinessAccountWithMissingRunway(1_000),
			want: ReadinessStatusUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := readyReadinessClient(1)
			client.payments.account = tc.acct
			checker := NewReadinessChecker(readyReadinessConfig(), client)

			got := checker.CheckRuntime(context.Background())

			requireResultStatus(t, got, tc.want)
			requireCheckStatus(t, got.Checks, "payment_account", ReadinessStatusReady)
			requireCheckStatus(t, got.Checks, "payment_runway", tc.want)
		})
	}
}

func TestReadinessCheckerStorageErrorsAreUnknownAndSanitized(t *testing.T) {
	cfg := readyReadinessConfig()
	client := readyReadinessClient(1)
	client.storage.info = nil
	client.storage.infoErr = errors.New("dial https://secret-token@example.invalid/rpc: boom")
	var logs bytes.Buffer
	checker := NewReadinessChecker(
		cfg,
		client,
		WithReadinessLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
	)

	got := checker.CheckRuntime(context.Background())

	requireResultStatus(t, got, ReadinessStatusUnknown)
	requireCheckStatus(t, got.Checks, "providers", ReadinessStatusUnknown)
	body := fmt.Sprintf("%#v", got)
	for _, leaked := range []string{"secret-token", "example.invalid", "boom"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("readiness response leaked %q: %s", leaked, body)
		}
	}
	if got.PartialErrors["storage_info"] != "RPC call failed" {
		t.Fatalf("partial storage error = %#v, want sanitized RPC failure", got.PartialErrors)
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("readiness partial error should not log warning: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "level=DEBUG") {
		t.Fatalf("readiness partial error should be debug logged: %s", logs.String())
	}
}

func TestReadinessCheckerDoesNotWarnForCancelledRequests(t *testing.T) {
	cfg := readyReadinessConfig()
	client := readyReadinessClient(1)
	client.storage.info = nil
	client.storage.infoErr = context.Canceled
	var logs bytes.Buffer
	checker := NewReadinessChecker(
		cfg,
		client,
		WithReadinessLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)

	_ = checker.CheckRuntime(context.Background())

	if strings.Contains(logs.String(), "filecoin readiness probe failed") {
		t.Fatalf("cancelled readiness request should not log warning: %s", logs.String())
	}
}

func TestReadinessCheckerWarnsWhenPrivateNetworksAreAllowed(t *testing.T) {
	cfg := readyReadinessConfig()
	cfg.AllowPrivateNetworks = true
	checker := NewReadinessChecker(cfg, readyReadinessClient(1))

	got := checker.CheckRuntime(context.Background())

	requireResultStatus(t, got, ReadinessStatusWarning)
	check := requireCheckStatus(t, got.Checks, "private_networks", ReadinessStatusWarning)
	if check.Message != "Private network provider URLs are allowed." {
		t.Fatalf("private_networks message = %q, want provider URL warning", check.Message)
	}
	if check.Action != "Use this only for trusted private infrastructure that serves retrieval and diagnostic URLs." {
		t.Fatalf("private_networks action = %q, want retrieval and diagnostic URL warning", check.Action)
	}
}

func TestReadinessCheckerDraftUsesTemporaryClientAndCloses(t *testing.T) {
	cfg := readyReadinessConfig()
	draft := cfg
	draft.Network = "mainnet"
	tempClient := readyReadinessClient(1)
	tempClient.chain = chain.Mainnet
	tempClient.payments.balances[chain.Mainnet.Addresses().USDFC] = big.NewInt(1_000)
	tempClient.storage.info.Providers = []spregistry.PDPProvider{readinessProvider(1)}
	called := false

	checker := NewReadinessChecker(cfg, nil, WithReadinessClientFactory(
		func(_ context.Context, got ReadinessConfig) (ReadinessClient, error) {
			called = true
			if got.Network != draft.Network {
				t.Fatalf("draft network = %q, want %q", got.Network, draft.Network)
			}
			return tempClient, nil
		},
	))

	got := checker.CheckDraft(context.Background(), draft)

	if !called {
		t.Fatal("draft client factory was not called")
	}
	if !tempClient.closed {
		t.Fatal("draft client was not closed")
	}
	if got.Mode != ReadinessModeDraft {
		t.Fatalf("Mode = %q, want %q", got.Mode, ReadinessModeDraft)
	}
	requireResultStatus(t, got, ReadinessStatusReady)
	if got := countReadinessChecks(got.Checks, "config_private_key"); got != 1 {
		t.Fatalf("config_private_key checks = %d, want 1", got)
	}
}

func TestReadinessStatusRollupPrioritizesBlockedUnknownWarning(t *testing.T) {
	result := newReadinessResult(ReadinessModeRuntime)
	result.addCheck("warning", ReadinessStatusWarning, "warning", "")
	result.addCheck("unknown", ReadinessStatusUnknown, "unknown", "")
	result.finish()
	if result.Status != ReadinessStatusWarning {
		t.Fatalf("Status = %q, want warning", result.Status)
	}
	result.addCheck("blocked", ReadinessStatusBlocked, "blocked", "")
	result.finish()
	if result.Status != ReadinessStatusBlocked {
		t.Fatalf("Status = %q, want blocked", result.Status)
	}
}

func readyReadinessConfig() ReadinessConfig {
	return ReadinessConfig{
		Network:              "calibration",
		RPCURL:               "https://api.calibration.node.glif.io/rpc/v1",
		PrivateKey:           "configured-private-key",
		Source:               "synaps3",
		DefaultCopies:        1,
		AllowPrivateNetworks: false,
	}
}

func readyReadinessClient(copies int) *fakeReadinessClient {
	c := &fakeReadinessClient{
		address: common.HexToAddress("0x1000000000000000000000000000000000000001"),
		chain:   chain.Calibration,
	}
	addrs := c.chain.Addresses()
	c.payments = &fakeReadinessPayments{
		balances: map[common.Address]*big.Int{
			payments.ZeroAddress: big.NewInt(1),
			addrs.USDFC:          big.NewInt(1_000),
		},
		account: readinessAccount(1_000),
	}
	providers := make([]spregistry.PDPProvider, 0, copies)
	for i := 0; i < copies; i++ {
		providers = append(providers, readinessProvider(uint64(i+1)))
	}
	c.storage = &fakeReadinessStorage{
		info: &storage.StorageInfo{
			Providers: providers,
			Allowances: &storage.Allowances{
				IsApproved: true,
			},
		},
		costs: readinessCosts(0, false, true),
	}
	return c
}

func readinessAccount(funds int64) *payments.AccountState {
	return &payments.AccountState{
		Funds:               big.NewInt(funds),
		LockupCurrent:       big.NewInt(0),
		LockupRate:          big.NewInt(0),
		LockupLastSettledAt: big.NewInt(0),
		FundedUntilEpoch:    big.NewInt(0),
	}
}

func readinessAccountWithRunway(funds, runwayDays int64) *payments.AccountState {
	fundedUntil := new(big.Int).Add(
		chain.CurrentEpoch(chain.Calibration),
		big.NewInt(runwayDays*chain.EpochsPerDay),
	)
	return readinessAccountWithFundedUntil(funds, fundedUntil)
}

func readinessAccountWithMissingRunway(funds int64) *payments.AccountState {
	return readinessAccountWithFundedUntil(funds, nil)
}

func readinessAccountWithFundedUntil(funds int64, fundedUntil *big.Int) *payments.AccountState {
	return &payments.AccountState{
		Funds:               big.NewInt(funds),
		LockupCurrent:       big.NewInt(0),
		LockupRate:          big.NewInt(1),
		LockupLastSettledAt: big.NewInt(0),
		FundedUntilEpoch:    fundedUntil,
	}
}

func readinessCosts(deposit int64, needsApproval, ready bool) *storage.MultiContextCosts {
	return &storage.MultiContextCosts{
		RatePerEpoch:         big.NewInt(1),
		RatePerMonth:         big.NewInt(100),
		DepositNeeded:        big.NewInt(deposit),
		NeedsFWSSMaxApproval: needsApproval,
		Ready:                ready,
	}
}

func readinessProvider(id uint64) spregistry.PDPProvider {
	return spregistry.PDPProvider{
		Info: spregistry.ProviderInfo{
			ID:              sdktypes.NewBigInt(id),
			ServiceProvider: common.BigToAddress(new(big.Int).SetUint64(id + 100)),
			Payee:           common.BigToAddress(new(big.Int).SetUint64(id + 200)),
			IsActive:        true,
		},
		Product: spregistry.ServiceProduct{
			ProductType: spregistry.ProductTypePDP,
			IsActive:    true,
		},
		Offering: spregistry.PDPOffering{
			ServiceURL:          fmt.Sprintf("https://provider-%d.example.invalid", id),
			PaymentTokenAddress: chain.Calibration.Addresses().USDFC,
		},
	}
}

func requireResultStatus(t *testing.T, got ReadinessResult, want ReadinessStatus) {
	t.Helper()
	if got.Status != want {
		t.Fatalf("Status = %q, want %q: %#v partial=%#v", got.Status, want, got.Checks, got.PartialErrors)
	}
}

func requireChecksStatus(t *testing.T, checks []ReadinessCheck, want ReadinessStatus, ids ...string) {
	t.Helper()
	for _, id := range ids {
		requireCheckStatus(t, checks, id, want)
	}
}

func requireCheckStatus(t *testing.T, checks []ReadinessCheck, id string, want ReadinessStatus) ReadinessCheck {
	t.Helper()
	check := requireReadinessCheck(t, checks, id)
	if check.Status != want {
		t.Fatalf("%s = %#v, want %s", id, check, want)
	}
	return check
}

func requireCheckMessage(t *testing.T, checks []ReadinessCheck, id, want string) {
	t.Helper()
	check := requireReadinessCheck(t, checks, id)
	if check.Message != want {
		t.Fatalf("%s message = %q, want %q", id, check.Message, want)
	}
}

func requireReadinessCheck(t *testing.T, checks []ReadinessCheck, id string) ReadinessCheck {
	t.Helper()
	check := findReadinessCheck(checks, id)
	if check == nil {
		t.Fatalf("missing readiness check %q in %#v", id, checks)
	}
	return *check
}

func findReadinessCheck(checks []ReadinessCheck, id string) *ReadinessCheck {
	for i := range checks {
		if checks[i].ID == id {
			return &checks[i]
		}
	}
	return nil
}

func countReadinessChecks(checks []ReadinessCheck, id string) int {
	count := 0
	for _, check := range checks {
		if check.ID == id {
			count++
		}
	}
	return count
}

type fakeReadinessClient struct {
	address  common.Address
	chain    chain.Chain
	addrs    sdk.ResolvedAddresses
	payments *fakeReadinessPayments
	storage  *fakeReadinessStorage
	closed   bool
}

func (f *fakeReadinessClient) Address() common.Address { return f.address }

func (f *fakeReadinessClient) Chain() chain.Chain { return f.chain }

func (f *fakeReadinessClient) ResolvedAddresses() sdk.ResolvedAddresses {
	if f.addrs != (sdk.ResolvedAddresses{}) {
		return f.addrs
	}
	addrs := f.chain.Addresses()
	return sdk.ResolvedAddresses{
		FWSS:               addrs.FWSS,
		PDPVerifier:        addrs.PDPVerifier,
		SPRegistry:         addrs.SPRegistry,
		USDFC:              addrs.USDFC,
		Payments:           addrs.Payments,
		ViewContract:       addrs.StateView,
		SessionKeyRegistry: addrs.SessionKeyRegistry,
	}
}

func (f *fakeReadinessClient) Payments() readinessPayments { return f.payments }

func (f *fakeReadinessClient) Storage() readinessStorage { return f.storage }

func (f *fakeReadinessClient) Close() error {
	f.closed = true
	return nil
}

type fakeReadinessPayments struct {
	balances map[common.Address]*big.Int
	account  *payments.AccountState
}

func (f *fakeReadinessPayments) WalletBalance(_ context.Context, token, _ common.Address) (*big.Int, error) {
	if f.balances == nil {
		return nil, errors.New("missing balance")
	}
	bal, ok := f.balances[token]
	if !ok {
		return nil, errors.New("missing balance")
	}
	return cloneBigInt(bal), nil
}

func (f *fakeReadinessPayments) AccountInfo(context.Context, common.Address, common.Address) (*payments.AccountState, error) {
	if f.account == nil {
		return nil, errors.New("missing account")
	}
	return f.account, nil
}

type fakeReadinessStorage struct {
	info         *storage.StorageInfo
	infoErr      error
	costs        *storage.MultiContextCosts
	costErr      error
	costRefs     []storage.ContextCostRef
	costPayer    common.Address
	costDataSize uint64
}

func (f *fakeReadinessStorage) GetStorageInfo(context.Context, *storage.GetStorageInfoOptions) (*storage.StorageInfo, error) {
	return f.info, f.infoErr
}

func (f *fakeReadinessStorage) CalculateMultiContextCosts(
	_ context.Context,
	dataSize uint64,
	refs []storage.ContextCostRef,
	_ storage.MultiCostOptions,
	payer common.Address,
) (*storage.MultiContextCosts, error) {
	f.costRefs = append([]storage.ContextCostRef(nil), refs...)
	f.costPayer = payer
	f.costDataSize = dataSize
	return f.costs, f.costErr
}
