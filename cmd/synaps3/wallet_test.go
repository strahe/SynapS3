package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
)

func TestWalletGenerateOutputsAddressAndPrivateKey(t *testing.T) {
	out, err := runWalletCommand(t, []string{"synaps3", "wallet", "generate", "--json"})
	if err != nil {
		t.Fatalf("wallet generate: %v\n%s", err, out)
	}

	var body map[string]string
	if err := json.Unmarshal([]byte(out), &body); err != nil {
		t.Fatalf("json output: %v\n%s", err, out)
	}
	if !isHexAddress(body["address"]) {
		t.Fatalf("address = %q, want hex address", body["address"])
	}
	if privateKey := body["private_key"]; !strings.HasPrefix(privateKey, "0x") || len(privateKey) != 66 {
		t.Fatalf("private_key = %q, want 0x-prefixed 32-byte hex", privateKey)
	}
}

func TestWalletGenerateTextOutputIsReadable(t *testing.T) {
	out, err := runWalletCommand(t, []string{"synaps3", "wallet", "generate"})
	if err != nil {
		t.Fatalf("wallet generate: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Address: 0x") || !strings.Contains(out, "Private key: 0x") {
		t.Fatalf("output = %q, want address and private key labels", out)
	}
}

func TestWalletFundTestnetClaimsBothFaucetTokens(t *testing.T) {
	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if got := r.URL.Query().Get("faucet_info"); got != "" {
			t.Fatalf("faucet_info query = %q, want aggregate faucet request", got)
		}
		if got := r.URL.Query().Get("address"); got != "0x1111111111111111111111111111111111111111" {
			t.Fatalf("address query = %q", got)
		}
		writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
			"result": []map[string]string{
				{"faucet_info": "CalibnetUSDFC", "tx_hash": "0x" + strings.Repeat("a", 64)},
				{"faucet_info": "CalibnetFIL", "tx_hash": "0x" + strings.Repeat("b", 64)},
			},
		})
	}))
	defer ts.Close()

	origEndpoint := walletFaucetEndpoint
	walletFaucetEndpoint = ts.URL
	t.Cleanup(func() { walletFaucetEndpoint = origEndpoint })

	out, err := runWalletCommand(t, []string{
		"synaps3", "wallet", "fund-testnet", "--json", "0x1111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("wallet fund-testnet: %v\n%s", err, out)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want one aggregate request", requests)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(out), &body); err != nil {
		t.Fatalf("json output: %v\n%s", err, out)
	}
	results, ok := body["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("results = %#v, want two token results", body["results"])
	}
	first, ok := results[0].(map[string]any)
	if !ok || first["faucet_info"] != "CalibnetUSDFC" {
		t.Fatalf("first result = %#v, want CalibnetUSDFC", results[0])
	}
	second, ok := results[1].(map[string]any)
	if !ok || second["faucet_info"] != "CalibnetFIL" {
		t.Fatalf("second result = %#v, want CalibnetFIL", results[1])
	}
}

func TestWalletFundTestnetRejectsInvalidAddress(t *testing.T) {
	out, err := runWalletCommand(t, []string{"synaps3", "wallet", "fund-testnet", "not-an-address"})
	if err == nil {
		t.Fatalf("wallet fund-testnet succeeded, output:\n%s", out)
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("error = %v, want invalid address", err)
	}
}

func TestWalletFundTestnetReportsTokenFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
			"result": []map[string]any{
				{
					"faucetInfo": "CalibnetUSDFC",
					"error":      map[string]string{"ServerError": "Faucet is empty, Request top-up"},
				},
				{"faucetInfo": "CalibnetFIL", "tx_hash": "0x" + strings.Repeat("b", 64)},
			},
		})
	}))
	defer ts.Close()

	origEndpoint := walletFaucetEndpoint
	walletFaucetEndpoint = ts.URL
	t.Cleanup(func() { walletFaucetEndpoint = origEndpoint })

	out, err := runWalletCommand(t, []string{
		"synaps3", "wallet", "fund-testnet", "0x1111111111111111111111111111111111111111",
	})
	if err == nil {
		t.Fatalf("wallet fund-testnet succeeded, output:\n%s", out)
	}
	if !strings.Contains(err.Error(), "CalibnetUSDFC") || !strings.Contains(err.Error(), "Faucet is empty") {
		t.Fatalf("error = %v, want token name", err)
	}
	if !strings.Contains(out, "CalibnetFIL: 0x"+strings.Repeat("b", 64)) {
		t.Fatalf("output = %s, want successful token hash", out)
	}
}

func TestWalletDepositParsesDecimalAmountAndUsesConfig(t *testing.T) {
	cfgPath := writeWalletTestConfig(t, "0x"+strings.Repeat("1", 64))
	var gotCfg *config.Config
	var gotAmount *big.Int
	var gotTimeout time.Duration

	origDeposit := walletDeposit
	walletDeposit = func(_ context.Context, cfg *config.Config, amount *big.Int, timeout time.Duration) (walletDepositResult, error) {
		gotCfg = cfg
		gotAmount = new(big.Int).Set(amount)
		gotTimeout = timeout
		return walletDepositResult{TxHash: "0x" + strings.Repeat("b", 64), Confirmed: true}, nil
	}
	t.Cleanup(func() { walletDeposit = origDeposit })

	out, err := runWalletCommand(t, []string{
		"synaps3", "--config", cfgPath, "wallet", "deposit", "--json", "--timeout", "7s", "1.25",
	})
	if err != nil {
		t.Fatalf("wallet deposit: %v\n%s", err, out)
	}
	if gotCfg == nil || gotCfg.Filecoin.RPCURL != "https://rpc.example.invalid" {
		t.Fatalf("config = %#v, want loaded filecoin config", gotCfg)
	}
	if gotAmount == nil || gotAmount.String() != "1250000000000000000" {
		t.Fatalf("amount = %v, want 1.25 USDFC base units", gotAmount)
	}
	if gotTimeout != 7*time.Second {
		t.Fatalf("timeout = %s, want 7s", gotTimeout)
	}
	if !strings.Contains(out, `"confirmed": true`) {
		t.Fatalf("output = %s, want confirmed JSON", out)
	}
}

func TestWalletDepositTextOutputShowsConfirmedTransaction(t *testing.T) {
	cfgPath := writeWalletTestConfig(t, "0x"+strings.Repeat("1", 64))
	origDeposit := walletDeposit
	walletDeposit = func(context.Context, *config.Config, *big.Int, time.Duration) (walletDepositResult, error) {
		return walletDepositResult{TxHash: "0x" + strings.Repeat("d", 64), Confirmed: true}, nil
	}
	t.Cleanup(func() { walletDeposit = origDeposit })

	out, err := runWalletCommand(t, []string{"synaps3", "--config", cfgPath, "wallet", "deposit", "1"})
	if err != nil {
		t.Fatalf("wallet deposit: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Transaction: 0x"+strings.Repeat("d", 64)) || !strings.Contains(out, "Status: confirmed") {
		t.Fatalf("output = %s, want confirmed transaction", out)
	}
}

func TestWalletDepositHelpMentionsConfiguredPrivateKey(t *testing.T) {
	out, err := runWalletCommand(t, []string{"synaps3", "wallet", "deposit", "--help"})
	if err != nil {
		t.Fatalf("wallet deposit help: %v\n%s", err, out)
	}
	if !strings.Contains(out, "configured Filecoin private key") {
		t.Fatalf("help output = %s, want configured private key guidance", out)
	}
}

func TestWalletDepositRejectsInvalidAmounts(t *testing.T) {
	for _, amount := range []string{"0", "-1", "1.1234567890123456789", "1e3"} {
		out, err := runWalletCommand(t, []string{"synaps3", "wallet", "deposit", amount})
		if err == nil {
			t.Fatalf("wallet deposit %q succeeded, output:\n%s", amount, out)
		}
		if !strings.Contains(err.Error(), "amount") && !strings.Contains(err.Error(), "USDFC") {
			t.Fatalf("amount %q error = %v, want amount validation", amount, err)
		}
	}
}

func TestWalletDepositRequiresPrivateKey(t *testing.T) {
	cfgPath := writeWalletTestConfig(t, "")
	out, err := runWalletCommand(t, []string{"synaps3", "--config", cfgPath, "wallet", "deposit", "1"})
	if err == nil {
		t.Fatalf("wallet deposit succeeded, output:\n%s", out)
	}
	if !strings.Contains(err.Error(), "filecoin.private_key") {
		t.Fatalf("error = %v, want filecoin.private_key", err)
	}
}

func TestWalletDepositPreservesHashOnWaitFailure(t *testing.T) {
	cfgPath := writeWalletTestConfig(t, "0x"+strings.Repeat("1", 64))
	origDeposit := walletDeposit
	walletDeposit = func(context.Context, *config.Config, *big.Int, time.Duration) (walletDepositResult, error) {
		return walletDepositResult{TxHash: "0x" + strings.Repeat("c", 64)}, errors.New("receipt timeout")
	}
	t.Cleanup(func() { walletDeposit = origDeposit })

	out, err := runWalletCommand(t, []string{"synaps3", "--config", cfgPath, "wallet", "deposit", "1"})
	if err == nil {
		t.Fatalf("wallet deposit succeeded, output:\n%s", out)
	}
	if !strings.Contains(out, "0x"+strings.Repeat("c", 64)) {
		t.Fatalf("output = %s, want tx hash", out)
	}
	if !strings.Contains(err.Error(), "receipt timeout") {
		t.Fatalf("error = %v, want receipt timeout", err)
	}
}

func runWalletCommand(t *testing.T, args []string) (string, error) {
	t.Helper()
	cmd := newRootCommand()
	var out bytes.Buffer
	cmd.Writer = &out
	cmd.ErrWriter = &out
	err := cmd.Run(context.Background(), args)
	return out.String(), err
}

func writeWalletTestConfig(t *testing.T, privateKey string) string {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	var body strings.Builder
	body.WriteString("[filecoin]\n")
	body.WriteString("rpc_url = \"https://rpc.example.invalid\"\n")
	if privateKey != "" {
		body.WriteString("private_key = \"")
		body.WriteString(privateKey)
		body.WriteString("\"\n")
	}
	if err := os.WriteFile(cfgPath, []byte(body.String()), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return cfgPath
}
