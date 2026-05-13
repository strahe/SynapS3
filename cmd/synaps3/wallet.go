package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/payments"
	"github.com/urfave/cli/v3"
)

const usdfcDecimals = 18

var walletFaucetTokenOrder = []string{"CalibnetUSDFC", "CalibnetFIL"}

var (
	walletFaucetEndpoint = "https://forest-explorer.chainsafe.dev/api/claim_token_all"
	walletDeposit        = runWalletDeposit
)

type walletDepositResult struct {
	TxHash    string `json:"tx_hash"`
	Confirmed bool   `json:"confirmed"`
}

type walletFaucetResult struct {
	FaucetInfo string `json:"faucet_info"`
	TxHash     string `json:"tx_hash,omitempty"`
	Error      string `json:"error,omitempty"`
}

type walletFaucetClaim struct {
	FaucetInfo string
	TxHash     string
	Error      map[string]string
}

func (c *walletFaucetClaim) UnmarshalJSON(data []byte) error {
	var raw struct {
		FaucetInfo      string            `json:"faucetInfo"`
		FaucetInfoSnake string            `json:"faucet_info"`
		TxHash          string            `json:"tx_hash"`
		Error           map[string]string `json:"error"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	c.FaucetInfo = strings.TrimSpace(raw.FaucetInfo)
	if c.FaucetInfo == "" {
		c.FaucetInfo = strings.TrimSpace(raw.FaucetInfoSnake)
	}
	c.TxHash = raw.TxHash
	c.Error = raw.Error
	return nil
}

func walletCommand() *cli.Command {
	return &cli.Command{
		Name:  "wallet",
		Usage: "prepare and fund Filecoin wallets",
		Commands: []*cli.Command{
			walletGenerateCommand(),
			walletFundTestnetCommand(),
			walletDepositCommand(),
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unknown wallet command %q, run wallet --help for available commands", cmd.Args().First())
			}
			return cli.ShowSubcommandHelp(cmd)
		},
	}
}

func walletGenerateCommand() *cli.Command {
	return &cli.Command{
		Name:  "generate",
		Usage: "generate a wallet address and private key",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "json",
				Usage: "output as JSON",
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unexpected argument %q, wallet generate takes no positional arguments", cmd.Args().First())
			}
			key, err := crypto.GenerateKey()
			if err != nil {
				return fmt.Errorf("generating wallet key: %w", err)
			}
			return writeWalletGenerated(cmd, key)
		},
	}
}

func walletFundTestnetCommand() *cli.Command {
	return &cli.Command{
		Name:  "fund-testnet",
		Usage: "claim Calibration tFIL and USDFC from the faucet",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "json",
				Usage: "output as JSON",
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Value: 20 * time.Second,
				Usage: "faucet request timeout",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() != 1 {
				return fmt.Errorf("wallet fund-testnet requires exactly one address")
			}
			address := strings.TrimSpace(cmd.Args().First())
			if !isHexAddress(address) {
				return fmt.Errorf("invalid address %q", address)
			}
			timeout := cmd.Duration("timeout")
			if timeout <= 0 {
				return fmt.Errorf("timeout must be positive")
			}
			results, err := claimWalletTestnetFunds(ctx, walletFaucetEndpoint, address, timeout)
			if writeErr := writeWalletFaucetResults(cmd, address, results); writeErr != nil {
				return writeErr
			}
			return err
		},
	}
}

func walletDepositCommand() *cli.Command {
	return &cli.Command{
		Name:  "deposit",
		Usage: "deposit USDFC using the configured Filecoin private key",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "json",
				Usage: "output as JSON",
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Value: 5 * time.Minute,
				Usage: "transaction receipt wait timeout",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() != 1 {
				return fmt.Errorf("wallet deposit requires exactly one amount")
			}
			amount, err := parseUSDFCAmount(cmd.Args().First())
			if err != nil {
				return err
			}
			timeout := cmd.Duration("timeout")
			if timeout <= 0 {
				return fmt.Errorf("timeout must be positive")
			}
			cfg, err := loadWalletConfig(cmd)
			if err != nil {
				return err
			}
			result, err := walletDeposit(ctx, cfg, amount, timeout)
			if writeErr := writeWalletDepositResult(cmd, result); writeErr != nil {
				return writeErr
			}
			return err
		},
	}
}

func writeWalletGenerated(cmd *cli.Command, key *ecdsa.PrivateKey) error {
	privateKey := "0x" + hex.EncodeToString(crypto.FromECDSA(key))
	address := crypto.PubkeyToAddress(key.PublicKey).Hex()
	if cmd.Bool("json") {
		return writeWalletJSON(cmd, map[string]string{
			"address":     address,
			"private_key": privateKey,
		})
	}
	_, err := fmt.Fprintf(cmd.Root().Writer, "Address: %s\nPrivate key: %s\n", address, privateKey)
	return err
}

func isHexAddress(address string) bool {
	return common.IsHexAddress(strings.TrimSpace(address))
}

func claimWalletTestnetFunds(ctx context.Context, endpoint, address string, timeout time.Duration) ([]walletFaucetResult, error) {
	client := &http.Client{Timeout: timeout}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing faucet endpoint: %w", err)
	}
	q := u.Query()
	q.Set("address", address)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating faucet request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting faucet: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("reading faucet response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		results, parseErr := parseWalletFaucetClaims(body)
		if len(results) > 0 {
			if parseErr != nil {
				return results, fmt.Errorf("faucet returned %s: %w", resp.Status, parseErr)
			}
			return results, fmt.Errorf("faucet returned %s", resp.Status)
		}
		return nil, fmt.Errorf("faucet returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return parseWalletFaucetClaims(body)
}

func parseWalletFaucetClaims(body []byte) ([]walletFaucetResult, error) {
	var claims []walletFaucetClaim
	if err := json.Unmarshal(body, &claims); err == nil && len(claims) > 0 {
		return walletFaucetClaimsToResults(claims)
	}

	var envelope struct {
		Result []walletFaucetClaim `json:"result"`
		Error  *struct {
			Message string              `json:"message"`
			Cause   []walletFaucetClaim `json:"cause"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decoding faucet response: %w", err)
	}
	claims = envelope.Result
	if len(claims) == 0 && envelope.Error != nil {
		claims = envelope.Error.Cause
	}
	if len(claims) == 0 {
		if envelope.Error != nil && strings.TrimSpace(envelope.Error.Message) != "" {
			return nil, fmt.Errorf("faucet error: %s", envelope.Error.Message)
		}
		return nil, fmt.Errorf("faucet response missing token results")
	}
	return walletFaucetClaimsToResults(claims)
}

func walletFaucetClaimsToResults(claims []walletFaucetClaim) ([]walletFaucetResult, error) {
	results := make([]walletFaucetResult, 0, len(claims))
	var errs []error
	for i, claim := range claims {
		token := strings.TrimSpace(claim.FaucetInfo)
		if token == "" && i < len(walletFaucetTokenOrder) {
			token = walletFaucetTokenOrder[i]
		}
		if token == "" {
			token = "unknown"
		}
		result := walletFaucetResult{FaucetInfo: token}
		if msg := walletFaucetClaimError(claim.Error); msg != "" {
			result.Error = msg
			errs = append(errs, fmt.Errorf("%s: %s", token, msg))
		} else if strings.TrimSpace(claim.TxHash) == "" {
			result.Error = "missing transaction hash"
			errs = append(errs, fmt.Errorf("%s: missing transaction hash", token))
		} else {
			result.TxHash = claim.TxHash
		}
		results = append(results, result)
	}
	return results, errors.Join(errs...)
}

func walletFaucetClaimError(errs map[string]string) string {
	if len(errs) == 0 {
		return ""
	}
	if msg := strings.TrimSpace(errs["ServerError"]); msg != "" {
		return msg
	}
	parts := make([]string, 0, len(errs))
	for key, value := range errs {
		value = strings.TrimSpace(value)
		if value == "" {
			parts = append(parts, key)
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %s", key, value))
	}
	return strings.Join(parts, "; ")
}

func writeWalletFaucetResults(cmd *cli.Command, address string, results []walletFaucetResult) error {
	if cmd.Bool("json") {
		return writeWalletJSON(cmd, map[string]any{
			"address": address,
			"results": results,
		})
	}
	for _, result := range results {
		if result.Error != "" {
			if _, err := fmt.Fprintf(cmd.Root().Writer, "%s: error: %s\n", result.FaucetInfo, result.Error); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(cmd.Root().Writer, "%s: %s\n", result.FaucetInfo, result.TxHash); err != nil {
			return err
		}
	}
	return nil
}

func parseUSDFCAmount(raw string) (*big.Int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, fmt.Errorf("amount is required")
	}
	if strings.HasPrefix(value, "-") {
		return nil, fmt.Errorf("amount must be positive")
	}
	if strings.Count(value, ".") > 1 {
		return nil, fmt.Errorf("amount must be a decimal number")
	}
	integer, fraction, hasFraction := strings.Cut(value, ".")
	if integer == "" && !hasFraction {
		return nil, fmt.Errorf("amount must be a decimal number")
	}
	if integer == "" {
		integer = "0"
	}
	if !decimalDigits(integer) || (hasFraction && (fraction == "" || !decimalDigits(fraction))) {
		return nil, fmt.Errorf("amount must be a decimal number")
	}
	if len(fraction) > usdfcDecimals {
		return nil, fmt.Errorf("USDFC supports up to %d decimal places", usdfcDecimals)
	}
	base := integer + fraction + strings.Repeat("0", usdfcDecimals-len(fraction))
	base = strings.TrimLeft(base, "0")
	if base == "" {
		base = "0"
	}
	amount, ok := new(big.Int).SetString(base, 10)
	if !ok {
		return nil, fmt.Errorf("amount must be a decimal number")
	}
	if amount.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be greater than 0")
	}
	return amount, nil
}

func decimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func loadWalletConfig(cmd *cli.Command) (*config.Config, error) {
	src, err := configSourceFromCommand(cmd)
	if err != nil {
		return nil, err
	}
	cfg, err := config.LoadSource(src)
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}
	if strings.TrimSpace(cfg.Filecoin.PrivateKey) == "" {
		return nil, fmt.Errorf("filecoin.private_key must be set in config or SYNAPS3_FILECOIN_PRIVATE_KEY")
	}
	if strings.TrimSpace(cfg.Filecoin.RPCURL) == "" {
		return nil, fmt.Errorf("filecoin.rpc_url must be set in config or SYNAPS3_FILECOIN_RPC_URL")
	}
	return cfg, nil
}

func runWalletDeposit(ctx context.Context, cfg *config.Config, amount *big.Int, timeout time.Duration) (walletDepositResult, error) {
	client, err := synapse.NewClient(ctx, synapse.ClientConfig{
		PrivateKey:           cfg.Filecoin.PrivateKey,
		RPCURL:               cfg.Filecoin.RPCURL,
		Source:               cfg.Filecoin.Source,
		WithCDN:              cfg.Filecoin.WithCDN,
		AllowPrivateNetworks: cfg.Filecoin.AllowPrivateNetworks,
	})
	if err != nil {
		return walletDepositResult{}, fmt.Errorf("initializing Filecoin SDK: %w", err)
	}
	defer func() { _ = client.Close() }()

	res, err := client.Payments().FundSync(ctx, amount, payments.WithWait(timeout))
	result := walletDepositResult{}
	if res != nil {
		result.TxHash = res.Hash.Hex()
		result.Confirmed = res.Receipt != nil && res.Receipt.Status == 1
	}
	if err != nil {
		return result, fmt.Errorf("depositing USDFC: %w", err)
	}
	return result, nil
}

func writeWalletDepositResult(cmd *cli.Command, result walletDepositResult) error {
	if cmd.Bool("json") {
		return writeWalletJSON(cmd, result)
	}
	if result.TxHash == "" {
		return nil
	}
	status := "submitted"
	if result.Confirmed {
		status = "confirmed"
	}
	_, err := fmt.Fprintf(cmd.Root().Writer, "Transaction: %s\nStatus: %s\n", result.TxHash, status)
	return err
}

func writeWalletJSON(cmd *cli.Command, value any) error {
	enc := json.NewEncoder(cmd.Root().Writer)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}
