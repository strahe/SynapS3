package synapse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectlimits"
	sdk "github.com/strahe/synapse-go"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/spregistry"
	"github.com/strahe/synapse-go/storage"
)

const (
	defaultReadinessTimeout = 20 * time.Second
	// Match the global readiness warning to Wallet's under-30-days runway tone.
	readinessRunwayWarningSeconds = 30 * 24 * 60 * 60
)

type ReadinessStatus string

const (
	ReadinessStatusReady   ReadinessStatus = "ready"
	ReadinessStatusWarning ReadinessStatus = "warning"
	ReadinessStatusBlocked ReadinessStatus = "blocked"
	ReadinessStatusUnknown ReadinessStatus = "unknown"
)

type ReadinessMode string

const (
	ReadinessModeRuntime ReadinessMode = "runtime"
	ReadinessModeDraft   ReadinessMode = "draft"
)

type ReadinessCheck struct {
	ID            string          `json:"id"`
	Status        ReadinessStatus `json:"status"`
	Message       string          `json:"message"`
	Action        string          `json:"action,omitempty"`
	RequiredUSDFC string          `json:"required_usdfc,omitempty"`
}

type ReadinessResult struct {
	Status        ReadinessStatus   `json:"status"`
	Mode          ReadinessMode     `json:"mode"`
	CheckedAt     time.Time         `json:"checked_at"`
	Checks        []ReadinessCheck  `json:"checks"`
	PartialErrors map[string]string `json:"partial_errors,omitempty"`
}

type ReadinessConfig struct {
	Network              string
	RPCURL               string
	PrivateKey           string
	Source               string
	WithCDN              bool
	AllowPrivateNetworks bool
	DefaultCopies        int
}

type readinessPayments interface {
	WalletBalance(ctx context.Context, token, account common.Address) (*big.Int, error)
	AccountInfo(ctx context.Context, token, owner common.Address) (*payments.AccountState, error)
}

type readinessStorage interface {
	GetStorageInfo(ctx context.Context, opts *storage.GetStorageInfoOptions) (*storage.StorageInfo, error)
	CalculateMultiContextCosts(
		ctx context.Context,
		dataSizeBytes uint64,
		refs []storage.ContextCostRef,
		opts storage.MultiCostOptions,
		payer common.Address,
	) (*storage.MultiContextCosts, error)
}

type ReadinessClient interface {
	Address() common.Address
	Chain() chain.Chain
	ResolvedAddresses() sdk.ResolvedAddresses
	Payments() readinessPayments
	Storage() readinessStorage
	Close() error
}

type ReadinessClientFactory func(context.Context, ReadinessConfig) (ReadinessClient, error)

type ReadinessCheckerOption func(*ReadinessChecker)

type ReadinessChecker struct {
	cfg       ReadinessConfig
	runtime   ReadinessClient
	newClient ReadinessClientFactory
	timeout   time.Duration
	logger    *slog.Logger
}

func NewReadinessChecker(cfg ReadinessConfig, runtime ReadinessClient, opts ...ReadinessCheckerOption) *ReadinessChecker {
	checker := &ReadinessChecker{
		cfg:       cfg,
		runtime:   runtime,
		newClient: defaultReadinessClientFactory,
		timeout:   defaultReadinessTimeout,
	}
	for _, opt := range opts {
		opt(checker)
	}
	return checker
}

func WithReadinessClientFactory(factory ReadinessClientFactory) ReadinessCheckerOption {
	return func(c *ReadinessChecker) {
		if factory != nil {
			c.newClient = factory
		}
	}
}

func WithReadinessLogger(logger *slog.Logger) ReadinessCheckerOption {
	return func(c *ReadinessChecker) {
		c.logger = logger
	}
}

func WithReadinessTimeout(timeout time.Duration) ReadinessCheckerOption {
	return func(c *ReadinessChecker) {
		if timeout > 0 {
			c.timeout = timeout
		}
	}
}

func (c *ReadinessChecker) CheckRuntime(ctx context.Context) ReadinessResult {
	result := newReadinessResult(ReadinessModeRuntime)
	c.checkClient(ctx, c.cfg, c.runtime, &result, true)
	result.finish()
	return result
}

func (c *ReadinessChecker) CheckDraft(ctx context.Context, cfg ReadinessConfig) ReadinessResult {
	result := newReadinessResult(ReadinessModeDraft)
	if !validateReadinessConfig(cfg, &result) {
		result.finish()
		return result
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	client, err := c.newClient(ctx, cfg)
	if err != nil {
		result.unknownAction(
			"sdk_client",
			"Filecoin SDK client could not be initialized.",
			"Check the Filecoin RPC URL, network, and private key in the runtime configuration.",
		)
		c.addPartialError(&result, "sdk_client", err)
		result.finish()
		return result
	}
	defer func() {
		if err := client.Close(); err != nil && c.logger != nil {
			c.logger.Warn("failed to close Filecoin readiness client", "error", err)
		}
	}()

	c.checkClient(ctx, cfg, client, &result, false)
	result.finish()
	return result
}

func (c *ReadinessChecker) checkClient(ctx context.Context, cfg ReadinessConfig, client ReadinessClient, result *ReadinessResult, validateConfig bool) {
	if validateConfig && !validateReadinessConfig(cfg, result) {
		return
	}
	if client == nil {
		result.unknownAction(
			"sdk_client",
			"Filecoin SDK client is not available.",
			"Restart SynapS3 with valid Filecoin configuration.",
		)
		return
	}

	if cfg.AllowPrivateNetworks {
		result.warning(
			"private_networks",
			"Private network provider URLs are allowed.",
			"Use this only for trusted private infrastructure that serves retrieval and diagnostic URLs.",
		)
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	expectedChain, _ := chainForReadinessNetwork(cfg.Network)
	if client.Chain() != expectedChain {
		result.blocked(
			"network_match",
			fmt.Sprintf("RPC chain is %s, but settings expect %s.", client.Chain().String(), normalizeReadinessNetwork(cfg.Network)),
			"Use an RPC URL for the selected Filecoin network.",
		)
		return
	}
	result.ready("network_match", "RPC network matches settings.")

	c.checkWallet(ctx, client, result)
	c.checkStorage(ctx, cfg, client, result)
}

func (c *ReadinessChecker) checkWallet(ctx context.Context, client ReadinessClient, result *ReadinessResult) {
	paymentsSvc := client.Payments()
	if paymentsSvc == nil {
		result.unknown("wallet_fil_gas", "Wallet balance checks are unavailable.")
		result.unknown("wallet_usdfc", "USDFC wallet balance is unavailable.")
		result.unknown("payment_account", "USDFC payment account is unavailable.")
		return
	}

	addrs := client.ResolvedAddresses()
	fil, err := paymentsSvc.WalletBalance(ctx, payments.ZeroAddress, client.Address())
	switch {
	case err != nil:
		c.addPartialError(result, "fil_gas_balance", err)
		result.unknown("wallet_fil_gas", "FIL gas balance could not be checked.")
	case fil == nil:
		result.unknown("wallet_fil_gas", "FIL gas balance is unavailable.")
	case fil.Sign() <= 0:
		result.blocked(
			"wallet_fil_gas",
			"FIL gas balance is empty.",
			"Add FIL for transaction gas before uploading to Filecoin.",
		)
	default:
		result.ready("wallet_fil_gas", "FIL gas balance is available.")
	}

	usdfc, err := paymentsSvc.WalletBalance(ctx, addrs.USDFC, client.Address())
	switch {
	case err != nil:
		c.addPartialError(result, "usdfc_wallet_balance", err)
		result.unknown("wallet_usdfc", "USDFC wallet balance could not be checked.")
	case usdfc == nil:
		result.unknown("wallet_usdfc", "USDFC wallet balance is unavailable.")
	case usdfc.Sign() <= 0:
		result.warning(
			"wallet_usdfc",
			"USDFC wallet balance is empty.",
			"Keep USDFC available for future payment-account top-ups.",
		)
	default:
		result.ready("wallet_usdfc", "USDFC wallet balance is available.")
	}

	account, err := paymentsSvc.AccountInfo(ctx, addrs.USDFC, client.Address())
	switch {
	case err != nil:
		c.addPartialError(result, "usdfc_account", err)
		result.unknown("payment_account", "USDFC payment account could not be checked.")
	case account == nil:
		result.unknown("payment_account", "USDFC payment account is unavailable.")
	default:
		converted := convertAccountState(account, client.Chain(), chain.CurrentEpoch(client.Chain()))
		if converted == nil {
			result.unknown("payment_account", "USDFC payment account funds are unavailable.")
		} else {
			result.ready("payment_account", "USDFC payment account is available.")
			addPaymentRunwayCheck(result, converted)
		}
	}
}

func addPaymentRunwayCheck(result *ReadinessResult, account *PaymentAccountInfo) {
	switch {
	case account == nil:
		result.unknown("payment_runway", "Payment account runway could not be calculated.")
	case account.NoActiveSpend:
		result.ready("payment_runway", "Payment account runway is healthy.")
	case account.RunwaySeconds == nil:
		result.unknown("payment_runway", "Payment account runway could not be calculated.")
	case *account.RunwaySeconds <= 0:
		result.blocked(
			"payment_runway",
			"Payment account runway has expired.",
			"Fund the payment account before uploading to Filecoin.",
		)
	case *account.RunwaySeconds < readinessRunwayWarningSeconds:
		result.warning(
			"payment_runway",
			"Payment account runway is under 30 days.",
			"Top up the payment account to maintain at least 30 days of runway.",
		)
	default:
		result.ready("payment_runway", "Payment account runway is healthy.")
	}
}

func (c *ReadinessChecker) checkStorage(
	ctx context.Context,
	cfg ReadinessConfig,
	client ReadinessClient,
	result *ReadinessResult,
) {
	storageSvc := client.Storage()
	if storageSvc == nil {
		result.unknown("providers", "Storage provider inventory is unavailable.")
		addStorageDependencyUnknowns(result, "Storage cost estimate is unavailable.", "Payment funding could not be checked.", "FWSS approval could not be checked.")
		return
	}

	info, err := storageSvc.GetStorageInfo(ctx, nil)
	if err != nil {
		c.addPartialError(result, "storage_info", err)
	}
	if info == nil {
		result.unknown("providers", "Approved storage providers could not be checked.")
		addStorageDependencyUnknowns(result, "Storage cost estimate could not be calculated.", "Payment funding could not be checked.", "FWSS approval could not be checked.")
		return
	}

	refs := readinessCostRefs(info.Providers, cfg.DefaultCopies, cfg.WithCDN)
	if len(refs) < cfg.DefaultCopies {
		result.blocked(
			"providers",
			fmt.Sprintf("Only %d approved active providers are available; %d copies are configured.", len(refs), cfg.DefaultCopies),
			"Lower the default copy count or wait for more approved providers.",
		)
		addStorageDependencyUnknowns(result, "Storage cost estimate needs enough approved providers.", "Payment funding needs a storage cost estimate.", "FWSS approval needs a storage cost estimate.")
		return
	}
	result.ready("providers", "Approved active providers are available.")

	costs, err := storageSvc.CalculateMultiContextCosts(
		ctx,
		readinessCostEstimateDataSize(),
		refs,
		storage.MultiCostOptions{EnableCDN: cfg.WithCDN},
		client.Address(),
	)
	if err != nil {
		c.addPartialError(result, "storage_cost", err)
		addStorageDependencyUnknowns(result, "Storage cost estimate could not be calculated.", "Payment funding could not be checked.", "FWSS approval could not be checked.")
		return
	}
	if costs == nil {
		addStorageDependencyUnknowns(result, "Storage cost estimate is unavailable.", "Payment funding could not be checked.", "FWSS approval could not be checked.")
		return
	}

	result.ready("storage_cost", "Storage cost estimate is available.")
	if costs.DepositNeeded == nil {
		result.unknown("payment_funding", "Payment funding requirement is unavailable.")
	} else if costs.DepositNeeded.Sign() > 0 {
		result.blockedRequiredUSDFC(
			"payment_funding",
			"Payment account needs additional USDFC for configured storage.",
			"Fund the USDFC payment account before uploading to Filecoin.",
			costs.DepositNeeded.String(),
		)
	} else {
		result.ready("payment_funding", "Payment account funding is sufficient.")
	}
	if costs.NeedsFWSSMaxApproval {
		result.blocked(
			"fwss_approval",
			"FWSS payment approval is missing or too low.",
			"Approve FWSS spending before uploading to Filecoin.",
		)
	} else {
		result.ready("fwss_approval", "FWSS payment approval is sufficient.")
	}
}

func readinessCostEstimateDataSize() uint64 {
	// Keep readiness cost estimation tied to the production minimum upload size
	// and make that constant directly assertable in tests.
	return uint64(objectlimits.MinFOCUploadSize)
}

func addStorageDependencyUnknowns(result *ReadinessResult, storageCost, paymentFunding, fwssApproval string) {
	result.unknown("storage_cost", storageCost)
	result.unknown("payment_funding", paymentFunding)
	result.unknown("fwss_approval", fwssApproval)
}

func validateReadinessConfig(cfg ReadinessConfig, result *ReadinessResult) bool {
	ok := true
	add := func(id string, blocked bool, readyMessage, blockedMessage, action string) {
		if blocked {
			ok = false
			result.blocked(id, blockedMessage, action)
			return
		}
		result.ready(id, readyMessage)
	}

	add(
		"config_private_key",
		strings.TrimSpace(cfg.PrivateKey) == "",
		"Filecoin private key is configured.",
		"Filecoin private key is missing.",
		"Set filecoin.private_key in the config file or SYNAPS3_FILECOIN_PRIVATE_KEY, then restart SynapS3.",
	)
	add(
		"config_rpc_url",
		!validReadinessRPCURL(cfg.RPCURL),
		"Filecoin RPC URL is configured.",
		"Filecoin RPC URL is missing or invalid.",
		"Set filecoin.rpc_url to an http or https JSON-RPC endpoint.",
	)
	_, networkOK := chainForReadinessNetwork(cfg.Network)
	add(
		"config_network",
		!networkOK,
		"Filecoin network is supported.",
		"Filecoin network is unsupported.",
		"Use calibration or mainnet.",
	)
	add(
		"config_source",
		strings.TrimSpace(cfg.Source) == "",
		"Filecoin source namespace is configured.",
		"Filecoin source namespace is missing.",
		"Set filecoin.source to a non-empty application namespace.",
	)
	add(
		"config_default_copies",
		!model.ValidStorageCopies(cfg.DefaultCopies),
		"Default Filecoin copy count is valid.",
		"Default Filecoin copy count is invalid.",
		fmt.Sprintf("Set filecoin.default_copies between %d and %d.", model.StorageCopiesMin, model.StorageCopiesMax),
	)
	return ok
}

func validReadinessRPCURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return u.Host != "" && (u.Scheme == "http" || u.Scheme == "https")
}

func chainForReadinessNetwork(network string) (chain.Chain, bool) {
	switch normalizeReadinessNetwork(network) {
	case "calibration":
		return chain.Calibration, true
	case "mainnet":
		return chain.Mainnet, true
	default:
		return 0, false
	}
}

func normalizeReadinessNetwork(network string) string {
	return strings.ToLower(strings.TrimSpace(network))
}

func readinessCostRefs(providers []spregistry.PDPProvider, copies int, withCDN bool) []storage.ContextCostRef {
	if copies <= 0 {
		return nil
	}
	refs := make([]storage.ContextCostRef, 0, copies)
	for _, provider := range providers {
		if len(refs) >= copies {
			break
		}
		if !readinessProviderUsable(provider) {
			continue
		}
		refs = append(refs, storage.ContextCostRef{
			Provider: storage.Provider{
				ID:              provider.Info.ID,
				ServiceURL:      provider.Offering.ServiceURL,
				ServiceProvider: provider.Info.ServiceProvider,
				Payee:           provider.Info.Payee,
			},
			WithCDN: withCDN,
		})
	}
	return refs
}

func readinessProviderUsable(provider spregistry.PDPProvider) bool {
	return provider.Info.IsActive &&
		provider.Product.IsActive &&
		!provider.Info.ID.IsZero()
}

func newReadinessResult(mode ReadinessMode) ReadinessResult {
	return ReadinessResult{
		Status:    ReadinessStatusUnknown,
		Mode:      mode,
		CheckedAt: time.Now().UTC(),
		Checks:    make([]ReadinessCheck, 0, 12),
	}
}

func (r *ReadinessResult) addCheck(id string, status ReadinessStatus, message, action string) {
	r.add(ReadinessCheck{
		ID:      id,
		Status:  status,
		Message: message,
		Action:  action,
	})
}

func (r *ReadinessResult) add(check ReadinessCheck) {
	if r == nil {
		return
	}
	r.Checks = append(r.Checks, check)
}

func (r *ReadinessResult) ready(id, message string) {
	r.addCheck(id, ReadinessStatusReady, message, "")
}

func (r *ReadinessResult) unknown(id, message string) {
	r.addCheck(id, ReadinessStatusUnknown, message, "")
}

func (r *ReadinessResult) unknownAction(id, message, action string) {
	r.addCheck(id, ReadinessStatusUnknown, message, action)
}

func (r *ReadinessResult) warning(id, message, action string) {
	r.addCheck(id, ReadinessStatusWarning, message, action)
}

func (r *ReadinessResult) blocked(id, message, action string) {
	r.addCheck(id, ReadinessStatusBlocked, message, action)
}

func (r *ReadinessResult) blockedRequiredUSDFC(id, message, action, amount string) {
	r.add(ReadinessCheck{
		ID:            id,
		Status:        ReadinessStatusBlocked,
		Message:       message,
		Action:        action,
		RequiredUSDFC: amount,
	})
}

func (r *ReadinessResult) addPartialError(field string, err error) {
	if r == nil || err == nil {
		return
	}
	if r.PartialErrors == nil {
		r.PartialErrors = make(map[string]string)
	}
	r.PartialErrors[field] = sanitizeRPCError(err)
}

func (c *ReadinessChecker) addPartialError(result *ReadinessResult, field string, err error) {
	if c != nil && c.logger != nil && err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Debug(
			"filecoin readiness probe failed",
			"check", field,
			"error", sanitizeRPCError(err),
			"error_type", fmt.Sprintf("%T", err),
		)
	}
	result.addPartialError(field, err)
}

func (r *ReadinessResult) finish() {
	r.Finish()
}

// Finish updates the aggregate readiness status from the contained checks.
func (r *ReadinessResult) Finish() {
	if r == nil {
		return
	}
	status := ReadinessStatusReady
	for _, check := range r.Checks {
		switch check.Status {
		case ReadinessStatusBlocked:
			r.Status = ReadinessStatusBlocked
			return
		case ReadinessStatusUnknown:
			if status == ReadinessStatusReady {
				status = ReadinessStatusUnknown
			}
		case ReadinessStatusWarning:
			if status == ReadinessStatusReady || status == ReadinessStatusUnknown {
				status = ReadinessStatusWarning
			}
		}
	}
	if len(r.Checks) == 0 {
		status = ReadinessStatusUnknown
	}
	r.Status = status
}

func defaultReadinessClientFactory(ctx context.Context, cfg ReadinessConfig) (ReadinessClient, error) {
	client, err := NewClient(ctx, ClientConfig{
		PrivateKey:           cfg.PrivateKey,
		RPCURL:               cfg.RPCURL,
		Source:               cfg.Source,
		WithCDN:              cfg.WithCDN,
		AllowPrivateNetworks: cfg.AllowPrivateNetworks,
	})
	if err != nil {
		return nil, err
	}
	return AdaptReadinessClient(client), nil
}

func AdaptReadinessClient(client *sdk.Client) ReadinessClient {
	if client == nil {
		return nil
	}
	return &sdkReadinessClient{client: client}
}

type sdkReadinessClient struct {
	client *sdk.Client
}

func (c *sdkReadinessClient) Address() common.Address { return c.client.Address() }

func (c *sdkReadinessClient) Chain() chain.Chain { return c.client.Chain() }

func (c *sdkReadinessClient) ResolvedAddresses() sdk.ResolvedAddresses {
	return c.client.ResolvedAddresses()
}

func (c *sdkReadinessClient) Payments() readinessPayments { return c.client.Payments() }

func (c *sdkReadinessClient) Storage() readinessStorage { return c.client.Storage() }

func (c *sdkReadinessClient) Close() error { return c.client.Close() }
