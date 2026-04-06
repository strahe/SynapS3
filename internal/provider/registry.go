package provider

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/data-preservation-programs/go-synapse/constants"
	"github.com/data-preservation-programs/go-synapse/spregistry"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// SPRegistry abstracts spregistry.Service for testability.
// *spregistry.Service satisfies this interface directly.
type SPRegistry interface {
	GetProviderCount(ctx context.Context) (int, error)
	GetProvider(ctx context.Context, id int) (*spregistry.ProviderInfo, error)
	GetAllActiveProviders(ctx context.Context) ([]*spregistry.ProviderInfo, error)
}

// ListOptions controls provider listing behavior.
type ListOptions struct {
	ActiveOnly bool
}

// ProviderDetail is the enriched output combining provider info, PDP offering, and health status.
type ProviderDetail struct {
	ID           int            `json:"id"`
	Name         string         `json:"name"`
	Description  string         `json:"description,omitempty"`
	Active       bool           `json:"active"`
	Address      common.Address `json:"address"`
	HasPDP       bool           `json:"has_pdp"`
	ServiceURL   string         `json:"service_url,omitempty"`
	MinPieceSize *big.Int       `json:"min_piece_size,omitempty"`
	MaxPieceSize *big.Int       `json:"max_piece_size,omitempty"`
	StoragePrice *big.Int       `json:"storage_price,omitempty"`
	Location     string         `json:"location,omitempty"`
	HealthStatus string         `json:"health_status"`
}

type contractCaller interface {
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
}

type registryService struct {
	client       contractCaller
	contractAddr common.Address
	parsedABI    abi.ABI
}

var (
	spRegistryABIOnce sync.Once
	spRegistryABI     abi.ABI
	spRegistryABIErr  error
)

// NewRegistryService creates a read-only spregistry.Service by connecting to the given
// RPC URL and looking up the SP Registry contract address for the specified network.
// It validates the chain ID against the expected network. The returned close function
// must be called to release the ethclient connection.
func NewRegistryService(ctx context.Context, rpcURL, network string) (SPRegistry, func(), error) {
	net := constants.Network(network)

	// Reject devnet — no baked-in SPRegistry address.
	if net == constants.NetworkDevnet {
		return nil, nil, fmt.Errorf("devnet is not supported for provider listing (no SPRegistry address)")
	}

	registryAddr, ok := constants.SPRegistryAddresses[net]
	if !ok || registryAddr == (common.Address{}) {
		return nil, nil, fmt.Errorf("no SPRegistry address for network %q", network)
	}

	ethClient, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to RPC %s: %w", rpcURL, err)
	}

	// Validate chain ID matches expected network.
	chainID, err := ethClient.ChainID(ctx)
	if err != nil {
		ethClient.Close()
		return nil, nil, fmt.Errorf("getting chain ID: %w", err)
	}

	expectedChainID, known := constants.ExpectedChainID(net)
	if known && chainID.Int64() != expectedChainID {
		ethClient.Close()
		return nil, nil, fmt.Errorf("chain ID mismatch: RPC returned %d but network %q expects %d", chainID.Int64(), network, expectedChainID)
	}

	parsedABI, err := loadSPRegistryABI()
	if err != nil {
		ethClient.Close()
		return nil, nil, fmt.Errorf("parsing SPRegistry ABI: %w", err)
	}

	return &registryService{
		client:       ethClient,
		contractAddr: registryAddr,
		parsedABI:    parsedABI,
	}, ethClient.Close, nil
}

func (s *registryService) GetProviderCount(ctx context.Context) (int, error) {
	raw, err := s.call(ctx, "getProviderCount")
	if err != nil {
		return 0, err
	}

	values, err := s.parsedABI.Unpack("getProviderCount", raw)
	if err != nil {
		return 0, fmt.Errorf("unpacking getProviderCount result: %w", err)
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("unexpected getProviderCount result length: %d", len(values))
	}

	count, ok := values[0].(*big.Int)
	if !ok {
		return 0, fmt.Errorf("unexpected getProviderCount result type: %T", values[0])
	}
	if !count.IsInt64() {
		return 0, fmt.Errorf("provider count exceeds int64: %s", count.String())
	}
	return int(count.Int64()), nil
}

func (s *registryService) GetProvider(ctx context.Context, providerID int) (*spregistry.ProviderInfo, error) {
	raw, err := s.call(ctx, "getProviderWithProduct", big.NewInt(int64(providerID)), uint8(spregistry.ProductTypePDP))
	if err != nil {
		if isProviderNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}

	providerInfo, err := decodeProviderWithProductResult(raw)
	if err != nil {
		return nil, err
	}
	if providerInfo.ServiceProvider == (common.Address{}) {
		return nil, nil
	}
	return providerInfo, nil
}

func (s *registryService) GetAllActiveProviders(ctx context.Context) ([]*spregistry.ProviderInfo, error) {
	var allProviders []*spregistry.ProviderInfo
	pageSize := big.NewInt(50)
	offset := big.NewInt(0)

	for {
		raw, err := s.call(ctx, "getAllActiveProviders", offset, pageSize)
		if err != nil {
			return nil, err
		}

		providerIDs, hasMore, err := decodeActiveProviderPage(raw)
		if err != nil {
			return nil, err
		}
		if hasMore && len(providerIDs) == 0 {
			return nil, fmt.Errorf("contract returned hasMore=true with empty results at offset %s", offset.String())
		}

		for _, id := range providerIDs {
			providerInfo, err := s.GetProvider(ctx, id)
			if err != nil {
				continue
			}
			if providerInfo != nil {
				allProviders = append(allProviders, providerInfo)
			}
		}

		if !hasMore {
			break
		}
		offset = new(big.Int).Add(offset, pageSize)
	}

	return allProviders, nil
}

func (s *registryService) call(ctx context.Context, method string, args ...any) ([]byte, error) {
	data, err := s.parsedABI.Pack(method, args...)
	if err != nil {
		return nil, fmt.Errorf("packing %s call: %w", method, err)
	}

	raw, err := s.client.CallContract(ctx, ethereum.CallMsg{
		To:   &s.contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("%s call failed: %w", method, err)
	}
	return raw, nil
}

func loadSPRegistryABI() (abi.ABI, error) {
	spRegistryABIOnce.Do(func() {
		spRegistryABI, spRegistryABIErr = abi.JSON(strings.NewReader(spregistry.SPRegistryABIJSON))
	})
	return spRegistryABI, spRegistryABIErr
}

func decodeProviderWithProductResult(raw []byte) (*spregistry.ProviderInfo, error) {
	parsedABI, err := loadSPRegistryABI()
	if err != nil {
		return nil, err
	}

	values, err := parsedABI.Unpack("getProviderWithProduct", raw)
	if err != nil {
		return nil, fmt.Errorf("unpacking getProviderWithProduct result: %w", err)
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("unexpected getProviderWithProduct result length: %d", len(values))
	}

	tuple := reflect.ValueOf(values[0])
	if tuple.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unexpected getProviderWithProduct tuple type: %T", values[0])
	}

	providerID, err := bigIntField(tuple, "ProviderId")
	if err != nil {
		return nil, err
	}
	if !providerID.IsInt64() {
		return nil, fmt.Errorf("provider ID exceeds int64: %s", providerID.String())
	}

	info, err := structField(tuple, "ProviderInfo")
	if err != nil {
		return nil, err
	}
	product, err := structField(tuple, "Product")
	if err != nil {
		return nil, err
	}
	productCapabilityValues, err := byteSlicesField(tuple, "ProductCapabilityValues")
	if err != nil {
		return nil, err
	}

	serviceProvider, err := addressField(info, "ServiceProvider")
	if err != nil {
		return nil, err
	}
	payee, err := addressField(info, "Payee")
	if err != nil {
		return nil, err
	}
	name, err := stringField(info, "Name")
	if err != nil {
		return nil, err
	}
	description, err := stringField(info, "Description")
	if err != nil {
		return nil, err
	}
	active, err := boolField(info, "IsActive")
	if err != nil {
		return nil, err
	}
	productActive, err := boolField(product, "IsActive")
	if err != nil {
		return nil, err
	}
	capabilityKeys, err := stringSliceField(product, "CapabilityKeys")
	if err != nil {
		return nil, err
	}

	products := make(map[string]*spregistry.ServiceProduct)
	if productActive {
		capabilities := spregistry.CapabilitiesListToMap(capabilityKeys, productCapabilityValues)
		products["PDP"] = &spregistry.ServiceProduct{
			Type:         "PDP",
			IsActive:     true,
			Capabilities: capabilities,
			Data:         spregistry.DecodePDPCapabilities(capabilities),
		}
	}

	return &spregistry.ProviderInfo{
		ID:              int(providerID.Int64()),
		ServiceProvider: serviceProvider,
		Payee:           payee,
		Name:            name,
		Description:     description,
		Active:          active,
		Products:        products,
	}, nil
}

func decodeActiveProviderPage(raw []byte) ([]int, bool, error) {
	parsedABI, err := loadSPRegistryABI()
	if err != nil {
		return nil, false, err
	}

	values, err := parsedABI.Unpack("getAllActiveProviders", raw)
	if err != nil {
		return nil, false, fmt.Errorf("unpacking getAllActiveProviders result: %w", err)
	}
	if len(values) != 2 {
		return nil, false, fmt.Errorf("unexpected getAllActiveProviders result length: %d", len(values))
	}

	rawIDs, ok := values[0].([]*big.Int)
	if !ok {
		return nil, false, fmt.Errorf("unexpected getAllActiveProviders IDs type: %T", values[0])
	}
	hasMore, ok := values[1].(bool)
	if !ok {
		return nil, false, fmt.Errorf("unexpected getAllActiveProviders hasMore type: %T", values[1])
	}

	ids := make([]int, 0, len(rawIDs))
	for _, id := range rawIDs {
		if id == nil {
			continue
		}
		if !id.IsInt64() {
			return nil, false, fmt.Errorf("active provider ID exceeds int64: %s", id.String())
		}
		ids = append(ids, int(id.Int64()))
	}

	return ids, hasMore, nil
}

func structField(v reflect.Value, name string) (reflect.Value, error) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return reflect.Value{}, fmt.Errorf("missing field %q in ABI tuple", name)
	}
	if field.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("field %q is %s, want struct", name, field.Kind())
	}
	return field, nil
}

func bigIntField(v reflect.Value, name string) (*big.Int, error) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return nil, fmt.Errorf("missing field %q in ABI tuple", name)
	}
	value, ok := field.Interface().(*big.Int)
	if !ok || value == nil {
		return nil, fmt.Errorf("field %q is %T, want *big.Int", name, field.Interface())
	}
	return value, nil
}

func addressField(v reflect.Value, name string) (common.Address, error) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return common.Address{}, fmt.Errorf("missing field %q in ABI tuple", name)
	}
	value, ok := field.Interface().(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("field %q is %T, want common.Address", name, field.Interface())
	}
	return value, nil
}

func stringField(v reflect.Value, name string) (string, error) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return "", fmt.Errorf("missing field %q in ABI tuple", name)
	}
	value, ok := field.Interface().(string)
	if !ok {
		return "", fmt.Errorf("field %q is %T, want string", name, field.Interface())
	}
	return value, nil
}

func boolField(v reflect.Value, name string) (bool, error) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return false, fmt.Errorf("missing field %q in ABI tuple", name)
	}
	value, ok := field.Interface().(bool)
	if !ok {
		return false, fmt.Errorf("field %q is %T, want bool", name, field.Interface())
	}
	return value, nil
}

func stringSliceField(v reflect.Value, name string) ([]string, error) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return nil, fmt.Errorf("missing field %q in ABI tuple", name)
	}
	value, ok := field.Interface().([]string)
	if !ok {
		return nil, fmt.Errorf("field %q is %T, want []string", name, field.Interface())
	}
	return value, nil
}

func byteSlicesField(v reflect.Value, name string) ([][]byte, error) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return nil, fmt.Errorf("missing field %q in ABI tuple", name)
	}
	value, ok := field.Interface().([][]byte)
	if !ok {
		return nil, fmt.Errorf("field %q is %T, want [][]byte", name, field.Interface())
	}
	return value, nil
}

func isProviderNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "Provider does not exist")
}

// ListProviders queries the on-chain SP Registry and returns enriched provider details.
// When opts.ActiveOnly is true, it uses the efficient paginated GetAllActiveProviders.
// Otherwise, it iterates all provider IDs from 1 to ProviderCount.
func ListProviders(ctx context.Context, reg SPRegistry, opts ListOptions) ([]ProviderDetail, error) {
	if opts.ActiveOnly {
		return listActiveProviders(ctx, reg)
	}
	return listAllProviders(ctx, reg)
}

func listActiveProviders(ctx context.Context, reg SPRegistry) ([]ProviderDetail, error) {
	providers, err := reg.GetAllActiveProviders(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching active providers: %w", err)
	}

	details := make([]ProviderDetail, 0, len(providers))
	for _, p := range providers {
		if p == nil {
			continue
		}
		details = append(details, toProviderDetail(p))
	}
	return details, nil
}

func listAllProviders(ctx context.Context, reg SPRegistry) ([]ProviderDetail, error) {
	count, err := reg.GetProviderCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching provider count: %w", err)
	}

	var skipped int
	details := make([]ProviderDetail, 0, count)
	for id := 1; id <= count; id++ {
		p, err := reg.GetProvider(ctx, id)
		if err != nil {
			// Abort on context cancellation or deadline exceeded.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			skipped++
			continue
		}
		if p == nil {
			continue
		}
		details = append(details, toProviderDetail(p))
	}
	if skipped > 0 {
		fmt.Fprintf(os.Stderr, "Warning: %d provider(s) skipped due to RPC errors\n", skipped)
	}
	return details, nil
}

func toProviderDetail(p *spregistry.ProviderInfo) ProviderDetail {
	d := ProviderDetail{
		ID:           p.ID,
		Name:         p.Name,
		Description:  p.Description,
		Active:       p.Active,
		Address:      p.ServiceProvider,
		HealthStatus: "skipped",
	}

	if pdp, ok := p.Products["PDP"]; ok && pdp != nil && pdp.IsActive && pdp.Data != nil {
		d.HasPDP = true
		d.ServiceURL = pdp.Data.ServiceURL
		d.MinPieceSize = pdp.Data.MinPieceSizeInBytes
		d.MaxPieceSize = pdp.Data.MaxPieceSizeInBytes
		d.StoragePrice = pdp.Data.StoragePricePerTiBPerDay
		d.Location = pdp.Data.Location
	}

	return d
}

// FormatSize converts a byte count to a human-readable size string (e.g., "1 MiB", "32 GiB").
func FormatSize(b *big.Int) string {
	if b == nil || b.Sign() <= 0 {
		return "—"
	}

	// Guard against values that exceed int64 range.
	if !b.IsInt64() {
		return b.String() + " B"
	}

	const (
		kib = 1024
		mib = kib * 1024
		gib = mib * 1024
		tib = gib * 1024
	)

	v := b.Int64()
	switch {
	case v >= tib && v%tib == 0:
		return fmt.Sprintf("%d TiB", v/tib)
	case v >= gib && v%gib == 0:
		return fmt.Sprintf("%d GiB", v/gib)
	case v >= mib && v%mib == 0:
		return fmt.Sprintf("%d MiB", v/mib)
	case v >= kib && v%kib == 0:
		return fmt.Sprintf("%d KiB", v/kib)
	default:
		return fmt.Sprintf("%d B", v)
	}
}
