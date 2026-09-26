package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
)

const approvedProviderViewABI = `[{"type":"function","name":"getApprovedProvidersLength","inputs":[],"outputs":[{"type":"uint256"}],"stateMutability":"view"},{"type":"function","name":"getApprovedProviders","inputs":[{"type":"uint256"},{"type":"uint256"}],"outputs":[{"type":"uint256[]"}],"stateMutability":"view"}]`

type approvedProviderCatalog struct {
	chain cleanupChainCaller
	view  common.Address
	abi   abi.ABI
}

// NewApprovedProviderCatalog reads every page of the approved list at one chain block.
func NewApprovedProviderCatalog(chain cleanupChainCaller, view common.Address) (*approvedProviderCatalog, error) {
	if chain == nil || view == (common.Address{}) {
		return nil, errors.New("approved provider chain and view are required")
	}
	contractABI, err := abi.JSON(strings.NewReader(approvedProviderViewABI))
	if err != nil {
		return nil, fmt.Errorf("parsing approved provider ABI: %w", err)
	}
	return &approvedProviderCatalog{chain: chain, view: view, abi: contractABI}, nil
}

func (c *approvedProviderCatalog) ReadApprovedProviderIDs(ctx context.Context) ([]types.OnChainID, error) {
	if c == nil || c.chain == nil || c.view == (common.Address{}) {
		return nil, errors.New("approved provider source unavailable")
	}
	height, err := c.chain.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading approved provider block: %w", err)
	}
	block := new(big.Int).SetUint64(height)
	call := func(method string, args ...any) ([]any, error) {
		input, err := c.abi.Pack(method, args...)
		if err != nil {
			return nil, err
		}
		output, err := c.chain.CallContract(ctx, ethereum.CallMsg{To: &c.view, Data: input}, block)
		if err != nil {
			return nil, err
		}
		return c.abi.Unpack(method, output)
	}
	length, err := call("getApprovedProvidersLength")
	if err != nil {
		return nil, fmt.Errorf("reading approved provider count: %w", err)
	}
	if len(length) != 1 {
		return nil, errors.New("invalid approved provider count")
	}
	count, ok := length[0].(*big.Int)
	if !ok || count == nil || !count.IsUint64() {
		return nil, errors.New("invalid approved provider count")
	}
	ids := make([]types.OnChainID, 0)
	for offset := uint64(0); offset < count.Uint64(); {
		limit := min(uint64(100), count.Uint64()-offset)
		page, err := call("getApprovedProviders", new(big.Int).SetUint64(offset), new(big.Int).SetUint64(limit))
		if err != nil {
			return nil, fmt.Errorf("reading approved provider page: %w", err)
		}
		if len(page) != 1 {
			return nil, errors.New("invalid approved provider page")
		}
		values, ok := page[0].([]*big.Int)
		if !ok || uint64(len(values)) != limit {
			return nil, errors.New("incomplete approved provider traversal")
		}
		for _, value := range values {
			id, err := sdktypes.BigIntFromBig(value)
			if err != nil {
				return nil, fmt.Errorf("invalid approved provider ID: %w", err)
			}
			ids = append(ids, types.OnChainIDFromSDK(id))
		}
		offset += limit
	}
	return ids, nil
}
