package synapse

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// blockNumberSource is satisfied by an Ethereum JSON-RPC client. On Filecoin the
// EVM block number is the chain epoch, which is the same source the storage SDK
// reads.
type blockNumberSource interface {
	BlockNumber(ctx context.Context) (uint64, error)
}

type chainEpochReader struct {
	source blockNumberSource
}

// NewChainEpochReader reads the epoch from the chain head.
//
// The wall-clock estimate available elsewhere in the SDK derives an epoch from
// genesis time and can run ahead of the chain, which would let a replacement
// retire a source before its service has really ended. Retirement therefore
// only trusts an observed block number.
func NewChainEpochReader(source blockNumberSource) ChainEpochReader {
	return &chainEpochReader{source: source}
}

func (r *chainEpochReader) CurrentEpoch(ctx context.Context) (int64, error) {
	if r == nil || r.source == nil {
		return 0, errors.New("chain epoch reader is not configured")
	}
	height, err := r.source.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("reading chain head epoch: %w", NormalizeProviderOperationError(ctx, err))
	}
	if height > math.MaxInt64 {
		return 0, fmt.Errorf("chain head epoch %d is out of range", height)
	}
	return int64(height), nil
}
