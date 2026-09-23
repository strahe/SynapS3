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
	sdktypes "github.com/strahe/synapse-go/types"
)

const cleanupVerifierABI = `[{"type":"function","name":"dataSetLive","inputs":[{"name":"setId","type":"uint256"}],"outputs":[{"type":"bool"}],"stateMutability":"view"},{"type":"function","name":"pieceLive","inputs":[{"name":"setId","type":"uint256"},{"name":"pieceId","type":"uint256"}],"outputs":[{"type":"bool"}],"stateMutability":"view"},{"type":"function","name":"getScheduledRemovals","inputs":[{"name":"setId","type":"uint256"}],"outputs":[{"type":"uint256[]"}],"stateMutability":"view"}]`

// CleanupPieceState is an observation of one exact on-chain piece at one block.
type CleanupPieceState struct {
	Live        bool
	Queued      bool
	BlockNumber uint64
}

type cleanupChainCaller interface {
	BlockNumber(context.Context) (uint64, error)
	CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error)
}

type cleanupPieceReader struct {
	chain    cleanupChainCaller
	verifier common.Address
	abi      abi.ABI
}

func newCleanupPieceReader(chain cleanupChainCaller, verifier common.Address) (*cleanupPieceReader, error) {
	if chain == nil || verifier == (common.Address{}) {
		return nil, fmt.Errorf("cleanup chain and verifier are required")
	}
	contractABI, err := abi.JSON(strings.NewReader(cleanupVerifierABI))
	if err != nil {
		return nil, fmt.Errorf("parsing cleanup verifier ABI: %w", err)
	}
	return &cleanupPieceReader{chain: chain, verifier: verifier, abi: contractABI}, nil
}

func (r *cleanupPieceReader) ObserveCleanupPiece(ctx context.Context, dataSetID, pieceID sdktypes.BigInt) (CleanupPieceState, error) {
	height, err := r.chain.BlockNumber(ctx)
	if err != nil {
		return CleanupPieceState{}, fmt.Errorf("reading cleanup chain head: %w", err)
	}
	block := new(big.Int).SetUint64(height)
	call := func(method string, args ...any) ([]any, error) {
		input, err := r.abi.Pack(method, args...)
		if err != nil {
			return nil, err
		}
		output, err := r.chain.CallContract(ctx, ethereum.CallMsg{To: &r.verifier, Data: input}, block)
		if err != nil {
			return nil, err
		}
		return r.abi.Unpack(method, output)
	}
	dataSetResult, err := call("dataSetLive", dataSetID.Big())
	if err != nil {
		return CleanupPieceState{}, fmt.Errorf("reading cleanup data set state: %w", err)
	}
	dataSetLive, ok := dataSetResult[0].(bool)
	if !ok {
		return CleanupPieceState{}, fmt.Errorf("invalid cleanup data set state")
	}
	if !dataSetLive {
		return CleanupPieceState{}, errors.New("cleanup data set is not live")
	}
	liveResult, err := call("pieceLive", dataSetID.Big(), pieceID.Big())
	if err != nil {
		return CleanupPieceState{}, fmt.Errorf("reading cleanup piece state: %w", err)
	}
	live, ok := liveResult[0].(bool)
	if !ok {
		return CleanupPieceState{}, fmt.Errorf("invalid cleanup piece state")
	}
	state := CleanupPieceState{Live: live, BlockNumber: height}
	if !live {
		return state, nil
	}
	queueResult, err := call("getScheduledRemovals", dataSetID.Big())
	if err != nil {
		return CleanupPieceState{}, fmt.Errorf("reading scheduled cleanup: %w", err)
	}
	ids, ok := queueResult[0].([]*big.Int)
	if !ok {
		return CleanupPieceState{}, fmt.Errorf("invalid scheduled cleanup state")
	}
	for _, id := range ids {
		if id.Cmp(pieceID.Big()) == 0 {
			state.Queued = true
			break
		}
	}
	return state, nil
}
