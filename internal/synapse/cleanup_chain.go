package synapse

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	sdktypes "github.com/strahe/synapse-go/types"
)

const cleanupVerifierABI = `[{"type":"function","name":"dataSetLive","inputs":[{"name":"setId","type":"uint256"}],"outputs":[{"type":"bool"}],"stateMutability":"view"},{"type":"function","name":"pieceLive","inputs":[{"name":"setId","type":"uint256"},{"name":"pieceId","type":"uint256"}],"outputs":[{"type":"bool"}],"stateMutability":"view"},{"type":"function","name":"getScheduledRemovals","inputs":[{"name":"setId","type":"uint256"}],"outputs":[{"type":"uint256[]"}],"stateMutability":"view"},{"type":"function","name":"getNextChallengeEpoch","inputs":[{"name":"setId","type":"uint256"}],"outputs":[{"type":"uint256"}],"stateMutability":"view"},{"type":"function","name":"VERSION","inputs":[],"outputs":[{"type":"string"}],"stateMutability":"view"},{"type":"error","name":"DataSetNotFound","inputs":[]}]`

type CleanupPieceStatus string

const (
	CleanupPiecePresent        CleanupPieceStatus = "present"
	CleanupPieceScheduled      CleanupPieceStatus = "scheduled"
	CleanupPieceDataSetCleanup CleanupPieceStatus = "dataset_cleanup"
	CleanupPieceRemoved        CleanupPieceStatus = "removed"
)

// CleanupPieceState is an observation of one exact on-chain piece at one block.
type CleanupPieceState struct {
	Status      CleanupPieceStatus
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
		versionResult, err := call("VERSION")
		if err != nil {
			return CleanupPieceState{}, fmt.Errorf("reading cleanup verifier version: %w", err)
		}
		if version, ok := versionResult[0].(string); !ok || (version != "3.4.0" && version != "3.5.0") {
			return CleanupPieceState{}, fmt.Errorf("unsupported cleanup verifier version: %v", versionResult[0])
		}
		epochResult, err := call("getNextChallengeEpoch", dataSetID.Big())
		if err != nil {
			notFound := r.abi.Errors["DataSetNotFound"].ID
			if data, ok := ethclient.RevertErrorData(err); ok && bytes.Equal(data, notFound[:4]) {
				// Historical storage slots can remain after the registration ends.
				return CleanupPieceState{Status: CleanupPieceRemoved, BlockNumber: height}, nil
			}
			return CleanupPieceState{}, fmt.Errorf("reading cleanup data set phase: %w", err)
		}
		epoch, ok := epochResult[0].(*big.Int)
		// PDP 3.4.0 and 3.5.0 retain data-set storage during batched cleanup;
		// only the uint256 sentinel identifies this phase, never a termination epoch.
		if !ok || epoch.BitLen() != 256 || epoch.Cmp(new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))) != 0 {
			return CleanupPieceState{}, fmt.Errorf("inconsistent cleanup data set phase: %v", epochResult[0])
		}
		return CleanupPieceState{Status: CleanupPieceDataSetCleanup, BlockNumber: height}, nil
	}
	liveResult, err := call("pieceLive", dataSetID.Big(), pieceID.Big())
	if err != nil {
		return CleanupPieceState{}, fmt.Errorf("reading cleanup piece state: %w", err)
	}
	live, ok := liveResult[0].(bool)
	if !ok {
		return CleanupPieceState{}, fmt.Errorf("invalid cleanup piece state")
	}
	state := CleanupPieceState{Status: CleanupPiecePresent, BlockNumber: height}
	if !live {
		state.Status = CleanupPieceRemoved
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
			state.Status = CleanupPieceScheduled
			break
		}
	}
	return state, nil
}
