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

// CleanupPiecesState contains one block's observations, keyed by decimal piece ID.
type CleanupPiecesState struct {
	Pieces          map[string]CleanupPieceStatus
	QueueLength     int
	BlockNumber     uint64
	VerifierVersion string
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
	states, err := r.ObserveCleanupPieces(ctx, dataSetID, []sdktypes.BigInt{pieceID})
	if err != nil {
		return CleanupPieceState{}, err
	}
	return CleanupPieceState{Status: states.Pieces[pieceID.String()], BlockNumber: states.BlockNumber}, nil
}

func (r *cleanupPieceReader) ObserveCleanupPieces(ctx context.Context, dataSetID sdktypes.BigInt, pieceIDs []sdktypes.BigInt) (CleanupPiecesState, error) {
	height, err := r.chain.BlockNumber(ctx)
	if err != nil {
		return CleanupPiecesState{}, fmt.Errorf("reading cleanup chain head: %w", err)
	}
	state := CleanupPiecesState{Pieces: make(map[string]CleanupPieceStatus, len(pieceIDs)), BlockNumber: height}
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
	versionResult, err := call("VERSION")
	if err != nil {
		return state, fmt.Errorf("reading cleanup verifier version: %w", err)
	}
	version, ok := versionResult[0].(string)
	if !ok || version == "" {
		return state, fmt.Errorf("invalid cleanup verifier version")
	}
	state.VerifierVersion = version
	liveResult, err := call("dataSetLive", dataSetID.Big())
	if err != nil {
		return state, fmt.Errorf("reading cleanup data set state: %w", err)
	}
	dataSetLive, ok := liveResult[0].(bool)
	if !ok {
		return state, fmt.Errorf("invalid cleanup data set state")
	}
	if !dataSetLive {
		if version != "3.4.0" && version != "3.5.0" {
			return state, fmt.Errorf("unsupported cleanup verifier version: %s", version)
		}
		epochResult, err := call("getNextChallengeEpoch", dataSetID.Big())
		status := CleanupPieceDataSetCleanup
		if err != nil {
			notFound := r.abi.Errors["DataSetNotFound"].ID
			if data, ok := ethclient.RevertErrorData(err); !ok || !bytes.Equal(data, notFound[:4]) {
				return state, fmt.Errorf("reading cleanup data set phase: %w", err)
			}
			status = CleanupPieceRemoved
		} else {
			epoch, ok := epochResult[0].(*big.Int)
			// A deleted data set retains its piece slots until batched cleanup finishes.
			if !ok || epoch.BitLen() != 256 || epoch.Cmp(new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))) != 0 {
				return state, fmt.Errorf("inconsistent cleanup data set phase: %v", epochResult[0])
			}
		}
		for _, id := range pieceIDs {
			state.Pieces[id.String()] = status
		}
		return state, nil
	}
	queueResult, err := call("getScheduledRemovals", dataSetID.Big())
	if err != nil {
		return state, fmt.Errorf("reading scheduled cleanup: %w", err)
	}
	queued, ok := queueResult[0].([]*big.Int)
	if !ok {
		return state, fmt.Errorf("invalid scheduled cleanup state")
	}
	state.QueueLength = len(queued)
	queue := make(map[string]struct{}, len(queued))
	for _, id := range queued {
		if id == nil {
			return state, fmt.Errorf("invalid scheduled cleanup piece")
		}
		queue[id.String()] = struct{}{}
	}
	for _, id := range pieceIDs {
		key := id.String()
		if _, observed := state.Pieces[key]; observed {
			continue
		}
		if _, found := queue[key]; found {
			state.Pieces[key] = CleanupPieceScheduled
			continue
		}
		pieceResult, err := call("pieceLive", dataSetID.Big(), id.Big())
		if err != nil {
			return state, fmt.Errorf("reading cleanup piece %s: %w", id, err)
		}
		live, ok := pieceResult[0].(bool)
		if !ok {
			return state, fmt.Errorf("invalid cleanup piece state")
		}
		state.Pieces[key] = CleanupPieceRemoved
		if live {
			state.Pieces[key] = CleanupPiecePresent
		}
	}
	return state, nil
}
