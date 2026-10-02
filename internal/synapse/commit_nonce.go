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
	"github.com/ipfs/go-cid"
	sdktypes "github.com/strahe/synapse-go/types"
)

// clientNonces lives on the FWSS view contract and getPieceCid on PDPVerifier;
// one ABI describes both because each call names its own contract address.
const commitNonceABI = `[{"type":"function","name":"clientNonces","inputs":[{"name":"payer","type":"address"},{"name":"nonce","type":"uint256"}],"outputs":[{"type":"uint256"}],"stateMutability":"view"},{"type":"function","name":"getPieceCid","inputs":[{"name":"setId","type":"uint256"},{"name":"pieceId","type":"uint256"}],"outputs":[{"type":"tuple","components":[{"name":"data","type":"bytes"}]}],"stateMutability":"view"}]`

// ClientNonceState is what FWSS recorded for one add-pieces nonce. Once the
// pieces it signed are added, the nonce stays consumed and names the data set
// they joined and the piece ID that data set will assign next.
type ClientNonceState struct {
	Consumed    bool
	DataSetID   sdktypes.BigInt
	NextPieceID sdktypes.BigInt
}

// PieceIDs returns the IDs a consumed nonce assigned to count pieces. FWSS
// numbers the pieces of one request consecutively, ending at NextPieceID-1.
func (s ClientNonceState) PieceIDs(count int) ([]sdktypes.BigInt, error) {
	if !s.Consumed || count <= 0 {
		return nil, errors.New("client nonce assigned no pieces")
	}
	next := s.NextPieceID.Big()
	if next.Cmp(big.NewInt(int64(count))) < 0 {
		return nil, fmt.Errorf("client nonce next piece ID %s cannot cover %d pieces", next, count)
	}
	first := next.Sub(next, big.NewInt(int64(count)))
	ids := make([]sdktypes.BigInt, count)
	for i := range ids {
		id, err := sdktypes.BigIntFromBig(new(big.Int).Add(first, big.NewInt(int64(i))))
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

// CommitNonceReader reads the chain record that settles whether a signed
// add-pieces request was added, without scanning blocks or event logs.
type CommitNonceReader interface {
	// ClientNonce reports what FWSS recorded for a nonce this node's payer
	// signed.
	ClientNonce(ctx context.Context, nonce sdktypes.BigInt) (ClientNonceState, error)
	// PieceCIDAt returns the piece CID a data set holds at a piece ID, or
	// cid.Undef when none is there.
	PieceCIDAt(ctx context.Context, dataSetID, pieceID sdktypes.BigInt) (cid.Cid, error)
}

type viewCaller interface {
	CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error)
}

type commitNonceReader struct {
	chain    viewCaller
	view     common.Address
	verifier common.Address
	payer    common.Address
	abi      abi.ABI
}

// NewCommitNonceReader reads nonces payer signed from the FWSS view contract
// and piece identities from PDPVerifier.
func NewCommitNonceReader(chain viewCaller, view, verifier, payer common.Address) (CommitNonceReader, error) {
	if chain == nil || view == (common.Address{}) || verifier == (common.Address{}) || payer == (common.Address{}) {
		return nil, errors.New("commit nonce chain, contracts, and payer are required")
	}
	contractABI, err := abi.JSON(strings.NewReader(commitNonceABI))
	if err != nil {
		return nil, fmt.Errorf("parsing commit nonce ABI: %w", err)
	}
	return &commitNonceReader{chain: chain, view: view, verifier: verifier, payer: payer, abi: contractABI}, nil
}

func (r *commitNonceReader) call(ctx context.Context, contract common.Address, method string, args ...any) ([]any, error) {
	input, err := r.abi.Pack(method, args...)
	if err != nil {
		return nil, err
	}
	output, err := r.chain.CallContract(ctx, ethereum.CallMsg{To: &contract, Data: input}, nil)
	if err != nil {
		return nil, err
	}
	values, err := r.abi.Unpack(method, output)
	if err != nil {
		return nil, err
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("%s returned %d values", method, len(values))
	}
	return values, nil
}

func (r *commitNonceReader) ClientNonce(ctx context.Context, nonce sdktypes.BigInt) (ClientNonceState, error) {
	values, err := r.call(ctx, r.view, "clientNonces", r.payer, nonce.Big())
	if err != nil {
		return ClientNonceState{}, fmt.Errorf("reading client nonce: %w", err)
	}
	raw, ok := values[0].(*big.Int)
	if !ok || raw == nil {
		return ClientNonceState{}, errors.New("reading client nonce: invalid value")
	}
	if raw.Sign() == 0 {
		return ClientNonceState{}, nil
	}
	// FWSS stores (next piece ID << 128) | data set ID.
	mask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
	dataSetID, err := sdktypes.BigIntFromBig(new(big.Int).And(raw, mask))
	if err != nil {
		return ClientNonceState{}, fmt.Errorf("reading client nonce data set: %w", err)
	}
	nextPieceID, err := sdktypes.BigIntFromBig(new(big.Int).Rsh(raw, 128))
	if err != nil {
		return ClientNonceState{}, fmt.Errorf("reading client nonce next piece: %w", err)
	}
	return ClientNonceState{Consumed: true, DataSetID: dataSetID, NextPieceID: nextPieceID}, nil
}

func (r *commitNonceReader) PieceCIDAt(ctx context.Context, dataSetID, pieceID sdktypes.BigInt) (cid.Cid, error) {
	values, err := r.call(ctx, r.verifier, "getPieceCid", dataSetID.Big(), pieceID.Big())
	if err != nil {
		return cid.Undef, fmt.Errorf("reading piece CID: %w", err)
	}
	encoded, ok := abi.ConvertType(values[0], new(struct{ Data []byte })).(*struct{ Data []byte })
	if !ok {
		return cid.Undef, errors.New("reading piece CID: invalid value")
	}
	if len(encoded.Data) == 0 {
		return cid.Undef, nil
	}
	pieceCID, err := cid.Cast(encoded.Data)
	if err != nil {
		return cid.Undef, fmt.Errorf("reading piece CID: %w", err)
	}
	return pieceCID, nil
}
