package synapse

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	sdktypes "github.com/strahe/synapse-go/types"
)

type cleanupChainStub struct {
	t           *testing.T
	abi         abi.ABI
	address     common.Address
	dataSetLive bool
	live        map[int64]bool
	queue       []*big.Int
	calls       int
}

func (s *cleanupChainStub) BlockNumber(context.Context) (uint64, error) { return 42, nil }

func (s *cleanupChainStub) CallContract(_ context.Context, msg ethereum.CallMsg, block *big.Int) ([]byte, error) {
	s.t.Helper()
	if msg.To == nil || *msg.To != s.address || block.Cmp(big.NewInt(42)) != 0 {
		s.t.Fatalf("cleanup call address=%v block=%v", msg.To, block)
	}
	method, err := s.abi.MethodById(msg.Data[:4])
	if err != nil {
		s.t.Fatalf("cleanup method: %v", err)
	}
	args, err := method.Inputs.Unpack(msg.Data[4:])
	if err != nil {
		s.t.Fatalf("cleanup arguments: %v", err)
	}
	if args[0].(*big.Int).Cmp(big.NewInt(11)) != 0 {
		s.t.Fatalf("cleanup data set = %v", args[0])
	}
	s.calls++
	if method.Name == "dataSetLive" {
		return method.Outputs.Pack(s.dataSetLive)
	}
	if method.Name == "pieceLive" {
		return method.Outputs.Pack(s.live[args[1].(*big.Int).Int64()])
	}
	return method.Outputs.Pack(s.queue)
}

func TestCleanupPieceReaderUsesExactPieceAndOneBlock(t *testing.T) {
	contractABI, err := abi.JSON(strings.NewReader(cleanupVerifierABI))
	if err != nil {
		t.Fatal(err)
	}
	address := common.HexToAddress("0x0000000000000000000000000000000000000001")
	chain := &cleanupChainStub{t: t, abi: contractABI, address: address, dataSetLive: true, live: map[int64]bool{7: true, 8: true}, queue: []*big.Int{big.NewInt(8)}}
	reader, err := newCleanupPieceReader(chain, address)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reader.ObserveCleanupPiece(t.Context(), sdktypes.NewBigInt(11), sdktypes.NewBigInt(7))
	if err != nil || !state.Live || state.Queued || state.BlockNumber != 42 {
		t.Fatalf("piece 7 = %#v, err=%v", state, err)
	}
	state, err = reader.ObserveCleanupPiece(t.Context(), sdktypes.NewBigInt(11), sdktypes.NewBigInt(8))
	if err != nil || !state.Live || !state.Queued || state.BlockNumber != 42 {
		t.Fatalf("piece 8 = %#v, err=%v", state, err)
	}
	chain.live[7] = false
	chain.calls = 0
	state, err = reader.ObserveCleanupPiece(t.Context(), sdktypes.NewBigInt(11), sdktypes.NewBigInt(7))
	if err != nil || state.Live || chain.calls != 2 {
		t.Fatalf("removed piece = %#v, calls=%d, err=%v", state, chain.calls, err)
	}
}

func TestCleanupPieceReaderDoesNotInferRemovalFromInactiveDataSet(t *testing.T) {
	contractABI, err := abi.JSON(strings.NewReader(cleanupVerifierABI))
	if err != nil {
		t.Fatal(err)
	}
	address := common.HexToAddress("0x0000000000000000000000000000000000000001")
	chain := &cleanupChainStub{t: t, abi: contractABI, address: address, live: map[int64]bool{7: false}}
	reader, err := newCleanupPieceReader(chain, address)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ObserveCleanupPiece(t.Context(), sdktypes.NewBigInt(11), sdktypes.NewBigInt(7)); err == nil || chain.calls != 1 {
		t.Fatalf("inactive data set observation: calls=%d, err=%v", chain.calls, err)
	}
}
