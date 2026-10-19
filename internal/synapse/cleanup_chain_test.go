package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
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
	version     string
	epoch       *big.Int
	errors      map[string]error
	raw         map[string][]byte
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
	if len(args) > 0 && args[0].(*big.Int).Cmp(big.NewInt(11)) != 0 {
		s.t.Fatalf("cleanup data set = %v", args[0])
	}
	s.calls++
	if err := s.errors[method.Name]; err != nil {
		return nil, err
	}
	if output, ok := s.raw[method.Name]; ok {
		return output, nil
	}
	if method.Name == "VERSION" {
		return method.Outputs.Pack(s.version)
	}
	if method.Name == "getNextChallengeEpoch" {
		return method.Outputs.Pack(s.epoch)
	}
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
	if err != nil || state.Status != CleanupPiecePresent || state.BlockNumber != 42 {
		t.Fatalf("piece 7 = %#v, err=%v", state, err)
	}
	state, err = reader.ObserveCleanupPiece(t.Context(), sdktypes.NewBigInt(11), sdktypes.NewBigInt(8))
	if err != nil || state.Status != CleanupPieceScheduled || state.BlockNumber != 42 {
		t.Fatalf("piece 8 = %#v, err=%v", state, err)
	}
	chain.live[7] = false
	chain.calls = 0
	state, err = reader.ObserveCleanupPiece(t.Context(), sdktypes.NewBigInt(11), sdktypes.NewBigInt(7))
	if err != nil || state.Status != CleanupPieceRemoved || chain.calls != 2 {
		t.Fatalf("removed piece = %#v, calls=%d, err=%v", state, chain.calls, err)
	}
}

func TestCleanupPieceReaderRequiresVerifiedInactiveDataSetEvidence(t *testing.T) {
	contractABI, err := abi.JSON(strings.NewReader(cleanupVerifierABI))
	if err != nil {
		t.Fatal(err)
	}
	address := common.HexToAddress("0x0000000000000000000000000000000000000001")
	notFound := contractABI.Errors["DataSetNotFound"].ID
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	for _, tt := range []struct {
		name    string
		version string
		epoch   *big.Int
		err     error
		raw     []byte
		want    CleanupPieceStatus
	}{
		{name: "batched cleanup", version: "3.4.0", epoch: maxUint256, want: CleanupPieceDataSetCleanup},
		{name: "fully removed", version: "3.4.0", err: fmt.Errorf("wrapped RPC: %w", cleanupRPCError{code: 3, data: hexutil.Encode(notFound[:4])}), want: CleanupPieceRemoved},
		{name: "batched cleanup 3.5.0", version: "3.5.0", epoch: maxUint256, want: CleanupPieceDataSetCleanup},
		{name: "fully removed 3.5.0", version: "3.5.0", err: fmt.Errorf("wrapped RPC: %w", cleanupRPCError{code: 3, data: hexutil.Encode(notFound[:4])}), want: CleanupPieceRemoved},
		{name: "unknown version", version: "3.6.0", epoch: maxUint256},
		{name: "termination epoch", version: "3.4.0", epoch: big.NewInt(100)},
		{name: "zero epoch", version: "3.4.0", epoch: new(big.Int)},
		{name: "RPC timeout", version: "3.4.0", err: context.DeadlineExceeded},
		{name: "text not found", version: "3.4.0", err: errors.New("execution reverted: DataSetNotFound()")},
		{name: "wrong selector", version: "3.4.0", err: cleanupRPCError{code: 3, data: "0x00000000"}},
		{name: "trailing revert data", version: "3.4.0", err: cleanupRPCError{code: 3, data: hexutil.Encode(append(notFound[:4:4], byte(0)))}},
		{name: "wrong RPC code", version: "3.4.0", err: cleanupRPCError{code: -32000, data: hexutil.Encode(notFound[:4])}},
		{name: "malformed RPC data", version: "3.4.0", err: cleanupRPCError{code: 3, data: "not hex"}},
		{name: "malformed epoch", version: "3.4.0", raw: []byte{1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			chain := &cleanupChainStub{t: t, abi: contractABI, address: address, version: tt.version, epoch: tt.epoch, errors: map[string]error{"getNextChallengeEpoch": tt.err}}
			if tt.raw != nil {
				chain.raw = map[string][]byte{"getNextChallengeEpoch": tt.raw}
			}
			reader, err := newCleanupPieceReader(chain, address)
			if err != nil {
				t.Fatal(err)
			}
			state, err := reader.ObserveCleanupPiece(t.Context(), sdktypes.NewBigInt(11), sdktypes.NewBigInt(7))
			if tt.want == "" {
				if err == nil || state.Status != "" {
					t.Fatalf("unconfirmed data set = %#v, err=%v", state, err)
				}
				return
			}
			if err != nil || state.Status != tt.want || state.BlockNumber != 42 || chain.calls != 3 {
				t.Fatalf("data set state = %#v, calls=%d, err=%v", state, chain.calls, err)
			}
		})
	}
}

type cleanupRPCError struct {
	code int
	data string
}

func (e cleanupRPCError) Error() string  { return "execution reverted" }
func (e cleanupRPCError) ErrorCode() int { return e.code }
func (e cleanupRPCError) ErrorData() any { return e.data }
