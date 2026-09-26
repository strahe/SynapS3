package synapse

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

type approvedCatalogChainStub struct {
	t       *testing.T
	abi     abi.ABI
	view    common.Address
	ids     []*big.Int
	calls   int
	partial bool
}

func (s *approvedCatalogChainStub) BlockNumber(context.Context) (uint64, error) { return 42, nil }

func (s *approvedCatalogChainStub) CallContract(_ context.Context, msg ethereum.CallMsg, block *big.Int) ([]byte, error) {
	s.t.Helper()
	if msg.To == nil || *msg.To != s.view || block == nil || block.Uint64() != 42 {
		s.t.Fatalf("approved list call address=%v block=%v", msg.To, block)
	}
	method, err := s.abi.MethodById(msg.Data[:4])
	if err != nil {
		s.t.Fatal(err)
	}
	s.calls++
	if method.Name == "getApprovedProvidersLength" {
		return method.Outputs.Pack(big.NewInt(int64(len(s.ids))))
	}
	args, err := method.Inputs.Unpack(msg.Data[4:])
	if err != nil {
		s.t.Fatal(err)
	}
	start := int(args[0].(*big.Int).Uint64())
	end := min(start+int(args[1].(*big.Int).Uint64()), len(s.ids))
	if s.partial && start > 0 {
		end = start
	}
	return method.Outputs.Pack(s.ids[start:end])
}

func TestApprovedProviderCatalogPinsPagesToOneBlock(t *testing.T) {
	contractABI, err := abi.JSON(strings.NewReader(approvedProviderViewABI))
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]*big.Int, 205)
	for i := range ids {
		ids[i] = big.NewInt(int64(i + 1))
	}
	view := common.HexToAddress("0x100")
	chain := &approvedCatalogChainStub{t: t, abi: contractABI, view: view, ids: ids}
	catalog, err := NewApprovedProviderCatalog(chain, view)
	if err != nil {
		t.Fatal(err)
	}
	got, err := catalog.ReadApprovedProviderIDs(t.Context())
	if err != nil || len(got) != 205 || got[0].String() != "1" || got[204].String() != "205" || chain.calls != 4 {
		t.Fatalf("approved snapshot: count=%d calls=%d err=%v", len(got), chain.calls, err)
	}
	chain.partial = true
	if _, err := catalog.ReadApprovedProviderIDs(t.Context()); err == nil {
		t.Fatal("incomplete page was accepted")
	}
}
