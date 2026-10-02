package synapse

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

type commitNonceChainStub struct {
	t        *testing.T
	abi      abi.ABI
	view     common.Address
	verifier common.Address
	nonce    *big.Int
	pieceCID []byte
}

func (s *commitNonceChainStub) CallContract(_ context.Context, msg ethereum.CallMsg, block *big.Int) ([]byte, error) {
	s.t.Helper()
	method, err := s.abi.MethodById(msg.Data[:4])
	if err != nil {
		s.t.Fatalf("commit nonce method: %v", err)
	}
	args, err := method.Inputs.Unpack(msg.Data[4:])
	if err != nil {
		s.t.Fatalf("commit nonce arguments: %v", err)
	}
	if block != nil {
		s.t.Fatalf("%s read block %v, want the latest", method.Name, block)
	}
	switch method.Name {
	case "clientNonces":
		if *msg.To != s.view || args[0].(common.Address) != common.HexToAddress("0xa1") || args[1].(*big.Int).Cmp(big.NewInt(7)) != 0 {
			s.t.Fatalf("clientNonces call to %v with %v", msg.To, args)
		}
		return method.Outputs.Pack(s.nonce)
	default:
		if *msg.To != s.verifier || args[0].(*big.Int).Cmp(big.NewInt(39911)) != 0 || args[1].(*big.Int).Cmp(big.NewInt(199)) != 0 {
			s.t.Fatalf("getPieceCid call to %v with %v", msg.To, args)
		}
		return method.Outputs.Pack(struct{ Data []byte }{Data: s.pieceCID})
	}
}

// FWSS records (next piece ID << 128) | data set ID for a consumed nonce, and
// PDPVerifier names the piece held at that ID.
func TestCommitNonceReaderDecodesChainRecords(t *testing.T) {
	contractABI, err := abi.JSON(strings.NewReader(commitNonceABI))
	if err != nil {
		t.Fatal(err)
	}
	pieceCID, err := cid.Parse("bafkqaaa")
	if err != nil {
		t.Fatal(err)
	}
	chain := &commitNonceChainStub{
		t: t, abi: contractABI,
		view: common.HexToAddress("0x1b"), verifier: common.HexToAddress("0x2c"),
		nonce: new(big.Int).Or(new(big.Int).Lsh(big.NewInt(200), 128), big.NewInt(39911)),
	}
	reader, err := NewCommitNonceReader(chain, chain.view, chain.verifier, common.HexToAddress("0xa1"))
	if err != nil {
		t.Fatal(err)
	}

	state, err := reader.ClientNonce(t.Context(), sdktypes.NewBigInt(7))
	if err != nil || !state.Consumed || !state.DataSetID.Equal(sdktypes.NewBigInt(39911)) || !state.NextPieceID.Equal(sdktypes.NewBigInt(200)) {
		t.Fatalf("client nonce = %#v err=%v, want data set 39911 next piece 200", state, err)
	}
	ids, err := state.PieceIDs(1)
	if err != nil || len(ids) != 1 || !ids[0].Equal(sdktypes.NewBigInt(199)) {
		t.Fatalf("piece IDs = %v err=%v, want [199]", ids, err)
	}
	if _, err := (ClientNonceState{Consumed: true, NextPieceID: sdktypes.NewBigInt(1)}).PieceIDs(2); err == nil {
		t.Fatal("a nonce whose next piece ID cannot cover its pieces was accepted")
	}

	chain.pieceCID = pieceCID.Bytes()
	got, err := reader.PieceCIDAt(t.Context(), sdktypes.NewBigInt(39911), sdktypes.NewBigInt(199))
	if err != nil || !got.Equals(pieceCID) {
		t.Fatalf("piece CID = %v err=%v, want %v", got, err, pieceCID)
	}
	chain.pieceCID = nil
	if got, err := reader.PieceCIDAt(t.Context(), sdktypes.NewBigInt(39911), sdktypes.NewBigInt(199)); err != nil || got.Defined() {
		t.Fatalf("empty piece slot = %v err=%v, want undefined", got, err)
	}

	chain.nonce = new(big.Int)
	if state, err := reader.ClientNonce(t.Context(), sdktypes.NewBigInt(7)); err != nil || state.Consumed {
		t.Fatalf("unused nonce = %#v err=%v", state, err)
	}
}

func TestClassifyCommitRejection(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		want CommitRejection
	}{
		{name: "piece dropped", err: &pdp.HTTPError{StatusCode: http.StatusBadRequest}, want: CommitRejectedByProvider},
		{name: "rate limited", err: &ProviderUnavailableError{Cause: &pdp.HTTPError{StatusCode: http.StatusTooManyRequests}}, want: CommitRejectedByProvider},
		{name: "data set unknown", err: &pdp.HTTPError{StatusCode: http.StatusNotFound}, want: CommitRejectedDataSetUnavailable},
		{name: "data set terminated", err: &pdp.HTTPError{StatusCode: http.StatusConflict}, want: CommitRejectedDataSetUnavailable},
		{name: "server error", err: &pdp.HTTPError{StatusCode: http.StatusInternalServerError}, want: CommitNotRejected},
		{name: "timeout", err: context.DeadlineExceeded, want: CommitNotRejected},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyCommitRejection(tt.err); got != tt.want {
				t.Fatalf("ClassifyCommitRejection(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

type dataSetWriteStub struct {
	info        *warmstorage.DataSetInfo
	readErr     error
	validateErr error
}

func (s dataSetWriteStub) GetDataSet(context.Context, sdktypes.BigInt) (*warmstorage.DataSetInfo, error) {
	return s.info, s.readErr
}

func (s dataSetWriteStub) ValidateDataSet(context.Context, sdktypes.BigInt) error {
	return s.validateErr
}

// The write check refuses a data set that has ended the way SubmitCommit
// would, and reports any other failure as an error to retry.
func TestCheckDataSetWritable(t *testing.T) {
	rpcErr := errors.New("chain RPC unavailable")
	for _, tt := range []struct {
		name    string
		reader  dataSetWriteStub
		refused bool
		wantErr error
	}{
		{name: "writable", reader: dataSetWriteStub{info: &warmstorage.DataSetInfo{}}},
		{name: "payment ended", reader: dataSetWriteStub{info: &warmstorage.DataSetInfo{PDPEndEpoch: 42}}, refused: true},
		{name: "service gone", reader: dataSetWriteStub{readErr: warmstorage.ErrNotFound}, refused: true},
		{name: "chain unreadable", reader: dataSetWriteStub{readErr: rpcErr}, wantErr: rpcErr},
		{name: "validation failed", reader: dataSetWriteStub{info: &warmstorage.DataSetInfo{}, validateErr: rpcErr}, wantErr: rpcErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := checkDataSetWritable(t.Context(), tt.reader, sdktypes.NewBigInt(1001))
			refused := IsDataSetWriteBlocked(err) || IsDataSetServiceEnded(err) || errors.Is(err, storage.ErrDataSetUnavailable)
			if refused != tt.refused || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) || (!tt.refused && tt.wantErr == nil && err != nil) {
				t.Fatalf("checkDataSetWritable = %v, want refused=%v err=%v", err, tt.refused, tt.wantErr)
			}
		})
	}
}
