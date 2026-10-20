package synapse

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/walletoperation"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type walletReplayRequest struct {
	identity walletoperation.ExecutionIdentity
	kind     model.WalletOperationType
	amount   *big.Int
	method   string
	args     []any
	key      *ecdsa.PrivateKey
	tx       ethtypes.DynamicFeeTx
}

func newWalletReplayRequest(t *testing.T, method string) walletReplayRequest {
	t.Helper()
	key, err := crypto.HexToECDSA("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	identity := walletoperation.NewExecutionIdentity(storage.ContextIdentity{
		Payer: crypto.PubkeyToAddress(key.PublicKey), ChainID: sdktypes.ChainID(314159), RecordKeeper: common.HexToAddress("0x222"),
	}, common.HexToAddress("0x333"), common.HexToAddress("0x444"))
	r := walletReplayRequest{
		identity: identity, kind: model.WalletOperationTypeFund, amount: big.NewInt(55), method: method, key: key,
		tx: ethtypes.DynamicFeeTx{ChainID: identity.Identity.ChainID.BigInt(), Nonce: 7, GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2), Gas: 100000, To: identity.Payments, Value: new(big.Int)},
	}
	deadline := big.NewInt(time.Now().Add(time.Hour).Unix())
	switch method {
	case "depositWithPermit":
		r.args = []any{*identity.USDFC, identity.Identity.Payer, r.amount, deadline, uint8(27), [32]byte{}, [32]byte{}}
	case "depositWithPermitAndApproveOperator":
		r.args = []any{*identity.USDFC, identity.Identity.Payer, r.amount, deadline, uint8(27), [32]byte{}, [32]byte{}, identity.Identity.RecordKeeper, big.NewInt(100), big.NewInt(100), big.NewInt(30)}
	case "setOperatorApproval":
		r.kind, r.amount = model.WalletOperationTypeApprove, new(big.Int)
		r.args = []any{*identity.USDFC, identity.Identity.RecordKeeper, true, big.NewInt(100), big.NewInt(100), big.NewInt(30)}
	case "withdraw":
		r.kind = model.WalletOperationTypeWithdraw
		r.args = []any{*identity.USDFC, r.amount}
	default:
		t.Fatalf("unsupported test method %s", method)
	}
	return r
}

func (r walletReplayRequest) submission(t *testing.T) WalletSubmission {
	t.Helper()
	data, err := walletPaymentABI.Pack(r.method, r.args...)
	if err != nil {
		t.Fatal(err)
	}
	r.tx.Data = data
	tx, err := ethtypes.SignNewTx(r.key, ethtypes.LatestSignerForChainID(r.tx.ChainID), &r.tx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return WalletSubmission{TxHash: tx.Hash().Hex(), SignedTransaction: raw}
}

func TestValidateWalletSubmissionChecksSignedIntent(t *testing.T) {
	for _, method := range []string{"depositWithPermit", "depositWithPermitAndApproveOperator", "setOperatorApproval", "withdraw"} {
		t.Run(method, func(t *testing.T) {
			r := newWalletReplayRequest(t, method)
			if err := ValidateWalletSubmission(r.identity, r.kind, r.amount, r.submission(t)); err != nil {
				t.Fatalf("valid intent rejected: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*walletReplayRequest)
	}{
		{name: "token", mutate: func(r *walletReplayRequest) { r.args[0] = common.HexToAddress("0x555") }},
		{name: "recipient", mutate: func(r *walletReplayRequest) { r.args[1] = common.HexToAddress("0x555") }},
		{name: "amount", mutate: func(r *walletReplayRequest) { r.args[2] = big.NewInt(56) }},
		{name: "operator", mutate: func(r *walletReplayRequest) { r.args[7] = common.HexToAddress("0x555") }},
		{name: "chain", mutate: func(r *walletReplayRequest) { r.tx.ChainID = big.NewInt(1) }},
		{name: "signer", mutate: func(r *walletReplayRequest) {
			r.key, _ = crypto.HexToECDSA("1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		}},
		{name: "target", mutate: func(r *walletReplayRequest) { r.tx.To = new(common.HexToAddress("0x555")) }},
		{name: "contract creation", mutate: func(r *walletReplayRequest) { r.tx.To = nil }},
		{name: "native value", mutate: func(r *walletReplayRequest) { r.tx.Value = big.NewInt(1) }},
		{name: "operation", mutate: func(r *walletReplayRequest) { r.kind = model.WalletOperationTypeWithdraw }},
		{name: "overflow", mutate: func(r *walletReplayRequest) { r.amount = new(big.Int).Lsh(big.NewInt(1), 256) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newWalletReplayRequest(t, "depositWithPermitAndApproveOperator")
			tc.mutate(&r)
			if err := ValidateWalletSubmission(r.identity, r.kind, r.amount, r.submission(t)); !errors.Is(err, ErrWalletReplayInvalidEvidence) {
				t.Fatalf("intent mismatch error = %v", err)
			}
		})
	}
	for _, method := range []string{"setOperatorApproval", "withdraw"} {
		t.Run(method+" mismatched intent", func(t *testing.T) {
			r := newWalletReplayRequest(t, method)
			if method == "setOperatorApproval" {
				r.args[2] = false
			} else {
				r.args[1] = big.NewInt(56)
			}
			if err := ValidateWalletSubmission(r.identity, r.kind, r.amount, r.submission(t)); !errors.Is(err, ErrWalletReplayInvalidEvidence) {
				t.Fatalf("intent mismatch error = %v", err)
			}
		})
	}
}

func TestValidateWalletSubmissionRequiresReliableHashAndCanonicalBytes(t *testing.T) {
	r := newWalletReplayRequest(t, "withdraw")
	submission := r.submission(t)
	data, err := walletPaymentABI.Pack(r.method, r.args...)
	if err != nil {
		t.Fatal(err)
	}
	setCode, err := ethtypes.SignTx(ethtypes.NewTx(&ethtypes.SetCodeTx{Nonce: r.tx.Nonce, To: *r.identity.Payments, Data: data}), ethtypes.LatestSignerForChainID(r.tx.ChainID), r.key)
	if err != nil {
		t.Fatal(err)
	}
	setCodeRaw, err := setCode.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		identity   walletoperation.ExecutionIdentity
		submission WalletSubmission
		wantErr    bool
	}{
		{name: "hash only", identity: r.identity, submission: WalletSubmission{TxHash: submission.TxHash}},
		{name: "no identity", submission: submission, wantErr: true},
		{name: "zero hash", identity: r.identity, submission: WalletSubmission{TxHash: (common.Hash{}).Hex()}, wantErr: true},
		{name: "short hash", identity: r.identity, submission: WalletSubmission{TxHash: "0x123"}, wantErr: true},
		{name: "invalid hex", identity: r.identity, submission: WalletSubmission{TxHash: "0xzz" + submission.TxHash[4:]}, wantErr: true},
		{name: "hash conflict", identity: r.identity, submission: WalletSubmission{TxHash: common.HexToHash("0x999").Hex(), SignedTransaction: submission.SignedTransaction}, wantErr: true},
		{name: "invalid bytes", identity: r.identity, submission: WalletSubmission{TxHash: submission.TxHash, SignedTransaction: []byte{1}}, wantErr: true},
		{name: "trailing bytes", identity: r.identity, submission: WalletSubmission{TxHash: submission.TxHash, SignedTransaction: append(bytes.Clone(submission.SignedTransaction), 0)}, wantErr: true},
		{name: "authorization transaction", identity: r.identity, submission: WalletSubmission{TxHash: setCode.Hash().Hex(), SignedTransaction: setCodeRaw}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateWalletSubmission(tc.identity, r.kind, r.amount, tc.submission)
			if (err != nil) != tc.wantErr || (err != nil && !errors.Is(err, ErrWalletReplayInvalidEvidence)) {
				t.Fatalf("validation error = %v, want error %t", err, tc.wantErr)
			}
		})
	}
}

type walletReplayRPC struct {
	chainID     *big.Int
	receipt     *ethtypes.Receipt
	receiptErr  error
	receiptWait bool
	visible     *ethtypes.Transaction
	visibleErr  error
	latest      uint64
	pending     uint64
	latestErr   error
	pendingErr  error
	chainErr    error
	sendErr     error
	readCalls   int
	sent        [][]byte
}

func (c *walletReplayRPC) ChainID(context.Context) (*big.Int, error) {
	c.readCalls++
	return c.chainID, c.chainErr
}

func (c *walletReplayRPC) TransactionReceipt(ctx context.Context, _ common.Hash) (*ethtypes.Receipt, error) {
	c.readCalls++
	if c.receiptWait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.receipt, c.receiptErr
}

func (c *walletReplayRPC) TransactionByHash(context.Context, common.Hash) (*ethtypes.Transaction, bool, error) {
	c.readCalls++
	return c.visible, true, c.visibleErr
}

func (c *walletReplayRPC) NonceAt(context.Context, common.Address, *big.Int) (uint64, error) {
	c.readCalls++
	return c.latest, c.latestErr
}

func (c *walletReplayRPC) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	c.readCalls++
	return c.pending, c.pendingErr
}

func (c *walletReplayRPC) SendTransaction(_ context.Context, tx *ethtypes.Transaction) error {
	raw, err := tx.MarshalBinary()
	if err != nil {
		return err
	}
	c.sent = append(c.sent, raw)
	return c.sendErr
}

func TestWalletReplayGuardsAndPreservesSignedBytes(t *testing.T) {
	rpcFailure := errors.New("RPC unavailable")
	for _, tc := range []struct {
		name        string
		setup       func(*walletReplayRequest, *walletReplayRPC)
		wantErr     error
		wantCause   error
		wantSend    bool
		wantNoReads bool
	}{
		{name: "send original", wantSend: true},
		{name: "ambiguous send", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.sendErr = rpcFailure }, wantErr: ErrWalletReplayBroadcast, wantCause: rpcFailure, wantSend: true},
		{name: "send echoes signed bytes", setup: func(r *walletReplayRequest, c *walletReplayRPC) {
			c.sendErr = fmt.Errorf("RPC rejected %x", r.submission(t).SignedTransaction)
		}, wantErr: ErrWalletReplayBroadcast, wantSend: true},
		{name: "RPC data echoes signed bytes", setup: func(r *walletReplayRequest, c *walletReplayRPC) {
			c.sendErr = cleanupRPCError{code: 3, data: "0x" + hex.EncodeToString(r.submission(t).SignedTransaction)}
		}, wantErr: ErrWalletReplayBroadcast, wantSend: true},
		{name: "receipt", setup: func(r *walletReplayRequest, c *walletReplayRPC) {
			c.receiptErr = nil
			c.receipt = &ethtypes.Receipt{TxHash: common.HexToHash(r.submission(t).TxHash)}
		}, wantErr: ErrWalletReplaySkipped},
		{name: "visible", setup: func(r *walletReplayRequest, c *walletReplayRPC) {
			c.visibleErr = nil
			c.visible = new(ethtypes.Transaction)
			_ = c.visible.UnmarshalBinary(r.submission(t).SignedTransaction)
		}, wantErr: ErrWalletReplaySkipped},
		{name: "expired permit", setup: func(r *walletReplayRequest, _ *walletReplayRPC) { r.args[3] = big.NewInt(1) }, wantErr: ErrWalletReplaySkipped},
		{name: "latest nonce", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.latest = 8 }, wantErr: ErrWalletReplaySkipped},
		{name: "pending nonce", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.pending = 8 }, wantErr: ErrWalletReplaySkipped},
		{name: "chain error", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.chainErr = rpcFailure }, wantErr: rpcFailure},
		{name: "receipt error", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.receiptErr = rpcFailure }, wantErr: rpcFailure},
		{name: "receipt echoes signed bytes", setup: func(r *walletReplayRequest, c *walletReplayRPC) {
			c.receiptErr = fmt.Errorf("RPC rejected %x: %w", r.submission(t).SignedTransaction, rpcFailure)
		}, wantErr: rpcFailure},
		{name: "visibility error", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.visibleErr = rpcFailure }, wantErr: rpcFailure},
		{name: "latest nonce error", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.latestErr = rpcFailure }, wantErr: rpcFailure},
		{name: "pending nonce error", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.pendingErr = rpcFailure }, wantErr: rpcFailure},
		{name: "RPC wrong chain", setup: func(_ *walletReplayRequest, c *walletReplayRPC) { c.chainID = big.NewInt(1) }, wantErr: ErrWalletReplayInvalidEvidence},
		{name: "wrong receipt hash", setup: func(_ *walletReplayRequest, c *walletReplayRPC) {
			c.receiptErr = nil
			c.receipt = &ethtypes.Receipt{TxHash: common.HexToHash("0x999")}
		}, wantErr: ErrWalletReplayInvalidEvidence},
		{name: "invalid intent before RPC", setup: func(r *walletReplayRequest, _ *walletReplayRPC) { r.args[0] = common.HexToAddress("0x555") }, wantErr: ErrWalletReplayInvalidEvidence, wantNoReads: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newWalletReplayRequest(t, "depositWithPermit")
			client := &walletReplayRPC{chainID: r.tx.ChainID, receiptErr: ethereum.NotFound, visibleErr: ethereum.NotFound, latest: 7, pending: 7}
			if tc.setup != nil {
				tc.setup(&r, client)
			}
			submission := r.submission(t)
			op := &walletOperator{identity: r.identity, replay: client}
			err := op.RebroadcastSignedTransaction(t.Context(), r.kind, r.amount, submission, time.Second)
			if (tc.wantErr == nil && err != nil) || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) || (tc.wantCause != nil && !errors.Is(err, tc.wantCause)) {
				t.Fatalf("replay error = %v, want %v", err, tc.wantErr)
			}
			if (len(client.sent) > 0) != tc.wantSend {
				t.Fatalf("sent %d transactions, want send %t", len(client.sent), tc.wantSend)
			}
			if tc.wantSend && !bytes.Equal(client.sent[0], submission.SignedTransaction) {
				t.Fatal("replay changed signed transaction bytes")
			}
			if err != nil && strings.Contains(strings.ToLower(err.Error()), hex.EncodeToString(submission.SignedTransaction)) {
				t.Fatal("error exposes signed transaction bytes")
			}
			if strings.Contains(strings.ToLower(errtext.Format(err)), hex.EncodeToString(submission.SignedTransaction)) {
				t.Fatal("formatted RPC diagnostic exposes signed transaction bytes")
			}
			if tc.wantNoReads && client.readCalls != 0 {
				t.Fatalf("read RPC %d times with invalid evidence", client.readCalls)
			}
		})
	}
}

func TestWalletReplayBoundsPreflightReads(t *testing.T) {
	r := newWalletReplayRequest(t, "withdraw")
	client := &walletReplayRPC{chainID: r.tx.ChainID, receiptWait: true}
	op := &walletOperator{identity: r.identity, replay: client}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := op.RebroadcastSignedTransaction(ctx, r.kind, r.amount, r.submission(t), time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrWalletReplayBroadcast) || len(client.sent) != 0 || ctx.Err() != nil {
		t.Fatalf("bounded preflight = %v, sent %d, parent error %v", err, len(client.sent), ctx.Err())
	}
}
