package synapse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/walletoperation"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/signer"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestWalletSubmissionAcceptsSDKPaymentTransactions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		kind          model.WalletOperationType
		needsApproval bool
		signature     string
		failureStage  string
	}{
		{name: "fund", kind: model.WalletOperationTypeFund, signature: "depositWithPermit(address,address,uint256,uint256,uint8,bytes32,bytes32)"},
		{name: "fund and approve", kind: model.WalletOperationTypeFund, needsApproval: true, signature: "depositWithPermitAndApproveOperator(address,address,uint256,uint256,uint8,bytes32,bytes32,address,uint256,uint256,uint256)"},
		{name: "approve", kind: model.WalletOperationTypeApprove, needsApproval: true, signature: "setOperatorApproval(address,address,bool,uint256,uint256,uint256)"},
		{name: "withdraw", kind: model.WalletOperationTypeWithdraw, signature: "withdraw(address,uint256)"},
		{name: "fund nonce unavailable", kind: model.WalletOperationTypeFund, failureStage: "nonce"},
		{name: "withdraw gas unavailable", kind: model.WalletOperationTypeWithdraw, failureStage: "gas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sg, err := signer.NewSecp256k1SignerFromBytes(common.FromHex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"))
			if err != nil {
				t.Fatal(err)
			}
			identity := walletoperation.NewExecutionIdentity(storage.ContextIdentity{
				Payer: sg.EVMAddress(), ChainID: sdktypes.ChainID(314159), RecordKeeper: common.HexToAddress("0x222"),
			}, common.HexToAddress("0x333"), common.HexToAddress("0x444"))
			backend := newWalletSDKBackend(t, *identity.USDFC)
			preflightErr := errors.New("transaction preparation unavailable")
			switch tc.failureStage {
			case "nonce":
				backend.nonceErr = preflightErr
			case "gas":
				backend.gasErr = preflightErr
			}
			service, err := payments.New(payments.Options{
				Backend: backend, Signer: sg, ChainID: identity.Identity.ChainID,
				FilPayAddress: *identity.Payments, WarmStorageAddress: identity.Identity.RecordKeeper, USDFCTokenAddress: *identity.USDFC,
				PermitReceiptWait: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			amount := big.NewInt(55)
			var result *sdktypes.WriteResult
			if tc.kind == model.WalletOperationTypeWithdraw {
				result, err = service.Withdraw(t.Context(), *identity.USDFC, amount, payments.WithSkipPrecheck())
			} else {
				if tc.kind == model.WalletOperationTypeApprove {
					amount.SetInt64(0)
				}
				result, err = service.Fund(t.Context(), amount, payments.WithFundNeedsFwssApproval(tc.needsApproval), payments.WithSkipPrecheck())
			}
			if tc.failureStage != "" {
				submission, submissionErr := WalletSubmissionFromWriteResult(result, err, tc.name)
				if result != nil || backend.sent != nil || !errors.Is(submissionErr, preflightErr) || !submission.NotSent || submission.TxHash != "" || len(submission.SignedTransaction) != 0 {
					t.Fatalf("SDK preparation result=%v sent=%v submission=%#v error=%v", result, backend.sent, submission, submissionErr)
				}
				return
			}
			if !errors.Is(err, backend.sendErr) || result != nil || backend.sent == nil {
				t.Fatalf("SDK broadcast result = %v, transaction = %v, error = %v", result, backend.sent, err)
			}
			submission, submissionErr := WalletSubmissionFromWriteResult(result, err, tc.name)
			if !errors.Is(submissionErr, backend.sendErr) {
				t.Fatalf("submission lost broadcast error: %v", submissionErr)
			}
			raw, err := backend.sent.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if submission.NotSent || submission.TxHash != backend.sent.Hash().Hex() || !bytes.Equal(submission.SignedTransaction, raw) {
				t.Fatal("submission does not retain the SDK's signed transaction")
			}
			selector := crypto.Keccak256([]byte(tc.signature))[:4]
			if data := backend.sent.Data(); len(data) < 4 || !bytes.Equal(data[:4], selector) {
				t.Fatalf("SDK selected an unexpected payment method: %x", data)
			}
			if err := ValidateWalletSubmission(identity, tc.kind, amount, submission); err != nil {
				t.Fatalf("SDK payment transaction rejected: %v", err)
			}
			if tc.kind == model.WalletOperationTypeFund {
				select {
				case <-backend.receiptChecked:
				case <-time.After(5 * time.Second):
					t.Fatal("SDK permit receipt watcher did not finish its receipt lookup")
				}
			}
		})
	}
}

type walletSDKBackend struct {
	token          common.Address
	replies        map[[4]byte][]byte
	sendErr        error
	nonceErr       error
	gasErr         error
	sent           *ethtypes.Transaction
	receiptChecked chan struct{}
}

func newWalletSDKBackend(t *testing.T, token common.Address) *walletSDKBackend {
	t.Helper()
	backend := &walletSDKBackend{
		token: token, replies: make(map[[4]byte][]byte), sendErr: errors.New("broadcast connection interrupted"), receiptChecked: make(chan struct{}),
	}
	for _, input := range []struct {
		signature string
		kind      string
		value     any
	}{
		{signature: "name()", kind: "string", value: "USD for Filecoin"},
		{signature: "version()", kind: "string", value: "1"},
		{signature: "nonces(address)", kind: "uint256", value: big.NewInt(3)},
	} {
		output, err := abi.NewType(input.kind, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		reply, err := (abi.Arguments{{Type: output}}).Pack(input.value)
		if err != nil {
			t.Fatal(err)
		}
		backend.replies[[4]byte(crypto.Keccak256([]byte(input.signature)))] = reply
	}
	return backend
}

func (b *walletSDKBackend) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	if call.To == nil || *call.To != b.token || len(call.Data) < 4 {
		return nil, errors.New("unexpected SDK contract call")
	}
	if reply, ok := b.replies[[4]byte(call.Data)]; ok {
		return reply, nil
	}
	return nil, fmt.Errorf("unexpected SDK read selector %x", call.Data[:4])
}

func (*walletSDKBackend) CodeAt(context.Context, common.Address, *big.Int) ([]byte, error) {
	return []byte{1}, nil
}

func (*walletSDKBackend) PendingCodeAt(context.Context, common.Address) ([]byte, error) {
	return []byte{1}, nil
}

func (*walletSDKBackend) HeaderByNumber(context.Context, *big.Int) (*ethtypes.Header, error) {
	return &ethtypes.Header{BaseFee: big.NewInt(1), Number: big.NewInt(1)}, nil
}

func (b *walletSDKBackend) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	return 7, b.nonceErr
}

func (*walletSDKBackend) SuggestGasPrice(context.Context) (*big.Int, error) {
	return big.NewInt(2), nil
}

func (*walletSDKBackend) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return big.NewInt(1), nil
}

func (b *walletSDKBackend) EstimateGas(context.Context, ethereum.CallMsg) (uint64, error) {
	return 100000, b.gasErr
}

func (b *walletSDKBackend) SendTransaction(_ context.Context, tx *ethtypes.Transaction) error {
	b.sent = tx
	return b.sendErr
}

func (*walletSDKBackend) FilterLogs(context.Context, ethereum.FilterQuery) ([]ethtypes.Log, error) {
	return nil, errors.New("unexpected SDK log query")
}

func (*walletSDKBackend) SubscribeFilterLogs(context.Context, ethereum.FilterQuery, chan<- ethtypes.Log) (ethereum.Subscription, error) {
	return nil, errors.New("unexpected SDK log subscription")
}

func (*walletSDKBackend) BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error) {
	return nil, errors.New("unexpected SDK native balance query")
}

func (b *walletSDKBackend) TransactionReceipt(_ context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
	close(b.receiptChecked)
	return &ethtypes.Receipt{TxHash: hash, Status: ethtypes.ReceiptStatusSuccessful}, nil
}

func (*walletSDKBackend) BlockNumber(context.Context) (uint64, error) {
	return 1, nil
}
