package worker

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/payments"
)

func TestWalletOperationRunner_SubmitsAndConfirmsPendingOperation(t *testing.T) {
	tests := []struct {
		name   string
		opType model.WalletOperationType
		amount string
	}{
		{name: "fund", opType: model.WalletOperationTypeFund, amount: "100"},
		{name: "withdraw", opType: model.WalletOperationTypeWithdraw, amount: "50"},
		{name: "approve", opType: model.WalletOperationTypeApprove, amount: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testutil.NewTestDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
				Type:            tt.opType,
				ClientRequestID: string(tt.opType) + "-1",
				Amount:          tt.amount,
			})
			if err != nil {
				t.Fatalf("CreateOrGet: %v", err)
			}

			txHash := common.HexToHash("0xabc")
			operator := &fakeWalletOperator{fundHash: txHash.Hex(), withdrawHash: txHash.Hex(), approveHash: txHash.Hex()}
			receipts := &fakeWalletReceiptChecker{receipts: map[common.Hash]*ethtypes.Receipt{
				txHash: {Status: ethtypes.ReceiptStatusSuccessful},
			}}
			publisher := &fakeWalletEventPublisher{}
			runner := NewWalletOperationRunner(repos, operator, receipts, time.Millisecond, nil, WithWalletOperationEventPublisher(publisher))

			runner.runOnce(ctx)

			got, err := repos.WalletOperations.GetByID(ctx, op.ID)
			if err != nil {
				t.Fatalf("GetByID: %v", err)
			}
			if got.Status != model.WalletOperationStatusConfirmed {
				t.Fatalf("status = %q, want confirmed", got.Status)
			}
			if got.TxHash == nil || *got.TxHash != txHash.Hex() {
				t.Fatalf("tx_hash = %v, want %s", got.TxHash, txHash.Hex())
			}
			switch tt.opType {
			case model.WalletOperationTypeFund:
				if operator.fundAmount == nil || operator.fundAmount.String() != tt.amount {
					t.Fatalf("fund amount = %v, want %s", operator.fundAmount, tt.amount)
				}
				if operator.withdrawAmount != nil {
					t.Fatalf("withdraw amount = %s, want no withdraw", operator.withdrawAmount)
				}
				if operator.approveCalled {
					t.Fatal("ApproveFWSS was called, want no approve")
				}
			case model.WalletOperationTypeWithdraw:
				if operator.withdrawAmount == nil || operator.withdrawAmount.String() != tt.amount {
					t.Fatalf("withdraw amount = %v, want %s", operator.withdrawAmount, tt.amount)
				}
				if operator.fundAmount != nil {
					t.Fatalf("fund amount = %s, want no fund", operator.fundAmount)
				}
				if operator.approveCalled {
					t.Fatal("ApproveFWSS was called, want no approve")
				}
			case model.WalletOperationTypeApprove:
				if !operator.approveCalled {
					t.Fatal("ApproveFWSS was not called")
				}
				if operator.fundAmount != nil || operator.withdrawAmount != nil {
					t.Fatalf("fund=%v withdraw=%v, want no fund or withdraw", operator.fundAmount, operator.withdrawAmount)
				}
			default:
				t.Fatalf("unexpected operation type %q", tt.opType)
			}
			if !publisher.hasStatus(model.WalletOperationStatusSubmitted) || !publisher.hasStatus(model.WalletOperationStatusConfirmed) {
				t.Fatalf("published statuses = %v, want submitted and confirmed", publisher.statuses())
			}
		})
	}
}

func TestWalletOperationRunner_ConfirmsAlreadyApprovedWithoutTransaction(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeApprove,
		ClientRequestID: "approve-already",
		Amount:          "0",
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}

	operator := &fakeWalletOperator{approveErr: payments.ErrNothingToFund}
	publisher := &fakeWalletEventPublisher{}
	runner := NewWalletOperationRunner(repos, operator, nil, time.Millisecond, nil, WithWalletOperationEventPublisher(publisher))

	runner.runOnce(ctx)

	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusConfirmed {
		t.Fatalf("status = %q, want confirmed", got.Status)
	}
	if got.TxHash != nil {
		t.Fatalf("tx_hash = %v, want nil", *got.TxHash)
	}
	if got.SubmittedAt != nil {
		t.Fatalf("submitted_at = %v, want nil", got.SubmittedAt)
	}
	if !operator.approveCalled {
		t.Fatal("ApproveFWSS was not called")
	}
	if !publisher.hasStatus(model.WalletOperationStatusConfirmed) {
		t.Fatalf("published statuses = %v, want confirmed", publisher.statuses())
	}
}

func TestWalletOperationRunner_RetriesSubmittingBroadcastTxHash(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	repos.WalletOperations = &flakyMarkSubmittedWalletOperationRepo{
		WalletOperationRepository: repos.WalletOperations,
		failuresRemaining:         1,
	}
	ctx := context.Background()
	op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "fund-retry-submitted",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}

	txHash := common.HexToHash("0xabc")
	operator := &fakeWalletOperator{fundHash: txHash.Hex()}
	receipts := &fakeWalletReceiptChecker{receipts: map[common.Hash]*ethtypes.Receipt{
		txHash: {Status: ethtypes.ReceiptStatusSuccessful},
	}}
	runner := NewWalletOperationRunner(repos, operator, receipts, time.Millisecond, nil)

	runner.runOnce(ctx)

	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusConfirmed {
		t.Fatalf("status = %q, want confirmed", got.Status)
	}
	if got.TxHash == nil || *got.TxHash != txHash.Hex() {
		t.Fatalf("tx_hash = %v, want %s", got.TxHash, txHash.Hex())
	}
	flaky := repos.WalletOperations.(*flakyMarkSubmittedWalletOperationRepo)
	if flaky.markSubmittedCalls != 2 {
		t.Fatalf("MarkSubmitted calls = %d, want 2", flaky.markSubmittedCalls)
	}
}

func TestWalletOperationRunner_PersistsBroadcastTxHashAfterContextCancellation(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx, cancel := context.WithCancel(context.Background())
	op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "fund-cancel-after-broadcast",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}

	txHash := common.HexToHash("0xabc")
	operator := &fakeWalletOperator{
		fundHash: txHash.Hex(),
		onFund: func(context.Context) {
			cancel()
		},
	}
	runner := NewWalletOperationRunner(repos, operator, nil, time.Millisecond, nil)

	runner.runOnce(ctx)

	got, err := repos.WalletOperations.GetByID(context.Background(), op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusSubmitted {
		t.Fatalf("status = %q, want submitted", got.Status)
	}
	if got.TxHash == nil || *got.TxHash != txHash.Hex() {
		t.Fatalf("tx_hash = %v, want %s", got.TxHash, txHash.Hex())
	}
}

func TestWalletOperationRunner_WaitsForSubmittedOperationBeforeBroadcastingNext(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	submitted := seedSubmittedWalletOperation(t, repos, model.WalletOperationTypeFund, "fund-1", "100", common.HexToHash("0xabc").Hex())
	if _, err := db.NewUpdate().
		Model(submitted).
		Set("submitted_at = ?", time.Now().Add(-3*time.Hour)).
		WherePK().
		Exec(ctx); err != nil {
		t.Fatalf("age submitted operation: %v", err)
	}
	pending, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "fund-2",
		Amount:          "200",
	})
	if err != nil {
		t.Fatalf("CreateOrGet pending: %v", err)
	}

	operator := &fakeWalletOperator{fundHash: common.HexToHash("0xdef").Hex()}
	receipts := &fakeWalletReceiptChecker{receipts: map[common.Hash]*ethtypes.Receipt{
		common.HexToHash(*submitted.TxHash): nil,
	}}
	runner := NewWalletOperationRunner(repos, operator, receipts, time.Millisecond, nil)

	runner.runOnce(ctx)

	got, err := repos.WalletOperations.GetByID(ctx, pending.ID)
	if err != nil {
		t.Fatalf("GetByID pending: %v", err)
	}
	if got.Status != model.WalletOperationStatusPending {
		t.Fatalf("pending status = %q, want pending", got.Status)
	}
	if operator.fundAmount != nil {
		t.Fatalf("fund amount = %s, want no broadcast", operator.fundAmount)
	}
}

func TestWalletOperationRunner_ReceiptRevertFailsSubmittedOperation(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	op := seedSubmittedWalletOperation(t, repos, model.WalletOperationTypeWithdraw, "withdraw-1", "50", common.HexToHash("0xdef").Hex())

	receipts := &fakeWalletReceiptChecker{receipts: map[common.Hash]*ethtypes.Receipt{
		common.HexToHash(*op.TxHash): {Status: ethtypes.ReceiptStatusFailed},
	}}
	runner := NewWalletOperationRunner(repos, &fakeWalletOperator{}, receipts, time.Millisecond, nil)

	runner.runOnce(ctx)

	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusFailed {
		t.Fatalf("status = %q, want failed", got.Status)
	}
	if got.LastError == nil || *got.LastError == "" {
		t.Fatal("last_error is empty, want revert reason")
	}
	if !strings.Contains(*got.LastError, "transaction reverted") || !strings.Contains(*got.LastError, "status=0") || !strings.Contains(*got.LastError, "tx_hash=") {
		t.Fatalf("last_error = %q, want detailed revert context", *got.LastError)
	}
}

func TestWalletOperationRunner_ReceiptLookupFailureKeepsSubmittedOperationInFlight(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	op := seedSubmittedWalletOperation(t, repos, model.WalletOperationTypeFund, "fund-rpc-error", "50", common.HexToHash("0x456").Hex())
	pending, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "fund-after-rpc-error",
		Amount:          "60",
	})
	if err != nil {
		t.Fatalf("CreateOrGet pending: %v", err)
	}

	operator := &fakeWalletOperator{fundHash: common.HexToHash("0x789").Hex()}
	receipts := &fakeWalletReceiptChecker{err: errors.New("rpc timeout while reading receipt")}
	runner := NewWalletOperationRunner(repos, operator, receipts, time.Millisecond, nil)

	runner.runOnce(ctx)

	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusSubmitted {
		t.Fatalf("status = %q, want submitted", got.Status)
	}
	gotPending, err := repos.WalletOperations.GetByID(ctx, pending.ID)
	if err != nil {
		t.Fatalf("GetByID pending: %v", err)
	}
	if gotPending.Status != model.WalletOperationStatusPending {
		t.Fatalf("pending status = %q, want pending", gotPending.Status)
	}
	if operator.fundAmount != nil {
		t.Fatalf("fund amount = %s, want no broadcast", operator.fundAmount)
	}
}

func TestWalletOperationRunner_ReceiptLookupTimeoutDoesNotBlockRunner(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	op := seedSubmittedWalletOperation(t, repos, model.WalletOperationTypeFund, "fund-receipt-timeout", "50", common.HexToHash("0x456").Hex())

	receipts := &fakeWalletReceiptChecker{block: true}
	runner := NewWalletOperationRunner(repos, &fakeWalletOperator{}, receipts, time.Millisecond, nil, WithWalletOperationTimeouts(0, time.Millisecond))

	started := time.Now()
	runner.runOnce(ctx)
	if time.Since(started) > time.Second {
		t.Fatal("runOnce did not return after receipt lookup timeout")
	}

	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusSubmitted {
		t.Fatalf("status = %q, want submitted", got.Status)
	}
}

func TestWalletOperationRunner_BroadcastTimeoutDoesNotFailOperation(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "fund-broadcast-timeout",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}

	operator := &fakeWalletOperator{blockFund: true}
	runner := NewWalletOperationRunner(repos, operator, nil, time.Millisecond, nil, WithWalletOperationTimeouts(time.Millisecond, 0))

	started := time.Now()
	runner.runOnce(ctx)
	if time.Since(started) > time.Second {
		t.Fatal("runOnce did not return after broadcast timeout")
	}

	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusRunning {
		t.Fatalf("status = %q, want running until lease expiry", got.Status)
	}
	if got.TxHash != nil {
		t.Fatalf("tx_hash = %v, want nil", got.TxHash)
	}
}

func TestWalletOperationRunner_RemainsHealthyWhileBroadcasting(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "fund-slow-broadcast",
		Amount:          "100",
	}); err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	operator := &fakeWalletOperator{
		fundHash: common.HexToHash("0x123").Hex(),
		onFund: func(context.Context) {
			close(started)
			<-release
		},
	}
	runner := NewWalletOperationRunner(repos, operator, nil, time.Nanosecond, nil)

	done := make(chan struct{})
	go func() {
		runner.runOnce(ctx)
		close(done)
	}()
	<-started

	if !runner.Healthy() {
		t.Fatal("runner is unhealthy during an active wallet broadcast")
	}

	close(release)
	<-done
}

func TestWalletOperationRunner_RecoversSubmittedAndMarksExpiredRunningUnknown(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	submitted := seedSubmittedWalletOperation(t, repos, model.WalletOperationTypeFund, "submitted-1", "10", common.HexToHash("0x123").Hex())
	expired, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "expired-1",
		Amount:          "20",
	})
	if err != nil {
		t.Fatalf("CreateOrGet expired: %v", err)
	}
	now := time.Now()
	if _, err := db.NewUpdate().
		Model(expired).
		Set("status = ?", model.WalletOperationStatusRunning).
		Set("started_at = ?", now.Add(-2*time.Second)).
		Set("lease_until = ?", now.Add(-time.Second)).
		Set("updated_at = ?", now).
		WherePK().
		Exec(ctx); err != nil {
		t.Fatalf("seed expired running operation: %v", err)
	}

	receipts := &fakeWalletReceiptChecker{receipts: map[common.Hash]*ethtypes.Receipt{
		common.HexToHash(*submitted.TxHash): {Status: ethtypes.ReceiptStatusSuccessful},
	}}
	publisher := &fakeWalletEventPublisher{}
	runner := NewWalletOperationRunner(repos, &fakeWalletOperator{}, receipts, time.Millisecond, nil, WithWalletOperationEventPublisher(publisher))

	runner.runOnce(ctx)

	gotSubmitted, err := repos.WalletOperations.GetByID(ctx, submitted.ID)
	if err != nil {
		t.Fatalf("GetByID submitted: %v", err)
	}
	if gotSubmitted.Status != model.WalletOperationStatusConfirmed {
		t.Fatalf("submitted status = %q, want confirmed", gotSubmitted.Status)
	}
	gotExpired, err := repos.WalletOperations.GetByID(ctx, expired.ID)
	if err != nil {
		t.Fatalf("GetByID expired: %v", err)
	}
	if gotExpired.Status != model.WalletOperationStatusUnknown {
		t.Fatalf("expired status = %q, want unknown", gotExpired.Status)
	}
	if !publisher.hasStatus(model.WalletOperationStatusUnknown) {
		t.Fatalf("published statuses = %v, want unknown", publisher.statuses())
	}
}

func seedSubmittedWalletOperation(t *testing.T, repos *repository.Repositories, opType model.WalletOperationType, requestID, amount, txHash string) *model.WalletOperation {
	t.Helper()
	ctx := context.Background()
	op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            opType,
		ClientRequestID: requestID,
		Amount:          amount,
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}
	claimed, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed.ID != op.ID {
		t.Fatalf("claimed ID = %d, want %d", claimed.ID, op.ID)
	}
	if err := repos.WalletOperations.MarkSubmitted(ctx, op.ID, txHash); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	return got
}

type fakeWalletOperator struct {
	fundHash       string
	withdrawHash   string
	approveHash    string
	fundAmount     *big.Int
	withdrawAmount *big.Int
	approveCalled  bool
	onFund         func(context.Context)
	blockFund      bool
	fundErr        error
	withdrawErr    error
	approveErr     error
}

func (f *fakeWalletOperator) FundUSDFC(ctx context.Context, amount *big.Int) (string, error) {
	f.fundAmount = new(big.Int).Set(amount)
	if f.onFund != nil {
		f.onFund(ctx)
	}
	if f.blockFund {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if f.fundErr != nil {
		return "", f.fundErr
	}
	return f.fundHash, nil
}

func (f *fakeWalletOperator) WithdrawUSDFC(_ context.Context, amount *big.Int) (string, error) {
	f.withdrawAmount = new(big.Int).Set(amount)
	if f.withdrawErr != nil {
		return "", f.withdrawErr
	}
	return f.withdrawHash, nil
}

func (f *fakeWalletOperator) ApproveFWSS(context.Context) (string, error) {
	f.approveCalled = true
	if f.approveErr != nil {
		return "", f.approveErr
	}
	return f.approveHash, nil
}

type fakeWalletReceiptChecker struct {
	receipts map[common.Hash]*ethtypes.Receipt
	err      error
	block    bool
}

func (f *fakeWalletReceiptChecker) TransactionReceipt(ctx context.Context, txHash common.Hash) (*ethtypes.Receipt, error) {
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.receipts[txHash], nil
}

type flakyMarkSubmittedWalletOperationRepo struct {
	repository.WalletOperationRepository
	failuresRemaining  int
	markSubmittedCalls int
}

func (f *flakyMarkSubmittedWalletOperationRepo) MarkSubmitted(ctx context.Context, id int64, txHash string) error {
	f.markSubmittedCalls++
	if f.failuresRemaining > 0 {
		f.failuresRemaining--
		return errors.New("temporary database error")
	}
	return f.WalletOperationRepository.MarkSubmitted(ctx, id, txHash)
}

type fakeWalletEventPublisher struct {
	mu     sync.Mutex
	events []map[string]any
}

func (f *fakeWalletEventPublisher) Publish(topic string, payload map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if topic != walletOperationUpdatedTopic {
		return
	}
	f.events = append(f.events, payload)
}

func (f *fakeWalletEventPublisher) hasStatus(status model.WalletOperationStatus) bool {
	for _, got := range f.statuses() {
		if got == status {
			return true
		}
	}
	return false
}

func (f *fakeWalletEventPublisher) statuses() []model.WalletOperationStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	statuses := make([]model.WalletOperationStatus, 0, len(f.events))
	for _, event := range f.events {
		op, ok := event["operation"].(walletOperationEventPayload)
		if ok {
			statuses = append(statuses, model.WalletOperationStatus(op.Status))
		}
	}
	return statuses
}

func TestWalletOperationRunner_BroadcastErrorFailsOperation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opType model.WalletOperationType
		amount string
		err    error
	}{
		{name: "fund", opType: model.WalletOperationTypeFund, amount: "100", err: errors.New("fund rpc node offline")},
		{name: "withdraw", opType: model.WalletOperationTypeWithdraw, amount: "100", err: errors.New("withdraw rpc node offline")},
		{name: "approve", opType: model.WalletOperationTypeApprove, amount: "0", err: errors.New("approve rpc node offline")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testutil.NewTestDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
				Type:            tc.opType,
				ClientRequestID: string(tc.opType) + "-broadcast-error",
				Amount:          tc.amount,
			})
			if err != nil {
				t.Fatalf("CreateOrGet: %v", err)
			}

			operator := &fakeWalletOperator{fundErr: tc.err, withdrawErr: tc.err, approveErr: tc.err}
			runner := NewWalletOperationRunner(repos, operator, nil, time.Millisecond, nil)

			runner.runOnce(ctx)

			got, err := repos.WalletOperations.GetByID(ctx, op.ID)
			if err != nil {
				t.Fatalf("GetByID: %v", err)
			}
			if got.Status != model.WalletOperationStatusFailed {
				t.Fatalf("status = %q, want failed", got.Status)
			}
			if got.LastError == nil {
				t.Fatalf("last_error is nil, want %q", tc.err.Error())
			}
			if !strings.Contains(*got.LastError, tc.err.Error()) {
				t.Fatalf("last_error = %q, want %q", *got.LastError, tc.err.Error())
			}
		})
	}
}
