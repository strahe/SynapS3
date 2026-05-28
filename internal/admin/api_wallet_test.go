package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
)

type stubWalletQuerier struct {
	info *synapse.WalletInfo
	err  error
}

func (s *stubWalletQuerier) GetWalletInfo(_ context.Context) (*synapse.WalletInfo, error) {
	return s.info, s.err
}

func TestHandleAPIWallet_ReturnsStructuredWalletResponse(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{
		Name:   "wallet-proofset",
		Status: model.BucketStatusActive,
	}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		ContentSize:     1,
		Checksum:        "wallet-checksum",
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	pieceCID := "piece-wallet"
	seedAdminCommittedCopies(t, repos, bucket.ID, upload.ID, pieceCID, []adminStorageCopySeed{{
		ProviderID:     onChainID(t, "101"),
		DataSetID:      onChainID(t, "1001"),
		PieceID:        onChainIDPtr(t, "1"),
		TransferMethod: model.StorageCopyTransferMethodIngress,
		RetrievalURL:   "https://provider.example/wallet",
	}})

	tasks := []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          1,
			RefVersionID:   "01J000000000000000WALLET1",
			IdempotencyKey: "wallet-upload-pending",
			Status:         model.TaskStatusQueued,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          2,
			RefVersionID:   "01J000000000000000WALLET2",
			IdempotencyKey: "wallet-upload-completed",
			Status:         model.TaskStatusCompleted,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          3,
			RefVersionID:   "01J000000000000000WALLET3",
			IdempotencyKey: "wallet-evict-pending",
			Status:         model.TaskStatusQueued,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		},
	}
	for _, task := range tasks {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("creating task %q: %v", task.IdempotencyKey, err)
		}
	}

	nonce := uint64(7)
	srv := New(":0", db, &stubCache{rootDir: t.TempDir()}, 1<<20, repos, nil, &stubWalletQuerier{
		info: &synapse.WalletInfo{
			Address:              "0xabc",
			Network:              "mainnet",
			ChainID:              314,
			Nonce:                &nonce,
			CurrentEpoch:         big.NewInt(3727006),
			EpochDurationSeconds: 30,
			PaymentsAddress:      "0xpay",
			USDFCAddress:         "0xusdfc",
			USDFCDecimals:        18,
			FILGasBalance:        big.NewInt(11),
			USDFCWalletBalance:   big.NewInt(22),
			PaymentAccount: &synapse.PaymentAccountInfo{
				Funds:               big.NewInt(100),
				AvailableFunds:      big.NewInt(80),
				LockupCurrent:       big.NewInt(20),
				LockupRate:          big.NewInt(2),
				LockupLastSettledAt: big.NewInt(3727000),
				FundedUntilEpoch:    big.NewInt(3727100),
				LockupRatePerDay:    big.NewInt(5760),
				LockupRatePerMonth:  big.NewInt(172800),
			},
		},
	}, config.DefaultFilecoinCopies, testLogger())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet", nil)
	rr := httptest.NewRecorder()
	srv.handleAPIWallet(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("Unmarshal raw: %v", err)
	}
	if _, ok := raw["fil_account"]; ok {
		t.Fatal("response contains fil_account, want new wallet schema without FIL payment account")
	}

	var resp walletResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if resp.Identity == nil || resp.Identity.Nonce == nil || *resp.Identity.Nonce != nonce {
		t.Fatalf("identity.nonce = %v, want %d", resp.Identity, nonce)
	}
	if resp.Chain == nil || resp.Chain.CurrentEpoch == nil || *resp.Chain.CurrentEpoch != "3727006" {
		t.Fatalf("chain.current_epoch = %#v, want 3727006", resp.Chain)
	}
	if resp.Contracts == nil || resp.Contracts.PaymentsAddress != "0xpay" {
		t.Fatalf("contracts.payments = %#v, want 0xpay", resp.Contracts)
	}
	if resp.Contracts.USDFCAddress != "0xusdfc" {
		t.Fatalf("contracts.usdfc = %q, want 0xusdfc", resp.Contracts.USDFCAddress)
	}
	if resp.Contracts.USDFCDecimals != 18 {
		t.Fatalf("contracts.usdfc_decimals = %d, want 18", resp.Contracts.USDFCDecimals)
	}
	if resp.WalletBalances == nil || resp.WalletBalances.FILGas == nil || *resp.WalletBalances.FILGas != "11" {
		t.Fatalf("wallet_balances.fil_gas = %#v, want 11", resp.WalletBalances)
	}
	if resp.PaymentAccount == nil || resp.PaymentAccount.LockupRatePerMonth == nil || *resp.PaymentAccount.LockupRatePerMonth != "172800" {
		t.Fatalf("payment_account.lockup_rate_per_month = %#v, want 172800", resp.PaymentAccount)
	}
	if resp.Business == nil {
		t.Fatal("business = nil, want populated")
	}
	if resp.Business.DataSetCount != 1 {
		t.Fatalf("data_set_count = %d, want 1", resp.Business.DataSetCount)
	}
	if resp.Business.OnchainTasksPending != 1 {
		t.Fatalf("onchain_tasks_pending = %d, want 1", resp.Business.OnchainTasksPending)
	}
	if resp.Business.OnchainTasksCompleted != 1 {
		t.Fatalf("onchain_tasks_completed = %d, want 1", resp.Business.OnchainTasksCompleted)
	}
}

func TestHandleAPIWalletFund_RejectsUnsafeOrInvalidRequests(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	srv := New("127.0.0.1:0", db, &stubCache{rootDir: t.TempDir()}, 1<<20, repos, nil, &stubWalletQuerier{
		info: &synapse.WalletInfo{Address: "0xabc"},
	}, config.DefaultFilecoinCopies, testLogger())

	tests := []struct {
		name        string
		body        string
		contentType string
		wantStatus  int
	}{
		{name: "non json", body: `{"client_request_id":"a","amount":"1"}`, contentType: "text/plain", wantStatus: http.StatusBadRequest},
		{name: "empty amount", body: `{"client_request_id":"a","amount":""}`, contentType: "application/json", wantStatus: http.StatusBadRequest},
		{name: "zero amount", body: `{"client_request_id":"a","amount":"0"}`, contentType: "application/json", wantStatus: http.StatusBadRequest},
		{name: "negative amount", body: `{"client_request_id":"a","amount":"-1"}`, contentType: "application/json", wantStatus: http.StatusBadRequest},
		{name: "decimal amount", body: `{"client_request_id":"a","amount":"1.1"}`, contentType: "application/json", wantStatus: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/fund", strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			rr := httptest.NewRecorder()
			srv.handleAPIWalletFund(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
		})
	}
}

func TestHandleAPIWalletFund_IsIdempotentByClientRequestID(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	srv := New("127.0.0.1:0", db, &stubCache{rootDir: t.TempDir()}, 1<<20, repos, nil, &stubWalletQuerier{
		info: &synapse.WalletInfo{Address: "0xabc"},
	}, config.DefaultFilecoinCopies, testLogger())

	create := func(amount string) (walletOperationDTO, int) {
		body := []byte(`{"client_request_id":"same-request","amount":"` + amount + `"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/fund", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		srv.handleAPIWalletFund(rr, req)
		var resp walletOperationResponse
		if rr.Code == http.StatusAccepted {
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Fatalf("Unmarshal operation response: %v", err)
			}
		}
		return resp.Operation, rr.Code
	}

	first, status := create("100")
	if status != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", status)
	}
	second, status := create("100")
	if status != http.StatusAccepted {
		t.Fatalf("second status = %d, want 202", status)
	}
	if first.ID == 0 || second.ID != first.ID {
		t.Fatalf("idempotent ID = %d then %d, want same non-zero ID", first.ID, second.ID)
	}
	if _, status := create("101"); status != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", status)
	}

	ops, err := repos.WalletOperations.ListRecent(context.Background(), 20)
	if err != nil {
		t.Fatalf("ListRecent: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("operation count = %d, want 1", len(ops))
	}
}
