package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestHandleAPIWallet_CompatibilityFields(t *testing.T) {
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
		BucketID:    bucket.ID,
		ContentSize: 1,
		Checksum:    "wallet-checksum",
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	pieceCID := "piece-wallet"
	providerID := onChainIDPtr(t, "101")
	dataSetID := onChainIDPtr(t, "1001")
	pieceID := onChainIDPtr(t, "1")
	retrievalURL := "https://provider.example/wallet"
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        &pieceCID,
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{{
			ProviderID:   providerID,
			DataSetID:    dataSetID,
			PieceID:      pieceID,
			Role:         "primary",
			RetrievalURL: &retrievalURL,
		}},
	}); err != nil {
		t.Fatalf("record upload result: %v", err)
	}

	tasks := []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          1,
			RefVersionID:   "01J000000000000000WALLET1",
			IdempotencyKey: "wallet-upload-pending",
			Status:         model.TaskStatusPending,
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
			Status:         model.TaskStatusPending,
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
			Address:         "0xabc",
			Network:         "mainnet",
			ChainID:         314,
			Nonce:           &nonce,
			PaymentsAddress: "0xpay",
			USDFCAddress:    "0xusdfc",
			USDFCDecimals:   18,
		},
	}, testLogger())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet", nil)
	rr := httptest.NewRecorder()
	srv.handleAPIWallet(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	var resp walletResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if resp.Nonce == nil || *resp.Nonce != nonce {
		t.Fatalf("nonce = %v, want %d", resp.Nonce, nonce)
	}
	if resp.PaymentsAddress != "0xpay" {
		t.Fatalf("payments_address = %q, want 0xpay", resp.PaymentsAddress)
	}
	if resp.USDFCAddress != "0xusdfc" {
		t.Fatalf("usdfc_address = %q, want 0xusdfc", resp.USDFCAddress)
	}
	if resp.USDFCDecimals != 18 {
		t.Fatalf("usdfc_decimals = %d, want 18", resp.USDFCDecimals)
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
