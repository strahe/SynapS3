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
		Name:       "wallet-proofset",
		Status:     model.BucketStatusActive,
		ProofSetID: ptrString("proofset-1"),
	}
	if _, err := db.NewInsert().Model(bucket).Exec(ctx); err != nil {
		t.Fatalf("inserting bucket: %v", err)
	}

	tasks := []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          1,
			RefGeneration:  1,
			IdempotencyKey: "wallet-upload-pending",
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          2,
			RefGeneration:  1,
			IdempotencyKey: "wallet-upload-completed",
			Status:         model.TaskStatusCompleted,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          3,
			RefGeneration:  1,
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
	if resp.Business.ProofSetCount != 1 {
		t.Fatalf("proof_set_count = %d, want 1", resp.Business.ProofSetCount)
	}
	if resp.Business.OnchainTasksPending != 1 {
		t.Fatalf("onchain_tasks_pending = %d, want 1", resp.Business.OnchainTasksPending)
	}
	if resp.Business.OnchainTasksCompleted != 1 {
		t.Fatalf("onchain_tasks_completed = %d, want 1", resp.Business.OnchainTasksCompleted)
	}
}

func ptrString(s string) *string {
	return &s
}
