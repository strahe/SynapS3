package repository_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestWalletOperationRepo_CreateOrGetIsIdempotentByTypeAndClientRequestID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	first, created, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-1",
		Amount:          "1000000000000000000",
	})
	if err != nil {
		t.Fatalf("CreateOrGet first: %v", err)
	}
	if !created {
		t.Fatal("first CreateOrGet created = false, want true")
	}

	second, created, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-1",
		Amount:          "1000000000000000000",
	})
	if err != nil {
		t.Fatalf("CreateOrGet second: %v", err)
	}
	if created {
		t.Fatal("second CreateOrGet created = true, want false")
	}
	if second.ID != first.ID {
		t.Fatalf("second ID = %d, want %d", second.ID, first.ID)
	}
}

func TestWalletOperationRepo_CreateOrGetRejectsInvalidAmount(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	for _, amount := range []string{"", "0", "00", "-1", "1.5", "abc"} {
		_, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
			Type:            model.WalletOperationTypeFund,
			ClientRequestID: "request-" + amount,
			Amount:          amount,
		})
		if !errors.Is(err, repository.ErrWalletOperationInvalidAmount) {
			t.Fatalf("CreateOrGet amount %q error = %v, want ErrWalletOperationInvalidAmount", amount, err)
		}
	}
}

func TestWalletOperationRepo_CreateOrGetRejectsSameRequestWithDifferentAmount(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	if _, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeWithdraw,
		ClientRequestID: "request-1",
		Amount:          "100",
	}); err != nil {
		t.Fatalf("CreateOrGet first: %v", err)
	}

	if _, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeWithdraw,
		ClientRequestID: "request-1",
		Amount:          "101",
	}); err == nil {
		t.Fatal("CreateOrGet with different amount error = nil, want conflict")
	}
}

func TestWalletOperationRepo_ClaimPendingAndMarkSubmitted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	created, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-1",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}

	claimed, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil || claimed.ID != created.ID {
		t.Fatalf("claimed = %#v, want operation %d", claimed, created.ID)
	}
	if claimed.Status != model.WalletOperationStatusRunning {
		t.Fatalf("claimed status = %q, want running", claimed.Status)
	}

	if err := repos.WalletOperations.MarkSubmitted(ctx, claimed.ID, "0xabc"); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	got, err := repos.WalletOperations.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusSubmitted {
		t.Fatalf("status = %q, want submitted", got.Status)
	}
	if got.TxHash == nil || *got.TxHash != "0xabc" {
		t.Fatalf("tx_hash = %v, want 0xabc", got.TxHash)
	}
}

func TestWalletOperationRepo_ClaimPendingWaitsForInFlightOperation(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	first, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-1",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet first: %v", err)
	}
	second, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-2",
		Amount:          "200",
	})
	if err != nil {
		t.Fatalf("CreateOrGet second: %v", err)
	}

	claimed, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending first: %v", err)
	}
	if claimed == nil || claimed.ID != first.ID {
		t.Fatalf("claimed = %#v, want first operation %d", claimed, first.ID)
	}
	blocked, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending while running: %v", err)
	}
	if blocked != nil {
		t.Fatalf("ClaimPending while running returned operation %d, want nil", blocked.ID)
	}

	if err := repos.WalletOperations.MarkSubmitted(ctx, first.ID, "0xabc"); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	blocked, err = repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending while submitted: %v", err)
	}
	if blocked != nil {
		t.Fatalf("ClaimPending while submitted returned operation %d, want nil", blocked.ID)
	}

	if err := repos.WalletOperations.MarkConfirmed(ctx, first.ID); err != nil {
		t.Fatalf("MarkConfirmed: %v", err)
	}
	claimed, err = repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending after confirmed: %v", err)
	}
	if claimed == nil || claimed.ID != second.ID {
		t.Fatalf("claimed after confirmed = %#v, want second operation %d", claimed, second.ID)
	}
}

func TestWalletOperationRepo_MarkExpiredRunningUnknownLeavesSubmittedAlone(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	running, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "running",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet running: %v", err)
	}
	submitted, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "submitted",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet submitted: %v", err)
	}

	now := time.Now()
	expiredLease := now.Add(-time.Second)
	if _, err := db.NewUpdate().
		Model(running).
		Set("status = ?", model.WalletOperationStatusRunning).
		Set("started_at = ?", now.Add(-2*time.Second)).
		Set("lease_until = ?", expiredLease).
		Set("updated_at = ?", now).
		WherePK().
		Exec(ctx); err != nil {
		t.Fatalf("seed running operation: %v", err)
	}
	if _, err := db.NewUpdate().
		Model(submitted).
		Set("status = ?", model.WalletOperationStatusSubmitted).
		Set("tx_hash = ?", "0xdef").
		Set("submitted_at = ?", now).
		Set("updated_at = ?", now).
		WherePK().
		Exec(ctx); err != nil {
		t.Fatalf("seed submitted operation: %v", err)
	}

	expired, err := repos.WalletOperations.MarkExpiredRunningUnknown(ctx)
	if err != nil {
		t.Fatalf("MarkExpiredRunningUnknown: %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expired count = %d, want 1", len(expired))
	}
	if expired[0].ID != running.ID {
		t.Fatalf("expired ID = %d, want %d", expired[0].ID, running.ID)
	}

	gotRunning, _ := repos.WalletOperations.GetByID(ctx, running.ID)
	gotSubmitted, _ := repos.WalletOperations.GetByID(ctx, submitted.ID)
	if gotRunning.Status != model.WalletOperationStatusUnknown {
		t.Fatalf("running status = %q, want unknown", gotRunning.Status)
	}
	if gotSubmitted.Status != model.WalletOperationStatusSubmitted {
		t.Fatalf("submitted status = %q, want submitted", gotSubmitted.Status)
	}
}

func TestWalletOperationRepo_ListRecentClampsLimitToMax(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	for i := 0; i < 101; i++ {
		_, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
			Type:            model.WalletOperationTypeFund,
			ClientRequestID: "request-" + time.Unix(int64(i), 0).UTC().Format("20060102150405"),
			Amount:          "100",
		})
		if err != nil {
			t.Fatalf("CreateOrGet %d: %v", i, err)
		}
	}

	ops, err := repos.WalletOperations.ListRecent(ctx, 101)
	if err != nil {
		t.Fatalf("ListRecent: %v", err)
	}
	if len(ops) != 100 {
		t.Fatalf("operation count = %d, want 100", len(ops))
	}
}
