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

func TestWalletOperationRepo_CreateOrGetValidatesAmount(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	approve, created, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeApprove,
		ClientRequestID: "approve-1",
		Amount:          "0",
	})
	if err != nil {
		t.Fatalf("CreateOrGet approve: %v", err)
	}
	if !created || approve.Amount != "0" {
		t.Fatalf("approve operation = %#v created=%v, want amount 0 and created", approve, created)
	}

	for _, tc := range []struct {
		name   string
		opType model.WalletOperationType
		amount string
	}{
		{name: "fund empty", opType: model.WalletOperationTypeFund, amount: ""},
		{name: "fund zero", opType: model.WalletOperationTypeFund, amount: "0"},
		{name: "fund leading zero", opType: model.WalletOperationTypeFund, amount: "00"},
		{name: "fund negative", opType: model.WalletOperationTypeFund, amount: "-1"},
		{name: "fund float", opType: model.WalletOperationTypeFund, amount: "1.5"},
		{name: "fund string", opType: model.WalletOperationTypeFund, amount: "abc"},
		{name: "approve positive", opType: model.WalletOperationTypeApprove, amount: "1"},
		{name: "withdraw zero", opType: model.WalletOperationTypeWithdraw, amount: "0"},
		{name: "unknown type", opType: model.WalletOperationType("unknown"), amount: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
				Type:            tc.opType,
				ClientRequestID: tc.name,
				Amount:          tc.amount,
			})
			if !errors.Is(err, repository.ErrWalletOperationInvalidAmount) {
				t.Fatalf("CreateOrGet type=%s amount=%q error = %v, want ErrWalletOperationInvalidAmount", tc.opType, tc.amount, err)
			}
		})
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

func TestWalletOperationRepo_MarkConfirmed(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "fund-confirm-1",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}
	claimed, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil || claimed.ID != op.ID {
		t.Fatalf("claimed = %#v, want operation %d", claimed, op.ID)
	}
	if err := repos.WalletOperations.MarkSubmitted(ctx, claimed.ID, "0xabc"); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	if err := repos.WalletOperations.MarkConfirmed(ctx, claimed.ID); err != nil {
		t.Fatalf("MarkConfirmed: %v", err)
	}

	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.WalletOperationStatusConfirmed {
		t.Fatalf("status = %q, want confirmed", got.Status)
	}
	if got.TxHash == nil || *got.TxHash != "0xabc" {
		t.Fatalf("tx_hash = %v, want 0xabc", got.TxHash)
	}
	if got.SubmittedAt == nil {
		t.Fatal("submitted_at = nil, want timestamp")
	}
	if got.CompletedAt == nil {
		t.Fatal("completed_at = nil, want timestamp")
	}
}

func TestWalletOperationRepo_MarkConfirmedWithoutTransaction(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeApprove,
		ClientRequestID: "approve-1",
		Amount:          "0",
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}
	claimed, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil || claimed.ID != op.ID {
		t.Fatalf("claimed = %#v, want operation %d", claimed, op.ID)
	}
	if err := repos.WalletOperations.MarkConfirmedWithoutTransaction(ctx, claimed.ID); err != nil {
		t.Fatalf("MarkConfirmedWithoutTransaction: %v", err)
	}

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
	if got.CompletedAt == nil {
		t.Fatal("completed_at = nil, want timestamp")
	}
}

func TestWalletOperationRepo_MarkFailed(t *testing.T) {
	claimPending := func(t *testing.T, repos *repository.Repositories, ctx context.Context, op *model.WalletOperation) *model.WalletOperation {
		t.Helper()
		claimed, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
		if err != nil {
			t.Fatalf("ClaimPending: %v", err)
		}
		if claimed == nil || claimed.ID != op.ID {
			t.Fatalf("claimed = %#v, want operation %d", claimed, op.ID)
		}
		return claimed
	}

	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, repos *repository.Repositories, ctx context.Context, op *model.WalletOperation)
	}{
		{name: "pending"},
		{
			name: "running",
			prepare: func(t *testing.T, repos *repository.Repositories, ctx context.Context, op *model.WalletOperation) {
				claimPending(t, repos, ctx, op)
			},
		},
		{
			name: "submitted",
			prepare: func(t *testing.T, repos *repository.Repositories, ctx context.Context, op *model.WalletOperation) {
				claimed := claimPending(t, repos, ctx, op)
				if err := repos.WalletOperations.MarkSubmitted(ctx, claimed.ID, "0xbad"); err != nil {
					t.Fatalf("MarkSubmitted: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()

			op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
				Type:            model.WalletOperationTypeFund,
				ClientRequestID: "request-failed-" + tc.name,
				Amount:          "100",
			})
			if err != nil {
				t.Fatalf("CreateOrGet: %v", err)
			}
			if tc.prepare != nil {
				tc.prepare(t, repos, ctx, op)
			}

			lastError := "failed from " + tc.name
			if err := repos.WalletOperations.MarkFailed(ctx, op.ID, lastError); err != nil {
				t.Fatalf("MarkFailed: %v", err)
			}

			got, err := repos.WalletOperations.GetByID(ctx, op.ID)
			if err != nil {
				t.Fatalf("GetByID: %v", err)
			}
			if got == nil {
				t.Fatal("GetByID returned nil operation")
			}
			if got.Status != model.WalletOperationStatusFailed {
				t.Fatalf("status = %q, want failed", got.Status)
			}
			if got.LastError == nil || *got.LastError != lastError {
				t.Fatalf("last_error = %v, want %q", got.LastError, lastError)
			}
			if got.LeaseUntil != nil {
				t.Fatalf("lease_until = %v, want nil", got.LeaseUntil)
			}
			if got.CompletedAt == nil {
				t.Fatal("completed_at = nil, want timestamp")
			}
		})
	}

	t.Run("confirmed rejected", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()

		op, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
			Type:            model.WalletOperationTypeApprove,
			ClientRequestID: "approve-not-failed",
			Amount:          "0",
		})
		if err != nil {
			t.Fatalf("CreateOrGet: %v", err)
		}
		claimed := claimPending(t, repos, ctx, op)
		if err := repos.WalletOperations.MarkConfirmedWithoutTransaction(ctx, claimed.ID); err != nil {
			t.Fatalf("MarkConfirmedWithoutTransaction: %v", err)
		}
		confirmed, err := repos.WalletOperations.GetByID(ctx, op.ID)
		if err != nil {
			t.Fatalf("GetByID confirmed: %v", err)
		}
		if confirmed == nil || confirmed.CompletedAt == nil {
			t.Fatalf("confirmed operation = %#v, want completed timestamp", confirmed)
		}

		if err := repos.WalletOperations.MarkFailed(ctx, op.ID, "failed after confirm"); !errors.Is(err, repository.ErrNotFound) {
			t.Fatalf("MarkFailed error = %v, want ErrNotFound", err)
		}
		got, err := repos.WalletOperations.GetByID(ctx, op.ID)
		if err != nil {
			t.Fatalf("GetByID after rejected MarkFailed: %v", err)
		}
		if got == nil {
			t.Fatal("GetByID returned nil operation")
		}
		if got.Status != model.WalletOperationStatusConfirmed {
			t.Fatalf("status = %q, want confirmed", got.Status)
		}
		if got.LastError != nil {
			t.Fatalf("last_error = %v, want nil", got.LastError)
		}
		if got.CompletedAt == nil || !got.CompletedAt.Equal(*confirmed.CompletedAt) {
			t.Fatalf("completed_at = %v, want unchanged %v", got.CompletedAt, confirmed.CompletedAt)
		}
	})
}

func TestWalletOperationRepo_ListSubmitted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	submitted, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-submitted",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet submitted: %v", err)
	}
	pending, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-pending",
		Amount:          "200",
	})
	if err != nil {
		t.Fatalf("CreateOrGet pending: %v", err)
	}
	emptyTx, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "request-empty-tx",
		Amount:          "300",
	})
	if err != nil {
		t.Fatalf("CreateOrGet empty tx: %v", err)
	}

	claimed, err := repos.WalletOperations.ClaimPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil || claimed.ID != submitted.ID {
		t.Fatalf("claimed = %#v, want operation %d", claimed, submitted.ID)
	}
	if err := repos.WalletOperations.MarkSubmitted(ctx, claimed.ID, "0x123"); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}

	now := time.Now()
	if _, err := db.NewUpdate().
		Model(emptyTx).
		Set("status = ?", model.WalletOperationStatusSubmitted).
		Set("tx_hash = ?", "").
		Set("submitted_at = ?", now).
		Set("updated_at = ?", now).
		WherePK().
		Exec(ctx); err != nil {
		t.Fatalf("seed submitted empty tx: %v", err)
	}

	ops, err := repos.WalletOperations.ListSubmitted(ctx, 10)
	if err != nil {
		t.Fatalf("ListSubmitted: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("submitted count = %d, want 1: %#v", len(ops), ops)
	}
	if ops[0].ID != submitted.ID {
		t.Fatalf("submitted ID = %d, want %d", ops[0].ID, submitted.ID)
	}
	if ops[0].TxHash == nil || *ops[0].TxHash != "0x123" {
		t.Fatalf("tx_hash = %v, want 0x123", ops[0].TxHash)
	}
	for _, op := range ops {
		if op.ID == pending.ID {
			t.Fatalf("ListSubmitted returned pending operation %d", pending.ID)
		}
		if op.ID == emptyTx.ID {
			t.Fatalf("ListSubmitted returned submitted operation %d with empty tx hash", emptyTx.ID)
		}
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

func TestWalletOperationRepo_MarkExpiredRunningUnknownLeavesPendingAndSubmittedAlone(t *testing.T) {
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
	pending, _, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type:            model.WalletOperationTypeFund,
		ClientRequestID: "pending",
		Amount:          "100",
	})
	if err != nil {
		t.Fatalf("CreateOrGet pending: %v", err)
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
	gotPending, _ := repos.WalletOperations.GetByID(ctx, pending.ID)
	if gotRunning.Status != model.WalletOperationStatusUnknown {
		t.Fatalf("running status = %q, want unknown", gotRunning.Status)
	}
	if gotSubmitted.Status != model.WalletOperationStatusSubmitted {
		t.Fatalf("submitted status = %q, want submitted", gotSubmitted.Status)
	}
	if gotPending.Status != model.WalletOperationStatusPending {
		t.Fatalf("pending status = %q, want pending", gotPending.Status)
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
