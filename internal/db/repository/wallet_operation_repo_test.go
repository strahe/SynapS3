package repository_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestWalletOperationRepoCreateOrGet(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	input := repository.CreateWalletOperationInput{
		Type: model.WalletOperationTypeFund, ClientRequestID: "request-1", Amount: "100",
	}
	first, created, err := repos.WalletOperations.CreateOrGet(ctx, input)
	if err != nil || !created {
		t.Fatalf("first CreateOrGet operation=%#v created=%t err=%v", first, created, err)
	}
	second, created, err := repos.WalletOperations.CreateOrGet(ctx, input)
	if err != nil || created || second == nil || second.ID != first.ID {
		t.Fatalf("second CreateOrGet operation=%#v created=%t err=%v", second, created, err)
	}
	input.Amount = "101"
	if _, _, err := repos.WalletOperations.CreateOrGet(ctx, input); !errors.Is(err, repository.ErrWalletOperationConflict) {
		t.Fatalf("conflicting CreateOrGet error=%v", err)
	}
}

func TestWalletOperationRepoValidatesAmounts(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	if op, created, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type: model.WalletOperationTypeApprove, ClientRequestID: "approve", Amount: "0",
	}); err != nil || !created || op.Amount != "0" {
		t.Fatalf("approve operation=%#v created=%t err=%v", op, created, err)
	}
	for _, input := range []repository.CreateWalletOperationInput{
		{Type: model.WalletOperationTypeFund, ClientRequestID: "zero", Amount: "0"},
		{Type: model.WalletOperationTypeWithdraw, ClientRequestID: "negative", Amount: "-1"},
		{Type: model.WalletOperationTypeApprove, ClientRequestID: "approve-positive", Amount: "1"},
		{Type: model.WalletOperationType("other"), ClientRequestID: "unknown", Amount: "1"},
	} {
		if _, _, err := repos.WalletOperations.CreateOrGet(ctx, input); !errors.Is(err, repository.ErrWalletOperationInvalidAmount) {
			t.Errorf("CreateOrGet(%#v) error=%v", input, err)
		}
	}
}

func TestWalletOperationRepoFencesBroadcastAndConfirmationByTask(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	op := createWalletOperation(t, repos, model.WalletOperationTypeFund, "fenced", "100")
	taskID := createWalletTask(t, repos, op.ID, "fenced")
	if err := repos.WalletOperations.BindTask(ctx, op.ID, taskID); err != nil {
		t.Fatalf("BindTask: %v", err)
	}
	if err := repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, taskID); err != nil {
		t.Fatalf("MarkBroadcastAttempted: %v", err)
	}
	if err := repos.WalletOperations.MarkSubmitted(ctx, op.ID, taskID, "0xabc"); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	if err := repos.WalletOperations.MarkConfirmed(ctx, op.ID, taskID+1, "0xabc"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("stale MarkConfirmed error=%v", err)
	}
	if err := repos.WalletOperations.MarkConfirmed(ctx, op.ID, taskID, "0xabc"); err != nil {
		t.Fatalf("MarkConfirmed: %v", err)
	}
	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID operation=%#v err=%v", got, err)
	}
	if got.Status != model.WalletOperationStatusConfirmed || got.TxHash == nil || *got.TxHash != "0xabc" || got.TaskID != nil || got.BroadcastAttemptedAt == nil || got.CompletedAt == nil {
		t.Fatalf("confirmed operation=%#v", got)
	}
}

func TestWalletOperationRepoUnknownRequiresAttemptEvidence(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	op := createWalletOperation(t, repos, model.WalletOperationTypeWithdraw, "unknown", "100")
	taskID := createWalletTask(t, repos, op.ID, "unknown")
	if err := repos.WalletOperations.BindTask(ctx, op.ID, taskID); err != nil {
		t.Fatalf("BindTask: %v", err)
	}
	if err := repos.WalletOperations.MarkUnknown(ctx, op.ID, taskID, "outcome unavailable"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("MarkUnknown without attempt error=%v", err)
	}
	if err := repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, taskID); err != nil {
		t.Fatalf("MarkBroadcastAttempted: %v", err)
	}
	if err := repos.WalletOperations.MarkUnknown(ctx, op.ID, taskID, "outcome unavailable"); err != nil {
		t.Fatalf("MarkUnknown: %v", err)
	}
	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil || got == nil || got.Status != model.WalletOperationStatusUnknown || got.TaskID != nil || got.CompletedAt == nil {
		t.Fatalf("unknown operation=%#v err=%v", got, err)
	}
}

func TestWalletOperationRepoConfirmedWithoutTransaction(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	op := createWalletOperation(t, repos, model.WalletOperationTypeApprove, "approve-no-tx", "0")
	taskID := createWalletTask(t, repos, op.ID, "approve-no-tx")
	if err := repos.WalletOperations.BindTask(ctx, op.ID, taskID); err != nil {
		t.Fatalf("BindTask: %v", err)
	}
	if err := repos.WalletOperations.MarkConfirmedWithoutTransaction(ctx, op.ID, taskID); err != nil {
		t.Fatalf("MarkConfirmedWithoutTransaction: %v", err)
	}
	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil || got == nil || got.Status != model.WalletOperationStatusConfirmed || got.TxHash != nil || got.SubmittedAt != nil || got.TaskID != nil {
		t.Fatalf("confirmed operation=%#v err=%v", got, err)
	}
}

func TestWalletTaskBindingRejectsInvalidLogicalReferences(t *testing.T) {
	for _, scenario := range []string{"missing", "wrong_type", "wrong_subject", "wrong_operation", "archived"} {
		t.Run(scenario, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			op := createWalletOperation(t, repos, model.WalletOperationTypeFund, scenario, "100")
			taskID := int64(1)
			if scenario != "missing" {
				taskID = createWalletTask(t, repos, op.ID, scenario)
				query := db.NewUpdate().Model((*model.Task)(nil)).Where("id = ?", taskID)
				switch scenario {
				case "wrong_type":
					query = query.Set("type = ?", model.TaskTypeCacheEvict)
				case "wrong_subject":
					query = query.Set("subject_key = ?", strconv.FormatInt(op.ID+1, 10))
				case "wrong_operation":
					query = query.Set("input_json = ?", []byte(fmt.Sprintf(`{"operation_id":%d}`, op.ID+1)))
				case "archived":
					query = query.Set("status = ?", model.TaskStatusFailed).Set("failure_reason = ?", "test_failure").Set("finished_at = ?", time.Now())
				}
				if _, err := query.Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				if scenario == "archived" {
					if err := repos.Tasks.AcknowledgeFailed(t.Context(), taskID); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := repos.WalletOperations.BindTask(t.Context(), op.ID, taskID); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("BindTask = %v, want conflict", err)
			}
			stored, err := repos.WalletOperations.GetByID(t.Context(), op.ID)
			if err != nil || stored == nil || stored.TaskID != nil || stored.Status != model.WalletOperationStatusPending {
				t.Fatalf("rejected binding changed owner: %#v, %v", stored, err)
			}
		})
	}
}

func createWalletOperation(t *testing.T, repos *repository.Repositories, operationType model.WalletOperationType, requestID, amount string) *model.WalletOperation {
	t.Helper()
	op, _, err := repos.WalletOperations.CreateOrGet(t.Context(), repository.CreateWalletOperationInput{
		Type: operationType, ClientRequestID: requestID, Amount: amount,
	})
	if err != nil {
		t.Fatalf("CreateOrGet: %v", err)
	}
	return op
}

func createWalletTask(t *testing.T, repos *repository.Repositories, operationID int64, key string) int64 {
	t.Helper()
	row, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
		Type: model.TaskTypeWalletOperation, IdempotencyKey: "wallet:" + key,
		InputVersion: 1, Input: []byte(fmt.Sprintf(`{"operation_id":%d}`, operationID)), InputHash: key,
		SubjectType: new("wallet_operation"), SubjectKey: new(strconv.FormatInt(operationID, 10)),
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
		AvailableAt: time.Now(),
	}))
	if err != nil || !created {
		t.Fatalf("enqueue wallet task row=%#v created=%t err=%v", row, created, err)
	}
	return row.ID
}
