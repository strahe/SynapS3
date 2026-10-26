package repository_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/walletoperation"
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
	maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	if _, created, err := repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{
		Type: model.WalletOperationTypeFund, ClientRequestID: "maximum", Amount: maximum.String(),
	}); err != nil || !created {
		t.Fatalf("maximum amount created=%t err=%v", created, err)
	}
	for _, input := range []repository.CreateWalletOperationInput{
		{Type: model.WalletOperationTypeFund, ClientRequestID: "zero", Amount: "0"},
		{Type: model.WalletOperationTypeWithdraw, ClientRequestID: "negative", Amount: "-1"},
		{Type: model.WalletOperationTypeApprove, ClientRequestID: "approve-positive", Amount: "1"},
		{Type: model.WalletOperationType("other"), ClientRequestID: "unknown", Amount: "1"},
		{Type: model.WalletOperationTypeFund, ClientRequestID: "overflow", Amount: maximum.Add(maximum, big.NewInt(1)).String()},
	} {
		if _, _, err := repos.WalletOperations.CreateOrGet(ctx, input); !errors.Is(err, repository.ErrWalletOperationInvalidAmount) {
			t.Errorf("CreateOrGet(%#v) error=%v", input, err)
		}
	}
}

func TestWalletOperationRepoPreservesSubmissionEvidence(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	op := createWalletOperation(t, repos, model.WalletOperationTypeFund, "signed", "100")
	taskID := createWalletTask(t, repos, op.ID, "signed")
	if err := repos.WalletOperations.BindTask(ctx, op.ID, taskID); err != nil {
		t.Fatal(err)
	}
	raw := []byte{0x02, 0x00, 0xff}
	if err := repos.WalletOperations.RecordSubmission(ctx, op.ID, taskID, "tx-one", raw, "send timeout"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("submission without broadcast evidence: %v", err)
	}
	if err := repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, taskID); err != nil {
		t.Fatal(err)
	}
	for _, submission := range []struct {
		hash string
		raw  []byte
	}{
		{"tx-one", nil}, {"tx-one", raw}, {"tx-one", bytes.Clone(raw)}, {"tx-one", nil},
	} {
		if err := repos.WalletOperations.RecordSubmission(ctx, op.ID, taskID, submission.hash, submission.raw, "send timeout"); err != nil {
			t.Fatalf("record submission: %v", err)
		}
	}
	for _, submission := range []struct {
		taskID int64
		hash   string
		raw    []byte
	}{
		{taskID + 1, "tx-one", raw}, {taskID, "tx-two", raw}, {taskID, "tx-one", []byte{3}},
	} {
		if err := repos.WalletOperations.RecordSubmission(ctx, op.ID, submission.taskID, submission.hash, submission.raw, "overwrite"); !errors.Is(err, repository.ErrNotFound) {
			t.Fatalf("conflicting submission: %v", err)
		}
	}
	injected := errors.New("rollback")
	if err := repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.WalletOperations.RecordConfirmationError(ctx, op.ID, taskID, "temporary"); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	stored, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil || stored.Status != model.WalletOperationStatusSubmitted || !bytes.Equal(stored.SignedTransaction, raw) || stored.TxHash == nil || *stored.TxHash != "tx-one" || stored.LastError == nil || *stored.LastError != "send timeout" {
		t.Fatalf("submission evidence changed: %#v err=%v", stored, err)
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

func TestWalletOperationRepoSubmissionUnknownKeepsTransactionEvidence(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	op := createWalletOperation(t, repos, model.WalletOperationTypeFund, "displaced", "100")
	taskID := createWalletTask(t, repos, op.ID, "displaced")
	if err := repos.WalletOperations.BindTask(ctx, op.ID, taskID); err != nil {
		t.Fatal(err)
	}
	if err := repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, taskID); err != nil {
		t.Fatal(err)
	}
	if err := repos.WalletOperations.MarkSubmissionUnknown(ctx, op.ID, taskID, "nonce used"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("pending operation marked as an unknown submission: %v", err)
	}
	raw := []byte{0x02, 0x00, 0xff}
	if err := repos.WalletOperations.RecordSubmission(ctx, op.ID, taskID, "0xdisplaced", raw, ""); err != nil {
		t.Fatal(err)
	}
	if err := repos.WalletOperations.MarkSubmissionUnknown(ctx, op.ID, taskID+1, "nonce used"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("stale task marked the submission unknown: %v", err)
	}
	if err := repos.WalletOperations.MarkSubmissionUnknown(ctx, op.ID, taskID, "nonce used"); err != nil {
		t.Fatal(err)
	}
	got, err := repos.WalletOperations.GetByID(ctx, op.ID)
	if err != nil || got.Status != model.WalletOperationStatusUnknown || got.TxHash == nil || *got.TxHash != "0xdisplaced" ||
		!bytes.Equal(got.SignedTransaction, raw) || got.TaskID != nil || got.CompletedAt == nil {
		t.Fatalf("unknown submission = %#v err=%v", got, err)
	}
	if err := repos.WalletOperations.MarkSubmissionUnknown(ctx, op.ID, taskID, "nonce used"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("terminal operation changed again: %v", err)
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
					query = query.Set("input_json = ?", json.RawMessage(fmt.Sprintf(`{"operation_id":%d}`, op.ID+1)))
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

func TestWalletOperationRepoReopensOnlyProvenUnsentRetry(t *testing.T) {
	for _, scenario := range []string{"allowed", "unrelated successor", "different identity", "missing proof", "source active", "submitted", "unknown", "transaction evidence"} {
		t.Run(scenario, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := t.Context()
			op := createWalletOperation(t, repos, model.WalletOperationTypeFund, scenario, "100")
			sourceID := createWalletTask(t, repos, op.ID, scenario)
			if err := repos.WalletOperations.BindTask(ctx, op.ID, sourceID); err != nil {
				t.Fatal(err)
			}
			if err := repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, sourceID); err != nil {
				t.Fatal(err)
			}
			if scenario == "submitted" || scenario == "transaction evidence" {
				if err := repos.WalletOperations.RecordSubmission(ctx, op.ID, sourceID, "known-transaction", nil, ""); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "unknown" {
				if err := repos.WalletOperations.MarkUnknown(ctx, op.ID, sourceID, "outcome unknown"); err != nil {
					t.Fatal(err)
				}
			} else if scenario != "submitted" {
				if err := repos.WalletOperations.MarkFailed(ctx, op.ID, sourceID, "not sent"); err != nil {
					t.Fatal(err)
				}
			}
			identity := testutil.DefaultWalletExecutionIdentity()
			input := walletoperation.Input{ExecutionIdentity: identity, OperationID: op.ID}
			checkpoint := walletoperation.Checkpoint{ExecutionIdentity: identity, BroadcastAttempted: true, BroadcastNotSent: scenario != "missing proof"}
			rawInput, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			rawCheckpoint, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			query := db.NewUpdate().Model((*model.Task)(nil)).Set("input_json = ?", json.RawMessage(rawInput)).Set("checkpoint_json = ?", json.RawMessage(rawCheckpoint)).Where("id = ?", sourceID)
			if scenario != "source active" {
				query = query.Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Set("superseded_at = ?", time.Now()).Set("failure_reason = ?", "wallet_broadcast_not_sent")
			}
			if _, err := query.Exec(ctx); err != nil {
				t.Fatal(err)
			}
			nextID := createWalletTask(t, repos, op.ID, scenario+"-next")
			if scenario == "different identity" {
				changed := *input.Identity
				changed.Payer[0] ^= 0xff
				input.Identity = &changed
				rawInput, err = json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
			}
			query = db.NewUpdate().Model((*model.Task)(nil)).Set("input_json = ?", json.RawMessage(rawInput)).Where("id = ?", nextID)
			if scenario != "unrelated successor" {
				query = query.Set("retry_of_task_id = ?", sourceID)
			}
			if _, err := query.Exec(ctx); err != nil {
				t.Fatal(err)
			}
			before, err := repos.WalletOperations.GetByID(ctx, op.ID)
			if err != nil {
				t.Fatal(err)
			}
			err = repos.WalletOperations.ReopenUnsentForRetry(ctx, op.ID, sourceID, nextID)
			stored, readErr := repos.WalletOperations.GetByID(ctx, op.ID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario == "allowed" {
				if err != nil || stored.Status != model.WalletOperationStatusPending || stored.TaskID == nil || *stored.TaskID != nextID || stored.BroadcastAttemptedAt != nil || stored.CompletedAt != nil || stored.LastError != nil || stored.StartedAt == nil || !stored.StartedAt.Equal(*before.StartedAt) {
					t.Fatalf("reopened operation = %#v, error=%v", stored, err)
				}
			} else if !errors.Is(err, repository.ErrConflict) || stored.Status != before.Status || !reflect.DeepEqual(stored.TaskID, before.TaskID) || !reflect.DeepEqual(stored.TxHash, before.TxHash) || !reflect.DeepEqual(stored.CompletedAt, before.CompletedAt) {
				t.Fatalf("rejected retry changed operation: before=%#v after=%#v error=%v", before, stored, err)
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
		Type:         model.TaskTypeWalletOperation,
		InputVersion: 1, Input: []byte(fmt.Sprintf(`{"operation_id":%d}`, operationID)),
		SubjectType: "wallet_operation", SubjectKey: strconv.FormatInt(operationID, 10),
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
		AvailableAt: time.Now(),
	}))
	if err != nil || !created {
		t.Fatalf("enqueue wallet task row=%#v created=%t err=%v", row, created, err)
	}
	return row.ID
}
