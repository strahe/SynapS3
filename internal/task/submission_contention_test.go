package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type submissionContentionHandler struct {
	taskengine.Handler
	returned func()
}

func (h submissionContentionHandler) Execute(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	result := h.Handler.Execute(ctx, execution)
	h.returned()
	return result
}

func (h submissionContentionHandler) Recover(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	result := h.Handler.Recover(ctx, execution)
	h.returned()
	return result
}

// Handler writes and the first final settlement transaction fail before the
// database permits the same evidence to be settled.
func submissionContention(t *testing.T, runtime *handlerTestRuntime, taskType model.TaskType, table, event, condition string) {
	t.Helper()
	for _, statement := range []string{
		"CREATE TABLE test_submission_contention_gate (callback_returned boolean NOT NULL)",
		"INSERT INTO test_submission_contention_gate VALUES (false)",
		"CREATE SEQUENCE test_submission_settlement_writes",
		`CREATE FUNCTION test_submission_contention() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  IF NOT (SELECT callback_returned FROM test_submission_contention_gate)
		     OR nextval('test_submission_settlement_writes') = 1 THEN
		    RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'submission evidence transaction contention';
		  END IF;
		  RETURN NEW;
		END $$`,
		fmt.Sprintf("CREATE TRIGGER test_submission_contention BEFORE %s ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION test_submission_contention()", event, table, condition),
	} {
		if _, err := runtime.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	returned := func() {
		t.Helper()
		if _, err := runtime.db.ExecContext(t.Context(), "UPDATE test_submission_contention_gate SET callback_returned = true"); err != nil {
			t.Fatal(err)
		}
	}
	registry := taskengine.NewRegistry()
	for _, currentType := range runtime.registry.Types() {
		handler, _ := runtime.registry.Handler(currentType)
		if currentType == taskType {
			handler = submissionContentionHandler{Handler: handler, returned: returned}
		}
		if err := registry.Register(handler); err != nil {
			t.Fatal(err)
		}
	}
	runtime.registry = registry
}

func TestDataSetCreationRetainsSubmissionThroughDatabaseContention(t *testing.T) {
	providerID := testOnChainID(t, 51000+storedObjectSequence.Add(1))
	var creates, observations atomic.Int64
	target := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		CreateDataSetFunc: func(ctx context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			creates.Add(1)
			opts.OnSubmitted(storage.CreateDataSetSubmission{TransactionID: "0xsubmitted", StatusURL: "https://provider.example/created/pending"})
			return nil, ctx.Err()
		},
		WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
			observations.Add(1)
			return nil, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return target, nil
		}}, policy: cache.EvictionPolicyNone,
	})
	fixture := seedDataSetEnsure(t, runtime, providerID, "creation-submission-contention")
	submissionContention(t, &runtime, model.TaskTypeStorageDataSetEnsure, "tasks", "UPDATE OF checkpoint_json", fmt.Sprintf("NEW.id = %d AND NEW.checkpoint_json->>'transaction_id' = '0xsubmitted'", fixture.ensureTask.ID))
	row := runStorageTaskClaims(t, runtime, fixture.ensureTask, model.TaskStatusPending, 1, 3*handlerTestLeaseDuration)
	if row.RetryCount != 0 || row.ResumeMode != model.TaskResumeModeRecover || creates.Load() != 1 {
		t.Fatalf("contention changed creation opportunity: task=%#v sends=%d", row, creates.Load())
	}
	var checkpoint struct {
		TxHash    string `json:"transaction_id"`
		StatusURL string `json:"status_url"`
	}
	if err := json.Unmarshal(row.Checkpoint, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.TxHash != "0xsubmitted" || checkpoint.StatusURL != "https://provider.example/created/pending" {
		t.Fatalf("creation checkpoint lost submission: %#v", checkpoint)
	}
	binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	if binding.TxHash == nil || *binding.TxHash != checkpoint.TxHash || binding.CreateStatusURL == nil || *binding.CreateStatusURL != checkpoint.StatusURL {
		t.Fatalf("creation ledger lost submission: %#v", binding)
	}
	wakeTask(t, runtime, row.ID)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if creates.Load() != 1 || observations.Load() != 1 || row.RetryCount != 0 {
		t.Fatalf("creation recovery did not observe the saved submission: task=%#v sends=%d reads=%d", row, creates.Load(), observations.Load())
	}
}

func TestRetirementRetainsSubmissionThroughDatabaseContention(t *testing.T) {
	terminator := &testServiceTerminator{
		result: &synapse.TerminationResult{TxHash: "0xsubmitted", ConfirmedTxHash: "0xconfirmed", ReceiptStatus: new(uint64(1))},
		err:    errors.New("service end epoch is not available yet"),
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, retireOnly: true})
	replacement, row := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	submissionContention(t, &runtime, model.TaskTypeStorageDataSetRetire, "storage_data_set_termination_attempts", "UPDATE", "NEW.tx_hash = '0xsubmitted'")
	row = runStorageTaskClaims(t, runtime, row, model.TaskStatusPending, 1, 3*handlerTestLeaseDuration)
	if row.RetryCount != 0 || row.ResumeMode != model.TaskResumeModeRecover || terminator.calls.Load() != 1 {
		t.Fatalf("contention changed retirement opportunity: task=%#v sends=%d", row, terminator.calls.Load())
	}
	var checkpoint struct {
		TxHash          string  `json:"transaction_hash"`
		ConfirmedTxHash string  `json:"confirmed_tx_hash"`
		ReceiptStatus   *uint64 `json:"receipt_status"`
	}
	if err := json.Unmarshal(row.Checkpoint, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.TxHash != "0xsubmitted" || checkpoint.ConfirmedTxHash != "0xconfirmed" || checkpoint.ReceiptStatus == nil || *checkpoint.ReceiptStatus != 1 {
		t.Fatalf("retirement checkpoint lost submission: %#v", checkpoint)
	}
	attempts, err := runtime.repos.Replacements.ListTerminationAttempts(t.Context(), []int64{replacement.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].TxHash == nil || *attempts[0].TxHash != checkpoint.TxHash || attempts[0].ConfirmedTxHash == nil || *attempts[0].ConfirmedTxHash != checkpoint.ConfirmedTxHash || attempts[0].ReceiptStatus == nil || *attempts[0].ReceiptStatus != 1 {
		t.Fatalf("retirement ledger lost submission: %#v", attempts)
	}
	wakeTask(t, runtime, row.ID)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if terminator.calls.Load() != 1 || row.RetryCount != 0 {
		t.Fatalf("retirement recovery sent another transaction: task=%#v sends=%d", row, terminator.calls.Load())
	}
}
