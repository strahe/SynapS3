package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestDataSetConfirmationWindowSurvivesWakeAndClaimRecovery(t *testing.T) {
	fixture := newDataSetConfirmationFixture(t, 2)
	waiting := runOneStorageTask(t, fixture.runtime, fixture.ensure.ensureTask, model.TaskStatusPending)
	initial := dataSetConfirmationRuntime(t, waiting)
	checkpoint := string(waiting.Checkpoint)
	if initial.OperationStartedAt == nil || initial.OperationKey != fmt.Sprintf("ensure:%d", fixture.ensure.binding.ID) {
		t.Fatalf("confirmation origin is missing: %#v", initial)
	}
	if _, err := fixture.runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("available_at = ?", time.Now().Add(time.Hour)).Where("id = ?", waiting.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if woken, err := fixture.runtime.repos.Tasks.WakePending(t.Context(), []int64{waiting.ID}); err != nil || woken != 1 {
		t.Fatalf("wake confirmation: woken=%d err=%v", woken, err)
	}
	waiting = runOneStorageTask(t, fixture.runtime, waiting, model.TaskStatusPending)
	if _, err := fixture.runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusRunning).
		Set("lease_until = ?", time.Now().Add(-time.Second)).
		Set("wait_reason = NULL").
		Where("id = ?", waiting.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire confirmation claim: %v", err)
	}
	waiting = runOneStorageTask(t, fixture.runtime, waiting, model.TaskStatusPending)
	current := dataSetConfirmationRuntime(t, waiting)
	if waiting.RetryCount != 0 || current.OperationStartedAt == nil || !current.OperationStartedAt.Equal(*initial.OperationStartedAt) || string(waiting.Checkpoint) != checkpoint {
		t.Fatalf("wake or claim recovery changed the confirmation opportunity: task=%#v runtime=%#v", waiting, current)
	}
	if fixture.creates.Load() != 1 || fixture.confirmations.Load() != 3 {
		t.Fatalf("confirmation recovery performed extra sends: creates=%d confirmations=%d", fixture.creates.Load(), fixture.confirmations.Load())
	}
}

func TestDataSetConfirmationManualRetryStartsFreshWindowAndPreservesRequest(t *testing.T) {
	fixture := newDataSetConfirmationFixture(t, 1)
	waiting := runOneStorageTask(t, fixture.runtime, fixture.ensure.ensureTask, model.TaskStatusPending)
	waiting = setDataSetConfirmationStart(t, fixture.runtime, waiting, time.Now().UTC().Add(-31*time.Minute))
	failed := runOneStorageTask(t, fixture.runtime, waiting, model.TaskStatusFailed)
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" {
		t.Fatalf("expired confirmation did not exhaust its budget: %#v", failed)
	}
	next, err := fixture.runtime.service.Retry(t.Context(), failed.ID)
	if err != nil {
		t.Fatal(err)
	}
	fresh := dataSetConfirmationRuntime(t, next)
	if fresh.OperationStartedAt != nil || fresh.OperationKey != "" || fresh.LastAdmittedAttempt != 0 || string(next.Checkpoint) != string(failed.Checkpoint) {
		t.Fatalf("manual retry did not reset runtime while retaining evidence: task=%#v runtime=%#v", next, fresh)
	}
	waiting = runOneStorageTask(t, fixture.runtime, next, model.TaskStatusPending)
	fresh = dataSetConfirmationRuntime(t, waiting)
	if waiting.RetryCount != 0 || fresh.OperationStartedAt == nil || time.Since(*fresh.OperationStartedAt) >= time.Minute || string(waiting.Checkpoint) != string(failed.Checkpoint) {
		t.Fatalf("manual retry reused an expired window or changed its request: task=%#v runtime=%#v", waiting, fresh)
	}
	if fixture.creates.Load() != 1 || fixture.confirmations.Load() != 3 {
		t.Fatalf("manual retry resent submitted creation: creates=%d confirmations=%d", fixture.creates.Load(), fixture.confirmations.Load())
	}
	binding, err := fixture.runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.ensure.binding.ID)
	if err != nil || binding == nil || binding.EnsureTaskID == nil || *binding.EnsureTaskID != next.ID || binding.ClientDataSetID == nil {
		t.Fatalf("manual retry lost the current request owner: binding=%#v err=%v", binding, err)
	}
	var request struct {
		ClientDataSetID string    `json:"client_data_set_id"`
		NextResendAt    time.Time `json:"next_resend_at"`
	}
	if err := json.Unmarshal(waiting.Checkpoint, &request); err != nil || request.ClientDataSetID != binding.ClientDataSetID.String() || request.NextResendAt.IsZero() {
		t.Fatalf("manual retry lost its request or resend deadline: checkpoint=%#v err=%v", request, err)
	}
}

func TestDataSetConfirmationSuccessWinsAfterWindowExpires(t *testing.T) {
	fixture := newDataSetConfirmationFixture(t, 1)
	waiting := runOneStorageTask(t, fixture.runtime, fixture.ensure.ensureTask, model.TaskStatusPending)
	waiting = setDataSetConfirmationStart(t, fixture.runtime, waiting, time.Now().UTC().Add(-31*time.Minute))
	fixture.ready.Store(true)
	completed := runOneStorageTask(t, fixture.runtime, waiting, model.TaskStatusCompleted)
	binding, err := fixture.runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.ensure.binding.ID)
	if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.DataSetID.String() != fixture.dataSetID.String() || completed.RetryCount != 0 {
		t.Fatalf("confirmed success lost to the expired observation window: task=%#v binding=%#v err=%v", completed, binding, err)
	}
	if fixture.creates.Load() != 1 || fixture.confirmations.Load() != 2 {
		t.Fatalf("late confirmation performed extra sends: creates=%d confirmations=%d", fixture.creates.Load(), fixture.confirmations.Load())
	}
}

type dataSetConfirmationFixture struct {
	runtime       handlerTestRuntime
	ensure        dataSetEnsureFixture
	dataSetID     sdktypes.BigInt
	creates       atomic.Int64
	confirmations atomic.Int64
	ready         atomic.Bool
}

func newDataSetConfirmationFixture(t *testing.T, budget int) *dataSetConfirmationFixture {
	t.Helper()
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 121000+sequence)
	fixture := &dataSetConfirmationFixture{dataSetID: sdktypes.NewBigInt(uint64(122000 + sequence))}
	target := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			fixture.creates.Add(1)
			options.OnSubmitted(storage.CreateDataSetSubmission{
				TransactionID: "0xconfirmation", StatusURL: "https://provider.example/pdp/data-sets/created/confirmation",
			})
			return nil, nil
		},
		WaitDataSetFunc: func(_ context.Context, _ string, clientID sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
			fixture.confirmations.Add(1)
			if !fixture.ready.Load() {
				return nil, nil
			}
			ref, err := storage.NewDataSetRef(providerID.SDK(), fixture.dataSetID, clientID)
			return &storage.CreateDataSetResult{DataSet: ref}, err
		},
		FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
			return storage.DataSetRef{}, false, nil
		},
	}
	fixture.runtime = newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return target, nil
		}},
		policy: cache.EvictionPolicyNone, maxAttempts: &budget,
	})
	fixture.ensure = seedDataSetEnsure(t, fixture.runtime, providerID, fmt.Sprintf("confirmation-window-%d", sequence))
	return fixture
}

func dataSetConfirmationRuntime(t *testing.T, row *model.Task) model.TaskRuntime {
	t.Helper()
	var operation model.TaskRuntime
	if err := json.Unmarshal(row.Runtime, &operation); err != nil {
		t.Fatalf("decode confirmation runtime: %v", err)
	}
	return operation
}

func setDataSetConfirmationStart(t *testing.T, runtime handlerTestRuntime, row *model.Task, startedAt time.Time) *model.Task {
	t.Helper()
	encoded, err := json.Marshal(startedAt.UTC().Truncate(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("runtime_json = jsonb_set(runtime_json, '{operation_started_at}', ?::jsonb)", string(encoded)).
		Where("id = ?", row.ID).Exec(t.Context()); err != nil {
		t.Fatalf("set confirmation origin: %v", err)
	}
	current, err := runtime.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || current == nil {
		t.Fatalf("load aged confirmation task: %#v err=%v", current, err)
	}
	return current
}
