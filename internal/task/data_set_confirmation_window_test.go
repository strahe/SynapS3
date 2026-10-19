package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
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

func TestDataSetGenerationCreatesOwnRequestAfterReturningToProvider(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerA, providerB := testOnChainID(t, 131000+sequence), testOnChainID(t, 132000+sequence)
	oldDataSetID, newDataSetID := sdktypes.NewBigInt(uint64(133000+sequence)), sdktypes.NewBigInt(uint64(134000+sequence))
	var createsA, createsB, metadataReads atomic.Int64
	var oldRequest atomic.Value
	var lateResultAvailable atomic.Bool
	targetA := &testutil.MockStorageTarget{
		ProviderIDValue: providerA.SDK(),
		CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			if createsA.Add(1) == 1 {
				oldRequest.Store(options.ClientDataSetID.Copy())
				return nil, context.DeadlineExceeded
			}
			ref, err := storage.NewDataSetRef(providerA.SDK(), newDataSetID, *options.ClientDataSetID)
			return &storage.CreateDataSetResult{DataSet: ref}, err
		},
	}
	targetB := &testutil.MockStorageTarget{
		ProviderIDValue: providerB.SDK(),
		CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			createsB.Add(1)
			ref, err := storage.NewDataSetRef(providerB.SDK(), sdktypes.NewBigInt(uint64(135000+sequence)), *options.ClientDataSetID)
			return &storage.CreateDataSetResult{DataSet: ref}, err
		},
	}
	budget := 1
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		policy: cache.EvictionPolicyNone, maxAttempts: &budget,
		storage: &testutil.MockStorageClient{
			OpenProviderTargetFunc: func(_ context.Context, providerID sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				if providerID.Equal(providerA.SDK()) {
					return targetA, nil
				}
				return targetB, nil
			},
			FindMatchingDataSetFunc: func(_ context.Context, providerID sdktypes.BigInt, _ map[string]string, _ bool) (*storage.DataSetRef, error) {
				metadataReads.Add(1)
				if !providerID.Equal(providerA.SDK()) || !lateResultAvailable.Load() {
					return nil, nil
				}
				ref, err := storage.NewDataSetRef(providerA.SDK(), oldDataSetID, oldRequest.Load().(sdktypes.BigInt))
				return &ref, err
			},
		},
	})
	ctx := t.Context()
	bucket := &model.Bucket{Name: fmt.Sprintf("generation-request-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: providerA, CopyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	firstEnsure, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeStorageDataSetEnsure, Input: storagepipeline.DataSetInput{ExecutionIdentity: testStorageExecutionIdentity(), DataSetID: source.ID}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, source.ID, row.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	runOneStorageTask(t, runtime, firstEnsure, model.TaskStatusFailed)
	first, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: providerB, ClientRequestID: "provider-b"})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeProviderReplacementCoordinate, Input: storagereplacement.CoordinateInput{ReplacementID: first.ID, Generation: first.TaskGeneration}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, first.ID, first.TaskGeneration, row.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		cancel, done := runHandlerEngine(t, runtime)
		defer stopHandlerEngine(t, cancel, done)
		waitForTask(t, runtime.repos, coordinator.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
	}()
	lateResultAvailable.Store(true)
	second, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: first.TargetDataSetID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: providerA, ClientRequestID: "provider-a"})
	if err != nil {
		t.Fatal(err)
	}
	nextEnsure, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeStorageDataSetEnsure, Input: storagepipeline.DataSetInput{ExecutionIdentity: testStorageExecutionIdentity(), DataSetID: second.TargetDataSetID}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, second.TargetDataSetID, row.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	runOneStorageTask(t, runtime, nextEnsure, model.TaskStatusCompleted)
	current, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, second.TargetDataSetID)
	if err != nil || current.Status != model.StorageDataSetStatusReady || current.DataSetID == nil || !current.DataSetID.SDK().Equal(newDataSetID) || current.ClientDataSetID == nil || current.ClientDataSetID.SDK().Equal(oldRequest.Load().(sdktypes.BigInt)) {
		t.Fatalf("new generation adopted an abandoned creation: binding=%#v err=%v", current, err)
	}
	abandoned, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, source.ID)
	if err != nil || abandoned.IsCurrent || abandoned.EnsureTaskID != nil || abandoned.DataSetID != nil || abandoned.ClientDataSetID == nil || !abandoned.ClientDataSetID.SDK().Equal(oldRequest.Load().(sdktypes.BigInt)) {
		t.Fatalf("abandoned request evidence changed: binding=%#v err=%v", abandoned, err)
	}
	if createsA.Load() != 2 || createsB.Load() != 1 || metadataReads.Load() != 0 {
		t.Fatalf("generation requests: provider A=%d provider B=%d metadata reads=%d", createsA.Load(), createsB.Load(), metadataReads.Load())
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
