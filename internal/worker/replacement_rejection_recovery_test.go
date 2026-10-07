package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

type observeTargetAfterCoordinatorRead struct {
	repository.StorageContentRepository
	repos    *repository.Repositories
	handlers *worker.TaskHandlers
	input    repository.BindObservedReplacementServiceInput
	observed atomic.Bool
	result   chan error
}

func (r *observeTargetAfterCoordinatorRead) GetDataSetBindingByID(ctx context.Context, id int64) (*model.StorageDataSet, error) {
	stale, err := r.StorageContentRepository.GetDataSetBindingByID(ctx, id)
	if err == nil && id == r.input.StorageDataSetID && r.observed.CompareAndSwap(false, true) {
		err = r.repos.WithTx(ctx, func(repos *repository.Repositories) error {
			if err := repos.Replacements.BindObservedService(ctx, r.input); err != nil {
				return err
			}
			ready, err := repos.Contents.GetDataSetBindingByID(ctx, id)
			if err != nil {
				return err
			}
			return r.handlers.ContinueReadyDataSet(ctx, repos, ready)
		})
		r.result <- err
	}
	return stale, err
}

func TestCoordinatorDiscardsRefusalAfterObservedService(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{register: func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
		if err := h.RegisterStorage(registry); err != nil {
			return err
		}
		return h.RegisterReplacement(registry)
	}})
	row, target, coordinator, input := seedRefusedReplacement(t, runtime)
	wrapper := &observeTargetAfterCoordinatorRead{
		StorageContentRepository: runtime.repos.Contents, repos: runtime.repos, handlers: runtime.handlers,
		input: input, result: make(chan error, 1),
	}
	runtime.repos.Contents = wrapper
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	select {
	case err := <-wrapper.result:
		if err != nil {
			t.Fatalf("observation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not read the refused target")
	}
	completed := waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusCompleted
	}, 15*time.Second)
	if completed.Status != model.TaskStatusCompleted {
		t.Fatalf("stale refusal stopped coordinator: %#v", completed)
	}
	stored, err := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
	if err != nil || stored.Status != storagereplacement.StatusCompleted || stored.FailureReason != nil {
		t.Fatalf("recovered replacement: %#v %v", stored, err)
	}
	ready, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
	if err != nil || ready.Status != model.StorageDataSetStatusReady || !ready.IsCurrent || ready.DataSetID == nil || !ready.DataSetID.Equal(testOnChainID(t, 2002)) || len(ready.CreationRejection) != 0 {
		t.Fatalf("observed service did not take the slot: %#v %v", ready, err)
	}
	ensureCount, err := runtime.db.NewSelect().Model((*model.Task)(nil)).Where("type = ?", model.TaskTypeStorageDataSetEnsure).Count(t.Context())
	if err != nil || ensureCount != 1 {
		t.Fatalf("unexpected new setup: count=%d %v", ensureCount, err)
	}
	retireCount, err := runtime.db.NewSelect().Model((*model.Task)(nil)).Where("type = ?", model.TaskTypeStorageDataSetRetire).Count(t.Context())
	if err != nil || retireCount != 0 {
		t.Fatalf("local source scheduled remote retirement: count=%d %v", retireCount, err)
	}
}

func seedRefusedReplacement(t *testing.T, runtime handlerTestRuntime) (*storagereplacement.Replacement, *model.StorageDataSet, *model.Task, repository.BindObservedReplacementServiceInput) {
	t.Helper()
	bucket := &model.Bucket{Name: "review-stale-rejection", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	source, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: testOnChainID(t, 101), CopyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	row, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 202), ClientRequestID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	ensure, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(row.TargetDataSetID), Input: storagepipeline.DataSetInput{DataSetID: row.TargetDataSetID}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, row.TargetDataSetID, task.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
	if err != nil {
		t.Fatal(err)
	}
	clientID := testOnChainID(t, 909)
	if err := runtime.repos.Contents.RecordDataSetClientID(t.Context(), target.ID, ensure.ID, clientID); err != nil {
		t.Fatal(err)
	}
	identity := testutil.DefaultContextIdentity
	evidence := model.DataSetCreationRejection{Version: 1, StatusCode: 403, RejectedAt: time.Now().Add(-time.Minute), AbsenceCheckedAt: time.Now(), ClientDataSetID: clientID, Payer: identity.Payer, ChainID: uint64(identity.ChainID), RecordKeeper: identity.RecordKeeper}
	if err := runtime.repos.Contents.RecordDataSetCreationRejection(t.Context(), target.ID, target.Generation, ensure.ID, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw("UPDATE tasks SET status = ?, finished_at = ?, failure_reason = ? WHERE id = ?", model.TaskStatusFailed, time.Now(), "dataset_provider_rejected", ensure.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	coordinator, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: storagereplacement.CoordinateTaskKey(row.ID, row.TaskGeneration), Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, task.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	target, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	row, err = runtime.repos.Replacements.GetByID(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	check := &repository.ReplacementPreflight{Source: *source, Targets: []repository.ReplacementTargetCheck{{Replacement: *row, DataSet: *target}}}
	return row, target, coordinator, repository.BindObservedReplacementServiceInput{Preflight: *check, StorageDataSetID: target.ID, DataSetID: testOnChainID(t, 2002), ClientDataSetID: clientID}
}

func TestObservedServiceAfterRefusalSettlementCanContinueOrRetire(t *testing.T) {
	for _, action := range []string{"retry", "another provider"} {
		t.Run(action, func(t *testing.T) {
			terminator := &testServiceTerminator{result: &synapse.TerminationResult{TxHash: "0xretired", EndEpoch: 84}}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, register: func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
				if err := h.RegisterStorage(registry); err != nil {
					return err
				}
				return h.RegisterReplacement(registry)
			}})
			row, target, coordinator, _ := seedRefusedReplacement(t, runtime)
			cancel, done := runHandlerEngine(t, runtime)
			waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
			stopHandlerEngine(t, cancel, done)
			check, err := runtime.repos.Replacements.Preflight(t.Context(), row.SourceDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.WithTx(t.Context(), func(repos *repository.Repositories) error {
				if err := repos.Replacements.BindObservedService(t.Context(), repository.BindObservedReplacementServiceInput{Preflight: *check, StorageDataSetID: target.ID, DataSetID: testOnChainID(t, 2002), ClientDataSetID: *target.ClientDataSetID}); err != nil {
					return err
				}
				ready, err := repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
				if err != nil {
					return err
				}
				return runtime.handlers.ContinueReadyDataSet(t.Context(), repos, ready)
			}); err != nil {
				t.Fatal(err)
			}
			if action == "retry" {
				row, err = runtime.repos.Replacements.Retry(t.Context(), repository.RetryReplacementInput{ReplacementID: row.ID})
			} else {
				_, _, err = runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: row.BucketID, SourceDataSetID: row.SourceDataSetID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 303), ClientRequestID: "successor"})
				if err == nil {
					row, err = runtime.repos.Replacements.GetByID(t.Context(), row.ID)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			coordinator, _, err = runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: storagereplacement.CoordinateTaskKey(row.ID, row.TaskGeneration), Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
				return repos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, task.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			cancel, done = runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			if action == "another provider" {
				target, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
				if err != nil || target.RetirementTaskID == nil {
					t.Fatalf("cleanup coordinator did not enqueue retirement: %#v err=%v", target, err)
				}
				retirementID := *target.RetirementTaskID
				waitForTask(t, runtime.repos, retirementID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusPending && terminator.calls.Load() == 1
				})
				wakeTask(t, runtime, retirementID)
				waitForTask(t, runtime.repos, retirementID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
				target, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
				if err != nil {
					t.Fatal(err)
				}
				stored, err := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
				if err != nil || target.Status != model.StorageDataSetStatusRetired || target.IsCurrent || stored.AbandonedTerminationEpoch == nil || *stored.AbandonedTerminationEpoch != 84 || terminator.calls.Load() != 1 {
					t.Fatalf("recovered abandoned service was not retired: target=%#v replacement=%#v calls=%d err=%v", target, stored, terminator.calls.Load(), err)
				}
				source, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), row.SourceDataSetID)
				if err != nil || !source.IsCurrent {
					t.Fatalf("source lost its slot before successor setup: %#v err=%v", source, err)
				}
			} else {
				target, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
				stored, loadErr := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
				if err != nil || loadErr != nil || !target.IsCurrent || stored.Status != storagereplacement.StatusCompleted || stored.FailureReason != nil || terminator.calls.Load() != 0 {
					t.Fatalf("recovered replacement did not continue: target=%#v replacement=%#v err=%v/%v", target, stored, err, loadErr)
				}
			}
		})
	}
}
