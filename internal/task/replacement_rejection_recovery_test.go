package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type observeTargetAfterCoordinatorRead struct {
	repository.StorageContentRepository
	repos    *repository.Repositories
	ready    *taskengine.Messenger
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
			return r.ready.Notify(ctx, repos, storagepipeline.DataSetReady{BindingID: ready.ID})
		})
		r.result <- err
	}
	return stale, err
}

func TestCoordinatorDiscardsRefusalAfterObservedService(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	row, target, coordinator, input := seedRefusedReplacement(t, runtime)
	wrapper := &observeTargetAfterCoordinatorRead{
		StorageContentRepository: runtime.repos.Contents, repos: runtime.repos, ready: runtime.ready,
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
	ensureCount, err := runtime.db.NewSelect().Model((*model.Task)(nil)).Where("type = ?", model.TaskTypeStorageDataSetEnsure).
		Where("subject_key = ?", strconv.FormatInt(target.ID, 10)).Count(t.Context())
	if err != nil || ensureCount != 1 {
		t.Fatalf("unexpected new setup: count=%d %v", ensureCount, err)
	}
	retireCount, err := runtime.db.NewSelect().Model((*model.Task)(nil)).Where("type = ?", model.TaskTypeStorageDataSetRetire).Count(t.Context())
	if err != nil || retireCount != 0 {
		t.Fatalf("local source scheduled remote retirement: count=%d %v", retireCount, err)
	}
}

func seedRefusedReplacement(t *testing.T, runtime handlerTestRuntime, readySource ...bool) (*storagereplacement.Replacement, *model.StorageDataSet, *model.Task, repository.BindObservedReplacementServiceInput) {
	t.Helper()
	bucket := &model.Bucket{Name: "review-stale-rejection", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	source, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: testOnChainID(t, 101), CopyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	sourceEnsure, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeStorageDataSetEnsure, Input: storagepipeline.DataSetInput{DataSetID: source.ID}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, source.ID, task.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).
		Set("finished_at = ?", time.Now()).Where("id = ?", sourceEnsure.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(readySource) != 0 && readySource[0] {
		if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: source.ID, DataSetID: testOnChainID(t, 1001)}); err != nil {
			t.Fatal(err)
		}
		if err := runtime.repos.Contents.CompleteDataSetEnsureTask(t.Context(), source.ID, sourceEnsure.ID); err != nil {
			t.Fatal(err)
		}
	}
	row, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 202), ClientRequestID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	ensure, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeStorageDataSetEnsure, Input: storagepipeline.DataSetInput{DataSetID: row.TargetDataSetID}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
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
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("creation_rejection = ?", json.RawMessage(encoded)).Where("id = ?", target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw("UPDATE tasks SET status = ?, finished_at = ?, failure_reason = ? WHERE id = ?", model.TaskStatusFailed, time.Now(), "dataset_provider_rejected", ensure.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	coordinator, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeProviderReplacementCoordinate, Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
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

func TestObservedServiceAfterRefusalSettlementKeepsReplacementReserved(t *testing.T) {
	for _, action := range []string{"retry", "another provider"} {
		t.Run(action, func(t *testing.T) {
			terminator := &testServiceTerminator{}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator})
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
				return runtime.ready.Notify(t.Context(), repos, storagepipeline.DataSetReady{BindingID: ready.ID})
			}); err != nil {
				t.Fatal(err)
			}
			if action == "another provider" {
				_, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: row.BucketID, SourceDataSetID: row.SourceDataSetID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 303), ClientRequestID: "successor"})
				if !errors.Is(err, storagereplacement.ErrActiveReplacement) {
					t.Fatalf("ready target released the replacement slot: %v", err)
				}
				target, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
				if err != nil || target.Status != model.StorageDataSetStatusReady || target.IsCurrent || target.RetirementTaskID != nil || terminator.calls.Load() != 0 {
					t.Fatalf("reserved target changed: %#v err=%v", target, err)
				}
				return
			}
			coordinator, err = runtime.service.Retry(t.Context(), coordinator.ID)
			if err != nil {
				t.Fatal(err)
			}
			cancel, done = runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			target, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
			stored, loadErr := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
			if err != nil || loadErr != nil || !target.IsCurrent || stored.Status != storagereplacement.StatusCompleted || stored.FailureReason != nil || terminator.calls.Load() != 0 {
				t.Fatalf("recovered replacement did not continue: target=%#v replacement=%#v err=%v/%v", target, stored, err, loadErr)
			}
		})
	}
}

func TestInitializationCoordinatorStopsOnFailedChild(t *testing.T) {
	for _, readySource := range []bool{false, true} {
		t.Run(fmt.Sprintf("ready_source=%v", readySource), func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
			row, target, coordinator, _ := seedRefusedReplacement(t, runtime, readySource)
			if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("creation_rejection = NULL").Where("id = ?", target.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			stopped := runStorageTaskClaims(t, runtime, coordinator, model.TaskStatusFailed, 1)
			if stopped.FailureReason == nil || *stopped.FailureReason != "replacement_target_failed" || stopped.RetryCount != 0 {
				t.Fatalf("coordinator reopened initialization budget: %#v", stopped)
			}
			if next, err := runtime.repos.Tasks.GetDirectSuccessor(t.Context(), *target.EnsureTaskID); err != nil || next != nil {
				t.Fatalf("automatic child successor: %#v %v", next, err)
			}
			stored, err := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
			if err != nil || stored.Status != storagereplacement.StatusFailed || stored.TaskID == nil || *stored.TaskID != coordinator.ID {
				t.Fatalf("failed coordinator owner: %#v %v", stored, err)
			}
			if _, _, err := runtime.repos.Replacements.SourceEligibility(t.Context(), row.SourceDataSetID); err != nil {
				t.Fatalf("failed initialization cannot be replaced: %v", err)
			}
		})
	}
}
