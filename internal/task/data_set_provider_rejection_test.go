package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	"github.com/uptrace/bun"
)

func TestDataSetProviderRefusalStopsSendingAndPreservesReplacementEvidence(t *testing.T) {
	for _, tc := range []struct {
		name                string
		status              int
		priorSend           bool
		checkpointedRefusal bool
		lookupFails         bool
		lookupPanics        bool
		settlementFails     bool
		found               bool
	}{
		{name: "400", status: 400},
		{name: "401", status: 401},
		{name: "403", status: 403},
		{name: "timeout then 403", status: 403, priorSend: true},
		{name: "checkpoint recovery", status: 403, priorSend: true, checkpointedRefusal: true},
		{name: "lookup retry", status: 403, lookupFails: true},
		{name: "lookup panic recovery", status: 403, lookupPanics: true},
		{name: "lookup panic finds service", status: 403, lookupPanics: true, found: true},
		{name: "settlement retry", status: 403, settlementFails: true},
		{name: "existing service", status: 403, found: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID := testOnChainID(t, 51000+sequence)
			var creates, lookups atomic.Int64
			var lookupFails atomic.Bool
			lookupFails.Store(tc.lookupFails)
			var lookupPanics atomic.Bool
			lookupPanics.Store(tc.lookupPanics)
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					creates.Add(1)
					return nil, &synapse.DataSetProviderRejectionError{StatusCode: tc.status, Cause: errors.New("provider refused creation")}
				},
				FindDataSetByClientIDFunc: func(_ context.Context, clientID sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					lookups.Add(1)
					if lookupPanics.Swap(false) {
						panic("injected refusal lookup panic")
					}
					if lookupFails.Load() {
						return storage.DataSetRef{}, false, errors.New("chain unavailable")
					}
					if tc.found {
						ref, err := storage.NewDataSetRef(providerID.SDK(), sdktypes.NewBigInt(1001), clientID)
						return ref, err == nil, err
					}
					return storage.DataSetRef{}, false, nil
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					return target, nil
				}},
				policy: cache.EvictionPolicyNone,
			})
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("provider-refusal-%d", sequence))
			var failedWrite *refusalSettlementObserver
			if tc.settlementFails {
				failedWrite = &refusalSettlementObserver{failed: make(chan struct{})}
				runtime.db.AddQueryHook(failedWrite)
				if _, err := runtime.db.ExecContext(t.Context(), `CREATE TRIGGER reject_refusal_settlement BEFORE UPDATE OF creation_rejection ON storage_data_sets BEGIN SELECT RAISE(ABORT, 'injected rejection settlement failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			if tc.priorSend {
				clientID := testOnChainID(t, 61000+sequence)
				if err := runtime.repos.Contents.RecordDataSetClientID(t.Context(), fixture.binding.ID, fixture.ensureTask.ID, clientID); err != nil {
					t.Fatal(err)
				}
				checkpoint := map[string]any{"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(), "identity": testutil.DefaultContextIdentity, "sends": 1}
				if tc.checkpointedRefusal {
					checkpoint["provider_rejection"] = map[string]any{"status_code": tc.status, "rejected_at": time.Now().UTC().Add(-time.Minute)}
				}
				encoded, err := json.Marshal(checkpoint)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewRaw("UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?", string(encoded), fixture.ensureTask.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewRaw("UPDATE tasks SET resume_mode = ? WHERE id = ?", model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			if tc.lookupPanics {
				stopped := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
				binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
				if err != nil || stopped.FailureReason == nil || *stopped.FailureReason != "handler_panic" || len(binding.CreationRejection) != 0 || !strings.Contains(string(stopped.Checkpoint), "provider_rejection") {
					t.Fatalf("interrupted refusal: binding=%#v task=%#v err=%v", binding, stopped, err)
				}
				if !runtime.service.Retryable(stopped) {
					t.Fatal("refusal lookup cannot recover before its settlement")
				}
				if err := runtime.service.Retry(t.Context(), stopped.ID); err != nil {
					t.Fatal(err)
				}
			}
			if failedWrite != nil {
				select {
				case <-failedWrite.failed:
				case <-time.After(3 * time.Second):
					t.Fatal("rejection settlement did not encounter the injected failure")
				}
				stored, err := runtime.repos.Tasks.GetByID(t.Context(), fixture.ensureTask.ID)
				if err != nil || !strings.Contains(string(stored.Checkpoint), "provider_rejection") {
					t.Fatalf("refusal checkpoint was lost: task=%#v err=%v", stored, err)
				}
				if _, err := runtime.db.ExecContext(t.Context(), "DROP TRIGGER reject_refusal_settlement"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.lookupFails {
				waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool { return lookups.Load() > 0 && task.Status == model.TaskStatusPending })
				lookupFails.Store(false)
				wakeTask(t, runtime, fixture.ensureTask.ID)
			}
			terminal := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusCompleted
			})
			binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantCreates := int64(1)
			if tc.checkpointedRefusal {
				wantCreates = 0
			}
			if creates.Load() != wantCreates {
				t.Fatalf("create calls = %d, want %d", creates.Load(), wantCreates)
			}
			if tc.found {
				if terminal.Status != model.TaskStatusCompleted || binding.Status != model.StorageDataSetStatusReady || len(binding.CreationRejection) != 0 || binding.LastError != nil {
					t.Fatalf("recovered service = %#v, task=%#v", binding, terminal)
				}
				return
			}
			evidence, err := binding.CreationRejectionEvidence()
			if err != nil || evidence == nil || evidence.StatusCode != tc.status || terminal.FailureReason == nil || *terminal.FailureReason != "dataset_provider_rejected" || runtime.service.Retryable(terminal) {
				t.Fatalf("refusal = %#v, err=%v, task=%#v", evidence, err, terminal)
			}
			if !binding.IsCurrent || binding.Status != model.StorageDataSetStatusPending || binding.EnsureTaskID == nil {
				t.Fatalf("replacement source = %#v", binding)
			}
			if _, local, err := runtime.repos.Replacements.SourceEligibility(t.Context(), binding.ID); err != nil || !local {
				t.Fatalf("source eligibility = local:%v err:%v", local, err)
			}
		})
	}
}

type refusalSettlementObserver struct {
	failed   chan struct{}
	reported atomic.Bool
}

func (*refusalSettlementObserver) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (r *refusalSettlementObserver) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if event.Err != nil && strings.Contains(event.Query, "creation_rejection") && !r.reported.Swap(true) {
		close(r.failed)
	}
}

func TestUncreatedReplacementCoordinatorWaitsForReadyTarget(t *testing.T) {
	for _, state := range []string{"already ready", "pending", "provider rejected", "chain rejected", "retryable task stopped", "previous replacement failed", "cancelled", "nonretryable task stopped", "slot missing"} {
		t.Run(state, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			sourceProvider, targetProvider := testOnChainID(t, 71000+sequence), testOnChainID(t, 81000+sequence)
			var creates atomic.Int64
			createStarted, allowCreate := make(chan struct{}), make(chan struct{})
			terminator := &testServiceTerminator{err: errors.New("unexpected remote retirement")}
			target := &testutil.MockStorageTarget{
				ProviderIDValue: targetProvider.SDK(),
				WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					return nil, synapse.ErrProviderTransactionRejected
				},
				FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					return storage.DataSetRef{}, false, nil
				},
				CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					creates.Add(1)
					if state == "pending" {
						close(createStarted)
						select {
						case <-allowCreate:
						case <-t.Context().Done():
							return nil, t.Context().Err()
						}
					}
					if state == "provider rejected" {
						return nil, &synapse.DataSetProviderRejectionError{StatusCode: 403, Cause: errors.New("refused")}
					}
					ref, err := storage.NewDataSetRef(targetProvider.SDK(), sdktypes.NewBigInt(2002), *options.ClientDataSetID)
					return &storage.CreateDataSetResult{DataSet: ref}, err
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(_ context.Context, provider sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					if provider.String() != targetProvider.String() {
						return nil, errors.New("old provider must not receive requests")
					}
					return target, nil
				}}, terminator: terminator,
			})
			bucket := &model.Bucket{Name: fmt.Sprintf("uncreated-coordinate-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
			source, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: sourceProvider, CopyIndex: 0})
			if err != nil {
				t.Fatal(err)
			}
			enqueueEnsure := func(id int64) *model.Task {
				task, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
					Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(id), Input: storagepipeline.DataSetInput{DataSetID: id}, SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(id),
				}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
					return repos.Contents.BindDataSetEnsureTask(ctx, id, task.ID)
				})
				if err != nil {
					t.Fatal(err)
				}
				return task
			}
			oldEnsure := enqueueEnsure(source.ID)
			row, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID,
				SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: targetProvider, ClientRequestID: bucket.Name,
			})
			if err != nil {
				t.Fatal(err)
			}
			var targetEnsure *model.Task
			if state == "already ready" {
				if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: row.TargetDataSetID, DataSetID: testOnChainID(t, 2002)}); err != nil {
					t.Fatal(err)
				}
			} else {
				targetEnsure = enqueueEnsure(row.TargetDataSetID)
				if state == "chain rejected" {
					clientID := testOnChainID(t, 909)
					if err := runtime.repos.Contents.MarkDataSetCreating(t.Context(), repository.MarkDataSetCreatingInput{ID: row.TargetDataSetID, TransactionID: "0xdead", ClientDataSetID: &clientID}); err != nil {
						t.Fatal(err)
					}
					checkpoint, err := json.Marshal(map[string]any{"attempted_at": time.Now().UTC().Add(-time.Minute), "client_data_set_id": clientID.String(), "identity": testutil.DefaultContextIdentity, "sends": 1, "transaction_id": "0xdead"})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := runtime.db.NewRaw("UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?", string(checkpoint), targetEnsure.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
					if _, err := runtime.db.NewRaw("UPDATE tasks SET resume_mode = ? WHERE id = ?", model.TaskResumeModeRecover, targetEnsure.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				if state == "retryable task stopped" || state == "previous replacement failed" || state == "nonretryable task stopped" || state == "cancelled" {
					status, reason, retention := model.TaskStatusFailed, any("handler_panic"), any(nil)
					switch state {
					case "nonretryable task stopped":
						reason = "dataset_correlation_conflict"
					case "cancelled":
						status, reason = model.TaskStatusCancelled, nil
						retention = time.Now().Add(time.Hour)
					}
					if _, err := runtime.db.NewRaw("UPDATE tasks SET status = ?, finished_at = ?, retention_until = ?, failure_reason = ? WHERE id = ?", status, time.Now(), retention, reason, targetEnsure.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
			}
			switch state {
			case "slot missing":
				if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("is_current = ?", false).Where("id = ?", source.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "previous replacement failed":
				if err := runtime.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "old coordinator stopped"); err != nil {
					t.Fatal(err)
				}
			}
			enqueueCoordinator := func() *model.Task {
				coordinator, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
					Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: storagereplacement.CoordinateTaskKey(row.ID, row.TaskGeneration),
					Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration}, SubjectType: "storage_replacement", SubjectKey: fmt.Sprint(row.ID),
				}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
					return repos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, task.ID)
				})
				if err != nil {
					t.Fatal(err)
				}
				return coordinator
			}
			coordinator := enqueueCoordinator()
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			if state == "retryable task stopped" || state == "previous replacement failed" {
				waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool {
					if state == "previous replacement failed" {
						return task.Status == model.TaskStatusFailed
					}
					stored, err := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
					return err == nil && stored.Status == storagereplacement.StatusWaiting && task.Status == model.TaskStatusPending
				})
				if err := runtime.repos.Replacements.RetryEligibility(t.Context(), row.ID); !errors.Is(err, storagereplacement.ErrNotRetryable) {
					t.Fatalf("stopped setup offered replacement retry: %v", err)
				}
				if err := runtime.service.Retry(t.Context(), targetEnsure.ID); err != nil {
					t.Fatal(err)
				}
				if state == "previous replacement failed" {
					row, err = runtime.repos.Replacements.Retry(t.Context(), repository.RetryReplacementInput{ReplacementID: row.ID})
					if err != nil {
						t.Fatal(err)
					}
					coordinator = enqueueCoordinator()
				} else {
					wakeTask(t, runtime, coordinator.ID)
				}
			}
			if state == "pending" {
				select {
				case <-createStarted:
				case <-time.After(5 * time.Second):
					close(allowCreate)
					t.Fatal("target creation did not start")
				}
				held, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
				preparing, loadErr := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
				close(allowCreate)
				if err != nil || loadErr != nil || !held.IsCurrent || preparing.IsCurrent {
					t.Fatalf("pending target took the slot: A=%#v B=%#v %v %v", held, preparing, err, loadErr)
				}
			}
			waitForTask(t, runtime.repos, oldEnsure.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCancelled })
			if targetEnsure != nil {
				waitForTask(t, runtime.repos, targetEnsure.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusCompleted || task.Status == model.TaskStatusCancelled
				})
				waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool { return task.Status != model.TaskStatusRunning })
				wakeTask(t, runtime, coordinator.ID)
			}
			waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusCompleted
			})
			stored, err := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if state == "provider rejected" {
				if stored.Status != storagereplacement.StatusFailed || stored.FailureReason == nil || *stored.FailureReason != storagereplacement.FailureReasonTargetRejected {
					t.Fatalf("rejected target=%#v", stored)
				}
			} else if state == "chain rejected" || state == "cancelled" || state == "nonretryable task stopped" || state == "slot missing" {
				if stored.Status != storagereplacement.StatusFailed {
					t.Fatalf("terminal target replacement=%#v", stored)
				}
				if err := runtime.repos.Replacements.RetryEligibility(t.Context(), row.ID); !errors.Is(err, storagereplacement.ErrNotRetryable) {
					t.Fatalf("terminal target retry: %v", err)
				}
			} else if stored.Status != storagereplacement.StatusCompleted || stored.TerminationEpoch != nil {
				t.Fatalf("completed replacement=%#v", stored)
			}
			if state == "chain rejected" || state == "provider rejected" || state == "cancelled" || state == "nonretryable task stopped" {
				retained, _, err := runtime.repos.Replacements.SourceEligibility(t.Context(), source.ID)
				if err != nil || !retained.IsCurrent {
					t.Fatalf("original source lost its slot: %#v %v", retained, err)
				}
				check, err := runtime.repos.Replacements.Preflight(t.Context(), source.ID)
				if err != nil {
					t.Fatal(err)
				}
				for i := range check.Targets {
					evidence, err := check.Targets[i].DataSet.CreationRejectionEvidence()
					if err != nil {
						t.Fatal(err)
					}
					check.Targets[i].VerifiedCreationRejection = evidence
				}
				next, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 91000+sequence), ClientRequestID: "another-provider", Preflight: check})
				if err != nil || next.SourceDataSetID != source.ID {
					t.Fatalf("original source could not choose C: %#v %v", next, err)
				}
			}
			if terminator.calls.Load() != 0 {
				t.Fatal("local replacement retired a remote service")
			}
			wantCreates := int64(1)
			if state == "already ready" || state == "chain rejected" || state == "cancelled" || state == "nonretryable task stopped" {
				wantCreates = 0
			}
			if creates.Load() != wantCreates {
				t.Fatalf("create calls=%d, want %d", creates.Load(), wantCreates)
			}
		})
	}
}

func TestReadyLocalTargetSchedulesRequiredContentRecovery(t *testing.T) {
	var opened atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return false }},
		storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			opened.Add(1)
			return nil, errors.New("unexpected creation")
		}},
	})
	fixture := seedDataSetEnsure(t, runtime, testOnChainID(t, 101), "local-content-recovery")
	copies, err := runtime.repos.Contents.ListIncompleteCopiesForDataSet(t.Context(), fixture.binding.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("source copies: %#v %v", copies, err)
	}
	oldCopyTask := bindCopyTask(t, runtime, &copies[0], model.TaskTypeStorageTransferPlan)
	row, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: fixture.binding.BucketID, SourceDataSetID: fixture.binding.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 202), ClientRequestID: "recover"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: row.TargetDataSetID, DataSetID: testOnChainID(t, 2002)}); err != nil {
		t.Fatal(err)
	}
	coordinator, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: storagereplacement.CoordinateTaskKey(row.ID, row.TaskGeneration), Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, task.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCancelled })
	waitForTask(t, runtime.repos, oldCopyTask.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCancelled })
	waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool {
		if task.Status == model.TaskStatusPending {
			wakeTask(t, runtime, task.ID)
		}
		return task.Status == model.TaskStatusFailed
	})
	item, err := runtime.repos.Replacements.ReplacementExecution(t.Context(), row.ID)
	if err != nil || !item.HasFailed {
		t.Fatalf("required content did not request attention: %#v %v", item, err)
	}
	var cancelled int
	if err := runtime.db.NewSelect().Model((*storagereplacement.Item)(nil)).Where("replacement_id = ? AND status = ?", row.ID, storagereplacement.ItemStatusCancelled).ColumnExpr("COUNT(*)").Scan(t.Context(), &cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled != 0 || opened.Load() != 0 {
		t.Fatalf("recovery was cancelled or old provider reopened: cancelled=%d opened=%d", cancelled, opened.Load())
	}
}
