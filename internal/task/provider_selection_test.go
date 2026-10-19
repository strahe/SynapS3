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

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	bindingtask "github.com/strahe/synaps3/internal/task/binding"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
	sdkcosts "github.com/strahe/synapse-go/costs"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type selectionHealthChecker struct {
	calls atomic.Int64
	check func(time.Time) ([]observability.ProviderState, error)
}

func (c *selectionHealthChecker) CheckProviders(_ context.Context, checkedAt time.Time, _ []observability.LocalDataSet) ([]observability.ProviderState, error) {
	c.calls.Add(1)
	return c.check(checkedAt)
}

func (*selectionHealthChecker) CheckDataSets(context.Context, time.Time, []observability.LocalDataSet) ([]observability.DataSetState, error) {
	return nil, nil
}

func providerSelectionWork(t *testing.T, runtime handlerTestRuntime, bucket *model.Bucket, taskType model.TaskType) *model.Task {
	t.Helper()
	request := taskengine.EnqueueRequest{Type: taskType, Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID, Copies: bucket.DefaultCopies}}
	if taskType == model.TaskTypeUploadPlan {
		content, err := runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
			BucketID: bucket.ID, ContentSize: 11, RequestedCopies: bucket.DefaultCopies, Checksum: testutil.StorageChecksum("provider-selection"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), &model.ObjectVersion{
			VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "object.bin", ContentID: &content.ID, Size: 11,
			ETag: "etag", ContentType: "application/octet-stream",
		}); err != nil {
			t.Fatal(err)
		}
		request.Input = storagepipeline.UploadPlanInput{ContentID: content.ID}
	}
	row, _, err := runtime.service.Enqueue(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func providerSelectionReports(t *testing.T, runtime handlerTestRuntime, taskID int64) []providerselect.SelectionDiagnostics {
	t.Helper()
	events, err := runtime.repos.Tasks.ListEvents(t.Context(), taskID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	reports := make([]providerselect.SelectionDiagnostics, 0)
	for _, event := range events {
		if event.Type != "provider_selection" {
			continue
		}
		var details struct {
			ProviderSelection providerselect.SelectionDiagnostics `json:"provider_selection"`
		}
		if err := json.Unmarshal(event.Details, &details); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(event.Details), "https://") {
			t.Fatal("selection event leaked a provider URL")
		}
		reports = append(reports, details.ProviderSelection)
	}
	return reports
}

func selectionProviderState(id idtypes.OnChainID, checkedAt time.Time) observability.ProviderState {
	active, pdp, url := true, true, "https://provider.example"
	return observability.ProviderState{
		ProviderID: id, Status: observability.StatusAvailable, Active: &active, HasPDP: &pdp, ServiceURL: &url,
		LastCheckedAt: checkedAt, Profile: &observability.ProviderProfile{ProviderID: id, Active: true, ServiceURL: url, RegistrySnapshot: json.RawMessage(`{"version":1}`)},
	}
}

func TestProviderSelectionStopsForUnknownPersistedStrategy(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeBucketProvision, model.TaskTypeUploadPlan} {
		for _, strategy := range []string{"", "future-strategy"} {
			t.Run(string(taskType)+"/"+strategy, func(t *testing.T) {
				var opened atomic.Int64
				runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						opened.Add(1)
						return nil, errors.New("unexpected provider access")
					},
					OpenDataSetTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
						opened.Add(1)
						return nil, errors.New("unexpected data set access")
					},
				}})
				bucket := &model.Bucket{Name: "unknown-strategy", Status: model.BucketStatusProvisioning, DefaultCopies: 1, MinimumDurableCopies: 1}
				if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
					t.Fatal(err)
				}
				binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
					BucketID: bucket.ID, CopyIndex: 0, ProviderID: testOnChainID(t, 202),
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: testOnChainID(t, 2202)}); err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.Bucket)(nil)).
					Set("provider_selection_strategy = ?", strategy).Where("id = ?", bucket.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				row := providerSelectionWork(t, runtime, bucket, taskType)
				cancel, done := runHandlerEngine(t, runtime)
				defer stopHandlerEngine(t, cancel, done)
				row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusFailed })
				if row.FailureReason == nil || *row.FailureReason != "invalid_provider_selection_strategy" || row.RetryCount != 0 || opened.Load() != 0 {
					t.Fatalf("unknown strategy task = %#v, provider accesses = %d", row, opened.Load())
				}
			})
		}
	}
}

func TestProviderSelectionShortagesWaitWithoutConsumingBudget(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeBucketProvision, model.TaskTypeUploadPlan} {
		for _, scenario := range []string{"approved", "endorsed", "none", "existing trusted", "inactive", "missing PDP", "ordinary shortage", "speed tests"} {
			t.Run(string(taskType)+"/"+scenario, func(t *testing.T) {
				tier := providerselect.TierApproved
				switch scenario {
				case "endorsed":
					tier = providerselect.TierEndorsed
				case "none":
					tier = providerselect.TierNone
				}
				var opened atomic.Int64
				runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{providerTier: tier, storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						opened.Add(1)
						dataSetID := sdktypes.NewBigInt(2202)
						return &testutil.MockStorageTarget{ProviderIDValue: id, DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(2)}, nil
					},
				}})
				providerID := testOnChainID(t, 202)
				seedProviderSelection(t, runtime, providerID.SDK())
				state := selectionProviderState(providerID, time.Now().UTC())
				state.Status = observability.StatusDegraded
				wantReason := "provider_unhealthy"
				switch scenario {
				case "inactive":
					inactive := false
					state.Active, state.Profile.Active = &inactive, false
					wantReason = "provider_inactive"
				case "missing PDP":
					noPDP := false
					state.HasPDP = &noPDP
					wantReason = "provider_missing_pdp"
				case "ordinary shortage":
					state.Status = observability.StatusAvailable
					wantReason = ""
				case "speed tests":
					state.Status = observability.StatusAvailable
					wantReason = "upload_speed_untested"
					if _, err := runtime.db.NewDelete().Model((*providerbenchmark.Result)(nil)).Where("provider_id = ?", providerID.String()).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				if err := runtime.repos.Observability.UpsertProviderObservation(t.Context(), time.Now().UTC(), state); err != nil {
					t.Fatal(err)
				}
				if tier == providerselect.TierEndorsed {
					if _, err := runtime.repos.Observability.RecordEndorsedProviders(t.Context(), time.Now().UTC(), []idtypes.OnChainID{providerID}); err != nil {
						t.Fatal(err)
					}
				}
				copies := 1
				if scenario == "existing trusted" || scenario == "ordinary shortage" {
					copies = 2
				}
				bucket := &model.Bucket{Name: "provider-wait", Status: model.BucketStatusProvisioning, DefaultCopies: copies, MinimumDurableCopies: 1}
				if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
					t.Fatal(err)
				}
				existingBindings := 0
				if scenario == "existing trusted" {
					existingBindings = 1
					trustedID := testOnChainID(t, 101)
					seedProviderSelection(t, runtime, trustedID.SDK())
					binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: trustedID, CopyIndex: 0})
					if err != nil {
						t.Fatal(err)
					}
					if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: testOnChainID(t, 1101)}); err != nil {
						t.Fatal(err)
					}
				}
				row := providerSelectionWork(t, runtime, bucket, taskType)
				waitReason := "providers"
				if scenario == "speed tests" {
					waitReason = "provider_speed_tests"
				}
				cancel, done := runHandlerEngine(t, runtime)
				defer stopHandlerEngine(t, cancel, done)
				for poll := 1; poll <= 7; poll++ {
					previousClaim := row.ClaimGeneration
					row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
						return row.ClaimGeneration > previousClaim && (row.Status == model.TaskStatusFailed || row.Status == model.TaskStatusPending && row.WaitReason != nil)
					})
					if row.Status != model.TaskStatusPending || row.RetryCount != 0 || row.LastError != nil || row.WaitReason == nil || *row.WaitReason != waitReason {
						t.Fatalf("provider wait %d = %#v", poll, row)
					}
					bindings, err := runtime.repos.Contents.ListDataSetBindings(t.Context(), bucket.ID)
					if err != nil || len(bindings) != existingBindings {
						t.Fatalf("wait committed a partial selection: bindings=%#v err=%v", bindings, err)
					}
					if poll < 7 {
						// Fresh checks alone do not change the waiting decision.
						state.LastCheckedAt = time.Now().UTC()
						if err := runtime.repos.Observability.UpsertProviderObservation(t.Context(), time.Now().UTC(), state); err != nil {
							t.Fatal(err)
						}
						wakeTask(t, runtime, row.ID)
					}
				}
				reports := providerSelectionReports(t, runtime, row.ID)
				eligibleCount := existingBindings
				if scenario == "ordinary shortage" {
					eligibleCount++
				}
				if len(reports) != 1 || reports[0].Tier != tier || reports[0].RequestedCount != copies || reports[0].EligibleCount != eligibleCount {
					t.Fatalf("unchanged waiting evidence = %#v", reports)
				}
				if wantReason != "" {
					found := false
					for _, rejection := range reports[0].Rejections {
						found = found || rejection.ProviderID.Equal(providerID) && rejection.Reason == wantReason && rejection.ObservationCheckedAt != nil
					}
					if !found || opened.Load() != 0 {
						t.Fatalf("ineligible provider was opened or missing evidence: opened=%d reports=%#v", opened.Load(), reports)
					}
				}
				if scenario == "ordinary shortage" {
					seedProviderSelection(t, runtime, providerID.SDK(), sdktypes.NewBigInt(303))
				} else if scenario == "speed tests" {
					seedProviderSelectionSpeed(t, runtime, providerID)
				} else if err := runtime.repos.Observability.UpsertProviderObservation(t.Context(), time.Now().UTC(), selectionProviderState(providerID, time.Now().UTC())); err != nil {
					t.Fatal(err)
				}
				wakeTask(t, runtime, row.ID)
				row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
					return row.Status == model.TaskStatusCompleted || row.Status == model.TaskStatusFailed || row.RetryCount > 0
				})
				if row.Status != model.TaskStatusCompleted || row.RetryCount != 0 {
					t.Fatalf("restored supply did not continue automatically: %#v", row)
				}
			})
		}
	}
}

func TestProviderSelectionWaitPreservesAllocatedRetry(t *testing.T) {
	providerID := testOnChainID(t, 202)
	var opens atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{providerTier: providerselect.TierNone, storage: &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			if opens.Add(1) == 1 {
				return nil, errors.New("provider context could not be opened")
			}
			dataSetID := sdktypes.NewBigInt(2202)
			return &testutil.MockStorageTarget{ProviderIDValue: id, DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(2)}, nil
		},
	}})
	seedProviderSelection(t, runtime, providerID.SDK())
	bucket := &model.Bucket{Name: "selection-retry-then-wait", Status: model.BucketStatusProvisioning, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	row := providerSelectionWork(t, runtime, bucket, model.TaskTypeBucketProvision)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
		return row.Status == model.TaskStatusFailed || row.Status == model.TaskStatusPending && row.RetryCount == 1
	})
	if row.Status != model.TaskStatusPending || row.RetryCount != 1 {
		t.Fatalf("context failure did not allocate retry: %#v", row)
	}
	state := selectionProviderState(providerID, time.Now().UTC())
	state.Status = observability.StatusDegraded
	if err := runtime.repos.Observability.UpsertProviderObservation(t.Context(), time.Now().UTC(), state); err != nil {
		t.Fatal(err)
	}
	wakeTask(t, runtime, row.ID)
	row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
		return row.ClaimGeneration == 2 && row.Status == model.TaskStatusPending && row.WaitReason != nil
	})
	if row.RetryCount != 1 || *row.WaitReason != "providers" {
		t.Fatalf("provider wait changed allocated retry: %#v", row)
	}
}

func TestProviderSelectionExhaustsDeclaredBudget(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeBucketProvision, model.TaskTypeUploadPlan} {
		t.Run(string(taskType), func(t *testing.T) {
			checker := &selectionHealthChecker{check: func(time.Time) ([]observability.ProviderState, error) {
				return nil, errors.New("Registry read failed")
			}}
			backoff := taskengine.BackoffPolicy{InitialDelay: time.Minute, Multiplier: 1, MaximumDelay: time.Minute}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				providerTier: providerselect.TierApproved, observabilityChecker: checker, testBackoff: &backoff,
			})
			providerID := testOnChainID(t, 202)
			seedProviderSelection(t, runtime, providerID.SDK())
			if _, err := runtime.db.NewUpdate().Table("observability_provider_states").Set("last_checked_at = ?", time.Now().UTC().Add(-time.Hour)).Where("provider_id = ?", providerID.String()).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			bucket := &model.Bucket{Name: "provider-exhaustion", Status: model.BucketStatusProvisioning, DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
			row := providerSelectionWork(t, runtime, bucket, taskType)
			definition, ok := runtime.registry.Definition(taskType)
			if !ok {
				t.Fatalf("task type %s is not registered", taskType)
			}
			attempts := definition.Policy.MaxAttempts
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			for attempt := 1; attempt <= attempts; attempt++ {
				row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
					return row.ClaimGeneration == int64(attempt) && (row.Status == model.TaskStatusFailed || row.Status == model.TaskStatusPending && row.RetryCount == attempt)
				})
				if attempt < attempts {
					if row.Status != model.TaskStatusPending || row.RetryCount != attempt || !row.AvailableAt.After(time.Now()) {
						t.Fatalf("attempt %d stopped prematurely: %#v", attempt, row)
					}
					wakeTask(t, runtime, row.ID)
				} else if row.Status != model.TaskStatusFailed || row.RetryCount != attempts-1 || row.FailureReason == nil || *row.FailureReason != "attempts_exhausted" {
					t.Fatalf("final attempt = %#v", row)
				}
			}
			if reports := providerSelectionReports(t, runtime, row.ID); len(reports) != attempts || checker.calls.Load() != int64(attempts) {
				t.Fatalf("per-attempt evidence=%d refreshes=%d, want %d", len(reports), checker.calls.Load(), attempts)
			}
		})
	}
}

func TestProviderSelectionRefreshesOnlyBlockingEvidence(t *testing.T) {
	for _, scenario := range []string{"enough healthy", "refresh recovers", "refresh remains stale", "refresh fails"} {
		t.Run(scenario, func(t *testing.T) {
			id := testOnChainID(t, 202)
			staleAt := time.Now().UTC().Add(-time.Hour)
			checker := &selectionHealthChecker{check: func(checkedAt time.Time) ([]observability.ProviderState, error) {
				if scenario == "refresh fails" {
					return nil, errors.New("Registry read failed")
				}
				if scenario == "refresh remains stale" {
					checkedAt = staleAt
				}
				return []observability.ProviderState{selectionProviderState(id, checkedAt)}, nil
			}}
			dataSetID := sdktypes.NewBigInt(2202)
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{providerTier: providerselect.TierApproved, observabilityChecker: checker, storage: &testutil.MockStorageClient{
				OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					return &testutil.MockStorageTarget{ProviderIDValue: id.SDK(), DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(2)}, nil
				},
			}})
			seedProviderSelection(t, runtime, id.SDK())
			staleID := id
			if scenario == "enough healthy" {
				staleID = testOnChainID(t, 303)
				seedProviderSelection(t, runtime, id.SDK(), staleID.SDK())
			}
			if _, err := runtime.db.NewUpdate().Table("observability_provider_states").Set("last_checked_at = ?", staleAt).Where("provider_id = ?", staleID.String()).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			bucket := &model.Bucket{Name: "selection-refresh", Status: model.BucketStatusProvisioning, DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
			row := providerSelectionWork(t, runtime, bucket, model.TaskTypeBucketProvision)
			runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1, countType: model.TaskTypeBucketProvision}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusCompleted || row.Status == model.TaskStatusFailed || row.Status == model.TaskStatusPending && row.ClaimGeneration > 0
			})
			wantRefreshes := int64(1)
			if scenario == "enough healthy" {
				wantRefreshes = 0
			}
			if checker.calls.Load() != wantRefreshes {
				t.Fatalf("health refreshes = %d, want %d", checker.calls.Load(), wantRefreshes)
			}
			if scenario == "enough healthy" || scenario == "refresh recovers" {
				if row.Status != model.TaskStatusCompleted || row.RetryCount != 0 {
					t.Fatalf("available provider did not complete: %#v", row)
				}
			} else {
				reports := providerSelectionReports(t, runtime, row.ID)
				wantRetries := 0
				if scenario == "refresh fails" {
					wantRetries = 1
				}
				if row.Status != model.TaskStatusPending || row.RetryCount != wantRetries || len(reports) != 1 || len(reports[0].Rejections) != 1 || reports[0].Rejections[0].Reason != "observation_stale" {
					t.Fatalf("stale evidence failure = %#v reports=%#v", row, reports)
				}
			}
		})
	}
}

func TestProviderSelectionRefreshesEmptyInventoryOnce(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeBucketProvision, model.TaskTypeUploadPlan} {
		for _, scenario := range []string{"recovers", "remains empty", "fails"} {
			t.Run(string(taskType)+"/"+scenario, func(t *testing.T) {
				providerID := testOnChainID(t, 202)
				checker := &selectionHealthChecker{check: func(checkedAt time.Time) ([]observability.ProviderState, error) {
					if scenario == "fails" {
						return nil, errors.New("Registry read failed")
					}
					if scenario == "remains empty" {
						return nil, nil
					}
					return []observability.ProviderState{selectionProviderState(providerID, checkedAt)}, nil
				}}
				runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{providerTier: providerselect.TierNone, observabilityChecker: checker, storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						dataSetID := sdktypes.NewBigInt(2202)
						return &testutil.MockStorageTarget{ProviderIDValue: id, DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(2)}, nil
					},
				}})
				seedProviderSelectionSpeed(t, runtime, providerID)
				bucket := &model.Bucket{Name: "empty-inventory", Status: model.BucketStatusProvisioning, DefaultCopies: 1, MinimumDurableCopies: 1}
				if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
					t.Fatal(err)
				}
				row := providerSelectionWork(t, runtime, bucket, taskType)
				runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1, countType: taskType}
				cancel, done := runHandlerEngine(t, runtime)
				defer stopHandlerEngine(t, cancel, done)
				row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
					return row.Status == model.TaskStatusCompleted || row.Status == model.TaskStatusFailed || row.Status == model.TaskStatusPending && row.ClaimGeneration > 0
				})
				if checker.calls.Load() != 1 {
					t.Fatalf("empty inventory refreshes = %d, want one", checker.calls.Load())
				}
				if scenario == "recovers" {
					if row.Status != model.TaskStatusCompleted || row.RetryCount != 0 {
						t.Fatalf("refreshed supply did not complete: %#v", row)
					}
					return
				}
				wantRetries := 0
				if scenario == "fails" {
					wantRetries = 1
				}
				reports := providerSelectionReports(t, runtime, row.ID)
				if row.Status != model.TaskStatusPending || row.RetryCount != wantRetries || len(reports) != 1 || reports[0].RequestedCount != 1 || reports[0].EligibleCount != 0 {
					t.Fatalf("empty inventory result = %#v reports=%#v", row, reports)
				}
			})
		}
	}
}

func TestProviderSelectionRefreshRetainsMissingCandidateFailure(t *testing.T) {
	freshIDs := []idtypes.OnChainID{testOnChainID(t, 202), testOnChainID(t, 303)}
	staleID := testOnChainID(t, 404)
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	staleAt := observedAt.Add(-time.Hour)
	opened := make(chan idtypes.OnChainID, 1)
	var failedID idtypes.OnChainID
	checker := &selectionHealthChecker{check: func(checkedAt time.Time) ([]observability.ProviderState, error) {
		states := []observability.ProviderState{selectionProviderState(staleID, staleAt)}
		for _, id := range freshIDs {
			if !id.Equal(failedID) {
				states = append(states, selectionProviderState(id, checkedAt))
			}
		}
		return states, nil
	}}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		providerTier: providerselect.TierNone, observabilityChecker: checker,
		storage: &testutil.MockStorageClient{
			OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				failedID = idtypes.OnChainIDFromSDK(id)
				select {
				case opened <- failedID:
				default:
				}
				return nil, &synapse.ProviderUnavailableError{Cause: errors.New("provider connection failed")}
			},
		},
	})
	seedProviderSelection(t, runtime, freshIDs[0].SDK(), freshIDs[1].SDK(), staleID.SDK())
	if _, err := runtime.db.NewUpdate().Table("observability_provider_states").Set("last_checked_at = ?", observedAt).Where("provider_id IN (?, ?)", freshIDs[0].String(), freshIDs[1].String()).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Table("observability_provider_states").Set("last_checked_at = ?", staleAt).Where("provider_id = ?", staleID.String()).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	bucket := &model.Bucket{Name: "selection-removed-provider", Status: model.BucketStatusProvisioning, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	row := providerSelectionWork(t, runtime, bucket, model.TaskTypeBucketProvision)
	runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1, countType: model.TaskTypeBucketProvision}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
		return row.Status == model.TaskStatusFailed || row.Status == model.TaskStatusPending && row.RetryCount > 0
	})
	var expectedID idtypes.OnChainID
	select {
	case expectedID = <-opened:
	default:
		t.Fatal("no fresh candidate was opened before refresh")
	}
	if row.Status != model.TaskStatusPending || row.RetryCount != 1 || checker.calls.Load() != 1 {
		t.Fatalf("selection transition = %#v, refreshes = %d", row, checker.calls.Load())
	}
	states, err := runtime.repos.Observability.ListProviderStates(t.Context(), observability.ListOptions{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states.Items {
		if state.ProviderID.Equal(expectedID) {
			t.Fatal("refresh did not remove the failed candidate from current observations")
		}
	}
	reports := providerSelectionReports(t, runtime, row.ID)
	if len(reports) != 1 || reports[0].RequestedCount != 2 || reports[0].RejectedTotal != 2 {
		t.Fatalf("selection reports = %#v", reports)
	}
	for _, rejection := range reports[0].Rejections {
		if rejection.ProviderID.Equal(expectedID) {
			if rejection.Reason != string(providerselect.ExclusionContextOpenFailed) || rejection.ObservationCheckedAt == nil || !rejection.ObservationCheckedAt.Equal(observedAt) {
				t.Fatalf("removed candidate failure = %#v", rejection)
			}
			return
		}
	}
	t.Fatalf("removed candidate %s is absent from selection report: %#v", expectedID, reports[0])
}

func TestProviderSelectionContextFailuresRetainCauseAndDiagnostics(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing_binding=%v", existing), func(t *testing.T) {
			cause := &synapse.ProviderUnavailableError{Cause: errors.New("provider connection failed")}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{providerTier: providerselect.TierApproved, storage: &testutil.MockStorageClient{
				OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					return nil, cause
				},
				OpenDataSetTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
					return nil, cause
				},
			}})
			id := testOnChainID(t, 202)
			seedProviderSelection(t, runtime, id.SDK())
			copies := 1
			if existing {
				copies = 2
			}
			bucket := &model.Bucket{Name: "context-failure", Status: model.BucketStatusProvisioning, DefaultCopies: copies, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
			if existing {
				binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: id, CopyIndex: 0})
				if err != nil {
					t.Fatal(err)
				}
				if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: testOnChainID(t, 2202)}); err != nil {
					t.Fatal(err)
				}
			}
			selector, err := bindingtask.NewSelector(bindingtask.SelectorDependencies{
				Repositories: runtime.repos, Storage: runtime.storage, AnchorProviderTier: providerselect.TierApproved,
				Observability: observability.NewService(observability.ServiceOptions{Store: runtime.repos.Observability}),
				Resolver:      bindingtask.NewResolver(bindingtask.ResolverDependencies{Repositories: runtime.repos, Storage: runtime.storage}),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = selector.SelectBucketBindings(t.Context(), bucket, copies)
			var selectionErr *providerselect.SelectionError
			if !errors.Is(err, cause) || !errors.As(err, &selectionErr) || errors.Is(err, providerselect.ErrCandidatesTemporarilyUnavailable) == existing {
				t.Fatalf("context cause classification = %v", err)
			}
			row := providerSelectionWork(t, runtime, bucket, model.TaskTypeBucketProvision)
			runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1, countType: model.TaskTypeBucketProvision}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusFailed || row.Status == model.TaskStatusPending && (row.RetryCount > 0 || row.WaitReason != nil)
			})
			reports := providerSelectionReports(t, runtime, row.ID)
			if row.Status != model.TaskStatusPending || row.RetryCount != 1 || len(reports) != 1 || len(reports[0].Rejections) != 1 || reports[0].Rejections[0].Reason != string(providerselect.ExclusionContextOpenFailed) {
				t.Fatalf("context error task = %#v, reports=%#v", row, reports)
			}
		})
	}
}

func TestProviderSelectionContextErrorsPreserveClassification(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, lifecycle := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%v/lifecycle=%v", existing, lifecycle), func(t *testing.T) {
				cause := errors.New("provider identity conflicts with binding")
				if lifecycle {
					cause = &synapse.DataSetServiceEndedError{Cause: storage.ErrDataSetUnavailable}
				}
				runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{providerTier: providerselect.TierApproved, storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						return nil, cause
					},
					OpenDataSetTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
						return nil, cause
					},
				}})
				id := testOnChainID(t, 202)
				seedProviderSelection(t, runtime, id.SDK())
				bucket := &model.Bucket{Name: "context-classification", DefaultCopies: 1, MinimumDurableCopies: 1}
				if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
					t.Fatal(err)
				}
				if existing {
					binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: id, CopyIndex: 0})
					if err != nil {
						t.Fatal(err)
					}
					if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: testOnChainID(t, 2202)}); err != nil {
						t.Fatal(err)
					}
				}
				selector, err := bindingtask.NewSelector(bindingtask.SelectorDependencies{
					Repositories: runtime.repos, Storage: runtime.storage, AnchorProviderTier: providerselect.TierApproved,
					Observability: observability.NewService(observability.ServiceOptions{Store: runtime.repos.Observability}),
					Resolver:      bindingtask.NewResolver(bindingtask.ResolverDependencies{Repositories: runtime.repos, Storage: runtime.storage}),
				})
				if err != nil {
					t.Fatal(err)
				}
				_, err = selector.SelectBucketBindings(t.Context(), bucket, 1)
				var failure *providerselect.SelectionError
				if !errors.Is(err, cause) || !errors.As(err, &failure) || failure.Kind != providerselect.SelectionProviderContext || errors.Is(err, providerselect.ErrCandidatesTemporarilyUnavailable) {
					t.Fatalf("context error lost its classification: %v", err)
				}
				if len(failure.Diagnostics.Rejections) != 1 || failure.Diagnostics.Rejections[0].Reason != string(providerselect.ExclusionContextOpenFailed) {
					t.Fatalf("context diagnostics = %#v", failure.Diagnostics)
				}
			})
		}
	}
}

func TestProviderSelectionWaitDiagnosticsRecordDecisionChanges(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	bucket := &model.Bucket{Name: "selection-decision", DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	row := providerSelectionWork(t, runtime, bucket, model.TaskTypeBucketProvision)
	checkedAt := time.Now().UTC()
	failure := &providerselect.SelectionError{Kind: providerselect.SelectionTemporary, Diagnostics: providerselect.SelectionDiagnostics{
		Tier: providerselect.TierApproved, TierCheckedAt: &checkedAt, RequestedCount: 1, RejectedTotal: 1,
		Rejections: []providerselect.ProviderRejection{{ProviderID: testOnChainID(t, 202), Reason: "provider_unhealthy", ObservationCheckedAt: &checkedAt}},
	}}
	record := func(settlement taskengine.Settlement) {
		t.Helper()
		if err := runtime.repos.WithTx(t.Context(), func(tx *repository.Repositories) error { return settlement(t.Context(), tx) }); err != nil {
			t.Fatal(err)
		}
	}
	record(bindingtask.SelectionWaitSettlement(row.ID, failure))
	newCheckedAt := checkedAt.Add(time.Minute)
	failure.Diagnostics.TierCheckedAt = &newCheckedAt
	failure.Diagnostics.Rejections[0].ObservationCheckedAt = &newCheckedAt
	record(bindingtask.SelectionWaitSettlement(row.ID, failure))
	if len(providerSelectionReports(t, runtime, row.ID)) != 1 {
		t.Fatal("timestamp-only change appended waiting evidence")
	}
	failure.Diagnostics.Rejections[0].Reason = "provider_inactive"
	record(bindingtask.SelectionWaitSettlement(row.ID, failure))
	if reports := providerSelectionReports(t, runtime, row.ID); len(reports) != 2 || reports[0].Rejections[0].Reason != "provider_inactive" {
		t.Fatalf("changed waiting reason was not recorded: %#v", reports)
	}
	if err := runtime.repos.Tasks.AppendEvent(t.Context(), row.ID, "intervening_decision", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	record(bindingtask.SelectionWaitSettlement(row.ID, failure))
	if len(providerSelectionReports(t, runtime, row.ID)) != 3 {
		t.Fatal("new waiting episode was suppressed after another event")
	}
	failure.Cause = errors.New("provider context failed")
	record(bindingtask.SelectionSettlement(row.ID, failure))
	record(bindingtask.SelectionSettlement(row.ID, failure))
	if len(providerSelectionReports(t, runtime, row.ID)) != 5 {
		t.Fatal("actual failure reports were deduplicated")
	}
}

func TestProviderSelectionDiagnosticsRollBackWithSettlement(t *testing.T) {
	for name, settle := range map[string]func(int64, error) taskengine.Settlement{
		"retry": bindingtask.SelectionSettlement,
		"wait":  bindingtask.SelectionWaitSettlement,
	} {
		t.Run(name, func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
			bucket := &model.Bucket{Name: "selection-rollback", DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
			row := providerSelectionWork(t, runtime, bucket, model.TaskTypeBucketProvision)
			failure := &providerselect.SelectionError{Kind: providerselect.SelectionTemporary, Diagnostics: providerselect.SelectionDiagnostics{
				Tier: providerselect.TierApproved, RequestedCount: 1, RejectedTotal: 1,
				Rejections: []providerselect.ProviderRejection{{ProviderID: testOnChainID(t, 202), Reason: "provider_unhealthy"}},
			}}
			settlement := settle(row.ID, failure)
			rollback := errors.New("domain settlement failed")
			err := runtime.repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
				if err := settlement(t.Context(), tx); err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) || len(providerSelectionReports(t, runtime, row.ID)) != 0 {
				t.Fatal("rolled-back selection diagnostics were persisted")
			}
			if err := runtime.repos.WithTx(t.Context(), func(tx *repository.Repositories) error { return settlement(t.Context(), tx) }); err != nil {
				t.Fatal(err)
			}
			if len(providerSelectionReports(t, runtime, row.ID)) != 1 {
				t.Fatal("successful replay did not record one selection report")
			}
		})
	}
}

func TestBucketProvisionDistinguishesMissingTierFromReadFailure(t *testing.T) {
	for _, scenario := range []string{"none", "missing trusted provider", "list unavailable", "replica shortage"} {
		t.Run(scenario, func(t *testing.T) {
			tier := providerselect.TierApproved
			if scenario == "none" {
				tier = providerselect.TierNone
			}
			providerID, dataSetID := sdktypes.NewBigInt(202), sdktypes.NewBigInt(2202)
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				providerTier: tier,

				storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					return &testutil.MockStorageTarget{ProviderIDValue: providerID, DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(2)}, nil
				}},
			})
			seedProviderSelection(t, runtime, providerID)
			ctx := t.Context()
			checkedAt := time.Now().UTC()
			if scenario == "list unavailable" || scenario == "none" {
				checkedAt = checkedAt.Add(-time.Hour)
			}
			var trustedIDs []idtypes.OnChainID
			copies := 1
			if scenario == "replica shortage" {
				trustedIDs = []idtypes.OnChainID{idtypes.OnChainIDFromSDK(providerID)}
				copies = 2
			}
			if _, err := runtime.repos.Observability.RecordApprovedProviders(ctx, checkedAt, trustedIDs); err != nil {
				t.Fatal(err)
			}
			// Replace the freshly seeded snapshot for the stale-read cases.
			if scenario == "list unavailable" || scenario == "none" {
				if _, err := runtime.db.NewUpdate().Table("provider_tier_snapshots").Set("checked_at = ?", checkedAt).Where("tier = ?", "approved").Exec(ctx); err != nil {
					t.Fatal(err)
				}
			}
			bucket := &model.Bucket{Name: "selection-test", Status: model.BucketStatusProvisioning, DefaultCopies: copies, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
				t.Fatal(err)
			}
			row, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
				Type:  model.TaskTypeBucketProvision,
				Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID, Copies: bucket.DefaultCopies},
			})
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusCompleted || row.Status == model.TaskStatusFailed || row.RetryCount > 0 || (row.WaitReason != nil && *row.WaitReason == "providers")
			})
			switch scenario {
			case "none":
				if row.Status != model.TaskStatusCompleted {
					t.Fatalf("none depended on membership: %#v", row)
				}
			case "missing trusted provider":
				if row.Status != model.TaskStatusFailed || row.FailureReason == nil || *row.FailureReason != "required_provider_unavailable" {
					t.Fatalf("confirmed missing provider = %#v", row)
				}
			case "list unavailable":
				if row.Status == model.TaskStatusFailed || row.RetryCount == 0 {
					t.Fatalf("list read failure did not retry: %#v", row)
				}
			case "replica shortage":
				if row.Status != model.TaskStatusPending || row.WaitReason == nil || *row.WaitReason != "providers" || row.RetryCount != 0 {
					t.Fatalf("ordinary replica shortage did not wait: %#v", row)
				}
			}
		})
	}
}

func TestBucketProvisionReplansWhenAnotherBindingTakesTheSlot(t *testing.T) {
	var runtime handlerTestRuntime
	var bucket *model.Bucket
	selectedID, competingID := sdktypes.NewBigInt(202), sdktypes.NewBigInt(303)
	dataSetID := sdktypes.NewBigInt(3303)
	openedSelected := false
	client := &testutil.MockStorageClient{OpenProviderTargetFunc: func(ctx context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
		if id.Equal(selectedID) {
			openedSelected = true
			if _, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: idtypes.OnChainIDFromSDK(competingID), CopyIndex: 0}); err != nil {
				return nil, err
			}
			return &testutil.MockStorageTarget{ProviderIDValue: selectedID}, nil
		}
		return &testutil.MockStorageTarget{ProviderIDValue: competingID, DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(3)}, nil
	}}
	runtime = newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: client, providerTier: providerselect.TierNone,
	})
	seedProviderSelection(t, runtime, selectedID)
	bucket = &model.Bucket{Name: "conflicting-slot", Status: model.BucketStatusProvisioning, DefaultCopies: 1, MinimumDurableCopies: 1}
	ctx := t.Context()
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	row, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
		Type:  model.TaskTypeBucketProvision,
		Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID, Copies: bucket.DefaultCopies},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted }, 15*time.Second)
	stopHandlerEngine(t, cancel, done)
	bindings, err := runtime.repos.Contents.ListDataSetBindings(ctx, bucket.ID)
	if err != nil || !openedSelected || len(bindings) != 1 || !bindings[0].ProviderID.Equal(idtypes.OnChainIDFromSDK(competingID)) || bindings[0].Status != model.StorageDataSetStatusReady {
		t.Fatalf("slot conflict left stale selection: bindings=%#v opened=%t err=%v", bindings, openedSelected, err)
	}
}

func TestUploadPlanWaitsForRequiredProvider(t *testing.T) {
	for _, tier := range []providerselect.Tier{providerselect.TierApproved, providerselect.TierEndorsed} {
		t.Run(string(tier), func(t *testing.T) {
			noRetries := 0
			providerID, dataSetID := sdktypes.NewBigInt(202), sdktypes.NewBigInt(2202)
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				providerTier: tier, maxAttempts: taskTestMaxAttempts(&noRetries),

				storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					return &testutil.MockStorageTarget{ProviderIDValue: providerID, DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(2)}, nil
				}},
			})
			seedProviderSelection(t, runtime, providerID)
			ctx := t.Context()
			recordTier := runtime.repos.Observability.RecordApprovedProviders
			if tier == providerselect.TierEndorsed {
				recordTier = runtime.repos.Observability.RecordEndorsedProviders
			}
			if _, err := recordTier(ctx, time.Now().UTC(), nil); err != nil {
				t.Fatal(err)
			}
			bucket := &model.Bucket{Name: "upload-selection", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
				t.Fatal(err)
			}
			content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
				BucketID: bucket.ID, ContentSize: 11, RequestedCopies: 1, Checksum: testutil.StorageChecksum("upload-selection"),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
				VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "object.bin", ContentID: &content.ID, Size: 11,
				ETag: "etag", ContentType: "application/octet-stream",
			}); err != nil {
				t.Fatal(err)
			}
			row, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
				Type:  model.TaskTypeUploadPlan,
				Input: storagepipeline.UploadPlanInput{ContentID: content.ID},
			})
			if err != nil {
				t.Fatal(err)
			}
			runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 3}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			for generation := int64(1); generation <= 2; generation++ {
				waiting := waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
					return row.ClaimGeneration == generation && (row.Status == model.TaskStatusFailed || (row.Status == model.TaskStatusPending && row.WaitReason != nil))
				})
				if waiting.Status != model.TaskStatusPending || waiting.WaitReason == nil || *waiting.WaitReason != "providers" || waiting.RetryCount != 0 || waiting.LastError != nil {
					t.Fatalf("missing %s provider did not wait: %#v", tier, waiting)
				}
				if generation == 1 {
					wakeTask(t, runtime, row.ID)
				}
			}
			if _, err := recordTier(ctx, time.Now().UTC(), []idtypes.OnChainID{idtypes.OnChainIDFromSDK(providerID)}); err != nil {
				t.Fatal(err)
			}
			wakeTask(t, runtime, row.ID)
			waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
			copies, err := runtime.repos.Contents.ListCopies(ctx, content.ID)
			if err != nil || len(copies) != 1 || !copies[0].ProviderID.Equal(idtypes.OnChainIDFromSDK(providerID)) {
				t.Fatalf("upload did not resume after provider recovery: copies=%#v err=%v", copies, err)
			}
		})
	}
}

func TestWorkerBindingSelectionRechecksUnfinishedReplacement(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeBucketProvision, model.TaskTypeUploadPlan} {
		t.Run(string(taskType), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			trustedID, selectedID, replacementID := testOnChainID(t, 101), testOnChainID(t, 202), testOnChainID(t, 303)
			var runtime handlerTestRuntime
			var bucket *model.Bucket
			var source *model.StorageDataSet
			var authorized atomic.Bool
			runtime = newHandlerTestRuntime(t, handlerRuntimeOptions{
				providerTier: providerselect.TierApproved,

				storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						dataSetID := sdktypes.NewBigInt(2202)
						return &testutil.MockStorageTarget{ProviderIDValue: id, DataSetIDValue: &dataSetID, ClientDataSetIDValue: sdktypes.NewBigInt(2)}, nil
					},
					PrepareUploadFunc: func(ctx context.Context, _ uint64, targets []synapse.StorageTarget) (*sdkcosts.MultiContextCosts, error) {
						if len(targets) != 2 || !targets[1].ProviderID().Equal(selectedID.SDK()) {
							return nil, fmt.Errorf("unexpected selected targets: %#v", targets)
						}
						if !authorized.CompareAndSwap(false, true) {
							return &sdkcosts.MultiContextCosts{Ready: true}, nil
						}
						// Authorize a valid replacement after selection but before binding settlement.
						_, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
							BucketID: bucket.ID, SourceDataSetID: source.ID, TargetProviderID: replacementID,
							SelectionMode: storagereplacement.SelectionModeManual, ClientRequestID: "selection-conflict",
						})
						return &sdkcosts.MultiContextCosts{Ready: true}, err
					},
				},
			})
			seedProviderSelection(t, runtime, trustedID.SDK(), selectedID.SDK(), replacementID.SDK())
			if _, err := runtime.repos.Observability.RecordApprovedProviders(ctx, time.Now().UTC(), []idtypes.OnChainID{trustedID, replacementID}); err != nil {
				t.Fatal(err)
			}
			// The replacement provider's existing load makes the untrusted provider rank first.
			loadedBucket := &model.Bucket{Name: "provider-load", DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(ctx, loadedBucket); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: loadedBucket.ID, ProviderID: replacementID, CopyIndex: 0}); err != nil {
				t.Fatal(err)
			}
			bucket = &model.Bucket{Name: "selection-conflict", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
				t.Fatal(err)
			}
			var err error
			source, err = runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: trustedID, CopyIndex: 0})
			if err != nil {
				t.Fatal(err)
			}
			if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{ID: source.ID, DataSetID: testOnChainID(t, 1101)}); err != nil {
				t.Fatal(err)
			}
			increasedCopies := 2
			if err := runtime.repos.Buckets.SetDefaultCopies(ctx, bucket.Name, &increasedCopies); err != nil {
				t.Fatal(err)
			}
			bucket.DefaultCopies = increasedCopies
			request := taskengine.EnqueueRequest{
				Type:  taskType,
				Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID, Copies: bucket.DefaultCopies},
			}
			var content *model.StorageContent
			if taskType == model.TaskTypeUploadPlan {
				content, err = runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
					BucketID: bucket.ID, ContentSize: 11, RequestedCopies: 2, Checksum: testutil.StorageChecksum("selection-conflict"),
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
					VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "object.bin", ContentID: &content.ID, Size: 11,
					ETag: "etag", ContentType: "application/octet-stream",
				}); err != nil {
					t.Fatal(err)
				}
				request.Input = storagepipeline.UploadPlanInput{ContentID: content.ID}
			}
			row, _, err := runtime.service.Enqueue(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			replanned := runStorageTaskClaims(t, runtime, row, model.TaskStatusPending, 2, 15*time.Second)
			if replanned.ClaimGeneration != 2 || replanned.WaitReason == nil || *replanned.WaitReason != "providers" || replanned.RetryCount != 0 || replanned.LastError != nil {
				t.Fatalf("task did not wait after replanning: %#v", replanned)
			}
			for generation := int64(3); generation <= 8; generation++ {
				wakeTask(t, runtime, row.ID)
				replanned = runOneStorageTask(t, runtime, replanned, model.TaskStatusPending)
				if replanned.Status != model.TaskStatusPending || replanned.RetryCount != 0 || replanned.LastError != nil || *replanned.WaitReason != "providers" {
					t.Fatalf("replacement reservation wait %d = %#v", generation-1, replanned)
				}
			}
			if reports := providerSelectionReports(t, runtime, row.ID); len(reports) != 1 {
				t.Fatalf("unchanged replacement reservation appended diagnostics: %#v", reports)
			}
			bindings, err := runtime.repos.Contents.ListDataSetBindings(ctx, bucket.ID)
			if err != nil || len(bindings) != 2 {
				t.Fatalf("binding rollback changed the existing generations: bindings=%#v err=%v", bindings, err)
			}
			for _, binding := range bindings {
				if binding.ProviderID.Equal(selectedID) {
					t.Fatalf("rejected selection was bound: %#v", binding)
				}
			}
			if content != nil {
				copies, err := runtime.repos.Contents.ListCopies(ctx, content.ID)
				if err != nil || len(copies) != 0 {
					t.Fatalf("rejected selection left an upload plan: copies=%#v err=%v", copies, err)
				}
			}
			replacement, err := runtime.repos.Replacements.GetByClientRequestID(ctx, bucket.ID, "selection-conflict")
			if err != nil || replacement == nil {
				t.Fatalf("replacement = %#v err=%v", replacement, err)
			}
			if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{ID: replacement.TargetDataSetID, DataSetID: testOnChainID(t, 2303)}); err != nil {
				t.Fatal(err)
			}
			if err := activateReplacement(t, runtime, replacement.ID); err != nil {
				t.Fatal(err)
			}
			if _, _, err := runtime.repos.Replacements.SeedMigrationBatch(ctx, replacement.ID, 128); err != nil {
				t.Fatal(err)
			}
			if content != nil {
				item, err := runtime.repos.Replacements.NextPendingReplacementItem(ctx, replacement.ID)
				if err != nil || item == nil {
					t.Fatalf("replacement migration item = %#v err=%v", item, err)
				}
				copies, err := runtime.repos.Contents.ListCopies(ctx, content.ID)
				if err != nil || len(copies) != 1 || copies[0].StorageDataSetID != replacement.TargetDataSetID {
					t.Fatalf("replacement target copies = %#v err=%v", copies, err)
				}
				pieceID := testOnChainID(t, 3303)
				testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
					StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0,
					PieceCID: testPieceCID(t, "replacement-release").String(), PieceID: &pieceID, RetrievalURL: "https://provider.example/piece",
				})
				if err := runtime.repos.Replacements.MarkReplacementItemCopied(ctx, replacement.ID, item.ID, copies[0].ID); err != nil {
					t.Fatal(err)
				}
			}
			if err := runtime.repos.Replacements.BeginRetirement(ctx, replacement.ID); err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.Replacements.RecordTerminationEpoch(ctx, repository.RecordTerminationEpochInput{ReplacementID: replacement.ID, Epoch: 100}); err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.Replacements.CompleteRetirement(ctx, replacement.ID, 100); err != nil {
				t.Fatal(err)
			}
			state := selectionProviderState(trustedID, time.Now().UTC())
			state.Status = observability.StatusDegraded
			if err := runtime.repos.Observability.UpsertProviderObservation(ctx, time.Now().UTC(), state); err != nil {
				t.Fatal(err)
			}
			wakeTask(t, runtime, row.ID)
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			row = waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusCompleted || row.Status == model.TaskStatusFailed || row.RetryCount > 0
			})
			if row.Status != model.TaskStatusCompleted || row.RetryCount != 0 {
				t.Fatalf("completed replacement did not release the waiting plan: %#v", row)
			}
		})
	}
}
