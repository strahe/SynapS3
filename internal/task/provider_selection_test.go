package task_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
	sdkcosts "github.com/strahe/synapse-go/costs"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

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
				Type:           model.TaskTypeBucketProvision,
				IdempotencyKey: bucketlifecycle.ProvisionKey(bucket.ID, copies), Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID}, SubjectType: "bucket", SubjectKey: fmt.Sprint(bucket.ID),
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
		Type:           model.TaskTypeBucketProvision,
		IdempotencyKey: bucketlifecycle.ProvisionKey(bucket.ID, 1), Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID}, SubjectType: "bucket", SubjectKey: fmt.Sprint(bucket.ID),
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
				Type: model.TaskTypeUploadPlan, IdempotencyKey: storagepipeline.UploadPlanKey(content.ID),
				Input: storagepipeline.UploadPlanInput{ContentID: content.ID}, SubjectType: "storage_content", SubjectKey: fmt.Sprint(content.ID),
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
			runtime = newHandlerTestRuntime(t, handlerRuntimeOptions{
				providerTier: providerselect.TierApproved,

				storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						return &testutil.MockStorageTarget{ProviderIDValue: id}, nil
					},
					PrepareUploadFunc: func(ctx context.Context, _ uint64, targets []synapse.StorageTarget) (*sdkcosts.MultiContextCosts, error) {
						if len(targets) != 2 || !targets[1].ProviderID().Equal(selectedID.SDK()) {
							return nil, fmt.Errorf("unexpected selected targets: %#v", targets)
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
			bucket = &model.Bucket{Name: "selection-conflict", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
				t.Fatal(err)
			}
			var err error
			source, err = runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: trustedID, CopyIndex: 0})
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: source.ID, DataSetID: testOnChainID(t, 1101)}); err != nil {
				t.Fatal(err)
			}
			request := taskengine.EnqueueRequest{
				Type: taskType, IdempotencyKey: bucketlifecycle.ProvisionKey(bucket.ID, 2),
				Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID}, SubjectType: "bucket", SubjectKey: fmt.Sprint(bucket.ID),
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
				request.IdempotencyKey, request.Input = storagepipeline.UploadPlanKey(content.ID), storagepipeline.UploadPlanInput{ContentID: content.ID}
				request.SubjectType, request.SubjectKey = "storage_content", fmt.Sprint(content.ID)
			}
			row, _, err := runtime.service.Enqueue(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			replanned := waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
				return row.ClaimGeneration == 2 && (row.Status == model.TaskStatusFailed || (row.Status == model.TaskStatusPending && row.WaitReason != nil && *row.WaitReason == "providers"))
			}, 15*time.Second)
			if taskType == model.TaskTypeBucketProvision && (replanned.LastError == nil || !strings.Contains(*replanned.LastError, providerselect.ErrNoTrustedProvider.Error())) {
				t.Fatalf("binding settlement did not reject the changed set: %#v", replanned)
			}
			if taskType == model.TaskTypeBucketProvision {
				if replanned.Status != model.TaskStatusFailed || replanned.FailureReason == nil || *replanned.FailureReason != "required_provider_unavailable" {
					t.Fatalf("provisioning did not preserve its missing-provider failure: %#v", replanned)
				}
			} else if replanned.Status != model.TaskStatusPending || replanned.RetryCount != 0 || replanned.LastError != nil {
				t.Fatalf("upload did not wait after replanning: %#v", replanned)
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
		})
	}
}
