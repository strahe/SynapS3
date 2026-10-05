package worker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synaps3/internal/worker"
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
				register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
					return handlers.RegisterStorage(registry)
				},
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
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
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
