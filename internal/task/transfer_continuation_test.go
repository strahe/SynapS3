package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestCommittedSourceWakesPeerPullPlanWaitingForSource(t *testing.T) {
	nonces := &testutil.MockCommitNonces{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{commitNonces: nonces})
	db, repos := runtime.db, runtime.repos
	bucket := &model.Bucket{Name: "wake-peer-pull", Status: model.BucketStatusActive, DefaultCopies: 3, MinimumDurableCopies: 2}
	if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	testutil.OpenBucketReplicaSlots(t, db, bucket.ID, 3)
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 16,
		Checksum: testutil.StorageChecksum("wake-peer-pull"), RequestedCopies: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindings := make([]*model.StorageDataSet, 3)
	for index := range bindings {
		providerID, err := types.ParseOnChainID("provider_id", fmt.Sprint(501+index))
		if err != nil {
			t.Fatal(err)
		}
		binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: index,
		})
		if err != nil {
			t.Fatal(err)
		}
		dataSetID, err := types.ParseOnChainID("data_set_id", fmt.Sprint(601+index))
		if err != nil {
			t.Fatal(err)
		}
		clientID, err := types.ParseOnChainID("client_data_set_id", fmt.Sprint(701+index))
		if err != nil {
			t.Fatal(err)
		}
		if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
		}); err != nil {
			t.Fatal(err)
		}
		bindings[index] = binding
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: bindings[0].ID, CopyIndex: 0, ProviderID: bindings[0].ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress},
		{StorageDataSetID: bindings[1].ID, CopyIndex: 1, ProviderID: bindings[1].ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull},
		{StorageDataSetID: bindings[2].ID, CopyIndex: 2, ProviderID: bindings[2].ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull},
	}); err != nil {
		t.Fatal(err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 3 {
		t.Fatalf("copies = %#v, err=%v", copies, err)
	}

	version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "source.bin", ContentID: &content.ID, Size: content.ContentSize, ETag: "source", ContentType: "application/octet-stream"}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
		StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0, LastError: "ingress failed",
	}); err != nil {
		t.Fatal(err)
	}
	promoted, err := repos.Contents.PromotePendingIngress(t.Context(), content.ID)
	if err != nil || promoted == nil || promoted.ID != copies[1].ID {
		t.Fatalf("promoted ingress = %#v, %v", promoted, err)
	}
	if err := repos.Contents.MarkUploadCopyPieceReady(t.Context(), repository.MarkUploadCopyPieceReadyInput{
		StorageCopyID: promoted.ID, ContentID: content.ID, CopyIndex: 1,
		PieceCID: testPieceCID(t, "wake-peer-piece").String(), RequireEligibleCopy: true,
	}); err != nil {
		t.Fatal(err)
	}
	provider := newRegistrationProvider(t, testOnChainID(t, 502).SDK(), testOnChainID(t, 602).SDK(), testOnChainID(t, 702).SDK(), nonces)
	runtime.storage.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return provider.target, nil
	}
	generation, err := repos.Contents.NextCopyWorkGeneration(t.Context(), copies[2].ID)
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour)
	peer, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageTransferPlan, IdempotencyKey: storagepipeline.TransferPlanKey(copies[2].ID, generation),
		Input: storagepipeline.CopyGenerationInput{CopyID: copies[2].ID, Generation: generation}, AvailableAt: future,
	})
	if err != nil || !created {
		t.Fatalf("peer plan = %#v, created=%v, %v", peer, created, err)
	}
	if err := repos.Contents.BindCopyTask(t.Context(), copies[2].ID, generation, peer.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).Set("wait_reason = ?", "source").Where("id = ?", peer.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture := registrationFixture{runtime: runtime, provider: provider, dataSet: bindings[1], copies: []*model.StorageCopy{promoted}}
	requestID, sourceTaskID := fixture.collect(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
		PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration, Retention: time.Hour,
		OnTaskSettled: func(claimed *model.Task, transition repository.TaskTransition) {
			if claimed.ID == sourceTaskID && transition.Status == model.TaskStatusCompleted {
				cancel()
			}
		},
	}, repos, runtime.registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = engine.Run(ctx); close(done) }()
	defer stopHandlerEngine(t, cancel, done)
	waitForCommitTask(t, runtime, sourceTaskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	stopHandlerEngine(t, cancel, done)
	if request := fixture.request(t, requestID); request.Status != storagecommit.RequestStatusConfirmed {
		t.Fatalf("source request = %#v", request)
	}
	source, err := repos.Contents.GetUploadCopyByID(t.Context(), promoted.ID)
	if err != nil || source.Status != model.StorageCopyStatusCommitted {
		t.Fatalf("source copy = %#v, %v", source, err)
	}
	reopened, err := repos.Contents.GetUploadCopyByID(t.Context(), copies[0].ID)
	if err != nil || reopened.Status != model.StorageCopyStatusPending || reopened.TransferMethod != model.StorageCopyTransferMethodPeerPull ||
		reopened.WorkGeneration != copies[0].WorkGeneration+1 || reopened.ActiveTaskID == nil {
		t.Fatalf("reopened ingress = %#v, %v", reopened, err)
	}
	recovery, err := repos.Tasks.GetByID(t.Context(), *reopened.ActiveTaskID)
	if err != nil || recovery == nil || recovery.Type != model.TaskTypeStorageTransferPlan || recovery.Status != model.TaskStatusPending {
		t.Fatalf("recovery task = %#v, %v", recovery, err)
	}
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(recovery.Input, &input); err != nil || input.CopyID != reopened.ID || input.Generation != reopened.WorkGeneration {
		t.Fatalf("recovery input = %#v, %v", input, err)
	}
	if recovery.IdempotencyKey != storagepipeline.TransferPlanKey(reopened.ID, reopened.WorkGeneration) {
		t.Fatalf("recovery key = %s", recovery.IdempotencyKey)
	}
	woken, err := repos.Tasks.GetByID(t.Context(), peer.ID)
	if err != nil || woken == nil || woken.Status != model.TaskStatusPending || !woken.AvailableAt.Before(future) {
		t.Fatalf("woken peer plan = %#v, %v", woken, err)
	}
}
