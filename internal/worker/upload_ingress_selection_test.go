package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/storagepipeline"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
)

type unusedSpeedProbe struct{}

func (unusedSpeedProbe) Probe(context.Context, string) (time.Duration, error) { return 0, nil }

func TestReadyBucketSchedulesOneSpeedTestWhenProviderBecomesAvailable(t *testing.T) {
	db := testutil.NewTestFileDB(t)
	repos := repository.NewRepositories(db)
	bucket := &model.Bucket{Name: "auto-speed", Status: model.BucketStatusReady, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	states := make([]observability.ProviderState, 2)
	for index := range 2 {
		parseID := func(prefix int) types.OnChainID {
			id, err := types.ParseOnChainID("provider_id", fmt.Sprint(prefix+index))
			if err != nil {
				t.Fatal(err)
			}
			return id
		}
		providerID := parseID(101)
		binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: index,
		})
		if err != nil {
			t.Fatal(err)
		}
		dataSetID, clientID := parseID(201), parseID(301)
		if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
		}); err != nil {
			t.Fatal(err)
		}
		url := fmt.Sprintf("https://provider-%d.example", index)
		states[index] = observability.ProviderState{
			ProviderID: providerID, Status: observability.StatusAvailable,
			Active: new(true), HasPDP: new(true), ServiceURL: &url,
			LastCheckedAt: now, ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
		}
	}
	if err := repos.Observability.ReplaceProviderStates(t.Context(), now, states[:1]); err != nil {
		t.Fatal(err)
	}
	h := &TaskHandlers{deps: TaskHandlerDependencies{
		Repositories: repos, Observability: observability.NewService(observability.ServiceOptions{Store: repos.Observability}),
		UploadSpeedProbe: unusedSpeedProbe{},
	}}
	registry := taskengine.NewRegistry()
	if err := registry.Register(h.providerUploadSpeedHandler()); err != nil {
		t.Fatal(err)
	}
	service, err := taskengine.NewService(registry, repos, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	h.taskService = service
	if err := h.scheduleMissingProviderSpeedTests(t.Context()); err != nil {
		t.Fatal(err)
	}
	first, err := repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || first == nil || first.State != providerbenchmark.StateTesting {
		t.Fatalf("first provider test = %#v, err=%v", first, err)
	}
	if second, err := repos.ProviderUploadSpeed.Get(t.Context(), "102"); err != nil || second != nil {
		t.Fatalf("unavailable provider test = %#v, err=%v", second, err)
	}
	if err := repos.Observability.ReplaceProviderStates(t.Context(), now, states); err != nil {
		t.Fatal(err)
	}
	if err := h.scheduleMissingProviderSpeedTests(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := repos.ProviderUploadSpeed.Get(t.Context(), "102")
	if err != nil || second == nil || second.State != providerbenchmark.StateTesting {
		t.Fatalf("recovered provider test = %#v, err=%v", second, err)
	}
	if err := h.scheduleMissingProviderSpeedTests(t.Context()); err != nil {
		t.Fatal(err)
	}
	firstAgain, err := repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || firstAgain.ActiveTaskID == nil || *firstAgain.ActiveTaskID != *first.ActiveTaskID {
		t.Fatalf("duplicate first provider test = %#v, err=%v", firstAgain, err)
	}
}

func TestFastestIngressRequiresValidSpeedForEveryProvider(t *testing.T) {
	db := testutil.NewTestFileDB(t)
	repos := repository.NewRepositories(db)
	now := time.Now().UTC()
	states := make([]observability.ProviderState, 2)
	plan := make([]uploadBindingPlan, 2)
	for i, textID := range []string{"101", "102"} {
		id, err := types.ParseOnChainID("provider_id", textID)
		if err != nil {
			t.Fatal(err)
		}
		url := "https://provider-" + textID + ".example"
		states[i] = observability.ProviderState{
			ProviderID: id, Status: observability.StatusAvailable,
			Active: new(true), HasPDP: new(true), ServiceURL: &url,
			LastCheckedAt: now, ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
		}
		plan[i] = uploadBindingPlan{copyIndex: i, provider: id}
		seconds := int64(10 + 10*i)
		duration := int64(1000)
		row := &providerbenchmark.Result{
			ProviderID: textID, State: providerbenchmark.StateSucceeded,
			ServiceURLHash: providerbenchmark.URLHash(url), SampleBytes: providerbenchmark.SampleBytes,
			DurationMS: &duration, BytesPerSecond: &seconds, TestedAt: &now,
			CreatedAt: now, UpdatedAt: now,
		}
		if _, err := db.NewInsert().Model(row).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := repos.Observability.ReplaceProviderStates(t.Context(), now, states); err != nil {
		t.Fatal(err)
	}
	h := &TaskHandlers{deps: TaskHandlerDependencies{
		Repositories: repos, Observability: observability.NewService(observability.ServiceOptions{Store: repos.Observability}),
	}}
	if got := h.fastestIngressIndex(t.Context(), plan); got != 1 {
		t.Fatalf("fastest ingress = %d, want 1", got)
	}
	if _, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("service_url_hash = ?", providerbenchmark.URLHash("https://changed.example")).
		Where("provider_id = ?", "102").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := h.fastestIngressIndex(t.Context(), plan); got != 0 {
		t.Fatalf("changed provider URL ingress = %d, want original order", got)
	}
	if _, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("service_url_hash = ?", providerbenchmark.URLHash(*states[1].ServiceURL)).
		Where("provider_id = ?", "102").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewDelete().Model((*providerbenchmark.Result)(nil)).Where("provider_id = ?", "101").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := h.fastestIngressIndex(t.Context(), plan); got != 0 {
		t.Fatalf("missing speed ingress = %d, want original order", got)
	}
}

func TestCommittedSourceWakesPeerPullPlanWaitingForSource(t *testing.T) {
	db := testutil.NewTestFileDB(t)
	repos := repository.NewRepositories(db)
	bucket := &model.Bucket{Name: "wake-peer-pull", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	testutil.OpenBucketReplicaSlots(t, db, bucket.ID, 2)
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 16,
		Checksum: testutil.StorageChecksum("wake-peer-pull"), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindings := make([]*model.StorageDataSet, 2)
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
	}); err != nil {
		t.Fatal(err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("copies = %#v, err=%v", copies, err)
	}

	h := &TaskHandlers{deps: TaskHandlerDependencies{Repositories: repos}}
	registry := taskengine.NewRegistry()
	if err := registry.Register(h.transferPlanHandler()); err != nil {
		t.Fatal(err)
	}
	service, err := taskengine.NewService(registry, repos, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	h.taskService = service
	generation, err := repos.Contents.NextCopyWorkGeneration(t.Context(), copies[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	taskRow, created, err := service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageTransferPlan, IdempotencyKey: storagepipeline.TransferPlanKey(copies[1].ID, generation),
		Input:       storagepipeline.CopyGenerationInput{CopyID: copies[1].ID, Generation: generation},
		SubjectType: "storage_copy", SubjectKey: fmt.Sprint(copies[1].ID),
	})
	if err != nil || !created {
		t.Fatalf("enqueue peer plan = %#v, created=%v, err=%v", taskRow, created, err)
	}
	if err := repos.Contents.BindCopyTask(t.Context(), copies[1].ID, generation, taskRow.ID); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour)
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).
		Set("available_at = ?", future).Set("wait_reason = ?", "source").
		Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := repos.WithTx(t.Context(), func(txRepos *repository.Repositories) error {
		return h.wakePeerPullPlans(t.Context(), txRepos, content.ID)
	}); err != nil {
		t.Fatal(err)
	}
	woken, err := repos.Tasks.GetByID(t.Context(), taskRow.ID)
	if err != nil || woken == nil || !woken.AvailableAt.Before(future) {
		t.Fatalf("woken peer plan = %#v, err=%v", woken, err)
	}
}
