package repository_test

import (
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/providerselect"
)

func TestProviderSelectionIgnoresRetiredLoadAndOldURLSpeed(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := &model.Bucket{Name: "selection-load", DefaultCopies: 2, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	old, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("status = ?", model.StorageDataSetStatusRetired).Set("is_current = false").Where("id = ?", old.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 0}); err != nil {
		t.Fatal(err)
	}
	row, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: model.TaskTypeProviderUploadSpeedTest, IdempotencyKey: "selection-speed", InputVersion: 1, Input: []byte(`{}`), InputHash: "selection-speed", Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.ProviderUploadSpeed.Begin(ctx, "101", providerbenchmark.URLHash("https://old.example"), row.ID); err != nil {
		t.Fatal(err)
	}
	if err := repos.ProviderUploadSpeed.Finish(ctx, "101", row.ID, providerbenchmark.StateSucceeded, 1000, providerbenchmark.SampleBytes, ""); err != nil {
		t.Fatal(err)
	}
	in := providerselect.Inventory{Admission: providerselect.Admission{Tier: providerselect.TierNone}, Candidates: []providerselect.Candidate{
		{ID: onChainID(t, "101"), ServiceURL: "https://new.example", BytesPerSecond: 99},
		{ID: onChainID(t, "202"), ServiceURL: "https://other.example"},
	}}
	if err := repos.EnrichProviderCandidates(ctx, &in); err != nil {
		t.Fatal(err)
	}
	if in.Candidates[0].Load != 0 || in.Candidates[0].BytesPerSecond != 0 || in.Candidates[1].Load != 1 {
		t.Fatalf("load and speed = %#v", in.Candidates)
	}
	excluded, _, err := repos.ProviderSelectionState(ctx, bucket.ID, 0, in.Admission)
	if err != nil || excluded["101"] || !excluded["202"] {
		t.Fatalf("provider reuse = %v, err=%v", excluded, err)
	}
	in.Candidates[0].ServiceURL = "https://old.example"
	if err := repos.EnrichProviderCandidates(ctx, &in); err != nil {
		t.Fatal(err)
	}
	if in.Candidates[0].BytesPerSecond <= 0 {
		t.Fatal("matching completed speed test was ignored")
	}
}
