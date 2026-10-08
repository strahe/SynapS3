package repository_test

import (
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
)

func TestProviderUploadSpeedKeepsOnlyLatestResultAcrossHealthRefresh(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	newTask := func(key string) int64 {
		t.Helper()
		row, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
			Type: model.TaskTypeProviderUploadSpeedTest, IdempotencyKey: key, InputVersion: 1,
			Input: []byte(`{}`), InputHash: key, Status: model.TaskStatusPending,
			ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		}))
		if err != nil || !created {
			t.Fatalf("enqueue task: %v, created=%v", err, created)
		}
		return row.ID
	}
	first := newTask("first-speed-test")
	hash := providerbenchmark.URLHash("https://provider.example")
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", hash, first); err != nil {
		t.Fatal(err)
	}
	second := newTask("second-speed-test")
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", hash, second); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("concurrent Begin = %v, want conflict", err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", first, providerbenchmark.StateSucceeded, 1000, providerbenchmark.SampleBytes, ""); err != nil {
		t.Fatal(err)
	}
	if err := repos.ProviderUploadSpeed.BeginIfAbsent(t.Context(), "101", hash, second); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("automatic retest after success = %v, want conflict", err)
	}
	if err := repos.Observability.ReplaceProviderStates(t.Context(), time.Now(), []observability.ProviderState{{
		ProviderID: onChainID(t, "101"), Status: observability.StatusAvailable,
		ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
	}}); err != nil {
		t.Fatal(err)
	}
	row, err := repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateSucceeded || row.BytesPerSecond == nil {
		t.Fatalf("result after refresh = %#v, %v", row, err)
	}
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", hash, second); err != nil {
		t.Fatal(err)
	}
	if err := repos.ProviderUploadSpeed.FailActiveTask(t.Context(), first, "handler_panic"); err != nil {
		t.Fatal(err)
	}
	if active, err := repos.ProviderUploadSpeed.Get(t.Context(), "101"); err != nil || active == nil || active.State != providerbenchmark.StateTesting || active.ActiveTaskID == nil || *active.ActiveTaskID != second {
		t.Fatalf("new active test after stale task failure = %#v, %v", active, err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", first, providerbenchmark.StateSucceeded, 1000, providerbenchmark.SampleBytes, ""); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale task Finish = %v, want conflict", err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", second, providerbenchmark.StateFailed, 0, 0, "timeout"); err != nil {
		t.Fatal(err)
	}
	row, err = repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateFailed || row.BytesPerSecond != nil || row.ActiveTaskID != nil || row.FailureCode == nil || *row.FailureCode != "timeout" {
		t.Fatalf("latest result = %#v, %v", row, err)
	}
}

func TestProviderUploadSpeedCompletionPreservesTaskHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	claimed := enqueueAndClaimTask(t, repos, "speed-task-gc", time.Minute)
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", providerbenchmark.URLHash("https://provider.example"), claimed.ID); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover,
	}); err != nil {
		t.Fatal(err)
	}
	if stored, err := repos.Tasks.GetByID(t.Context(), claimed.ID); err != nil || stored == nil {
		t.Fatalf("task history missing: %#v, %v", stored, err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", claimed.ID, providerbenchmark.StateSucceeded, 1000, providerbenchmark.SampleBytes, ""); err != nil {
		t.Fatal(err)
	}
	if stored, err := repos.Tasks.GetByID(t.Context(), claimed.ID); err != nil || stored == nil {
		t.Fatalf("task history missing: %#v, %v", stored, err)
	}
	row, err := repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateSucceeded {
		t.Fatalf("result after task cleanup = %#v, %v", row, err)
	}
}
