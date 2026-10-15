package repository_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
)

func TestProviderUploadSpeedBeginIfDueSerializesConcurrentStarts(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	hash := providerbenchmark.URLHash("https://provider.example")
	input, err := json.Marshal(providerbenchmark.Input{ProviderID: "101", ServiceURLHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
				task, _, err := tx.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
					Type: model.TaskTypeProviderUploadSpeedTest, InputVersion: 1, Input: input,
					SubjectType: "provider", SubjectKey: "101",
				}))
				if err != nil {
					return err
				}
				return tx.ProviderUploadSpeed.BeginIfDue(t.Context(), "101", hash, task.ID, time.Time{}, providerbenchmark.NonMeasuredFailureCodes())
			})
		}()
	}
	close(start)
	started, conflicted := 0, 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			started++
		case errors.Is(err, repository.ErrConflict):
			conflicted++
		default:
			t.Fatalf("concurrent measurement start: %v", err)
		}
	}
	if started != 1 || conflicted != 1 {
		t.Fatalf("started=%d, conflicted=%d", started, conflicted)
	}
	if count, err := db.NewSelect().Model((*model.Task)(nil)).Count(t.Context()); err != nil || count != 1 {
		t.Fatalf("committed task count=%d, err=%v", count, err)
	}
}

func TestProviderUploadSpeedBeginIfDueAdmitsOnlyDueMeasurements(t *testing.T) {
	for _, tc := range []struct {
		name        string
		state       providerbenchmark.State
		failure     string
		changedURL  bool
		periodic    bool
		expired     bool
		wantStarted bool
	}{
		{name: "first measurement", wantStarted: true},
		{name: "successful result", state: providerbenchmark.StateSucceeded},
		{name: "upload failure", state: providerbenchmark.StateFailed, failure: "upload_failed"},
		{name: "timeout", state: providerbenchmark.StateFailed, failure: "timeout"},
		{name: "unknown failure", state: providerbenchmark.StateFailed, failure: "future_failure"},
		{name: "interrupted", state: providerbenchmark.StateFailed, failure: "interrupted", wantStarted: true},
		{name: "changed URL", state: providerbenchmark.StateSucceeded, changedURL: true, wantStarted: true},
		{name: "active changed URL", state: providerbenchmark.StateTesting, changedURL: true},
		{name: "periodic success", state: providerbenchmark.StateSucceeded, periodic: true, expired: true, wantStarted: true},
		{name: "recent success", state: providerbenchmark.StateSucceeded, periodic: true},
		{name: "periodic failure", state: providerbenchmark.StateFailed, failure: "timeout", periodic: true, expired: true, wantStarted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			hash := providerbenchmark.URLHash("https://provider.example")
			if tc.state != "" {
				prior := enqueueProviderSpeedTask(t, repos, "101", hash, "previous")
				if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", hash, prior.ID); err != nil {
					t.Fatal(err)
				}
				if tc.state != providerbenchmark.StateTesting {
					if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", prior.ID, tc.state, providerbenchmark.MinimumSampleBytes, 1000, providerbenchmark.MinimumSampleBytes, tc.failure); err != nil {
						t.Fatal(err)
					}
					if tc.expired {
						if _, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).Set("tested_at = ?", time.Now().Add(-2*time.Hour)).Where("provider_id = ?", "101").Exec(t.Context()); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if tc.changedURL {
				hash = providerbenchmark.URLHash("https://changed.example")
			}
			var testedBefore time.Time
			if tc.periodic {
				testedBefore = time.Now().Add(-time.Hour)
			}
			before, err := db.NewSelect().Model((*model.Task)(nil)).Count(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			err = repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
				next := enqueueProviderSpeedTask(t, tx, "101", hash, "automatic")
				return tx.ProviderUploadSpeed.BeginIfDue(t.Context(), "101", hash, next.ID, testedBefore, providerbenchmark.NonMeasuredFailureCodes())
			})
			if tc.wantStarted && err != nil || !tc.wantStarted && !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("BeginIfDue = %v, want started=%v", err, tc.wantStarted)
			}
			after, err := db.NewSelect().Model((*model.Task)(nil)).Count(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			wantCount := before
			if tc.wantStarted {
				wantCount++
			}
			if after != wantCount {
				t.Fatalf("task count=%d, want %d", after, wantCount)
			}
		})
	}
}

func TestProviderUploadSpeedKeepsOnlyLatestResultAcrossHealthRefresh(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	hash := providerbenchmark.URLHash("https://provider.example")
	first := enqueueProviderSpeedTask(t, repos, "101", hash, "first-speed-test").ID
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", providerbenchmark.URLHash("https://changed.example"), first); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("Begin with mismatched URL = %v, want conflict", err)
	}
	if result, err := repos.ProviderUploadSpeed.Get(t.Context(), "101"); err != nil || result != nil {
		t.Fatalf("rejected test created result: %#v, %v", result, err)
	}
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", hash, first); err != nil {
		t.Fatal(err)
	}
	second := enqueueProviderSpeedTask(t, repos, "101", hash, "second-speed-test").ID
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", hash, second); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("concurrent Begin = %v, want conflict", err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", first, providerbenchmark.StateSucceeded, providerbenchmark.MinimumSampleBytes, 1000, providerbenchmark.MinimumSampleBytes, ""); err != nil {
		t.Fatal(err)
	}
	if err := repos.ProviderUploadSpeed.BeginIfDue(t.Context(), "101", hash, second, time.Time{}, nil); !errors.Is(err, repository.ErrConflict) {
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
	if active, err := repos.ProviderUploadSpeed.Get(t.Context(), "101"); err != nil || active == nil {
		t.Fatalf("retesting result = %#v, %v", active, err)
	} else if speed, settled := active.Outcome("https://provider.example"); !settled || speed != providerbenchmark.MinimumSampleBytes || active.SampleBytes != providerbenchmark.MinimumSampleBytes || active.State != providerbenchmark.StateTesting {
		t.Fatalf("retesting result lost measurement: %#v", active)
	}
	if err := repos.ProviderUploadSpeed.FailActiveTask(t.Context(), first, "handler_panic"); err != nil {
		t.Fatal(err)
	}
	if active, err := repos.ProviderUploadSpeed.Get(t.Context(), "101"); err != nil || active == nil || active.State != providerbenchmark.StateTesting || active.ActiveTaskID == nil || *active.ActiveTaskID != second {
		t.Fatalf("new active test after stale task failure = %#v, %v", active, err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", first, providerbenchmark.StateSucceeded, providerbenchmark.SampleBytes, 1000, providerbenchmark.SampleBytes, ""); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale task Finish = %v, want conflict", err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", second, providerbenchmark.StateFailed, 0, 0, 0, "timeout"); err != nil {
		t.Fatal(err)
	}
	row, err = repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateFailed || row.BytesPerSecond != nil || row.ActiveTaskID != nil || row.FailureCode == nil || *row.FailureCode != "timeout" {
		t.Fatalf("latest result = %#v, %v", row, err)
	}
	third := enqueueProviderSpeedTask(t, repos, "101", hash, "third-speed-test").ID
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", hash, third); err != nil {
		t.Fatal(err)
	}
	row, err = repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateTesting || row.FailureCode == nil || *row.FailureCode != "timeout" {
		t.Fatalf("retesting failure = %#v, %v", row, err)
	}
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", third, providerbenchmark.StateSucceeded, providerbenchmark.SampleBytes, 2000, providerbenchmark.SampleBytes/2, ""); err != nil {
		t.Fatal(err)
	}
	changedHash := providerbenchmark.URLHash("https://changed.example")
	fourth := enqueueProviderSpeedTask(t, repos, "101", changedHash, "changed-speed-test").ID
	if err := repos.ProviderUploadSpeed.Begin(t.Context(), "101", changedHash, fourth); err != nil {
		t.Fatal(err)
	}
	row, err = repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.DurationMS != nil || row.BytesPerSecond != nil || row.TestedAt != nil || row.FailureCode != nil || row.ServiceURLHash != changedHash {
		t.Fatalf("changed URL retained a result: %#v, %v", row, err)
	}
}

func TestProviderUploadSpeedCompletionPreservesTaskHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	enqueueProviderSpeedTask(t, repos, "101", providerbenchmark.URLHash("https://provider.example"), "speed-task-history")
	claimed, err := repos.Tasks.ClaimNext(t.Context(), time.Minute, repository.TaskClaimFilter{})
	if err != nil || claimed == nil {
		t.Fatalf("claim speed task = %#v, %v", claimed, err)
	}
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
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", claimed.ID, providerbenchmark.StateSucceeded, providerbenchmark.SampleBytes, 1000, providerbenchmark.SampleBytes, ""); err != nil {
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
