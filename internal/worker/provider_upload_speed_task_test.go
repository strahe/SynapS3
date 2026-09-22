package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	taskengine "github.com/strahe/synaps3/internal/task"
)

type uploadSpeedProbeFunc func(context.Context, string) (time.Duration, error)

func (f uploadSpeedProbeFunc) Probe(ctx context.Context, url string) (time.Duration, error) {
	return f(ctx, url)
}

func seedSpeedTest(t *testing.T, runtime handlerTestRuntime) *model.Task {
	t.Helper()
	serviceURL := "https://provider.example"
	if err := runtime.repos.Observability.ReplaceProviderStates(t.Context(), time.Now().UTC(), []observability.ProviderState{{
		ProviderID: testOnChainID(t, 101), Status: observability.StatusAvailable,
		Active: new(true), HasPDP: new(true), ServiceURL: &serviceURL, LastCheckedAt: time.Now().UTC(),
		ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
	}}); err != nil {
		t.Fatal(err)
	}
	input := providerbenchmark.Input{ProviderID: "101", ServiceURLHash: providerbenchmark.URLHash(serviceURL)}
	taskRow, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeProviderUploadSpeedTest, IdempotencyKey: "speed-101", Input: input,
		SubjectType: "provider", SubjectKey: "101",
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.ProviderUploadSpeed.Begin(ctx, "101", input.ServiceURLHash, taskRow.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return taskRow
}

func TestProviderUploadSpeedTaskRecordsSuccessfulMeasurement(t *testing.T) {
	var calls atomic.Int32
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(_ context.Context, url string) (time.Duration, error) {
		calls.Add(1)
		if url != "https://provider.example" {
			t.Errorf("probe URL = %q", url)
		}
		return 2 * time.Second, nil
	})})
	taskRow := seedSpeedTest(t, runtime)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	row, err := runtime.repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateSucceeded || row.DurationMS == nil || *row.DurationMS != 2000 || row.ActiveTaskID != nil {
		t.Fatalf("speed result = %#v, %v", row, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("probe calls = %d", calls.Load())
	}
}

func TestProviderUploadSpeedEngineFailureReleasesActiveTest(t *testing.T) {
	for _, tt := range []struct {
		name      string
		reason    string
		breakTask func(*testing.T, handlerTestRuntime, *model.Task)
	}{
		{
			name: "handler panic", reason: "handler_panic",
			breakTask: func(*testing.T, handlerTestRuntime, *model.Task) {},
		},
		{
			name: "invalid input hash", reason: "invalid_input_hash",
			breakTask: func(t *testing.T, runtime handlerTestRuntime, taskRow *model.Task) {
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("input_hash = ?", providerbenchmark.URLHash("wrong input")).
					Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(context.Context, string) (time.Duration, error) {
				calls.Add(1)
				panic("probe failed unexpectedly")
			})})
			taskRow := seedSpeedTest(t, runtime)
			tt.breakTask(t, runtime, taskRow)
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
			if failed.FailureReason == nil || *failed.FailureReason != tt.reason {
				t.Fatalf("task failure reason = %v, want %s", failed.FailureReason, tt.reason)
			}
			row, err := runtime.repos.ProviderUploadSpeed.Get(t.Context(), "101")
			if err != nil || row == nil || row.State != providerbenchmark.StateFailed || row.ActiveTaskID != nil ||
				row.FailureCode == nil || *row.FailureCode != tt.reason {
				t.Fatalf("speed result after engine failure = %#v, err=%v", row, err)
			}
			if tt.reason == "handler_panic" && calls.Load() != 1 || tt.reason == "invalid_input_hash" && calls.Load() != 0 {
				t.Fatalf("probe calls = %d, reason=%s", calls.Load(), tt.reason)
			}
		})
	}
}

func TestProviderUploadSpeedRecoveryNeverReuploads(t *testing.T) {
	var calls atomic.Int32
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(context.Context, string) (time.Duration, error) {
		calls.Add(1)
		return 0, errors.New("should not upload")
	})})
	taskRow := seedSpeedTest(t, runtime)
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().TableExpr("task_payloads").Set("checkpoint_json = ?", `{"attempted":true}`).
		Where("task_id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	row, err := runtime.repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateFailed || row.ActiveTaskID != nil {
		t.Fatalf("recovered result = %#v, %v", row, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("probe calls during recovery = %d", calls.Load())
	}
}

func TestProviderUploadSpeedRecoverySettlesCompletedPutWithoutReupload(t *testing.T) {
	var calls atomic.Int32
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(context.Context, string) (time.Duration, error) {
		calls.Add(1)
		return 0, errors.New("should not upload")
	})})
	taskRow := seedSpeedTest(t, runtime)
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().TableExpr("task_payloads").
		Set("checkpoint_json = ?", `{"attempted":true,"duration_ms":2000,"bytes_per_second":16777216}`).
		Where("task_id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	row, err := runtime.repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateSucceeded || row.BytesPerSecond == nil || *row.BytesPerSecond != 16777216 || row.ActiveTaskID != nil {
		t.Fatalf("recovered result = %#v, %v", row, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("probe calls during recovery = %d", calls.Load())
	}
}
