package task_test

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
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type uploadSpeedProbeFunc func(context.Context, string) (providerbenchmark.Measurement, error)

func (f uploadSpeedProbeFunc) Probe(ctx context.Context, url string) (providerbenchmark.Measurement, error) {
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
		Type: model.TaskTypeProviderUploadSpeedTest, Input: input,
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
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(_ context.Context, url string) (providerbenchmark.Measurement, error) {
		calls.Add(1)
		if url != "https://provider.example" {
			t.Errorf("probe URL = %q", url)
		}
		return providerbenchmark.Measurement{Bytes: providerbenchmark.MinimumSampleBytes, Duration: 2 * time.Second}, nil
	})})
	taskRow := seedSpeedTest(t, runtime)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	row, err := runtime.repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateSucceeded || row.DurationMS == nil || *row.DurationMS != 2000 ||
		row.SampleBytes != providerbenchmark.MinimumSampleBytes || row.BytesPerSecond == nil || *row.BytesPerSecond != providerbenchmark.MinimumSampleBytes/2 || row.ActiveTaskID != nil {
		t.Fatalf("speed result = %#v, %v", row, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("probe calls = %d", calls.Load())
	}
}

func TestProviderUploadSpeedEngineFailureReleasesActiveTest(t *testing.T) {
	var calls atomic.Int32
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(context.Context, string) (providerbenchmark.Measurement, error) {
		calls.Add(1)
		panic("probe failed unexpectedly")
	})})
	taskRow := seedSpeedTest(t, runtime)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "handler_panic" {
		t.Fatalf("task failure reason = %v, want handler_panic", failed.FailureReason)
	}
	row, err := runtime.repos.ProviderUploadSpeed.Get(t.Context(), "101")
	if err != nil || row == nil || row.State != providerbenchmark.StateFailed || row.ActiveTaskID != nil ||
		row.FailureCode == nil || *row.FailureCode != "handler_panic" || calls.Load() != 1 {
		t.Fatalf("speed result after engine failure = %#v, calls=%d, err=%v", row, calls.Load(), err)
	}
}

func TestProviderUploadSpeedRecoveryNeverReuploads(t *testing.T) {
	var calls atomic.Int32
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(context.Context, string) (providerbenchmark.Measurement, error) {
		calls.Add(1)
		return providerbenchmark.Measurement{}, errors.New("should not upload")
	})})
	taskRow := seedSpeedTest(t, runtime)
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().TableExpr("tasks").Set("checkpoint_json = ?", `{"attempted":true}`).
		Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
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
	for _, tc := range []struct {
		name       string
		checkpoint string
		bytes      int64
		speed      int64
	}{
		{"implicit sample size", `{"attempted":true,"duration_ms":2000,"bytes_per_second":16777216}`, providerbenchmark.SampleBytes, 16777216},
		{"partial sample", `{"attempted":true,"sample_bytes":8388608,"duration_ms":2000,"bytes_per_second":4194304}`, providerbenchmark.MinimumSampleBytes, 4194304},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{uploadSpeedProbe: uploadSpeedProbeFunc(func(context.Context, string) (providerbenchmark.Measurement, error) {
				calls.Add(1)
				return providerbenchmark.Measurement{}, errors.New("should not upload")
			})})
			taskRow := seedSpeedTest(t, runtime)
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("resume_mode = ?", model.TaskResumeModeRecover).
				Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().TableExpr("tasks").
				Set("checkpoint_json = ?", tc.checkpoint).
				Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			row, err := runtime.repos.ProviderUploadSpeed.Get(t.Context(), "101")
			if err != nil || row == nil || row.State != providerbenchmark.StateSucceeded || row.SampleBytes != tc.bytes ||
				row.BytesPerSecond == nil || *row.BytesPerSecond != tc.speed || row.ActiveTaskID != nil {
				t.Fatalf("recovered result = %#v, %v", row, err)
			}
			if calls.Load() != 0 {
				t.Fatalf("probe calls during recovery = %d", calls.Load())
			}
		})
	}
}
