package provider

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type UploadSpeedTestDependencies struct {
	Observability    *observability.Service
	UploadSpeedProbe providerbenchmark.UploadProbe
	Logger           *slog.Logger
}
type UploadSpeedTestHandler struct {
	*taskengine.FuncHandler
	deps UploadSpeedTestDependencies
}

func NewUploadSpeedTestHandler(deps UploadSpeedTestDependencies) (*UploadSpeedTestHandler, error) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &UploadSpeedTestHandler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}

func (h *UploadSpeedTestHandler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeProviderUploadSpeedTest, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		MaxConcurrency: 1,
		Codec: taskengine.StrictJSONCodec(func(input *providerbenchmark.Input) error {
			if input.ProviderID == "" || len(input.ServiceURLHash) != 64 {
				return errors.New("invalid provider upload speed input")
			}
			_, err := types.ParseOnChainID("provider_id", input.ProviderID)
			return err
		}),
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 1, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
		OnEngineFailure: func(task *model.Task, reason string) taskengine.Settlement {
			return func(ctx context.Context, repos *repository.Repositories) error {
				return repos.ProviderUploadSpeed.FailActiveTask(ctx, task.ID, reason)
			}
		},
	}
	// Measurements are not deduplicated; the provider's speed test row admits
	// one at a time.
	definition.Identity = func(raw json.RawMessage) (taskengine.Identity, error) {
		var input providerbenchmark.Input
		if err := json.Unmarshal(raw, &input); err != nil {
			return taskengine.Identity{}, err
		}
		return taskengine.Identity{SubjectType: "provider", SubjectKey: input.ProviderID}, nil
	}
	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
		var input providerbenchmark.Input
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return err
		}
		latest, err := repos.Tasks.LatestForSubject(ctx, "provider", input.ProviderID, model.TaskTypeProviderUploadSpeedTest)
		if err != nil {
			return err
		}
		if latest == nil || latest.ID != source.ID {
			return repository.ErrConflict
		}
		row, err := repos.ProviderUploadSpeed.Get(ctx, input.ProviderID)
		if err != nil {
			return err
		}
		if row != nil && row.State == providerbenchmark.StateTesting {
			return repository.ErrConflict
		}
		return nil
	}
	definition.PrepareRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
		var input providerbenchmark.Input
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return taskengine.RetryPreparation{}, err
		}
		id, err := types.ParseOnChainID("provider_id", input.ProviderID)
		if err != nil {
			return taskengine.RetryPreparation{}, err
		}
		url, eligible, err := providerbenchmark.CurrentServiceURL(ctx, h.deps.Observability, id)
		if err != nil {
			return taskengine.RetryPreparation{}, err
		}
		if !eligible {
			return taskengine.RetryPreparation{}, repository.ErrConflict
		}
		input.ServiceURLHash = providerbenchmark.URLHash(url)
		return taskengine.RetryPreparation{Request: taskengine.EnqueueRequest{Type: source.Type, Input: input}, ResumeMode: model.TaskResumeModeExecute, Bind: func(ctx context.Context, repos *repository.Repositories, _, next *model.Task) error {
			return repos.ProviderUploadSpeed.Begin(ctx, input.ProviderID, input.ServiceURLHash, next.ID)
		}}, nil
	}
	definition.BindSuccessor = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
		return repos.ProviderUploadSpeed.TransferTaskOwner(ctx, old.ID, next.ID)
	}
	return taskengine.NewFuncHandler(definition, h.executeProviderUploadSpeed, h.recoverProviderUploadSpeed)
}

func (h *UploadSpeedTestHandler) executeProviderUploadSpeed(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[providerbenchmark.Input](execution)
	if err != nil {
		return h.failInvalidProviderUploadSpeedInput(execution, err)
	}
	if h.deps.Observability == nil || h.deps.UploadSpeedProbe == nil {
		return h.failProviderUploadSpeed(execution, input, errors.New("upload speed probe unavailable"), "unavailable")
	}
	id, err := types.ParseOnChainID("provider_id", input.ProviderID)
	if err != nil {
		return h.failProviderUploadSpeed(execution, input, err, "invalid_input")
	}
	serviceURL, eligible, err := providerbenchmark.CurrentServiceURL(ctx, h.deps.Observability, id)
	if err != nil {
		return h.failProviderUploadSpeed(execution, input, err, "unavailable")
	}
	if !eligible || providerbenchmark.URLHash(serviceURL) != input.ServiceURLHash {
		return h.failProviderUploadSpeed(execution, input, errors.New("provider is no longer available at the tested address"), "provider_changed")
	}
	var measurement providerbenchmark.Measurement
	_, err = execution.WithCheckpointedEffect(ctx, "speed:"+input.ProviderID,
		providerbenchmark.Checkpoint{Attempted: true}, nil, func(ctx context.Context) error {
			var probeErr error
			measurement, probeErr = h.deps.UploadSpeedProbe.Probe(ctx, serviceURL)
			return probeErr
		})
	if err != nil {
		code := "upload_failed"
		if errors.Is(err, context.DeadlineExceeded) {
			code = "timeout"
		}
		return h.failProviderUploadSpeed(execution, input, err, code)
	}
	if measurement.Duration < time.Millisecond {
		measurement.Duration = time.Millisecond
	}
	checkpoint := providerbenchmark.Checkpoint{
		Attempted: true, SampleBytes: measurement.Bytes, DurationMS: measurement.Duration.Milliseconds(),
		BytesPerSecond: measurement.Bytes * 1000 / measurement.Duration.Milliseconds(),
	}
	if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
		return h.failProviderUploadSpeed(execution, input, err, "record_failed")
	}
	return h.completeProviderUploadSpeed(execution, input, checkpoint)
}

func (h *UploadSpeedTestHandler) recoverProviderUploadSpeed(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[providerbenchmark.Input](execution)
	if err != nil {
		return h.failInvalidProviderUploadSpeedInput(execution, err)
	}
	checkpoint, present, err := taskengine.DecodeCheckpoint[providerbenchmark.Checkpoint](execution)
	if err != nil {
		return h.failProviderUploadSpeed(execution, input, err, "invalid_checkpoint")
	}
	if present && checkpoint.DurationMS > 0 && checkpoint.BytesPerSecond > 0 {
		return h.completeProviderUploadSpeed(execution, input, checkpoint)
	}
	return h.failProviderUploadSpeed(execution, input, errors.New("upload speed test interrupted"), "interrupted")
}

func (h *UploadSpeedTestHandler) completeProviderUploadSpeed(execution taskengine.Execution, input providerbenchmark.Input, checkpoint providerbenchmark.Checkpoint) taskengine.Result {
	if checkpoint.SampleBytes == 0 {
		checkpoint.SampleBytes = providerbenchmark.SampleBytes
	}
	return taskengine.Complete("Provider upload speed tested", func(ctx context.Context, repos *repository.Repositories) error {
		return repos.ProviderUploadSpeed.Finish(ctx, input.ProviderID, execution.ID(), providerbenchmark.StateSucceeded,
			checkpoint.SampleBytes, checkpoint.DurationMS, checkpoint.BytesPerSecond, "")
	})
}

func (h *UploadSpeedTestHandler) failProviderUploadSpeed(execution taskengine.Execution, input providerbenchmark.Input, err error, code string) taskengine.Result {
	h.deps.Logger.Warn("provider upload speed test failed", "provider_id", input.ProviderID, "reason", code, "error", err)
	message := "Provider upload speed test failed"
	if code == "timeout" {
		message = "Provider upload speed test timed out"
	}
	if code == "provider_changed" {
		message = "Provider is no longer ready for this test"
	}
	return taskengine.Fail(errors.New(message), code, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.ProviderUploadSpeed.Finish(ctx, input.ProviderID, execution.ID(), providerbenchmark.StateFailed, providerbenchmark.SampleBytes, 0, 0, code)
	})
}

func (h *UploadSpeedTestHandler) failInvalidProviderUploadSpeedInput(execution taskengine.Execution, err error) taskengine.Result {
	h.deps.Logger.Warn("provider upload speed task input is invalid", "task_id", execution.ID(), "error", err)
	return taskengine.Fail(errors.New("upload speed test could not complete"), "invalid_input", func(ctx context.Context, repos *repository.Repositories) error {
		return repos.ProviderUploadSpeed.FailActiveTask(ctx, execution.ID(), "invalid_input")
	})
}
