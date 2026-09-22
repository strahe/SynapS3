package worker

import (
	"context"
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/types"
)

func (h *TaskHandlers) providerUploadSpeedHandler() taskengine.Handler {
	return taskHandler{
		definition: taskengine.Definition{
			Type: model.TaskTypeProviderUploadSpeedTest, InputVersion: 1,
			Codec: taskengine.StrictJSONCodec(func(input *providerbenchmark.Input) error {
				if input.ProviderID == "" || len(input.ServiceURLHash) != 64 {
					return errors.New("invalid provider upload speed input")
				}
				_, err := types.ParseOnChainID("provider_id", input.ProviderID)
				return err
			}),
			RetryLimit: new(int), AllowRetry: false,
			OnEngineFailure: func(task *model.Task, reason string) taskengine.Settlement {
				return func(ctx context.Context, repos *repository.Repositories) error {
					return repos.ProviderUploadSpeed.FailActiveTask(ctx, task.ID, reason)
				}
			},
		},
		execute: h.executeProviderUploadSpeed,
		recover: h.recoverProviderUploadSpeed,
	}
}

func (h *TaskHandlers) executeProviderUploadSpeed(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[providerbenchmark.Input](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_input", nil)
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
	var duration time.Duration
	err = execution.WithResource(ctx, taskengine.ResourceProviderUploadSpeed, func(ctx context.Context) error {
		_, effectErr := execution.WithCheckpointedEffect(ctx, taskengine.ResourceProviderMutation,
			providerbenchmark.Checkpoint{Attempted: true}, nil, func(ctx context.Context) error {
				var probeErr error
				duration, probeErr = h.deps.UploadSpeedProbe.Probe(ctx, serviceURL)
				return probeErr
			})
		return effectErr
	})
	if errors.Is(err, taskengine.ErrResourceBusy) {
		return taskengine.ResourceWait("Waiting to test provider upload speed")
	}
	if err != nil {
		code := "upload_failed"
		if errors.Is(err, context.DeadlineExceeded) {
			code = "timeout"
		}
		return h.failProviderUploadSpeed(execution, input, err, code)
	}
	if duration < time.Millisecond {
		duration = time.Millisecond
	}
	checkpoint := providerbenchmark.Checkpoint{
		Attempted: true, DurationMS: duration.Milliseconds(),
		BytesPerSecond: int64(float64(providerbenchmark.SampleBytes) / duration.Seconds()),
	}
	if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
		return h.failProviderUploadSpeed(execution, input, err, "record_failed")
	}
	return h.completeProviderUploadSpeed(execution, input, checkpoint)
}

func (h *TaskHandlers) recoverProviderUploadSpeed(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[providerbenchmark.Input](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_input", nil)
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

func (h *TaskHandlers) completeProviderUploadSpeed(execution taskengine.Execution, input providerbenchmark.Input, checkpoint providerbenchmark.Checkpoint) taskengine.Result {
	return taskengine.Complete("Provider upload speed tested", func(ctx context.Context, repos *repository.Repositories) error {
		return repos.ProviderUploadSpeed.Finish(ctx, input.ProviderID, execution.ID(), providerbenchmark.StateSucceeded,
			checkpoint.DurationMS, checkpoint.BytesPerSecond, "")
	})
}

func (h *TaskHandlers) failProviderUploadSpeed(execution taskengine.Execution, input providerbenchmark.Input, err error, code string) taskengine.Result {
	h.deps.Logger.Warn("provider upload speed test failed", "provider_id", input.ProviderID, "reason", code, "error", err)
	message := "Provider upload speed test failed"
	if code == "timeout" {
		message = "Provider upload speed test timed out"
	}
	if code == "provider_changed" {
		message = "Provider is no longer available for this test"
	}
	return taskengine.Fail(errors.New(message), code, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.ProviderUploadSpeed.Finish(ctx, input.ProviderID, execution.ID(), providerbenchmark.StateFailed, 0, 0, code)
	})
}
