package provider

import (
	"context"
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type (
	EventPublisher            interface{ Publish(string, map[string]any) }
	ObservabilityDependencies struct {
		Repositories                  *repository.Repositories
		Observability                 *observability.Service
		UploadSpeedProbe              providerbenchmark.UploadProbe
		Events                        EventPublisher
		Scheduler                     *taskengine.Scheduler
		UploadSpeedTestAutoScope      string
		UploadSpeedTestRetestInterval time.Duration
	}
)

type ObservabilityHandler struct {
	*taskengine.FuncHandler
	deps ObservabilityDependencies
}

func NewObservabilityHandler(deps ObservabilityDependencies) (*ObservabilityHandler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("observability handler requires repositories")
	}
	h := &ObservabilityHandler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}

func (h *ObservabilityHandler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeObservabilityRefresh, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec: taskengine.CycleCodec, Identity: taskengine.CycleIdentity("observability"),
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
	}
	run := func(ctx context.Context, _ taskengine.Execution) taskengine.Result {
		if h.deps.Observability == nil {
			return taskengine.Fail(errors.New("observability service is unavailable"), "dependency_unavailable", nil)
		}
		providerErr := h.deps.Observability.RefreshProviderStates(ctx)
		if providerErr == nil && h.deps.Events != nil {
			h.deps.Events.Publish("provider_catalog_updated", map[string]any{})
		}
		dataSetErr := h.deps.Observability.RefreshDataSetStates(ctx)
		speedErr := h.scheduleMissingProviderSpeedTests(ctx)
		if err := errors.Join(providerErr, dataSetErr); err != nil {
			return retryTask(errors.Join(err, speedErr), "observability_refresh_failed")
		}
		if speedErr != nil {
			return retryTask(speedErr, "provider_speed_schedule_failed")
		}
		return taskengine.CompleteCycle("Storage health refreshed", nil)
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.Retry(err, reason, nil)
}
