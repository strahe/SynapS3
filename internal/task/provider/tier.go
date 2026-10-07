package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type TierRefreshDependencies struct {
	Observability *observability.Service
	Events        EventPublisher
}
type (
	ApprovedRefreshHandler struct{ *taskengine.FuncHandler }
	EndorsedRefreshHandler struct{ *taskengine.FuncHandler }
)

func NewApprovedRefreshHandler(deps TierRefreshDependencies) (*ApprovedRefreshHandler, error) {
	return &ApprovedRefreshHandler{FuncHandler: newTierHandler(deps, model.TaskTypeApprovedProviderRefresh, "approved")}, nil
}

func NewEndorsedRefreshHandler(deps TierRefreshDependencies) (*EndorsedRefreshHandler, error) {
	return &EndorsedRefreshHandler{FuncHandler: newTierHandler(deps, model.TaskTypeEndorsedProviderRefresh, "endorsed")}, nil
}

func newTierHandler(deps TierRefreshDependencies, taskType model.TaskType, tier string) *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: taskType, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec:      taskengine.StrictJSONCodec(func(input *systemtask.Input) error { return systemtask.ValidateInput(*input) }),
		RetryLimit: nil, AllowRetry: true,
	}
	run := func(ctx context.Context, _ taskengine.Execution) taskengine.Result {
		if deps.Observability == nil {
			return taskengine.Fail(errors.New("observability service is unavailable"), "dependency_unavailable", nil)
		}
		var err error
		switch taskType {
		case model.TaskTypeApprovedProviderRefresh:
			_, _, err = deps.Observability.RefreshApprovedProviders(ctx)
		case model.TaskTypeEndorsedProviderRefresh:
			_, _, err = deps.Observability.RefreshEndorsedProviders(ctx)
		default:
			return taskengine.Fail(fmt.Errorf("unknown provider tier %s", tier), "invalid_tier", nil)
		}
		if err != nil {
			return retryTask(err, tier+"_provider_refresh_failed")
		}
		if deps.Events != nil {
			deps.Events.Publish("provider_catalog_updated", map[string]any{})
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, deps.Observability.RefreshInterval(), "scheduled", "Provider list refreshed", nil)
	}
	return taskengine.NewFuncHandler(definition, run, run)
}
