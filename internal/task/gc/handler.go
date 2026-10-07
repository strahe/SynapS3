package gc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type (
	Dependencies struct{ Repositories *repository.Repositories }
	Handler      struct {
		*taskengine.FuncHandler
		deps Dependencies
	}
)

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("task GC handler requires repositories")
	}
	h := &Handler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}

const (
	taskGCInterval    = time.Hour
	cleanupGCPageSize = 500
)

func (h *Handler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeGC, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec:      taskengine.StrictJSONCodec(func(input *systemtask.Input) error { return systemtask.ValidateInput(*input) }),
		RetryLimit: nil, AllowRetry: true,
	}
	run := func(ctx context.Context, _ taskengine.Execution) taskengine.Result {
		deleted, err := h.deps.Repositories.Tasks.DeleteRetained(ctx, time.Now(), cleanupGCPageSize)
		if err != nil {
			return retryTask(err, "task_gc_failed")
		}
		delay := taskGCInterval
		if deleted == cleanupGCPageSize {
			delay = 0
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, delay, "scheduled", fmt.Sprintf("Removed %d expired task records", deleted), nil)
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}
