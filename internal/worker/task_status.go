package worker

import (
	"context"
	"log/slog"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func scheduleTaskRetry(ctx context.Context, repos *repository.Repositories, task *model.Task, workerName string, logger *slog.Logger, err error) model.TaskStatus {
	if repos == nil || repos.Tasks == nil || task == nil || err == nil {
		return ""
	}
	status, retryErr := repos.Tasks.ScheduleRetryRunning(ctx, task, err.Error(), retryDelay(task.RetryCount))
	if retryErr != nil {
		if logger != nil {
			logger.Error("failed to schedule task retry", "error", retryErr)
		}
		return ""
	}
	if status == model.TaskStatusExhausted {
		admin.TasksExhaustedTotal.WithLabelValues(workerName, string(task.Type)).Inc()
	}
	return status
}

func completeWorkerTask(ctx context.Context, repos *repository.Repositories, task *model.Task, workerName string, logger *slog.Logger) bool {
	if repos == nil || repos.Tasks == nil || task == nil {
		return false
	}
	if err := repos.Tasks.Complete(ctx, task); err != nil {
		if logger != nil {
			logger.Error("failed to complete task", "taskID", task.ID, "error", err)
		}
		admin.WorkerTasksProcessed.WithLabelValues(workerName, "failure").Inc()
		return false
	}
	admin.WorkerTasksProcessed.WithLabelValues(workerName, "success").Inc()
	return true
}
