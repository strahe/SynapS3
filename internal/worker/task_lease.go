package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

const (
	taskLeaseOperationTimeout = 2 * time.Second
)

func startTaskLeaseRenewal(logger *slog.Logger, repos *repository.Repositories, task *model.Task, leaseTTL time.Duration) func() {
	if repos == nil || repos.Tasks == nil || task == nil || task.ID == 0 || leaseTTL <= 0 {
		return func() {}
	}
	taskID := task.ID
	renewCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(taskLeaseRenewInterval(leaseTTL))
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				opCtx, opCancel := context.WithTimeout(context.Background(), taskLeaseOperationTimeout)
				err := repos.Tasks.RenewLease(opCtx, task, leaseTTL)
				opCancel()
				if err != nil && logger != nil {
					logger.Warn("failed to renew task lease", "taskID", taskID, "error", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func taskLeaseRenewInterval(leaseTTL time.Duration) time.Duration {
	if leaseTTL <= 0 {
		return time.Second
	}
	interval := leaseTTL / 3
	if interval <= 0 {
		return leaseTTL
	}
	return interval
}

func releaseTaskOnWorkerShutdown(ctx context.Context, logger *slog.Logger, repos *repository.Repositories, task *model.Task) {
	if ctx.Err() == nil || repos == nil || repos.Tasks == nil || task == nil || task.ID == 0 {
		return
	}
	taskID := task.ID
	opCtx, cancel := context.WithTimeout(context.Background(), taskLeaseOperationTimeout)
	defer cancel()
	if err := repos.Tasks.ReleaseRunning(opCtx, task); err != nil && logger != nil {
		logger.Debug("skipped task release on worker shutdown", "taskID", taskID, "error", err)
	}
}
