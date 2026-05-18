package observability

import (
	"context"
	"log/slog"
	"time"
)

type Runner struct {
	service  *Service
	interval time.Duration
	logger   *slog.Logger
}

func NewRunner(service *Service, logger *slog.Logger) *Runner {
	interval := 5 * time.Minute
	if service != nil {
		interval = service.RefreshInterval()
	}
	return &Runner{
		service:  service,
		interval: interval,
		logger:   logger,
	}
}

func (r *Runner) Run(ctx context.Context) {
	if r == nil || r.service == nil {
		return
	}
	r.refresh(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refresh(ctx)
		}
	}
}

func (r *Runner) refresh(ctx context.Context) {
	if err := r.service.RefreshAll(ctx); err != nil && r.logger != nil && ctx.Err() == nil {
		r.logger.Warn("observability refresh failed", "error", err)
	}
}
