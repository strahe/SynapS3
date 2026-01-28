package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Worker defines a background processing unit.
type Worker interface {
	// Name returns a human-readable identifier.
	Name() string
	// Run starts the worker loop; it should block until ctx is cancelled.
	Run(ctx context.Context) error
}

// Manager coordinates the lifecycle of all background workers.
type Manager struct {
	workers []Worker
	logger  *slog.Logger
}

// NewManager creates a new worker manager.
func NewManager(logger *slog.Logger, workers ...Worker) *Manager {
	return &Manager{
		workers: workers,
		logger:  logger,
	}
}

// Start launches all registered workers and blocks until ctx is cancelled.
// It returns after all workers have stopped.
func (m *Manager) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for _, w := range m.workers {
		wg.Add(1)
		go func(w Worker) {
			defer wg.Done()
			m.logger.Info("starting worker", "worker", w.Name())
			if err := w.Run(ctx); err != nil {
				m.logger.Error("worker exited with error", "worker", w.Name(), "error", err)
			} else {
				m.logger.Info("worker stopped", "worker", w.Name())
			}
		}(w)
	}

	wg.Wait()
}

// pollLoop is a helper that calls fn on a fixed interval until ctx is cancelled.
func pollLoop(ctx context.Context, interval time.Duration, fn func(ctx context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := fn(ctx); err != nil {
				slog.Error("poll iteration failed", "error", err)
			}
		}
	}
}
