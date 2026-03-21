package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

// Worker defines a background processing unit.
type Worker interface {
	// Name returns a human-readable identifier.
	Name() string
	// Run starts the worker loop; it should block until ctx is cancelled.
	Run(ctx context.Context) error
	// Healthy returns true if the worker has ticked recently.
	Healthy() bool
}

// Manager coordinates the lifecycle of all background workers.
type Manager struct {
	repos             *repository.Repositories
	evictAfterOnChain bool
	workers           []Worker
	logger            *slog.Logger
}

// NewManager creates a new worker manager.
func NewManager(repos *repository.Repositories, evictAfterOnChain bool, logger *slog.Logger, workers ...Worker) *Manager {
	return &Manager{
		repos:             repos,
		evictAfterOnChain: evictAfterOnChain,
		workers:           workers,
		logger:            logger,
	}
}

// Start launches all registered workers and blocks until ctx is cancelled.
// It performs startup recovery before launching workers.
func (m *Manager) Start(ctx context.Context) {
	m.recoverOnStartup(ctx)

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

// recoverOnStartup releases expired task leases and resets objects stuck
// in intermediate states from a previous crash.
func (m *Manager) recoverOnStartup(ctx context.Context) {
	// Release expired task leases
	released, err := m.repos.Tasks.ReleaseExpiredLeases(ctx)
	if err != nil {
		m.logger.Error("failed to release expired leases", "error", err)
	} else if released > 0 {
		m.logger.Info("released expired task leases", "count", released)
	}

	// Reset stale object states (objects stuck mid-transition from a crash)
	staleThreshold := time.Now().Add(-10 * time.Minute)

	// uploading → cached (upload was interrupted)
	if n, err := m.repos.Objects.ResetStaleStates(ctx,
		model.ObjectStateUploading, model.ObjectStateCached, staleThreshold); err != nil {
		m.logger.Error("failed to reset uploading objects", "error", err)
	} else if n > 0 {
		m.logger.Info("reset stale uploading objects to cached", "count", n)
	}

	// onchaining → uploaded (on-chain submission was interrupted)
	if n, err := m.repos.Objects.ResetStaleStates(ctx,
		model.ObjectStateOnChaining, model.ObjectStateUploaded, staleThreshold); err != nil {
		m.logger.Error("failed to reset onchaining objects", "error", err)
	} else if n > 0 {
		m.logger.Info("reset stale onchaining objects to uploaded", "count", n)
	}

	// Reconcile: ensure objects in cached/uploaded/onchained have corresponding tasks.
	// The idempotency key prefix must match the format used during normal task creation
	// (object.go uses "upload:", uploader.go uses "add_roots:", onchain.go uses "evict_cache:").
	m.reconcileTasks(ctx, model.ObjectStateCached, model.TaskTypeUploadToSP, "upload")
	m.reconcileTasks(ctx, model.ObjectStateUploaded, model.TaskTypeAddRoots, "add_roots")
	if m.evictAfterOnChain {
		m.reconcileTasks(ctx, model.ObjectStateOnChained, model.TaskTypeEvictCache, "evict_cache")
	}

	// Log dead-letter task count for operator awareness
	deadLetters, err := m.repos.Tasks.ListDeadLetters(ctx, 100)
	if err != nil {
		m.logger.Error("failed to check dead-letter tasks", "error", err)
	} else if len(deadLetters) > 0 {
		m.logger.Warn("dead-letter tasks found on startup, review via GET /admin/dead-letters", "count", len(deadLetters))
	}
}

// reconcileTasks finds objects in the given state and ensures each has a corresponding
// pending task. Uses idempotency keys to safely skip objects that already have tasks.
// keyPrefix must match the prefix used by the normal task creation path for deduplication.
func (m *Manager) reconcileTasks(ctx context.Context, objState model.ObjectState, taskType model.TaskType, keyPrefix string) {
	objects, err := m.repos.Objects.ListByState(ctx, objState, 100)
	if err != nil {
		m.logger.Error("failed to list objects for reconciliation", "state", objState, "error", err)
		return
	}
	created := 0
	for _, obj := range objects {
		task := &model.Task{
			Type:           taskType,
			RefType:        "object",
			RefID:          obj.ID,
			RefGeneration:  obj.Generation,
			IdempotencyKey: fmt.Sprintf("%s:%d:%d", keyPrefix, obj.ID, obj.Generation),
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		if err := m.repos.Tasks.Create(ctx, task); err != nil {
			// Idempotency key collision means task already exists — skip
			continue
		}
		created++
	}
	if created > 0 {
		m.logger.Info("reconciled missing tasks", "state", objState, "type", taskType, "created", created)
	}
}

// WorkerHealth returns a map of worker name → healthy status.
func (m *Manager) WorkerHealth() map[string]bool {
	health := make(map[string]bool, len(m.workers))
	for _, w := range m.workers {
		health[w.Name()] = w.Healthy()
	}
	return health
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
