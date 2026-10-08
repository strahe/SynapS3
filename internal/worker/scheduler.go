package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

// Scheduler restricts a task owner to its declared enqueue and wake types.
type Scheduler struct {
	service *Service
	owner   string
	types   map[model.TaskType]struct{}
}

// RetryInTransaction is reserved for a coordinator's budgeted retry settlement.
// It performs no network preparation and preserves an unresolved operation.
func (s *Scheduler) RetryInTransaction(ctx context.Context, tx *repository.Repositories, sourceID int64, availableAt time.Time) (*model.Task, error) {
	if err := requireTransaction(tx); err != nil {
		return nil, err
	}
	source, err := tx.Tasks.GetForUpdate(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, repository.ErrNotFound
	}
	if err := s.authorize(source.Type); err != nil {
		return nil, err
	}
	if next, err := tx.Tasks.GetDirectSuccessor(ctx, source.ID); err != nil {
		return nil, err
	} else if next != nil {
		return next, nil
	}
	if source.Status != model.TaskStatusFailed || source.SupersededAt != nil {
		return nil, repository.ErrConflict
	}
	definition, ok := s.service.registry.Definition(source.Type)
	if !ok {
		return nil, ErrUnknownType
	}
	if !definition.manualRetryAllowed(source) {
		return nil, ErrRetryUnsupported
	}
	if definition.InspectRetry != nil {
		if err := definition.InspectRetry(ctx, tx, source); err != nil {
			return nil, err
		}
	}
	prepared, err := s.service.prepare(EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: source.Input, SubjectType: dereference(source.SubjectType), SubjectKey: dereference(source.SubjectKey), AvailableAt: availableAt})
	if err != nil {
		return nil, err
	}
	prepared.RetryOfTaskID = &source.ID
	prepared.RetryGroupKey = source.RetryGroupKey
	prepared.Checkpoint = source.Checkpoint
	prepared.CancellationRequestedAt = source.CancellationRequestedAt
	prepared.CancellationReason = source.CancellationReason
	prepared.ResumeMode = model.TaskResumeModeRecover
	if err := tx.Tasks.SupersedeTerminal(ctx, source.ID); err != nil {
		return nil, err
	}
	next, created, err := s.service.enqueuePrepared(ctx, tx.Tasks, prepared)
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, repository.ErrConflict
	}
	if definition.LegacyHandoff != nil {
		if err := definition.LegacyHandoff(ctx, tx, source, next); err != nil {
			return nil, err
		}
	}
	if err := tx.Tasks.AppendEvent(ctx, source.ID, "superseded", mustEvent(map[string]any{"retry_task_id": next.ID})); err != nil {
		return nil, err
	}
	return next, nil
}

// EnqueueRecoveryInTransaction connects replanned work to its stopped source.
// The domain caller validates and binds the business generation in this transaction.
func (s *Scheduler) EnqueueRecoveryInTransaction(ctx context.Context, tx *repository.Repositories, sourceID int64, request EnqueueRequest) (*model.Task, error) {
	if err := requireTransaction(tx); err != nil {
		return nil, err
	}
	if err := s.authorize(request.Type); err != nil {
		return nil, err
	}
	source, err := tx.Tasks.GetForUpdate(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, repository.ErrNotFound
	}
	if err := s.authorize(source.Type); err != nil {
		return nil, err
	}
	if next, err := tx.Tasks.GetDirectSuccessor(ctx, sourceID); err != nil {
		return nil, err
	} else if next != nil {
		return next, nil
	}
	if source.Status != model.TaskStatusFailed || source.SupersededAt != nil {
		return nil, repository.ErrConflict
	}
	prepared, err := s.service.prepare(request)
	if err != nil {
		return nil, err
	}
	prepared.RetryOfTaskID = &source.ID
	prepared.RetryGroupKey = source.RetryGroupKey
	if err := tx.Tasks.SupersedeTerminal(ctx, source.ID); err != nil {
		return nil, err
	}
	next, created, err := s.service.enqueuePrepared(ctx, tx.Tasks, prepared)
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, repository.ErrConflict
	}
	if err := tx.Tasks.AppendEvent(ctx, source.ID, "superseded", mustEvent(map[string]any{"retry_task_id": next.ID})); err != nil {
		return nil, err
	}
	return next, nil
}

type WakePendingFilter struct {
	Types           []model.TaskType
	SkipWaitReasons []string
}

func (s *Service) Scheduler(owner string, types ...model.TaskType) (*Scheduler, error) {
	if s == nil || s.registry == nil || strings.TrimSpace(owner) == "" || len(types) == 0 {
		return nil, fmt.Errorf("scheduler requires service, owner, and task types: %w", repository.ErrInvalidInput)
	}
	scheduler := &Scheduler{service: s, owner: owner, types: make(map[model.TaskType]struct{}, len(types))}
	for _, taskType := range types {
		if taskType == "" {
			return nil, fmt.Errorf("scheduler %q has an empty task type: %w", owner, repository.ErrInvalidInput)
		}
		scheduler.types[taskType] = struct{}{}
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	if s.registry.frozen {
		return nil, ErrRegistryFrozen
	}
	s.registry.schedulers = append(s.registry.schedulers, scheduler)
	return scheduler, nil
}

func requireTransaction(tx *repository.Repositories) error {
	if tx == nil || !tx.IsTransaction() || tx.Tasks == nil {
		return fmt.Errorf("transaction-scoped repositories are required: %w", repository.ErrInvalidInput)
	}
	return nil
}

func (s *Scheduler) authorize(taskType model.TaskType) error {
	if s == nil {
		return fmt.Errorf("scheduler is required: %w", repository.ErrInvalidInput)
	}
	if _, allowed := s.types[taskType]; !allowed {
		return fmt.Errorf("scheduler %q does not authorize task type %q: %w", s.owner, taskType, repository.ErrInvalidInput)
	}
	return nil
}

func (s *Scheduler) EnqueueInTransaction(ctx context.Context, tx *repository.Repositories, request EnqueueRequest) (*model.Task, bool, error) {
	if err := s.authorize(request.Type); err != nil {
		return nil, false, err
	}
	if err := requireTransaction(tx); err != nil {
		return nil, false, err
	}
	return s.service.EnqueueInTransaction(ctx, tx, request)
}

func (s *Scheduler) WakeInTransaction(ctx context.Context, tx *repository.Repositories, ids []int64, filter WakePendingFilter) (int, error) {
	if len(filter.Types) == 0 {
		return 0, fmt.Errorf("wake task types are required: %w", repository.ErrInvalidInput)
	}
	for _, taskType := range filter.Types {
		if err := s.authorize(taskType); err != nil {
			return 0, err
		}
	}
	if err := requireTransaction(tx); err != nil {
		return 0, err
	}
	return tx.Tasks.WakePendingOfTypes(ctx, ids, filter.Types, filter.SkipWaitReasons)
}
