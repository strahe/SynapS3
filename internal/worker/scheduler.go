package worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

// Scheduler restricts a task owner to its declared enqueue and wake types.
type Scheduler struct {
	service *Service
	owner   string
	types   map[model.TaskType]struct{}
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
