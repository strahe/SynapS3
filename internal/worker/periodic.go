package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type ScheduleDefinition struct {
	Key      string
	Type     model.TaskType
	Subject  Subject
	Input    any
	Interval time.Duration
}

func (r *Registry) RegisterSchedule(def ScheduleDefinition) error {
	if def.Key == "" || !def.Type.IsRecurringSystem() || def.Interval <= 0 {
		return repository.ErrInvalidInput
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrRegistryFrozen
	}
	for _, existing := range r.periodic {
		if existing.Key == def.Key || existing.Type == def.Type {
			return repository.ErrConflict
		}
	}
	if _, ok := r.definitions[def.Type]; !ok {
		return ErrUnknownType
	}
	r.periodic = append(r.periodic, def)
	return nil
}

func (r *Registry) scheduleFor(taskType model.TaskType) (ScheduleDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, def := range r.periodic {
		if def.Type == taskType {
			return def, true
		}
	}
	return ScheduleDefinition{}, false
}

func (s *Service) bootstrap(ctx context.Context) error {
	for _, def := range s.registry.periodic {
		if err := s.repos.TaskSchedules.Ensure(ctx, def.Key, time.Now().UTC()); err != nil {
			return err
		}
		// Attach the old active head before any process can dispatch a new cycle.
		legacy, err := s.repos.Tasks.GetByIdentity(ctx, def.Type, def.Key)
		if err != nil {
			return err
		}
		if legacy != nil && legacyPolicy(legacy) && (legacy.Status == model.TaskStatusPending || legacy.Status == model.TaskStatusRunning) {
			if err := s.repos.WithTx(ctx, func(tx *repository.Repositories) error {
				schedule, err := tx.TaskSchedules.GetForUpdate(ctx, def.Key)
				if err != nil {
					return err
				}
				if schedule.LatestTaskID != nil {
					return nil
				}
				return tx.TaskSchedules.SetHead(ctx, def.Key, schedule.Generation, schedule.Generation, nil, &legacy.ID, legacy.AvailableAt)
			}); err != nil {
				return err
			}
		}
	}
	var before int64
	for {
		page, err := s.repos.Tasks.List(ctx, repository.TaskListFilter{Status: model.TaskStatusPending, BeforeID: before, Limit: 100})
		if err != nil {
			return err
		}
		for i := range page.Tasks {
			task, err := s.repos.Tasks.GetByID(ctx, page.Tasks[i].ID)
			if err != nil {
				return err
			}
			if task == nil || task.Status != model.TaskStatusPending {
				continue
			}
			if legacyPolicy(task) || task.Type == model.TaskTypeGC {
				if err := s.handoff(ctx, task); err != nil {
					if !errors.Is(err, errInvalidLegacyTask) {
						return fmt.Errorf("handing off task %d: %w", task.ID, err)
					}
					if err := s.failInvalidLegacyPending(ctx, task, err); err != nil {
						return fmt.Errorf("failing invalid legacy task %d: %w", task.ID, err)
					}
				}
			}
		}
		if page.NextBeforeID == 0 {
			break
		}
		before = page.NextBeforeID
	}
	return s.dispatchSchedules(ctx)
}

func (s *Service) dispatchSchedules(ctx context.Context) error {
	due, err := s.repos.TaskSchedules.ListDue(ctx, time.Now().UTC())
	if err != nil {
		return err
	}
	for _, candidate := range due {
		var def ScheduleDefinition
		found := false
		for _, entry := range s.registry.periodic {
			if entry.Key == candidate.Key {
				def, found = entry, true
				break
			}
		}
		if !found {
			continue
		}
		err := s.repos.WithTx(ctx, func(tx *repository.Repositories) error {
			schedule, err := tx.TaskSchedules.GetForUpdate(ctx, def.Key)
			if err != nil {
				return err
			}
			if schedule == nil || schedule.NextRunAt.After(time.Now()) {
				return nil
			}
			var previous *model.Task
			if schedule.LatestTaskID != nil {
				previous, err = tx.Tasks.GetByID(ctx, *schedule.LatestTaskID)
				if err != nil {
					return err
				}
				if previous == nil {
					return repository.ErrConflict
				}
				if previous.Status == model.TaskStatusPending || previous.Status == model.TaskStatusRunning {
					return nil
				}
			}
			generation := schedule.Generation + 1
			task, err := s.prepare(EnqueueRequest{Type: def.Type, IdempotencyKey: fmt.Sprintf("%s:%d", def.Key, generation), Input: def.Input, SubjectType: def.Subject.Type, SubjectKey: def.Subject.Key})
			if err != nil {
				return err
			}
			definition, _ := s.registry.Definition(def.Type)
			if previous != nil && definition.NextCycleCheckpoint != nil {
				task.Checkpoint, err = definition.NextCycleCheckpoint(ctx, tx, previous)
				if err != nil {
					return err
				}
			}
			created, _, err := s.enqueuePrepared(ctx, tx.Tasks, task)
			if err != nil {
				return err
			}
			return tx.TaskSchedules.SetHead(ctx, def.Key, schedule.Generation, generation, schedule.LatestTaskID, &created.ID, schedule.NextRunAt)
		})
		if err != nil && !errors.Is(err, repository.ErrConflict) {
			return err
		}
	}
	return nil
}

func (e *Engine) settleSchedule(ctx context.Context, tx *repository.Repositories, claimed *model.Task, result Result, transition repository.TaskTransition) error {
	if !claimed.Type.IsRecurringSystem() || transition.Status == model.TaskStatusPending {
		return nil
	}
	def, ok := e.registry.scheduleFor(claimed.Type)
	if !ok {
		return nil
	}
	schedule, err := tx.TaskSchedules.GetForUpdate(ctx, def.Key)
	if err != nil {
		return err
	}
	if schedule == nil || schedule.LatestTaskID == nil || *schedule.LatestTaskID != claimed.ID {
		return repository.ErrConflict
	}
	delay := def.Interval
	if result.cycleDelay > 0 {
		delay = result.cycleDelay
	}
	if err := tx.TaskSchedules.ScheduleNext(ctx, def.Key, claimed.ID, time.Now().UTC().Add(delay)); err != nil {
		return err
	}
	if transition.Status == model.TaskStatusCancelled {
		return nil
	}
	// Earlier failures remain actionable until a subsequent cycle ends.
	var before int64
	for {
		page, err := tx.Tasks.ListCurrentFailedForSubject(ctx, dereference(claimed.SubjectType), dereference(claimed.SubjectKey), before, 100, claimed.Type)
		if err != nil {
			return err
		}
		for _, old := range page.Tasks {
			if old.ID < claimed.ID {
				if err := tx.Tasks.SupersedeTerminal(ctx, old.ID); err != nil {
					return err
				}
			}
		}
		if page.NextBeforeID == 0 {
			break
		}
		before = page.NextBeforeID
	}
	return nil
}

func mustEvent(value any) json.RawMessage { raw, _ := json.Marshal(value); return raw }
