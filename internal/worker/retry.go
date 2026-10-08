package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func (s *Service) RetryableContext(ctx context.Context, task *model.Task) (bool, error) {
	if !s.Retryable(task) {
		return false, nil
	}
	definition, _ := s.registry.Definition(task.Type)
	if !legacyPolicy(task) {
		if _, err := DecodePolicy(task); err != nil {
			return false, nil
		}
	}
	if task.InputVersion != definition.InputVersion {
		return false, nil
	}
	canonical, err := canonicalizeInput(definition.Codec, task.Input)
	if err != nil {
		return false, nil
	}
	sum := sha256.Sum256(canonical)
	if !bytes.Equal(sum[:], decodeHash(task.InputHash)) {
		return false, nil
	}
	if task.Type.IsRecurringSystem() {
		schedule, err := s.repos.TaskSchedules.GetByTaskID(ctx, task.ID)
		if err != nil {
			return false, err
		}
		if schedule == nil {
			return false, nil
		}
	}
	if definition.InspectRetry != nil {
		if err := definition.InspectRetry(ctx, s.repos, task); err != nil {
			if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) || errors.Is(err, ErrRetryUnsupported) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func (s *Service) Retry(ctx context.Context, id int64) (*model.Task, error) {
	source, err := s.repos.Tasks.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, repository.ErrNotFound
	}
	if successor, err := s.repos.Tasks.GetDirectSuccessor(ctx, id); err != nil {
		return nil, err
	} else if successor != nil {
		return successor, nil
	}
	eligible, err := s.RetryableContext(ctx, source)
	if err != nil {
		return nil, err
	}
	if !eligible {
		return s.retryReplay(ctx, id, fmt.Errorf("retry source is no longer eligible: %w", repository.ErrConflict))
	}
	definition, _ := s.registry.Definition(source.Type)
	preparation := RetryPreparation{
		Request:    EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: source.Input, SubjectType: dereference(source.SubjectType), SubjectKey: dereference(source.SubjectKey)},
		Checkpoint: source.Checkpoint, ResumeMode: model.TaskResumeModeRecover,
	}
	if definition.PrepareRetry != nil {
		preparation, err = definition.PrepareRetry(ctx, s.repos, source)
		if err != nil {
			return s.retryReplay(ctx, id, err)
		}
	}
	if preparation.Release != nil {
		defer preparation.Release()
	}
	prepared, err := s.prepare(preparation.Request)
	if err != nil {
		return nil, err
	}
	prepared.RetryOfTaskID = &source.ID
	prepared.RetryGroupKey = source.RetryGroupKey
	prepared.Checkpoint = preparation.Checkpoint
	if preparation.ResumeMode != "" {
		prepared.ResumeMode = preparation.ResumeMode
	}
	var successor *model.Task
	err = s.repos.WithTx(ctx, func(tx *repository.Repositories) error {
		current, err := tx.Tasks.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if current == nil {
			return repository.ErrNotFound
		}
		if successor, err = tx.Tasks.GetDirectSuccessor(ctx, id); err != nil {
			return err
		} else if successor != nil {
			return nil
		}
		if current.Status != model.TaskStatusFailed || current.SupersededAt != nil || !definition.manualRetryAllowed(current) {
			return repository.ErrConflict
		}
		if current.ClaimGeneration != source.ClaimGeneration || current.InputHash != source.InputHash {
			return repository.ErrConflict
		}
		if definition.InspectRetry != nil {
			if err := definition.InspectRetry(ctx, tx, current); err != nil {
				return err
			}
		}
		if preparation.Validate != nil {
			if err := preparation.Validate(ctx, tx, current); err != nil {
				return err
			}
		}
		var schedule *model.TaskSchedule
		if current.Type.IsRecurringSystem() {
			schedule, err = tx.TaskSchedules.GetByTaskID(ctx, current.ID)
			if err != nil {
				return err
			}
			if schedule == nil {
				return repository.ErrConflict
			}
			schedule, err = tx.TaskSchedules.GetForUpdate(ctx, schedule.Key)
			if err != nil {
				return err
			}
			if schedule.LatestTaskID == nil || *schedule.LatestTaskID != current.ID {
				return repository.ErrConflict
			}
		}
		if err := tx.Tasks.SupersedeTerminal(ctx, id); err != nil {
			return err
		}
		var created bool
		successor, created, err = s.enqueuePrepared(ctx, tx.Tasks, prepared)
		if err != nil {
			return err
		}
		if !created {
			return repository.ErrConflict
		}
		if preparation.Bind != nil {
			if err := preparation.Bind(ctx, tx, current, successor); err != nil {
				return err
			}
		}
		if schedule != nil {
			if err := tx.TaskSchedules.SetHead(ctx, schedule.Key, schedule.Generation, schedule.Generation, schedule.LatestTaskID, &successor.ID, time.Now().UTC()); err != nil {
				return err
			}
		}
		return tx.Tasks.AppendEvent(ctx, current.ID, "superseded", mustEvent(map[string]any{"retry_task_id": successor.ID}))
	})
	if err != nil {
		return s.retryReplay(ctx, id, err)
	}
	return successor, nil
}

func (s *Service) retryReplay(ctx context.Context, id int64, cause error) (*model.Task, error) {
	// A concurrent request may have won while domain preparation was in flight.
	next, err := s.repos.Tasks.GetDirectSuccessor(ctx, id)
	if err != nil {
		return nil, err
	}
	if next != nil {
		return next, nil
	}
	return nil, cause
}
