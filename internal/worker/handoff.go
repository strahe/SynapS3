package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

var errInvalidLegacyTask = errors.New("legacy task cannot be verified")

// Handoff changes the task owner directly while preserving recovery evidence.
func (s *Service) handoff(ctx context.Context, source *model.Task) error {
	if source.Type != model.TaskTypeGC && !legacyPolicy(source) {
		return ErrInvalidPolicy
	}
	var prepared *model.Task
	var definition Definition
	if source.Type != model.TaskTypeGC {
		var ok bool
		definition, ok = s.registry.Definition(source.Type)
		if !ok {
			return fmt.Errorf("%w: %w", errInvalidLegacyTask, ErrUnknownType)
		}
		if definition.InputVersion != source.InputVersion {
			return fmt.Errorf("%w: unsupported input version", errInvalidLegacyTask)
		}
		canonical, err := canonicalizeInput(definition.Codec, source.Input)
		if err != nil {
			return fmt.Errorf("%w: %w", errInvalidLegacyTask, err)
		}
		sum := sha256.Sum256(canonical)
		if !bytes.Equal(sum[:], decodeHash(source.InputHash)) {
			return fmt.Errorf("%w: input hash mismatch", errInvalidLegacyTask)
		}
		prepared, err = s.prepare(EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: source.Input, SubjectType: dereference(source.SubjectType), SubjectKey: dereference(source.SubjectKey), AvailableAt: source.AvailableAt})
		if err != nil {
			return fmt.Errorf("%w: %w", errInvalidLegacyTask, err)
		}
		prepared.InputHash = hex.EncodeToString(sum[:])
		prepared.RetryOfTaskID = &source.ID
	}
	return s.repos.WithTx(ctx, func(tx *repository.Repositories) error {
		if prepared != nil {
			prepared.ID = 0
		}
		current, err := tx.Tasks.GetForUpdate(ctx, source.ID)
		if err != nil {
			return err
		}
		if current == nil {
			return repository.ErrNotFound
		}
		if current.SupersededAt != nil {
			return nil
		}
		if current.Status != source.Status || current.ClaimGeneration != source.ClaimGeneration {
			return repository.ErrConflict
		}
		if current.Status != model.TaskStatusPending && current.Status != model.TaskStatusRunning {
			return nil
		}
		if prepared != nil {
			// Cancellation and wakes can commit without changing the claim generation.
			prepared.AvailableAt = current.AvailableAt
			prepared.ResumeMode = current.ResumeMode
			if current.Status == model.TaskStatusRunning {
				prepared.ResumeMode = model.TaskResumeModeRecover
			}
			prepared.Checkpoint = bytes.Clone(current.Checkpoint)
			prepared.CancellationRequestedAt = current.CancellationRequestedAt
			prepared.CancellationReason = current.CancellationReason
		}
		generation := current.ClaimGeneration
		if current.Status == model.TaskStatusPending {
			generation = 0
		}
		if err := tx.Tasks.CloseLegacy(ctx, current.ID, generation); err != nil {
			return err
		}
		if current.Type == model.TaskTypeGC {
			return tx.Tasks.AppendEvent(ctx, current.ID, "retired", mustEvent(map[string]any{"reason": "task_gc_removed"}))
		}
		if current.Type.IsRecurringSystem() && current.Status == model.TaskStatusPending && !current.CancellationRequested() && dereference(current.WaitReason) == "scheduled" {
			def, ok := s.registry.scheduleFor(current.Type)
			if !ok {
				return repository.ErrConflict
			}
			schedule, err := tx.TaskSchedules.GetForUpdate(ctx, def.Key)
			if err != nil {
				return err
			}
			if schedule == nil {
				return repository.ErrConflict
			}
			if schedule.LatestTaskID == nil || *schedule.LatestTaskID == current.ID {
				return tx.TaskSchedules.SetHead(ctx, def.Key, schedule.Generation, schedule.Generation, schedule.LatestTaskID, &current.ID, current.AvailableAt)
			}
			return nil
		}
		if err := tx.Tasks.SupersedeTerminal(ctx, current.ID); err != nil {
			return err
		}
		newTask, created, err := s.enqueuePrepared(ctx, tx.Tasks, prepared)
		if err != nil {
			return err
		}
		if !created {
			return repository.ErrConflict
		}
		if definition.LegacyHandoff != nil {
			if err := definition.LegacyHandoff(ctx, tx, current, newTask); err != nil {
				return err
			}
		}
		if current.Type.IsRecurringSystem() {
			def, ok := s.registry.scheduleFor(current.Type)
			if !ok {
				return repository.ErrConflict
			}
			schedule, err := tx.TaskSchedules.GetForUpdate(ctx, def.Key)
			if err != nil {
				return err
			}
			if schedule == nil {
				return repository.ErrConflict
			}
			if schedule.LatestTaskID != nil && *schedule.LatestTaskID != current.ID {
				return repository.ErrConflict
			}
			if err := tx.TaskSchedules.SetHead(ctx, def.Key, schedule.Generation, schedule.Generation, schedule.LatestTaskID, &newTask.ID, current.AvailableAt); err != nil {
				return err
			}
		}
		return tx.Tasks.AppendEvent(ctx, current.ID, "legacy_handoff", mustEvent(map[string]any{"task_id": newTask.ID}))
	})
}

func (s *Service) failInvalidLegacyPending(ctx context.Context, source *model.Task, cause error) error {
	return s.repos.WithTx(ctx, func(tx *repository.Repositories) error {
		current, err := tx.Tasks.GetForUpdate(ctx, source.ID)
		if err != nil {
			return err
		}
		if current == nil || current.Status != model.TaskStatusPending || current.SupersededAt != nil || !legacyPolicy(current) {
			return nil
		}
		if current.InputVersion != source.InputVersion || current.InputHash != source.InputHash || !bytes.Equal(current.Input, source.Input) {
			return repository.ErrConflict
		}
		// Invalid input cannot authorize domain settlement or release unresolved effects.
		if err := tx.Tasks.FailLegacyPending(ctx, current.ID, current.ClaimGeneration, dereference(errorPointer(cause))); err != nil {
			return err
		}
		return tx.Tasks.AppendEvent(ctx, current.ID, string(model.TaskStatusFailed), mustEvent(map[string]any{"failure_reason": "invalid_legacy_task", "next_attempt": current.RetryCount + 1}))
	})
}
