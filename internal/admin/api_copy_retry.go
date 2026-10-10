package admin

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func (s *Server) retryTaskForSubject(ctx context.Context, subjectType, subjectKey string, types ...model.TaskType) (*int64, string, error) {
	if s.taskService == nil {
		return nil, "", nil
	}
	row, err := s.repos.Tasks.LatestForSubject(ctx, subjectType, subjectKey, types...)
	if err != nil {
		return nil, "", err
	}
	if row == nil {
		return nil, "Task history is unavailable.", nil
	}
	return s.inspectTaskRetry(ctx, row)
}

func (s *Server) inspectTaskRetry(ctx context.Context, row *model.Task) (*int64, string, error) {
	if s.taskService == nil || row == nil || row.Status != model.TaskStatusFailed {
		return nil, "", nil
	}
	if err := s.taskService.InspectRetry(ctx, row); err != nil {
		if reason, known := taskRetryUnavailableReason(err); known {
			return nil, reason, nil
		}
		return nil, "", err
	}
	return &row.ID, "", nil
}

func taskRetryUnavailableReason(err error) (string, bool) {
	if blocked, ok := errors.AsType[*repository.CopyRetryBlockedError](err); ok {
		return copyRetryUnavailableReason(blocked.Block), true
	}
	if reason, ok := errors.AsType[*storagereplacement.NotRetryableError](err); ok {
		return reason.Message, true
	}
	switch {
	case errors.Is(err, storagereplacement.ErrSuperseded):
		return "A newer replacement has taken over this replica.", true
	case errors.Is(err, storagereplacement.ErrTargetInUse):
		return "That provider already stores a replica of this bucket.", true
	case errors.Is(err, storagereplacement.ErrNotRetryable):
		return "This replacement cannot be retried in its current state.", true
	case errors.Is(err, repository.ErrConflict), errors.Is(err, repository.ErrNotFound), errors.Is(err, taskengine.ErrRetryUnsupported):
		return "Retry is unavailable for this task.", true
	default:
		return "", false
	}
}

func copyRetryUnavailableReason(block storagepipeline.CopyRetryBlock) string {
	//exhaustive:enforce
	switch block {
	case storagepipeline.CopyRetryObjectDeleted:
		return "Object has been deleted."
	case storagepipeline.CopyRetryReplacementInProgress:
		return "Provider replacement is in progress."
	case storagepipeline.CopyRetryStorageServiceUnavailable:
		return "Filecoin storage is unavailable."
	case storagepipeline.CopyRetryNoSource:
		return "No available source."
	case storagepipeline.CopyRetryRecoveryRequiresAttention:
		return "Previous transfer needs recovery."
	default:
		return "Retry is unavailable for this task."
	}
}
