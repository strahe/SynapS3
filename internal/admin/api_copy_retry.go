package admin

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
)

type copyRetryResponse struct {
	Available  bool                           `json:"available"`
	ReasonCode storagepipeline.CopyRetryBlock `json:"reason_code,omitempty"`
}

type taskCopyRetryResponse struct {
	CopyID int64 `json:"copy_id"`
	copyRetryResponse
}

func copyRetryResponseFor(state repository.CopyRetryState) *copyRetryResponse {
	if !state.Available && state.Block == "" {
		return nil
	}
	return &copyRetryResponse{Available: state.Available, ReasonCode: state.Block}
}

func (s *Server) handleAPIRetryStorageCopy(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid copy id"})
		return
	}
	ctx := r.Context()
	copyRow, err := s.repos.Contents.GetUploadCopyByID(ctx, id)
	if err != nil {
		s.writeCopyRetryError(w, err)
		return
	}
	if copyRow == nil {
		s.writeCopyRetryError(w, repository.ErrNotFound)
		return
	}
	bucket, err := s.repos.Buckets.GetByID(ctx, copyRow.BucketID)
	if err != nil {
		s.writeCopyRetryError(w, err)
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		s.writeCopyRetryError(w, repository.ErrNotFound)
		return
	}
	if s.taskService == nil || s.copyRetryMessages == nil || s.cacheGate == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "replica recovery unavailable"})
		return
	}
	release := s.cacheGate.HoldRead(model.ContentCacheKey(copyRow.ContentID))
	defer release()
	var taskID int64
	err = s.repos.WithTx(ctx, func(repos *repository.Repositories) error {
		taskID = 0
		reopened, err := repos.Contents.RetryFailedCopy(ctx, id)
		if err != nil {
			return err
		}
		if reopened == nil {
			return repository.ErrConflict
		}
		if err := s.copyRetryMessages.Handover(ctx, repos, storagepipeline.StartCopyTransfer{CopyID: id}); err != nil {
			return err
		}
		copyRow, err := repos.Contents.GetUploadCopyByID(ctx, id)
		if err != nil {
			return err
		}
		if copyRow == nil || copyRow.ActiveTaskID == nil || copyRow.WorkGeneration != reopened.WorkGeneration+1 {
			return repository.ErrConflict
		}
		row, err := repos.Tasks.GetByID(ctx, *copyRow.ActiveTaskID)
		if err != nil {
			return err
		}
		if row == nil || row.Type != model.TaskTypeStorageTransferPlan {
			return repository.ErrConflict
		}
		if err := s.taskService.AcknowledgeSubjectInTransaction(ctx, repos, "storage_copy", strconv.FormatInt(id, 10)); err != nil {
			return err
		}
		taskID = row.ID
		return nil
	})
	if err != nil {
		s.writeCopyRetryError(w, err)
		return
	}
	if s.events != nil {
		s.events.Publish("upload_state_changed", map[string]any{"subject_type": "storage_copy", "subject_key": strconv.FormatInt(id, 10), "task_id": taskID})
	}
	writeJSON(w, http.StatusAccepted, map[string]int64{"copy_id": id, "task_id": taskID})
}

func (s *Server) writeCopyRetryError(w http.ResponseWriter, err error) {
	var blocked *repository.CopyRetryBlockedError
	switch {
	case errors.As(err, &blocked):
		writeJSON(w, http.StatusConflict, map[string]string{"code": string(blocked.Block), "error": "replica cannot be retried"})
	case errors.Is(err, repository.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"code": "copy_retry_in_progress", "error": "replica is no longer available for retry"})
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "replica not found"})
	default:
		s.logger.Error("api: failed to retry storage copy", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
	}
}

func (s *Server) attachTaskCopyRetry(ctx context.Context, rows []model.Task, items []taskListItem) {
	ids := make([]int64, 0)
	byIndex := make(map[int]int64)
	for i, row := range rows {
		if row.Status != model.TaskStatusFailed || row.SubjectType == nil || *row.SubjectType != "storage_copy" || row.SubjectKey == nil {
			continue
		}
		if row.Type != model.TaskTypeStorageTransferPlan && row.Type != model.TaskTypeStorageStore && row.Type != model.TaskTypeStoragePull {
			continue
		}
		id, err := strconv.ParseInt(*row.SubjectKey, 10, 64)
		if err == nil && id > 0 {
			ids = append(ids, id)
			byIndex[i] = id
		}
	}
	if len(ids) == 0 {
		return
	}
	states, err := s.repos.Contents.CopyRetryStates(ctx, ids)
	if err != nil {
		s.logger.Warn("api: failed to load task replica recovery", "error", err)
		return
	}
	for i, id := range byIndex {
		if response := copyRetryResponseFor(states[id]); response != nil {
			items[i].CopyRetry = &taskCopyRetryResponse{CopyID: id, copyRetryResponse: *response}
			items[i].Retryable = false
		}
	}
}
