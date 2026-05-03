package admin

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type taskListItem struct {
	ID           int64   `json:"id"`
	Type         string  `json:"type"`
	Stage        *string `json:"stage,omitempty"`
	UploadID     *int64  `json:"upload_id,omitempty"`
	CopyIndex    *int    `json:"copy_index,omitempty"`
	RefType      string  `json:"ref_type"`
	RefID        int64   `json:"ref_id"`
	RefVersionID string  `json:"ref_version_id"`
	Status       string  `json:"status"`
	RetryCount   int     `json:"retry_count"`
	MaxRetries   int     `json:"max_retries"`
	LastError    *string `json:"last_error,omitempty"`
	ScheduledAt  string  `json:"scheduled_at"`
	ClaimedAt    *string `json:"claimed_at,omitempty"`
	CompletedAt  *string `json:"completed_at,omitempty"`
}

type taskListResponse struct {
	Tasks  []taskListItem `json:"tasks"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}

type taskRefDetailResponse struct {
	RefType      string               `json:"ref_type"`
	RefID        int64                `json:"ref_id"`
	RefVersionID string               `json:"ref_version_id"`
	Object       *taskRefObjectDetail `json:"object"`
}

type taskRefObjectDetail struct {
	BucketName   string         `json:"bucket_name"`
	Key          string         `json:"key"`
	VersionID    string         `json:"version_id"`
	Size         int64          `json:"size"`
	State        string         `json:"state"`
	Status       string         `json:"status"`
	UploadStatus *string        `json:"upload_status,omitempty"`
	Location     objectLocation `json:"location"`
	ContentType  string         `json:"content_type"`
	UpdatedAt    string         `json:"updated_at"`
}

func (s *Server) handleAPITasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	taskType := r.URL.Query().Get("type")
	stage := r.URL.Query().Get("stage")
	status := r.URL.Query().Get("status")
	if stage != "" && taskType == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "stage filter requires type"})
		return
	}

	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	const maxOffset = 100000
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= maxOffset {
			offset = n
		}
	}

	tasks, total, err := s.repos.Tasks.List(ctx, taskType, stage, status, limit, offset)
	if err != nil {
		s.logger.Error("api: failed to list tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	items := make([]taskListItem, 0, len(tasks))
	for _, t := range tasks {
		item := taskListItem{
			ID:           t.ID,
			Type:         string(t.Type),
			Stage:        taskStage(&t),
			UploadID:     taskPayloadInt64(t.Payload, "upload_id"),
			CopyIndex:    taskPayloadInt(t.Payload, "copy_index"),
			RefType:      t.RefType,
			RefID:        t.RefID,
			RefVersionID: t.RefVersionID,
			Status:       string(t.Status),
			RetryCount:   t.RetryCount,
			MaxRetries:   t.MaxRetries,
			LastError:    t.LastError,
			ScheduledAt:  t.ScheduledAt.Format(time.RFC3339),
		}
		if t.ClaimedAt != nil {
			v := t.ClaimedAt.Format(time.RFC3339)
			item.ClaimedAt = &v
		}
		if t.CompletedAt != nil {
			v := t.CompletedAt.Format(time.RFC3339)
			item.CompletedAt = &v
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, taskListResponse{
		Tasks:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	})
}

func taskStage(task *model.Task) *string {
	if task == nil {
		return nil
	}
	if task.Stage != nil && *task.Stage != "" {
		return task.Stage
	}
	if stage, ok := task.Payload["stage"].(string); ok && stage != "" {
		return &stage
	}
	if task.Type == model.TaskTypeUpload {
		stage := "prepare_upload"
		return &stage
	}
	return nil
}

func taskPayloadInt(payload map[string]interface{}, key string) *int {
	if value := taskPayloadInt64(payload, key); value != nil {
		n := int(*value)
		return &n
	}
	return nil
}

func taskPayloadInt64(payload map[string]interface{}, key string) *int64 {
	if payload == nil {
		return nil
	}
	raw, ok := payload[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case int:
		n := int64(v)
		return &n
	case int64:
		n := v
		return &n
	case float64:
		n := int64(v)
		return &n
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return &n
		}
	}
	return nil
}

func (s *Server) handleAPITaskRefDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}

	task, err := s.repos.Tasks.GetByID(ctx, id)
	if err != nil {
		s.logger.Error("api: failed to get task ref detail", "error", err, "taskID", id)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if task == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

	resp := taskRefDetailResponse{
		RefType:      task.RefType,
		RefID:        task.RefID,
		RefVersionID: task.RefVersionID,
	}
	if task.RefType != "object" {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	var version *model.ObjectVersion
	if strings.TrimSpace(task.RefVersionID) != "" {
		version, err = s.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	} else {
		version, err = s.repos.Objects.GetCurrentVersionByObjectID(ctx, task.RefID)
	}
	if err != nil {
		s.logger.Error("api: failed to get task object ref", "error", err, "taskID", id)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if version == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object reference not found"})
		return
	}
	if version.ObjectID != task.RefID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object reference not found"})
		return
	}

	bucket, err := s.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil {
		s.logger.Error("api: failed to get task object bucket", "error", err, "taskID", id, "bucketID", version.BucketID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object reference not found"})
		return
	}
	uploadInfo, err := s.objectAdminUploadInfo(ctx, *version)
	if err != nil {
		s.logger.Error("api: failed to get task object upload status", "error", err, "taskID", id, "versionID", version.VersionID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	resp.Object = &taskRefObjectDetail{
		BucketName:   bucket.Name,
		Key:          version.Key,
		VersionID:    version.VersionID,
		Size:         version.Size,
		State:        string(version.State),
		Status:       objectAdminStatusWithUpload(version.State, version.InCache, version.InFilecoin, uploadInfo.Status),
		UploadStatus: uploadStatusString(uploadInfo.Status),
		Location:     objectLocation{Cache: version.InCache, Filecoin: version.InFilecoin},
		ContentType:  version.ContentType,
		UpdatedAt:    version.UpdatedAt.Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAPITaskStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.repos.Tasks.CountByStatus(r.Context())
	if err != nil {
		s.logger.Error("api: failed to get task stats", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if counts == nil {
		counts = []repository.TaskStatusCount{}
	}
	writeJSON(w, http.StatusOK, counts)
}
