package admin

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

type taskListItem struct {
	ID            int64                   `json:"id"`
	Type          string                  `json:"type"`
	Stage         *string                 `json:"stage,omitempty"`
	UploadID      *int64                  `json:"upload_id,omitempty"`
	CopyIndex     *int                    `json:"copy_index,omitempty"`
	RefType       string                  `json:"ref_type"`
	RefID         int64                   `json:"ref_id"`
	RefVersionID  string                  `json:"ref_version_id"`
	Status        string                  `json:"status"`
	Progress      *uploadProgressResponse `json:"progress,omitempty"`
	RetryCount    int                     `json:"retry_count"`
	MaxRetries    int                     `json:"max_retries"`
	LastError     *string                 `json:"last_error,omitempty"`
	StatusMessage *string                 `json:"status_message,omitempty"`
	WaitReason    *string                 `json:"wait_reason,omitempty"`
	ScheduledAt   string                  `json:"scheduled_at"`
	ClaimedAt     *string                 `json:"claimed_at,omitempty"`
	CompletedAt   *string                 `json:"completed_at,omitempty"`
}

type taskListResponse struct {
	Tasks  []taskListItem `json:"tasks"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}

type taskRefDetailResponse struct {
	RefType        string                       `json:"ref_type"`
	RefID          int64                        `json:"ref_id"`
	RefVersionID   string                       `json:"ref_version_id"`
	Object         *taskRefObjectDetail         `json:"object"`
	StorageCleanup *taskRefStorageCleanupDetail `json:"storage_cleanup,omitempty"`
}

type taskRefObjectDetail struct {
	BucketName   string                  `json:"bucket_name"`
	Key          string                  `json:"key"`
	VersionID    string                  `json:"version_id"`
	Size         int64                   `json:"size"`
	State        string                  `json:"state"`
	Status       string                  `json:"status"`
	UploadStatus *string                 `json:"upload_status,omitempty"`
	Progress     *uploadProgressResponse `json:"progress,omitempty"`
	Location     objectLocation          `json:"location"`
	ContentType  string                  `json:"content_type"`
	UpdatedAt    string                  `json:"updated_at"`
}

type taskRefStorageCleanupDetail struct {
	UploadID        int64                                 `json:"upload_id"`
	DeletedVersions []taskRefStorageCleanupDeletedVersion `json:"deleted_versions"`
	Copies          []taskRefStorageCleanupCopy           `json:"copies"`
}

type taskRefStorageCleanupDeletedVersion struct {
	BucketName string `json:"bucket_name"`
	Key        string `json:"key"`
	VersionID  string `json:"version_id"`
	Size       int64  `json:"size"`
	DeletedAt  string `json:"deleted_at"`
}

type taskRefStorageCleanupCopy struct {
	CopyIndex       int     `json:"copy_index"`
	ProviderID      *string `json:"provider_id,omitempty"`
	DataSetID       *string `json:"data_set_id,omitempty"`
	ClientDataSetID *string `json:"client_data_set_id,omitempty"`
	PieceID         *string `json:"piece_id,omitempty"`
	PieceCID        string  `json:"piece_cid"`
	Status          string  `json:"status"`
	DeleteTxHash    *string `json:"delete_tx_hash,omitempty"`
	LastError       *string `json:"last_error,omitempty"`
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
	progressByTaskID := s.taskUploadProgresses(ctx, tasks)

	items := make([]taskListItem, 0, len(tasks))
	for i := range tasks {
		items = append(items, taskListItemFromModel(&tasks[i], progressByTaskID[tasks[i].ID]))
	}

	writeJSON(w, http.StatusOK, taskListResponse{
		Tasks:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	})
}

func taskListItemFromModel(t *model.Task, progress *uploadProgressResponse) taskListItem {
	item := taskListItem{
		ID:            t.ID,
		Type:          string(t.Type),
		Stage:         taskStage(t),
		UploadID:      taskPayloadInt64(t.Payload, "upload_id"),
		CopyIndex:     taskPayloadInt(t.Payload, "copy_index"),
		RefType:       t.RefType,
		RefID:         t.RefID,
		RefVersionID:  t.RefVersionID,
		Status:        string(t.Status),
		Progress:      progress,
		RetryCount:    t.RetryCount,
		MaxRetries:    t.MaxRetries,
		LastError:     t.LastError,
		StatusMessage: t.StatusMessage,
		ScheduledAt:   t.ScheduledAt.Format(time.RFC3339),
	}
	if t.WaitReason != nil {
		v := string(*t.WaitReason)
		item.WaitReason = &v
	}
	if t.ClaimedAt != nil {
		v := t.ClaimedAt.Format(time.RFC3339)
		item.ClaimedAt = &v
	}
	if t.CompletedAt != nil {
		v := t.CompletedAt.Format(time.RFC3339)
		item.CompletedAt = &v
	}
	return item
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

func (s *Server) taskUploadProgresses(ctx context.Context, tasks []model.Task) map[int64]*uploadProgressResponse {
	progressByTaskID := make(map[int64]*uploadProgressResponse)
	if s == nil || s.repos == nil || s.repos.Uploads == nil || len(tasks) == 0 {
		return progressByTaskID
	}

	taskUploadIDs := make(map[int64]int64)
	taskVersionIDs := make(map[int64]string)
	uploadIDSet := make(map[int64]struct{})
	versionIDSet := make(map[string]struct{})
	for i := range tasks {
		task := &tasks[i]
		if !taskWantsUploadProgress(task) {
			continue
		}
		if uploadID := taskPayloadInt64(task.Payload, "upload_id"); uploadID != nil {
			taskUploadIDs[task.ID] = *uploadID
			uploadIDSet[*uploadID] = struct{}{}
			continue
		}
		versionID := strings.TrimSpace(task.RefVersionID)
		if versionID == "" {
			continue
		}
		taskVersionIDs[task.ID] = versionID
		versionIDSet[versionID] = struct{}{}
	}

	uploadsByID := make(map[int64]model.StorageUpload)
	if len(uploadIDSet) > 0 {
		uploadIDs := make([]int64, 0, len(uploadIDSet))
		for uploadID := range uploadIDSet {
			uploadIDs = append(uploadIDs, uploadID)
		}
		var err error
		uploadsByID, err = s.repos.Uploads.GetByIDs(ctx, uploadIDs)
		if err != nil {
			s.logger.Warn("api: failed to load task upload progress by upload id", "error", err)
			uploadsByID = nil
		}
	}

	uploadsByVersionID := make(map[string]model.StorageUpload)
	if len(versionIDSet) > 0 {
		versionIDs := make([]string, 0, len(versionIDSet))
		for versionID := range versionIDSet {
			versionIDs = append(versionIDs, versionID)
		}
		var err error
		uploadsByVersionID, err = s.repos.Uploads.FindLatestUploadsBySourceVersions(ctx, versionIDs)
		if err != nil {
			s.logger.Warn("api: failed to load task upload progress by version id", "error", err)
			uploadsByVersionID = nil
		}
	}

	for taskID, uploadID := range taskUploadIDs {
		upload, ok := uploadsByID[uploadID]
		if !ok {
			continue
		}
		progressByTaskID[taskID] = uploadProgressResponseFromUpload(&upload)
	}
	for taskID, versionID := range taskVersionIDs {
		upload, ok := uploadsByVersionID[versionID]
		if !ok {
			continue
		}
		progressByTaskID[taskID] = uploadProgressResponseFromUpload(&upload)
	}
	return progressByTaskID
}

func taskWantsUploadProgress(task *model.Task) bool {
	if task == nil || task.Type != model.TaskTypeUpload {
		return false
	}
	stage := taskStage(task)
	return stage != nil && (*stage == "ingress_store" || *stage == "")
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
	if task.RefType == "storage_upload" && task.Type == model.TaskTypeStorageCleanup {
		copies, err := s.repos.StorageCleanup.ListCopiesForTask(ctx, task.ID)
		if err != nil {
			s.logger.Error("api: failed to list storage cleanup copies", "error", err, "taskID", id)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		deletedVersions, err := s.taskStorageCleanupDeletedVersions(ctx, task)
		if err != nil {
			s.logger.Error("api: failed to list storage cleanup deleted versions", "error", err, "taskID", id)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		resp.StorageCleanup = &taskRefStorageCleanupDetail{
			UploadID:        task.RefID,
			DeletedVersions: deletedVersions,
			Copies:          make([]taskRefStorageCleanupCopy, 0, len(copies)),
		}
		for _, copy := range copies {
			resp.StorageCleanup.Copies = append(resp.StorageCleanup.Copies, taskRefStorageCleanupCopy{
				CopyIndex:       copy.CopyIndex,
				ProviderID:      onChainIDStringPtr(copy.ProviderID),
				DataSetID:       onChainIDStringPtr(copy.DataSetID),
				ClientDataSetID: onChainIDStringPtr(copy.ClientDataSetID),
				PieceID:         onChainIDStringPtr(copy.PieceID),
				PieceCID:        copy.PieceCID,
				Status:          string(copy.Status),
				DeleteTxHash:    copy.DeleteTxHash,
				LastError:       copy.LastError,
			})
		}
		writeJSON(w, http.StatusOK, resp)
		return
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
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object details not found"})
		return
	}
	if version.ObjectID != task.RefID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object details not found"})
		return
	}

	bucket, err := s.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil {
		s.logger.Error("api: failed to get task object bucket", "error", err, "taskID", id, "bucketID", version.BucketID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object details not found"})
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
		Progress:     uploadInfo.Progress,
		Location:     objectLocation{Cache: version.InCache, Filecoin: version.InFilecoin},
		ContentType:  version.ContentType,
		UpdatedAt:    version.UpdatedAt.Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) taskStorageCleanupDeletedVersions(ctx context.Context, task *model.Task) ([]taskRefStorageCleanupDeletedVersion, error) {
	if task == nil {
		return nil, nil
	}
	versionIDs := taskPayloadStringSlice(task.Payload, "deleted_source_versions")
	if len(versionIDs) == 0 {
		if versionID, ok := task.Payload["deleted_source_version"].(string); ok && strings.TrimSpace(versionID) != "" {
			versionIDs = append(versionIDs, strings.TrimSpace(versionID))
		}
	}

	type deletedVersionRow struct {
		BucketName string    `bun:"bucket_name"`
		Key        string    `bun:"key"`
		VersionID  string    `bun:"version_id"`
		Size       int64     `bun:"size"`
		DeletedAt  time.Time `bun:"deleted_at"`
	}
	var rows []deletedVersionRow
	q := s.db.NewSelect().
		TableExpr("object_deletions AS deletion").
		ColumnExpr("bucket.name AS bucket_name").
		ColumnExpr("deletion.key AS key").
		ColumnExpr("deletion.version_id AS version_id").
		ColumnExpr("deletion.size AS size").
		ColumnExpr("deletion.deleted_at AS deleted_at").
		Join("JOIN buckets AS bucket ON bucket.id = deletion.bucket_id")
	if len(versionIDs) > 0 {
		q = q.Where("deletion.version_id IN (?)", bun.List(versionIDs))
	} else {
		q = q.Where("deletion.storage_upload_id = ?", task.RefID)
	}
	if err := q.OrderExpr("deletion.deleted_at DESC, deletion.id DESC").Scan(ctx, &rows); err != nil {
		return nil, err
	}

	deletedVersions := make([]taskRefStorageCleanupDeletedVersion, 0, len(rows))
	for _, row := range rows {
		deletedVersions = append(deletedVersions, taskRefStorageCleanupDeletedVersion{
			BucketName: row.BucketName,
			Key:        row.Key,
			VersionID:  row.VersionID,
			Size:       row.Size,
			DeletedAt:  row.DeletedAt.Format(time.RFC3339),
		})
	}
	return deletedVersions, nil
}

func taskPayloadStringSlice(payload map[string]interface{}, key string) []string {
	if payload == nil {
		return nil
	}
	raw, ok := payload[key]
	if !ok {
		return nil
	}

	var values []string
	switch v := raw.(type) {
	case []string:
		values = v
	case []interface{}:
		for _, item := range v {
			value, ok := item.(string)
			if !ok {
				continue
			}
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return nil
	}

	clean := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		clean = append(clean, value)
	}
	return clean
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
