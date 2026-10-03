package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
)

type taskListItem struct {
	ID              int64   `json:"id"`
	Type            string  `json:"type"`
	Operation       string  `json:"operation"`
	Status          string  `json:"status"`
	Presentation    string  `json:"presentation_status"`
	SubjectType     *string `json:"subject_type,omitempty"`
	SubjectKey      *string `json:"subject_key,omitempty"`
	RetryCount      int     `json:"retry_count"`
	RetryLimit      *int    `json:"retry_limit,omitempty"`
	Retryable       bool    `json:"retryable"`
	Acknowledgeable bool    `json:"acknowledgeable"`
	WaitReason      *string `json:"wait_reason,omitempty"`
	FailureReason   *string `json:"failure_reason,omitempty"`
	LastError       *string `json:"last_error,omitempty"`
	StatusMessage   *string `json:"status_message,omitempty"`
	AvailableAt     string  `json:"available_at"`
	StartedAt       *string `json:"started_at,omitempty"`
	FinishedAt      *string `json:"finished_at,omitempty"`
	AcknowledgedAt  *string `json:"acknowledged_at,omitempty"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`

	StorageConfirmation *taskStorageConfirmation `json:"storage_confirmation,omitempty"`
}

// taskStorageConfirmation describes the storage registration a Confirm
// storage task holds while it is flagged for attention.
type taskStorageConfirmation struct {
	RequestID     string   `json:"request_id"`
	ReasonCode    string   `json:"reason_code"`
	ProviderID    string   `json:"provider_id"`
	DataSetID     string   `json:"data_set_id,omitempty"`
	PieceCount    int      `json:"piece_count"`
	PieceCIDs     []string `json:"piece_cids"`
	TransactionID string   `json:"transaction_id,omitempty"`
	SubmitError   string   `json:"submit_error,omitempty"`
	SubmittedAt   string   `json:"submitted_at"`
	AttentionAt   string   `json:"attention_at"`
}

type taskListResponse struct {
	Tasks      []taskListItem `json:"tasks"`
	NextCursor *int64         `json:"next_cursor,omitempty"`
}

func (s *Server) handleAPITasks(w http.ResponseWriter, r *http.Request) {
	filter, err := parseTaskListFilter(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	page, err := s.repos.Tasks.List(r.Context(), filter)
	if err != nil {
		s.logger.Error("api: failed to list tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	items := make([]taskListItem, 0, len(page.Tasks))
	for i := range page.Tasks {
		items = append(items, s.taskListItem(&page.Tasks[i]))
	}
	s.attachTaskStorageConfirmations(r.Context(), page.Tasks, items)
	response := taskListResponse{Tasks: items}
	if page.NextBeforeID > 0 {
		response.NextCursor = &page.NextBeforeID
	}
	writeJSON(w, http.StatusOK, response)
}

func parseTaskListFilter(r *http.Request) (repository.TaskListFilter, error) {
	for _, removed := range []string{"category", "stage", "offset"} {
		if r.URL.Query().Has(removed) {
			return repository.TaskListFilter{}, &taskQueryError{removed + " is no longer supported"}
		}
	}
	filter := repository.TaskListFilter{
		Type:                       model.TaskType(r.URL.Query().Get("type")),
		Limit:                      50,
		HideHealthyRecurringSystem: true,
	}
	status := r.URL.Query().Get("status")
	if filter.Type != "" && !validTaskType(filter.Type) {
		return repository.TaskListFilter{}, &taskQueryError{"unknown task type"}
	}
	if status != "" && !validTaskStatus(status) {
		return repository.TaskListFilter{}, &taskQueryError{"status must be pending, running, completed, failed, cancelled, or dismissed"}
	}
	switch status {
	case "failed":
		filter.Status = model.TaskStatusFailed
		filter.Acknowledged = new(false)
	case "dismissed":
		filter.Status = model.TaskStatusFailed
		filter.Acknowledged = new(true)
	default:
		filter.Status = model.TaskStatus(status)
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			return repository.TaskListFilter{}, &taskQueryError{"limit must be between 1 and 100"}
		}
		filter.Limit = limit
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		cursor, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || cursor < 1 {
			return repository.TaskListFilter{}, &taskQueryError{"cursor must be a positive task ID"}
		}
		filter.BeforeID = cursor
	}
	return filter, nil
}

type taskQueryError struct{ message string }

func (e *taskQueryError) Error() string { return e.message }

func validTaskStatus(status string) bool {
	switch status {
	case string(model.TaskStatusPending), string(model.TaskStatusRunning), string(model.TaskStatusCompleted),
		string(model.TaskStatusFailed), string(model.TaskStatusCancelled), "dismissed":
		return true
	default:
		return false
	}
}

func validTaskType(taskType model.TaskType) bool {
	switch taskType {
	case model.TaskTypeBucketProvision,
		model.TaskTypeUploadPlan,
		model.TaskTypeStorageDataSetEnsure,
		model.TaskTypeStorageTransferPlan,
		model.TaskTypeStorageStore,
		model.TaskTypeStoragePull,
		model.TaskTypeStorageCommit,
		model.TaskTypeProviderReplacementCoordinate,
		model.TaskTypeCacheCapacityReconcile,
		model.TaskTypeCacheEvict,
		model.TaskTypeCacheReconcileDurability,
		model.TaskTypeStorageCleanup,
		model.TaskTypeStorageDataSetRetire,
		model.TaskTypeWalletOperation,
		model.TaskTypeObservabilityRefresh,
		model.TaskTypeApprovedProviderRefresh,
		model.TaskTypeEndorsedProviderRefresh,
		model.TaskTypeProviderUploadSpeedTest,
		model.TaskTypeGC:
		return true
	default:
		return false
	}
}

func (s *Server) taskListItem(row *model.Task) taskListItem {
	item := taskListItem{
		ID: row.ID, Type: string(row.Type), Operation: taskOperationLabel(row.Type),
		Status: string(row.Status), Presentation: taskPresentationStatus(row, time.Now()),
		SubjectType: row.SubjectType, SubjectKey: row.SubjectKey,
		RetryCount: row.RetryCount, RetryLimit: row.RetryLimit,
		WaitReason: row.WaitReason, FailureReason: row.FailureReason,
		LastError: row.LastError, StatusMessage: row.StatusMessage,
		AvailableAt: row.AvailableAt.Format(time.RFC3339),
		CreatedAt:   row.CreatedAt.Format(time.RFC3339), UpdatedAt: row.UpdatedAt.Format(time.RFC3339),
	}
	if s.taskService != nil {
		item.Retryable = s.taskService.Retryable(row)
		item.Acknowledgeable = s.taskService.Acknowledgeable(row)
	}
	item.StartedAt = formattedTime(row.StartedAt)
	item.FinishedAt = formattedTime(row.FinishedAt)
	item.AcknowledgedAt = formattedTime(row.AcknowledgedAt)
	return item
}

// attachTaskStorageConfirmations adds the confirmation each Confirm storage
// task holds while it is flagged for attention. A failed lookup leaves the
// list without it; dismissal is still refused by the repository.
func (s *Server) attachTaskStorageConfirmations(ctx context.Context, rows []model.Task, items []taskListItem) {
	var commitTaskIDs []int64
	for i := range rows {
		if rows[i].Type == model.TaskTypeStorageCommit {
			commitTaskIDs = append(commitTaskIDs, rows[i].ID)
		}
	}
	if len(commitTaskIDs) == 0 {
		return
	}
	records, err := s.repos.Contents.ListCommitAttentionForTasks(ctx, commitTaskIDs)
	if err != nil {
		s.logger.Warn("api: failed to list task storage confirmations", "error", err)
		return
	}
	byTask := make(map[int64]storagecommit.AttentionRecord, len(records))
	for _, record := range records {
		if record.TaskID != nil {
			byTask[*record.TaskID] = record
		}
	}
	for i := range rows {
		record, ok := byTask[rows[i].ID]
		if !ok {
			continue
		}
		items[i].Acknowledgeable = false
		pieceCIDs := record.PieceCIDs
		if pieceCIDs == nil {
			pieceCIDs = []string{}
		}
		items[i].StorageConfirmation = &taskStorageConfirmation{
			RequestID: record.RequestID, ReasonCode: string(record.Code),
			ProviderID: record.ProviderID, DataSetID: record.DataSetID,
			PieceCount: len(pieceCIDs), PieceCIDs: pieceCIDs,
			TransactionID: record.TransactionID,
			SubmitError:   record.SubmitError,
			SubmittedAt:   record.SubmittedAt.Format(time.RFC3339), AttentionAt: record.AttentionAt.Format(time.RFC3339),
		}
	}
}

func taskPresentationStatus(row *model.Task, now time.Time) string {
	if row == nil {
		return ""
	}
	if row.Status == model.TaskStatusFailed && row.AcknowledgedAt != nil {
		return "dismissed"
	}
	if row.Status != model.TaskStatusPending {
		return string(row.Status)
	}
	if row.WaitReason != nil && *row.WaitReason != "" {
		return "waiting"
	}
	if row.AvailableAt.After(now) {
		return "scheduled"
	}
	return "queued"
}

func taskOperationLabel(taskType model.TaskType) string {
	switch taskType {
	case model.TaskTypeBucketProvision:
		return "Prepare bucket"
	case model.TaskTypeUploadPlan:
		return "Prepare upload"
	case model.TaskTypeStorageDataSetEnsure:
		return "Create dataset"
	case model.TaskTypeStorageTransferPlan:
		return "Prepare copy"
	case model.TaskTypeStorageStore:
		return "Upload data"
	case model.TaskTypeStoragePull:
		return "Copy data"
	case model.TaskTypeStorageCommit:
		return "Confirm storage"
	case model.TaskTypeProviderReplacementCoordinate:
		return "Replace provider"
	case model.TaskTypeCacheCapacityReconcile:
		return "Manage cache"
	case model.TaskTypeCacheEvict:
		return "Clear cache"
	case model.TaskTypeCacheReconcileDurability:
		return "Apply cache policy"
	case model.TaskTypeStorageCleanup:
		return "Delete remote copies"
	case model.TaskTypeStorageDataSetRetire:
		return "Close dataset"
	case model.TaskTypeWalletOperation:
		return "Wallet operation"
	case model.TaskTypeObservabilityRefresh:
		return "Refresh health"
	case model.TaskTypeApprovedProviderRefresh:
		return "Refresh approved providers"
	case model.TaskTypeEndorsedProviderRefresh:
		return "Refresh endorsed providers"
	case model.TaskTypeProviderUploadSpeedTest:
		return "Test upload speed"
	case model.TaskTypeGC:
		return "Clean task history"
	default:
		return "Background operation"
	}
}

func formattedTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.Format(time.RFC3339Nano)
	return &formatted
}

type taskStatsItem struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Count  int64  `json:"count"`
}

type taskAcknowledgeRequest struct {
	Type         string `json:"type,omitempty"`
	FailedBefore string `json:"failed_before,omitempty"`
}

type taskAcknowledgeResponse struct {
	Acknowledged int `json:"acknowledged"`
}

type taskAcknowledgePreviewResponse struct {
	Count int    `json:"count"`
	AsOf  string `json:"as_of"`
}

// taskAcknowledgeMaxBodyBytes bounds the bulk dismissal body. It carries an
// operation name and a timestamp and nothing else.
const taskAcknowledgeMaxBodyBytes = 4096

// handleAPITaskAcknowledgePreview reports how many failures a bulk dismissal
// would cover, and the cutoff it counted them at. Confirming with that same
// cutoff dismisses exactly what was counted: failures recorded in between stay
// visible instead of being swept up by a number the operator never saw.
func (s *Server) handleAPITaskAcknowledgePreview(w http.ResponseWriter, r *http.Request) {
	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	filter := repository.TaskAcknowledgeFilter{FailedBefore: time.Now().UTC()}
	if taskType := r.URL.Query().Get("type"); taskType != "" {
		if !validTaskType(model.TaskType(taskType)) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown task type"})
			return
		}
		filter.Type = model.TaskType(taskType)
	}
	count, err := s.taskService.CountAcknowledgeable(r.Context(), filter)
	if err != nil {
		s.logger.Error("api: failed to count dismissable tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, taskAcknowledgePreviewResponse{
		Count: count, AsOf: filter.FailedBefore.Format(time.RFC3339Nano),
	})
}

// handleAPITaskAcknowledgeMatching dismisses a backlog of failures in one call.
// It takes the same selection the Tasks page offers: an optional operation and
// the moment the operator decided, so failures recorded later stay visible.
func (s *Server) handleAPITaskAcknowledgeMatching(w http.ResponseWriter, r *http.Request) {
	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	// A body that does not decode cleanly is refused rather than partially
	// applied: a mistyped field would otherwise widen the dismissal to everything
	// instead of the selection the operator confirmed. An empty body, however it
	// is framed, still selects every failure before now.
	var request taskAcknowledgeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, taskAcknowledgeMaxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	filter := repository.TaskAcknowledgeFilter{FailedBefore: time.Now()}
	if request.Type != "" {
		if !validTaskType(model.TaskType(request.Type)) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown task type"})
			return
		}
		filter.Type = model.TaskType(request.Type)
	}
	if request.FailedBefore != "" {
		failedBefore, err := time.Parse(time.RFC3339, request.FailedBefore)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed_before must be an RFC 3339 time"})
			return
		}
		filter.FailedBefore = failedBefore
	}
	acknowledged, err := s.taskService.AcknowledgeMatching(r.Context(), filter)
	if err != nil {
		s.logger.Error("api: failed to acknowledge tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, taskAcknowledgeResponse{Acknowledged: acknowledged})
}

func (s *Server) handleAPITaskStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.repos.Tasks.CountByPresentationStatus(r.Context())
	if err != nil {
		s.logger.Error("api: failed to count tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	items := make([]taskStatsItem, 0, len(counts))
	for _, count := range counts {
		if model.TaskType(count.Type).IsRecurringSystem() && count.Status != string(model.TaskStatusFailed) && count.Status != "dismissed" {
			continue
		}
		items = append(items, taskStatsItem{Type: count.Type, Status: count.Status, Count: count.Count})
	}
	writeJSON(w, http.StatusOK, items)
}
