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
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type taskListItem struct {
	ID                     int64   `json:"id"`
	Type                   string  `json:"type"`
	Operation              string  `json:"operation"`
	Status                 string  `json:"status"`
	Presentation           string  `json:"presentation_status"`
	SubjectType            *string `json:"subject_type,omitempty"`
	SubjectKey             *string `json:"subject_key,omitempty"`
	RetryCount             int     `json:"retry_count"`
	MaxAttempts            *int    `json:"max_attempts"`
	RetryOfTaskID          *int64  `json:"retry_of_task_id"`
	SupersededAt           *string `json:"superseded_at,omitempty"`
	RetryTaskID            *int64  `json:"retry_task_id"`
	Retryable              bool    `json:"retryable"`
	RetryUnavailableReason string  `json:"retry_unavailable_reason,omitempty"`
	Acknowledgeable        bool    `json:"acknowledgeable"`
	WaitReason             *string `json:"wait_reason,omitempty"`
	FailureReason          *string `json:"failure_reason,omitempty"`
	LastError              *string `json:"last_error,omitempty"`
	StatusMessage          *string `json:"status_message,omitempty"`
	AvailableAt            string  `json:"available_at"`
	StartedAt              *string `json:"started_at,omitempty"`
	FinishedAt             *string `json:"finished_at,omitempty"`
	AcknowledgedAt         *string `json:"acknowledged_at,omitempty"`
	CreatedAt              string  `json:"created_at"`
	UpdatedAt              string  `json:"updated_at"`

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
		items = append(items, s.taskListItem(r.Context(), &page.Tasks[i]))
	}
	if err := s.attachTaskStorageConfirmations(r.Context(), page.Tasks, items); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
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
	scope, err := parseTaskScope(r)
	if err != nil {
		return repository.TaskListFilter{}, err
	}
	filter := repository.TaskListFilter{
		Type:  model.TaskType(r.URL.Query().Get("type")),
		Scope: scope,
		Limit: 50,
	}
	status := r.URL.Query().Get("status")
	if filter.Type != "" && !validTaskType(filter.Type) {
		return repository.TaskListFilter{}, &taskQueryError{"unknown task type"}
	}
	if status != "" && !validTaskScopeStatus(scope, status) {
		return repository.TaskListFilter{}, &taskQueryError{"status is unavailable in this scope"}
	}
	filter.Status = model.TaskStatus(status)
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

func parseTaskScope(r *http.Request) (repository.TaskScope, error) {
	scope := repository.TaskScope(r.URL.Query().Get("scope"))
	if scope == "" {
		return repository.TaskScopeWork, nil
	}
	if scope != repository.TaskScopeWork && scope != repository.TaskScopeHistory {
		return "", &taskQueryError{"scope must be work or history"}
	}
	return scope, nil
}

func validTaskScopeStatus(scope repository.TaskScope, status string) bool {
	if status == string(model.TaskStatusFailed) {
		return true
	}
	if scope == repository.TaskScopeHistory {
		return status == string(model.TaskStatusCompleted) || status == string(model.TaskStatusCancelled)
	}
	return status == string(model.TaskStatusPending) || status == string(model.TaskStatusRunning)
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

func (s *Server) taskListItem(_ context.Context, row *model.Task) taskListItem {
	item := taskListItem{
		ID: row.ID, Type: string(row.Type), Operation: taskOperationLabel(row.Type),
		Status: string(row.Status), Presentation: taskPresentationStatus(row, time.Now()),
		SubjectType: row.SubjectType, SubjectKey: row.SubjectKey,
		RetryCount: row.RetryCount, MaxAttempts: taskMaxAttempts(row),
		RetryOfTaskID: row.RetryOfTaskID, SupersededAt: formattedTime(row.SupersededAt),
		WaitReason: row.WaitReason, FailureReason: row.FailureReason,
		LastError: row.LastError, StatusMessage: row.StatusMessage,
		AvailableAt: row.AvailableAt.Format(time.RFC3339),
		CreatedAt:   row.CreatedAt.Format(time.RFC3339), UpdatedAt: row.UpdatedAt.Format(time.RFC3339),
	}
	if s.taskService != nil {
		item.Retryable = s.taskService.Retryable(row)
		if item.Retryable {
			item.RetryTaskID = &row.ID
		}
		item.Acknowledgeable = s.taskService.Acknowledgeable(row)
	}
	if row.Status == model.TaskStatusRunning && row.WaitReason != nil && *row.WaitReason != "" {
		item.WaitReason = nil
		item.StatusMessage = nil
	}
	item.StartedAt = formattedTime(row.WorkStartedAt)
	item.FinishedAt = formattedTime(row.FinishedAt)
	item.AcknowledgedAt = formattedTime(row.AcknowledgedAt)
	return item
}

func taskMaxAttempts(row *model.Task) *int {
	var snapshot struct {
		Version     int  `json:"version"`
		Legacy      bool `json:"legacy"`
		MaxAttempts *int `json:"max_attempts"`
	}
	if json.Unmarshal(row.Policy, &snapshot) != nil || snapshot.MaxAttempts == nil || *snapshot.MaxAttempts < 1 {
		return nil
	}
	if snapshot.Version == 2 || (snapshot.Version == 0 && snapshot.Legacy) {
		return snapshot.MaxAttempts
	}
	return nil
}

// Unresolved storage attention blocks acknowledgement on every task view.
func (s *Server) attachTaskStorageConfirmations(ctx context.Context, rows []model.Task, items []taskListItem) error {
	var commitTaskIDs []int64
	for i := range rows {
		if rows[i].Type == model.TaskTypeStorageCommit {
			commitTaskIDs = append(commitTaskIDs, rows[i].ID)
		}
	}
	if len(commitTaskIDs) == 0 {
		return nil
	}
	records, err := s.repos.Contents.ListCommitAttentionForTasks(ctx, commitTaskIDs)
	if err != nil {
		s.logger.Error("api: failed to list task storage confirmations", "error", err)
		return err
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
	return nil
}

func taskPresentationStatus(row *model.Task, now time.Time) string {
	if row == nil {
		return ""
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
		return "Create data set"
	case model.TaskTypeStorageTransferPlan:
		return "Prepare replica"
	case model.TaskTypeStorageStore:
		return "Upload data"
	case model.TaskTypeStoragePull:
		return "Copy data"
	case model.TaskTypeStorageCommit:
		return "Submit batch"
	case model.TaskTypeProviderReplacementCoordinate:
		return "Replace provider"
	case model.TaskTypeCacheCapacityReconcile:
		return "Manage cache"
	case model.TaskTypeCacheEvict:
		return "Clear cache"
	case model.TaskTypeCacheReconcileDurability:
		return "Apply cache policy"
	case model.TaskTypeStorageCleanup:
		return "Delete remote replicas"
	case model.TaskTypeStorageDataSetRetire:
		return "Close data set"
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
		return "Legacy task cleanup"
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

// taskAcknowledgeMaxBodyBytes bounds the bulk acknowledgement body. It carries an
// operation name and a timestamp and nothing else.
const taskAcknowledgeMaxBodyBytes = 4096

// handleAPITaskAcknowledgePreview reports how many failures a bulk acknowledgement
// would cover, and the cutoff it counted them at. Confirming with that same
// cutoff acknowledges exactly what was counted: failures recorded in between stay
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
		s.logger.Error("api: failed to count unacknowledged tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, taskAcknowledgePreviewResponse{
		Count: count, AsOf: filter.FailedBefore.Format(time.RFC3339Nano),
	})
}

// handleAPITaskAcknowledgeMatching acknowledges a backlog of failures in one call.
// It takes the same selection the Tasks page offers: an optional operation and
// the moment the operator decided, so failures recorded later stay visible.
func (s *Server) handleAPITaskAcknowledgeMatching(w http.ResponseWriter, r *http.Request) {
	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	// A body that does not decode cleanly is refused rather than partially
	// applied: a mistyped field would otherwise widen the acknowledgement to everything
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
	scope, err := parseTaskScope(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if status := r.URL.Query().Get("status"); status != "" && !validTaskScopeStatus(scope, status) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "status is unavailable in this scope"})
		return
	}
	counts, err := s.repos.Tasks.CountByScope(r.Context(), scope)
	if err != nil {
		s.logger.Error("api: failed to count tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	items := make([]taskStatsItem, 0, len(counts))
	for _, count := range counts {
		items = append(items, taskStatsItem{Type: count.Type, Status: count.Status, Count: count.Count})
	}
	writeJSON(w, http.StatusOK, items)
}

func taskIDFromRequest(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return 0, false
	}
	return id, true
}

func (s *Server) handleAPIGetTask(w http.ResponseWriter, r *http.Request) {
	id, ok := taskIDFromRequest(w, r)
	if !ok {
		return
	}
	row, err := s.repos.Tasks.GetByID(r.Context(), id)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "internal"})
		return
	}
	if row == nil {
		writeJSON(w, 404, map[string]string{"error": "task not found"})
		return
	}
	item := s.taskListItem(r.Context(), row)
	retryID, retryReason, err := s.inspectTaskRetry(r.Context(), row)
	if err != nil {
		s.logger.Error("api: failed to inspect task retry", "error", err, "taskID", id)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	item.Retryable, item.RetryTaskID, item.RetryUnavailableReason = retryID != nil, retryID, retryReason
	items := []taskListItem{item}
	if err := s.attachTaskStorageConfirmations(r.Context(), []model.Task{*row}, items); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	response := struct {
		Task   taskListItem    `json:"task"`
		Policy *taskPolicyView `json:"policy"`
	}{Task: items[0]}
	if policy, err := taskengine.DecodePolicy(row); err == nil {
		response.Policy = &taskPolicyView{MaxAttempts: new(policy.MaxAttempts), InitialDelay: policy.Backoff.InitialDelay.String(), MaximumDelay: policy.Backoff.MaximumDelay.String(), Multiplier: policy.Backoff.Multiplier, Jitter: policy.Backoff.Jitter, InvocationTimeout: policy.InvocationTimeout.String(), ObservationWindow: policy.ObservationWindow.String()}
	} else {
		var snapshot struct {
			Version           int                      `json:"version"`
			Legacy            bool                     `json:"legacy"`
			Backoff           taskengine.BackoffPolicy `json:"backoff"`
			InvocationTimeout time.Duration            `json:"invocation_timeout"`
			ObservationWindow time.Duration            `json:"observation_window"`
			MaxAttempts       *int                     `json:"max_attempts"`
		}
		if json.Unmarshal(row.Policy, &snapshot) == nil && snapshot.Version == 0 && snapshot.Legacy {
			response.Policy = &taskPolicyView{Legacy: true, InitialDelay: snapshot.Backoff.InitialDelay.String(), MaximumDelay: snapshot.Backoff.MaximumDelay.String(), Multiplier: snapshot.Backoff.Multiplier, Jitter: snapshot.Backoff.Jitter, InvocationTimeout: snapshot.InvocationTimeout.String(), ObservationWindow: snapshot.ObservationWindow.String()}
			response.Policy.MaxAttempts = snapshot.MaxAttempts
		}
	}
	writeJSON(w, 200, response)
}

type taskPolicyView struct {
	MaxAttempts       *int    `json:"max_attempts"`
	InitialDelay      string  `json:"initial_delay"`
	MaximumDelay      string  `json:"maximum_delay"`
	Multiplier        float64 `json:"multiplier"`
	Jitter            float64 `json:"jitter"`
	InvocationTimeout string  `json:"invocation_timeout"`
	ObservationWindow string  `json:"observation_window"`
	Legacy            bool    `json:"legacy"`
}

func taskPageParameters(w http.ResponseWriter, r *http.Request) (int64, int, bool) {
	limit := 50
	cursor := int64(0)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil || v < 1 || v > 100 {
			writeJSON(w, 400, map[string]string{"error": "limit must be between 1 and 100"})
			return 0, 0, false
		}
		limit = v
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		v, e := strconv.ParseInt(raw, 10, 64)
		if e != nil || v < 1 {
			writeJSON(w, 400, map[string]string{"error": "invalid cursor"})
			return 0, 0, false
		}
		cursor = v
	}
	return cursor, limit, true
}

func (s *Server) handleAPITaskHistory(w http.ResponseWriter, r *http.Request) {
	id, ok := taskIDFromRequest(w, r)
	if !ok {
		return
	}
	cursor, limit, ok := taskPageParameters(w, r)
	if !ok {
		return
	}
	page, err := s.repos.Tasks.ListHistory(r.Context(), id, cursor, limit)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeJSON(w, 404, map[string]string{"error": "task not found"})
		} else if errors.Is(err, repository.ErrInvalidInput) {
			writeJSON(w, 400, map[string]string{"error": "invalid cursor"})
		} else {
			writeJSON(w, 500, map[string]string{"error": "internal"})
		}
		return
	}
	response := taskListResponse{Tasks: make([]taskListItem, 0, len(page.Tasks))}
	for i := range page.Tasks {
		response.Tasks = append(response.Tasks, s.taskListItem(r.Context(), &page.Tasks[i]))
	}
	if err := s.attachTaskStorageConfirmations(r.Context(), page.Tasks, response.Tasks); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if page.NextBeforeID > 0 {
		response.NextCursor = &page.NextBeforeID
	}
	writeJSON(w, 200, response)
}

func (s *Server) handleAPITaskEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := taskIDFromRequest(w, r)
	if !ok {
		return
	}
	cursor, limit, ok := taskPageParameters(w, r)
	if !ok {
		return
	}
	row, err := s.repos.Tasks.GetByID(r.Context(), id)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "internal"})
		return
	}
	if row == nil {
		writeJSON(w, 404, map[string]string{"error": "task not found"})
		return
	}
	events, err := s.repos.Tasks.ListEvents(r.Context(), id, cursor, limit+1)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "internal"})
		return
	}
	response := struct {
		Events     []taskEventView `json:"events"`
		NextCursor *int64          `json:"next_cursor,omitempty"`
	}{Events: make([]taskEventView, 0, len(events))}
	if len(events) > limit {
		response.NextCursor = &events[limit-1].Sequence
		events = events[:limit]
	}
	for _, event := range events {
		var safe struct {
			FailureReason *string `json:"failure_reason"`
			Attempt       *int    `json:"attempt"`
			NextAttempt   *int    `json:"next_attempt"`
			RetryTaskID   *int64  `json:"retry_task_id"`
		}
		_ = json.Unmarshal(event.Details, &safe)
		response.Events = append(response.Events, taskEventView{Sequence: event.Sequence, Type: event.Type, CreatedAt: event.CreatedAt.Format(time.RFC3339Nano), FailureReason: safe.FailureReason, Attempt: safe.Attempt, NextAttempt: safe.NextAttempt, RetryTaskID: safe.RetryTaskID})
	}
	writeJSON(w, 200, response)
}

type taskEventView struct {
	FailureReason *string `json:"failure_reason,omitempty"`
	Attempt       *int    `json:"attempt,omitempty"`
	NextAttempt   *int    `json:"next_attempt,omitempty"`
	RetryTaskID   *int64  `json:"retry_task_id,omitempty"`
	Sequence      int64   `json:"sequence"`
	Type          string  `json:"type"`
	CreatedAt     string  `json:"created_at"`
}
