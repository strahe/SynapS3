package admin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
)

func (s *Server) WithCommitPolicy(policy config.TaskWorkerConfig, legacyLimit uint64) *Server {
	s.commitMaxPieces, s.commitMaxWait, s.legacyPieceStorageLimit = policy.CommitMaxPieces, policy.CommitMaxWait, legacyLimit
	return s
}

type commitBatchResponse struct {
	RequestID          string                      `json:"request_id"`
	Status             storagecommit.RequestStatus `json:"status"`
	BucketName         string                      `json:"bucket_name"`
	ProviderID         string                      `json:"provider_id"`
	ProviderName       *string                     `json:"provider_name"`
	DataSetID          *string                     `json:"data_set_id"`
	DataSetRowID       int64                       `json:"data_set_row_id"`
	MemberCount        int                         `json:"member_count"`
	MaxPieces          int                         `json:"max_pieces"`
	TotalBytes         *int64                      `json:"total_bytes"`
	OldestReadyAt      *time.Time                  `json:"oldest_ready_at"`
	CollectionDeadline *time.Time                  `json:"collection_deadline"`
	SealRequestedAt    *time.Time                  `json:"seal_requested_at"`
	SealedAt           *time.Time                  `json:"sealed_at"`
	SubmittedAt        *time.Time                  `json:"submitted_at"`
	ConfirmedAt        *time.Time                  `json:"confirmed_at"`
	CreatedAt          time.Time                   `json:"created_at"`
	CanSeal            bool                        `json:"can_seal"`
	TaskID             *int64                      `json:"task_id"`
	TaskStatus         *model.TaskStatus           `json:"task_status"`
	StatusMessage      *string                     `json:"status_message"`
	LastError          *string                     `json:"last_error"`
	TransactionID      *string                     `json:"transaction_id"`
	Task               *taskListItem               `json:"task,omitempty"`
}

type commitBatchCursor struct {
	CreatedAt time.Time `json:"created_at"`
	RequestID string    `json:"request_id"`
}

type commitBatchMemberResponse struct {
	ContentID int64            `json:"content_id"`
	PieceCID  string           `json:"piece_cid"`
	Size      *int64           `json:"size"`
	File      *taskSubjectFile `json:"file"`
}

type commitBatchDetailResponse struct {
	commitBatchResponse
	Members []commitBatchMemberResponse `json:"members"`
}

func (s *Server) commitBatchResponse(row *repository.CommitBatch) commitBatchResponse {
	maxPieces, maxWait := s.commitMaxPieces, s.commitMaxWait
	if maxPieces <= 0 {
		maxPieces, maxWait = config.DefaultCommitMaxPieces, config.DefaultCommitMaxWait
	}
	var dataSetID *string
	if row.DataSetID != nil {
		id := row.DataSetID.String()
		dataSetID = &id
		maxPieces = storagecommit.MaxPieces(*row.DataSetID, s.legacyPieceStorageLimit, maxPieces)
	}
	var deadline *time.Time
	if row.Status == storagecommit.RequestStatusCollecting && row.OldestReadyAt != nil {
		t := row.OldestReadyAt.Add(maxWait)
		deadline = &t
	}
	canSeal := row.Status == storagecommit.RequestStatusCollecting && row.MemberCount > 0 && row.SealRequestedAt == nil &&
		row.TaskStatus != nil && (*row.TaskStatus == model.TaskStatusPending || *row.TaskStatus == model.TaskStatusRunning)
	lastError := row.TaskError
	if lastError == nil {
		lastError = row.LastError
	}
	return commitBatchResponse{
		RequestID: row.RequestID, Status: row.Status, BucketName: row.BucketName,
		ProviderID: row.ProviderID.String(), ProviderName: row.ProviderName, DataSetID: dataSetID, DataSetRowID: row.StorageDataSetID,
		MemberCount: row.MemberCount, MaxPieces: maxPieces, TotalBytes: row.TotalBytes, OldestReadyAt: row.OldestReadyAt,
		CollectionDeadline: deadline, SealRequestedAt: row.SealRequestedAt, SealedAt: row.SealedAt, SubmittedAt: row.SubmittedAt,
		ConfirmedAt: row.ConfirmedAt, CreatedAt: row.CreatedAt, CanSeal: canSeal, TaskID: row.TaskID, TaskStatus: row.TaskStatus,
		StatusMessage: row.TaskMessage, LastError: lastError, TransactionID: row.TransactionID,
	}
}

func (s *Server) handleAPIListCommitBatches(w http.ResponseWriter, r *http.Request) {
	filter := repository.CommitBatchFilter{Status: storagecommit.RequestStatus(r.URL.Query().Get("status")), Limit: 20}
	if filter.Status != "" && !filter.Status.Valid() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Unknown batch status"})
		return
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 100"})
			return
		}
		filter.Limit = limit
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		if len(raw) > 1024 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid page cursor"})
			return
		}
		var cursor commitBatchCursor
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.CreatedAt.IsZero() || cursor.RequestID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid page cursor"})
			return
		}
		filter.BeforeCreatedAt, filter.BeforeRequestID = cursor.CreatedAt, cursor.RequestID
	}
	limit := filter.Limit
	filter.Limit++
	rows, err := s.repos.Contents.ListCommitBatches(r.Context(), filter)
	if err != nil {
		s.commitBatchError(w, err)
		return
	}
	response := struct {
		Batches    []commitBatchResponse `json:"batches"`
		NextCursor *string               `json:"next_cursor,omitempty"`
	}{Batches: make([]commitBatchResponse, 0, min(len(rows), limit))}
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		data, err := json.Marshal(commitBatchCursor{CreatedAt: last.CreatedAt, RequestID: last.RequestID})
		if err != nil {
			s.commitBatchError(w, err)
			return
		}
		cursor := base64.RawURLEncoding.EncodeToString(data)
		response.NextCursor = &cursor
	}
	for i := range rows {
		response.Batches = append(response.Batches, s.commitBatchResponse(&rows[i]))
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleAPIGetCommitBatch(w http.ResponseWriter, r *http.Request) {
	s.writeCommitBatch(w, r, http.StatusOK)
}

func (s *Server) handleAPISealCommitBatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	if err != nil || len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "This action takes an empty body"})
		return
	}
	request, err := s.repos.Contents.RequestCommitSeal(r.Context(), r.PathValue("id"))
	if err != nil {
		s.commitBatchError(w, err)
		return
	}
	status := http.StatusOK
	if request.Status == storagecommit.RequestStatusCollecting {
		status = http.StatusAccepted
	}
	s.writeCommitBatch(w, r, status)
}

func (s *Server) writeCommitBatch(w http.ResponseWriter, r *http.Request, status int) {
	row, err := s.repos.Contents.GetCommitBatch(r.Context(), r.PathValue("id"))
	if err != nil {
		s.commitBatchError(w, err)
		return
	}
	response := s.commitBatchResponse(row)
	if row.TaskID != nil {
		task, err := s.repos.Tasks.GetByID(r.Context(), *row.TaskID)
		if err != nil && !errors.Is(err, repository.ErrNotFound) {
			s.commitBatchError(w, err)
			return
		}
		if task != nil {
			items := []taskListItem{s.taskListItem(r.Context(), task)}
			if err := s.attachTaskStorageConfirmations(r.Context(), []model.Task{*task}, items); err != nil {
				s.commitBatchError(w, err)
				return
			}
			response.Task = &items[0]
		}
	}
	if r.Method == http.MethodGet {
		members, err := s.repos.Contents.ListCommitBatchMembers(r.Context(), row.RequestID)
		if err != nil {
			s.commitBatchError(w, err)
			return
		}
		detail := commitBatchDetailResponse{commitBatchResponse: response, Members: make([]commitBatchMemberResponse, 0, len(members))}
		for _, member := range members {
			item := commitBatchMemberResponse{ContentID: member.ContentID, PieceCID: member.PieceCID, Size: member.Size}
			file, err := s.repos.Objects.GetContentFileSample(r.Context(), member.ContentID)
			if err != nil && !errors.Is(err, repository.ErrNotFound) {
				s.commitBatchError(w, err)
				return
			}
			if err == nil {
				item.File = &taskSubjectFile{Key: file.Key, Source: file.Source, OtherVersions: file.OtherVersions}
			}
			detail.Members = append(detail.Members, item)
		}
		writeJSON(w, status, detail)
		return
	}
	writeJSON(w, status, response)
}

func (s *Server) commitBatchError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Batch not found"})
	case errors.Is(err, repository.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "This batch can't be submitted now. Refresh to see its current state."})
	case errors.Is(err, repository.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid batch request"})
	default:
		s.logger.Error("api: registration batch operation failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Couldn't load the batch. Try again."})
	}
}
