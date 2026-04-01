package admin

import (
	"net/http"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
)

type taskListItem struct {
	ID            int64   `json:"id"`
	Type          string  `json:"type"`
	RefType       string  `json:"ref_type"`
	RefID         int64   `json:"ref_id"`
	RefGeneration int64   `json:"ref_generation"`
	Status        string  `json:"status"`
	RetryCount    int     `json:"retry_count"`
	MaxRetries    int     `json:"max_retries"`
	LastError     *string `json:"last_error,omitempty"`
	ScheduledAt   string  `json:"scheduled_at"`
	ClaimedAt     *string `json:"claimed_at,omitempty"`
	CompletedAt   *string `json:"completed_at,omitempty"`
}

type taskListResponse struct {
	Tasks  []taskListItem `json:"tasks"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}

func (s *Server) handleAPITasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	taskType := r.URL.Query().Get("type")
	status := r.URL.Query().Get("status")

	limit := 50
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

	tasks, total, err := s.repos.Tasks.List(ctx, taskType, status, limit, offset)
	if err != nil {
		s.logger.Error("api: failed to list tasks", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	items := make([]taskListItem, 0, len(tasks))
	for _, t := range tasks {
		item := taskListItem{
			ID:            t.ID,
			Type:          string(t.Type),
			RefType:       t.RefType,
			RefID:         t.RefID,
			RefGeneration: t.RefGeneration,
			Status:        string(t.Status),
			RetryCount:    t.RetryCount,
			MaxRetries:    t.MaxRetries,
			LastError:     t.LastError,
			ScheduledAt:   t.ScheduledAt.Format(time.RFC3339),
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
