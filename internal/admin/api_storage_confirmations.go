package admin

import (
	"net/http"
	"strconv"
	"time"
)

type storageConfirmationAttentionResponse struct {
	CopyID        int64     `json:"copy_id"`
	TaskID        *int64    `json:"task_id,omitempty"`
	ContentID     int64     `json:"content_id"`
	CopyIndex     int       `json:"copy_index"`
	DataSetRowID  int64     `json:"data_set_row_id"`
	ProviderID    string    `json:"provider_id"`
	DataSetID     string    `json:"data_set_id,omitempty"`
	PieceCID      string    `json:"piece_cid,omitempty"`
	AttemptID     string    `json:"attempt_id"`
	TransactionID string    `json:"transaction_id,omitempty"`
	SubmitError   string    `json:"submit_error,omitempty"`
	ReasonCode    string    `json:"reason_code"`
	AttemptedAt   time.Time `json:"attempted_at"`
	AttentionAt   time.Time `json:"attention_at"`
}

func (s *Server) handleAPIListStorageConfirmations(w http.ResponseWriter, r *http.Request) {
	if status := r.URL.Query().Get("status"); status != "" && status != "needs_attention" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid confirmation status"})
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 1000 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 1000"})
			return
		}
		limit = parsed
	}
	records, err := s.repos.Contents.ListCommitAttention(r.Context(), limit)
	if err != nil {
		s.logger.Error("api: failed to list storage confirmations", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	response := make([]storageConfirmationAttentionResponse, 0, len(records))
	for _, record := range records {
		response = append(response, storageConfirmationAttentionResponse{
			CopyID: record.CopyID, TaskID: record.TaskID, ContentID: record.ContentID, CopyIndex: record.CopyIndex,
			DataSetRowID: record.DataSetRowID, ProviderID: record.ProviderID, DataSetID: record.DataSetID,
			PieceCID: record.PieceCID, AttemptID: record.AttemptID, TransactionID: record.TransactionID,
			SubmitError: record.SubmitError, ReasonCode: string(record.Code),
			AttemptedAt: record.AttemptedAt, AttentionAt: record.AttentionAt,
		})
	}
	writeJSON(w, http.StatusOK, response)
}
