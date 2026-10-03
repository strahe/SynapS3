package admin

import (
	"net/http"
	"strconv"
	"time"
)

type storageConfirmationAttentionResponse struct {
	RequestID     string    `json:"request_id"`
	TaskID        *int64    `json:"task_id,omitempty"`
	DataSetRowID  int64     `json:"data_set_row_id"`
	ProviderID    string    `json:"provider_id"`
	DataSetID     string    `json:"data_set_id,omitempty"`
	PieceCount    int       `json:"piece_count"`
	PieceCIDs     []string  `json:"piece_cids"`
	TransactionID string    `json:"transaction_id,omitempty"`
	SubmitError   string    `json:"submit_error,omitempty"`
	ReasonCode    string    `json:"reason_code"`
	SubmittedAt   time.Time `json:"submitted_at"`
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
		pieceCIDs := record.PieceCIDs
		if pieceCIDs == nil {
			pieceCIDs = []string{}
		}
		response = append(response, storageConfirmationAttentionResponse{
			RequestID: record.RequestID, TaskID: record.TaskID, DataSetRowID: record.DataSetRowID,
			ProviderID: record.ProviderID, DataSetID: record.DataSetID,
			PieceCount: len(pieceCIDs), PieceCIDs: pieceCIDs, TransactionID: record.TransactionID,
			SubmitError: record.SubmitError, ReasonCode: string(record.Code),
			SubmittedAt: record.SubmittedAt, AttentionAt: record.AttentionAt,
		})
	}
	writeJSON(w, http.StatusOK, response)
}
