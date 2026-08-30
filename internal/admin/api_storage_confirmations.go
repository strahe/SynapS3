package admin

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/storagecommit"
)

type storageConfirmationAttentionResponse struct {
	CopyID        int64     `json:"copy_id"`
	UploadID      int64     `json:"upload_id"`
	CopyIndex     int       `json:"copy_index"`
	DataSetRowID  int64     `json:"data_set_row_id"`
	ProviderID    string    `json:"provider_id"`
	DataSetID     string    `json:"data_set_id,omitempty"`
	PieceCID      string    `json:"piece_cid,omitempty"`
	AttemptID     string    `json:"attempt_id"`
	TransactionID string    `json:"transaction_id,omitempty"`
	ReasonCode    string    `json:"reason_code"`
	AttemptedAt   time.Time `json:"attempted_at"`
	AttentionAt   time.Time `json:"attention_at"`
}

type releaseStorageConfirmationRequest struct {
	AcknowledgePossibleDuplicate bool `json:"acknowledge_possible_duplicate"`
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
	records, err := s.repos.Uploads.ListCommitAttention(r.Context(), limit)
	if err != nil {
		s.logger.Error("api: failed to list storage confirmations", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	response := make([]storageConfirmationAttentionResponse, 0, len(records))
	for _, record := range records {
		response = append(response, storageConfirmationAttentionResponse{
			CopyID: record.CopyID, UploadID: record.UploadID, CopyIndex: record.CopyIndex,
			DataSetRowID: record.DataSetRowID, ProviderID: record.ProviderID, DataSetID: record.DataSetID,
			PieceCID: record.PieceCID, AttemptID: record.AttemptID, TransactionID: record.TransactionID,
			ReasonCode: string(record.Code), AttemptedAt: record.AttemptedAt, AttentionAt: record.AttentionAt,
		})
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleAPIReleaseStorageConfirmation(w http.ResponseWriter, r *http.Request) {
	copyID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || copyID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid copy id"})
		return
	}
	var req releaseStorageConfirmationRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	if !req.AcknowledgePossibleDuplicate {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "acknowledge_possible_duplicate must be true",
		})
		return
	}
	err = s.repos.Uploads.ReleaseCommitAttention(r.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyID, AcknowledgePossibleDuplicate: true,
	})
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"copy_id": copyID, "status": "released"})
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "storage confirmation not found"})
	case errors.Is(err, repository.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "storage confirmation changed; refresh and try again"})
	case errors.Is(err, repository.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid release request"})
	default:
		s.logger.Error("api: failed to release storage confirmation", "error", err, "copyID", copyID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
	}
}
