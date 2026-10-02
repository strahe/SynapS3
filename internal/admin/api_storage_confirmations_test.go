package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/storagecommit"
)

type storageConfirmationAPIRepo struct {
	repository.StorageContentRepository
	records []storagecommit.AttentionRecord
}

func (r *storageConfirmationAPIRepo) ListCommitAttention(context.Context, int) ([]storagecommit.AttentionRecord, error) {
	return r.records, nil
}

func TestAPIStorageConfirmationsList(t *testing.T) {
	now := time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)
	repo := &storageConfirmationAPIRepo{records: []storagecommit.AttentionRecord{{
		CopyID: 12, ContentID: 7, CopyIndex: 1, DataSetRowID: 9,
		ProviderID: "provider-1", DataSetID: "dataset-1", PieceCID: "piece-1",
		AttemptID: "attempt-1", SubmitError: "provider returned HTTP 500: piece not found",
		Code: storagecommit.AttentionAttemptOnlyAmbiguous, AttemptedAt: now.Add(-time.Second), AttentionAt: now,
	}}}
	srv := &Server{repos: &repository.Repositories{Contents: repo}, logger: testLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/storage-confirmations", srv.handleAPIListStorageConfirmations)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/storage-confirmations?status=needs_attention&limit=25", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", rec.Code, rec.Body.String())
	}
	var listed []storageConfirmationAttentionResponse
	if err := json.NewDecoder(rec.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed) != 1 || listed[0].CopyID != 12 || listed[0].ReasonCode != "attempt_only_ambiguous" ||
		listed[0].SubmitError != "provider returned HTTP 500: piece not found" {
		t.Fatalf("listed = %#v", listed)
	}
}
