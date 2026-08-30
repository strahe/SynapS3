package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/storagecommit"
)

type storageConfirmationAPIRepo struct {
	repository.StorageUploadRepository
	records      []storagecommit.AttentionRecord
	releaseInput storagecommit.ManualReleaseInput
	releaseErr   error
}

func (r *storageConfirmationAPIRepo) ListCommitAttention(context.Context, int) ([]storagecommit.AttentionRecord, error) {
	return r.records, nil
}

func (r *storageConfirmationAPIRepo) ReleaseCommitAttention(_ context.Context, input storagecommit.ManualReleaseInput) error {
	r.releaseInput = input
	return r.releaseErr
}

func TestAPIStorageConfirmationsListAndRelease(t *testing.T) {
	now := time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)
	repo := &storageConfirmationAPIRepo{records: []storagecommit.AttentionRecord{{
		CopyID: 12, UploadID: 7, CopyIndex: 1, DataSetRowID: 9,
		ProviderID: "provider-1", DataSetID: "dataset-1", PieceCID: "piece-1",
		AttemptID: "attempt-1", Code: storagecommit.AttentionAttemptOnlyAmbiguous,
		AttemptedAt: now.Add(-time.Second), AttentionAt: now,
	}}}
	srv := &Server{repos: &repository.Repositories{Uploads: repo}, logger: testLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/storage-confirmations", srv.handleAPIListStorageConfirmations)
	mux.HandleFunc("POST /api/v1/storage-confirmations/{id}/release", srv.handleAPIReleaseStorageConfirmation)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/storage-confirmations?status=needs_attention&limit=25", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", rec.Code, rec.Body.String())
	}
	var listed []storageConfirmationAttentionResponse
	if err := json.NewDecoder(rec.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed) != 1 || listed[0].CopyID != 12 || listed[0].ReasonCode != "attempt_only_ambiguous" {
		t.Fatalf("listed = %#v", listed)
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/storage-confirmations/12/release", strings.NewReader(`{"acknowledge_possible_duplicate":true}`))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("release status = %d body=%s", rec.Code, rec.Body.String())
	}
	if repo.releaseInput.CopyID != 12 || !repo.releaseInput.AcknowledgePossibleDuplicate {
		t.Fatalf("release input = %#v", repo.releaseInput)
	}
}

func TestAPIStorageConfirmationReleaseRequiresAcknowledgement(t *testing.T) {
	repo := &storageConfirmationAPIRepo{}
	srv := &Server{repos: &repository.Repositories{Uploads: repo}, logger: testLogger()}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/storage-confirmations/12/release", strings.NewReader(`{}`))
	req.SetPathValue("id", "12")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.handleAPIReleaseStorageConfirmation(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if repo.releaseInput.CopyID != 0 {
		t.Fatalf("release was called: %#v", repo.releaseInput)
	}
}
