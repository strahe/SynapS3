package admin

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storageremoval"
)

type removalBatchTaskRepo struct {
	repository.StorageRemovalRepository
	records []storageremoval.TaskBatch
	err     error
}

func (r *removalBatchTaskRepo) ListRemovalBatchesForTasks(context.Context, []int64) ([]storageremoval.TaskBatch, error) {
	return r.records, r.err
}

func TestAPITasksShowHeldRemovalBatch(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	stopped := fixture.enqueue(t, model.TaskTypeStorageRemoval, "stopped-removal", time.Now(), "storage_removal_request", "removal-1")
	fixture.transition(t, stopped.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("transaction_reverted"),
	})
	dataSetID := onChainID(t, "39")
	repo := &removalBatchTaskRepo{StorageRemovalRepository: fixture.repos.StorageRemoval, records: []storageremoval.TaskBatch{{
		TaskID: stopped.ID, RequestID: "removal-1", Status: storageremoval.RequestStatusSubmitted,
		ProviderID: onChainID(t, "32"), DataSetID: &dataSetID, PieceCount: 2,
		AttentionCode: new("transaction_reverted"), AttentionAt: new(time.Now().UTC()), TxHash: new("0xremoval"),
		LastError: new("provider returned HTTP 500: https://user:password@provider.example/pdp/data-sets/39?token=secret#fragment"),
	}}}
	fixture.repos.StorageRemoval = repo
	path := "/api/v1/tasks/" + strconv.FormatInt(stopped.ID, 10)
	endpoints := []string{"/api/v1/tasks?type=storage_removal&status=failed", path, path + "/history"}
	for _, endpoint := range endpoints {
		rr := fixture.request(http.MethodGet, endpoint, nil)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", endpoint, rr.Code, rr.Body.String())
		}
		var body struct {
			Tasks []taskListItem `json:"tasks"`
			Task  taskListItem   `json:"task"`
		}
		decodeJSON(t, rr, &body)
		item := body.Task
		if len(body.Tasks) > 0 {
			item = body.Tasks[0]
		}
		batch := item.RemovalBatch
		if item.Acknowledgeable || !item.Retryable || batch == nil || batch.RequestID != "removal-1" || batch.ProviderID != "32" || batch.DataSetID != "39" || batch.PieceCount != 2 || batch.TxHash == nil || *batch.TxHash != "0xremoval" || batch.AttentionCode == nil || *batch.AttentionCode != "transaction_reverted" {
			t.Fatalf("%s: removal task = %#v", endpoint, item)
		}
		if batch.LastError == nil || !strings.Contains(*batch.LastError, "HTTP 500") || strings.Contains(*batch.LastError, "password") || strings.Contains(*batch.LastError, "secret") {
			t.Fatalf("%s: removal diagnostic = %#v", endpoint, batch.LastError)
		}
	}
	repo.err = errors.New("removal lookup failed")
	for _, endpoint := range endpoints {
		if rr := fixture.request(http.MethodGet, endpoint, nil); rr.Code != http.StatusInternalServerError {
			t.Fatalf("%s: status = %d, want 500", endpoint, rr.Code)
		}
	}
}
