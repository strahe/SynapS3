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

func TestAPITasksShowRemovalBatchAndCurrentOwnership(t *testing.T) {
	for _, scenario := range []struct {
		name            string
		status          model.TaskStatus
		batchStatus     storageremoval.RequestStatus
		ownsRequest     bool
		acknowledgeable bool
	}{
		{name: "current owner", status: model.TaskStatusFailed, batchStatus: storageremoval.RequestStatusSubmitted, ownsRequest: true},
		{name: "former owner", status: model.TaskStatusFailed, batchStatus: storageremoval.RequestStatusSubmitted, acknowledgeable: true},
		{name: "settled", status: model.TaskStatusCompleted, batchStatus: storageremoval.RequestStatusSettled},
		{name: "abandoned", status: model.TaskStatusFailed, batchStatus: storageremoval.RequestStatusAbandoned, acknowledgeable: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newAdminTaskFixture(t)
			stopped := fixture.enqueue(t, model.TaskTypeStorageRemoval, "stopped-removal", time.Now(), "storage_removal_request", "removal-1")
			var failureReason *string
			if scenario.status == model.TaskStatusFailed {
				failureReason = new("transaction_reverted")
			}
			fixture.transition(t, stopped.ID, repository.TaskTransition{
				Status: scenario.status, ResumeMode: model.TaskResumeModeRecover,
				FailureReason: failureReason,
			})
			dataSetID := onChainID(t, "39")
			repo := &removalBatchTaskRepo{StorageRemovalRepository: fixture.repos.StorageRemoval, records: []storageremoval.TaskBatch{{
				TaskID: stopped.ID, OwnsRequest: scenario.ownsRequest, RequestID: "removal-1", Status: scenario.batchStatus,
				ProviderID: onChainID(t, "32"), DataSetID: &dataSetID, PieceCount: 2,
				AttentionCode: new("transaction_reverted"), AttentionAt: new(time.Now().UTC()), TxHash: new("0xremoval"),
				LastError: new("provider returned HTTP 500: https://user:password@provider.example/pdp/data-sets/39?token=secret#fragment"),
			}}}
			if scenario.batchStatus.Terminal() {
				repo.records[0].AttentionCode, repo.records[0].AttentionAt = nil, nil
			}
			if scenario.batchStatus == storageremoval.RequestStatusAbandoned {
				repo.records[0].TxHash, repo.records[0].PieceCount = nil, 0
			}
			fixture.repos.StorageRemoval = repo
			path := "/api/v1/tasks/" + strconv.FormatInt(stopped.ID, 10)
			endpoints := []string{"/api/v1/tasks?type=storage_removal&status=failed", path, path + "/history"}
			if scenario.status == model.TaskStatusCompleted {
				endpoints[0] = "/api/v1/tasks?scope=history&type=storage_removal"
			}
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
				if item.Acknowledgeable != scenario.acknowledgeable || batch == nil || batch.RequestID != "removal-1" || batch.Status != string(scenario.batchStatus) || batch.ProviderID != "32" || batch.DataSetID != "39" || batch.PieceCount != repo.records[0].PieceCount {
					t.Fatalf("%s: removal task = %#v", endpoint, item)
				}
				if scenario.ownsRequest && !item.Retryable {
					t.Fatalf("%s: owning task is not retryable", endpoint)
				}
				if scenario.batchStatus == storageremoval.RequestStatusAbandoned {
					if batch.TxHash != nil {
						t.Fatalf("%s: abandoned batch has transaction %v", endpoint, batch.TxHash)
					}
				} else if batch.TxHash == nil || *batch.TxHash != "0xremoval" {
					t.Fatalf("%s: batch transaction = %v", endpoint, batch.TxHash)
				}
				if scenario.batchStatus.Terminal() {
					if batch.AttentionCode != nil {
						t.Fatalf("%s: terminal batch has attention %v", endpoint, batch.AttentionCode)
					}
				} else if batch.AttentionCode == nil || *batch.AttentionCode != "transaction_reverted" {
					t.Fatalf("%s: batch attention = %v", endpoint, batch.AttentionCode)
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
		})
	}
}
