package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/uptrace/bun"
)

type adminTaskHandler struct {
	definition taskengine.Definition
}

func (h adminTaskHandler) Definition() taskengine.Definition { return h.definition }
func (adminTaskHandler) Execute(context.Context, taskengine.Execution) taskengine.Result {
	return taskengine.Complete("", nil)
}

func (adminTaskHandler) Recover(context.Context, taskengine.Execution) taskengine.Result {
	return taskengine.Complete("", nil)
}

func newAdminTestTaskService(t *testing.T, repos *repository.Repositories, overrides ...func(*taskengine.Definition)) *taskengine.Service {
	t.Helper()

	registry := taskengine.NewRegistry()
	for _, taskType := range []model.TaskType{
		model.TaskTypeBucketProvision,
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
		model.TaskTypeProviderUploadSpeedTest,
		model.TaskTypeGC,
	} {
		definition := taskengine.Definition{
			Type:         taskType,
			WorkStart:    taskengine.WorkStartOnEffect,
			InputVersion: 1,
			Codec:        taskengine.StrictJSONCodec[map[string]any](nil),
			Policy:       taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()},
			AllowRetry:   taskType != model.TaskTypeProviderReplacementCoordinate,
		}
		if taskType == model.TaskTypeWalletOperation {
			definition.CanManualRetry = func(task *model.Task) bool {
				return task.FailureReason != nil && *task.FailureReason == "wallet_broadcast_not_started"
			}
		}
		if taskType == model.TaskTypeStorageStore {
			definition.CanManualRetry = func(task *model.Task) bool {
				return task.FailureReason != nil && (*task.FailureReason == "store_not_started" || *task.FailureReason == "store_outcome_unknown")
			}
		}
		if taskType == model.TaskTypeStorageDataSetRetire {
			definition.CanManualRetry = func(task *model.Task) bool {
				return task.FailureReason == nil || *task.FailureReason != "termination_outcome_unknown"
			}
		}
		for _, override := range overrides {
			override(&definition)
		}
		if err := registry.Register(adminTaskHandler{definition: definition}); err != nil {
			t.Fatalf("Register(%s): %v", definition.Type, err)
		}
	}
	service, err := taskengine.NewService(registry, repos)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

func TestAPITasksUsesCursorAndServerPresentation(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	now := time.Now()
	waiting := fixture.enqueue(t, model.TaskTypeCacheEvict, "waiting", now, "object_version", "version-3")
	fixture.transition(t, waiting.ID, repository.TaskTransition{
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover,
		AvailableAt: now.Add(time.Minute), WaitReason: new("durability"),
	})
	queued := fixture.enqueue(t, model.TaskTypeUploadPlan, "queued", now, "object_version", "version-1")
	scheduled := fixture.enqueue(t, model.TaskTypeUploadPlan, "scheduled", now.Add(time.Hour), "object_version", "version-2")

	rr := fixture.request(http.MethodGet, "/api/v1/tasks?type=upload_plan&status=pending&limit=1", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var first taskListResponse
	decodeJSON(t, rr, &first)
	if len(first.Tasks) != 1 || first.Tasks[0].ID != scheduled.ID || first.Tasks[0].Presentation != "scheduled" {
		t.Fatalf("first page = %#v", first)
	}
	if first.NextCursor == nil || *first.NextCursor != scheduled.ID {
		t.Fatalf("next cursor = %v, want %d", first.NextCursor, scheduled.ID)
	}
	if strings.Contains(rr.Body.String(), `"total"`) || strings.Contains(rr.Body.String(), `"input"`) || strings.Contains(rr.Body.String(), `"resume_mode"`) || strings.Contains(rr.Body.String(), `"claim_generation"`) {
		t.Fatalf("response exposes removed or internal fields: %s", rr.Body.String())
	}

	rr = fixture.request(http.MethodGet, "/api/v1/tasks?type=upload_plan&status=pending&limit=1&cursor="+strconv.FormatInt(*first.NextCursor, 10), nil)
	var second taskListResponse
	decodeJSON(t, rr, &second)
	if len(second.Tasks) != 1 || second.Tasks[0].ID != queued.ID || second.Tasks[0].Presentation != "queued" || second.NextCursor != nil {
		t.Fatalf("second page = %#v", second)
	}

	rr = fixture.request(http.MethodGet, "/api/v1/tasks?type=cache_evict", nil)
	var cachePage taskListResponse
	decodeJSON(t, rr, &cachePage)
	if len(cachePage.Tasks) != 1 || cachePage.Tasks[0].Presentation != "waiting" || cachePage.Tasks[0].WaitReason == nil || *cachePage.Tasks[0].WaitReason != "durability" {
		t.Fatalf("waiting task = %#v", cachePage)
	}
}

func TestAPITasksPreservesDurationPrecision(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	row := fixture.enqueue(t, model.TaskTypeStorageStore, "short-store", time.Now(), "storage_copy", "447")
	fixture.transition(t, row.ID, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute,
	})
	started := time.Date(2026, 10, 3, 0, 0, 0, 900_000_000, time.UTC)
	finished := started.Add(200 * time.Millisecond)
	if _, err := fixture.db.NewUpdate().Table("task_history").
		Set("started_at = ?", started.Add(-30*time.Minute)).Set("work_started_at = ?", started).Set("finished_at = ?", finished).
		Where("task_id = ?", row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	rr := fixture.request(http.MethodGet, "/api/v1/tasks?scope=history&type=storage_store", nil)
	var response taskListResponse
	decodeJSON(t, rr, &response)
	if rr.Code != http.StatusOK || len(response.Tasks) != 1 {
		t.Fatalf("response = %d %s", rr.Code, rr.Body.String())
	}
	item := response.Tasks[0]
	if item.StartedAt == nil || item.FinishedAt == nil {
		t.Fatalf("missing task times: %#v", item)
	}
	wireStart, err := time.Parse(time.RFC3339Nano, *item.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	wireFinish, err := time.Parse(time.RFC3339Nano, *item.FinishedAt)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := wireFinish.Sub(wireStart); elapsed != 200*time.Millisecond {
		t.Fatalf("serialized duration = %s, want 200ms: %s", elapsed, rr.Body.String())
	}
}

func TestAPITasksHidesStaleRunningWaitWithoutInventingStart(t *testing.T) {
	claimedAt := time.Now().Add(-30 * time.Minute)
	row := &model.Task{
		Type: model.TaskTypeStorageStore, Status: model.TaskStatusRunning,
		WaitReason: new("resource"), StatusMessage: new("Old wait"), LastError: new("Previous request failed"),
		StartedAt: &claimedAt, AvailableAt: claimedAt, CreatedAt: claimedAt,
	}
	item := new(Server).taskListItem(t.Context(), row)
	if item.StartedAt != nil || item.WaitReason != nil || item.StatusMessage != nil || item.LastError == nil {
		t.Fatalf("legacy running presentation = %#v", item)
	}
	row.WaitReason = nil
	row.StatusMessage = new("Checking storage transfer")
	item = new(Server).taskListItem(t.Context(), row)
	if item.StatusMessage == nil || *item.StatusMessage != "Checking storage transfer" {
		t.Fatalf("current progress was hidden: %#v", item)
	}
}

func TestAPITasksRejectsRemovedAndInvalidFilters(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	for _, path := range []string{
		"/api/v1/tasks?category=upload",
		"/api/v1/tasks?stage=store",
		"/api/v1/tasks?offset=10",
		"/api/v1/tasks?type=future_task_type",
		"/api/v1/tasks?status=waiting",
		"/api/v1/tasks?scope=invalid",
		"/api/v1/tasks?status=completed",
		"/api/v1/tasks?scope=history&status=running",
		"/api/v1/tasks/stats?scope=invalid",
		"/api/v1/tasks/stats?scope=history&status=pending",
		"/api/v1/tasks?limit=0",
		"/api/v1/tasks?cursor=nope",
	} {
		rr := fixture.request(http.MethodGet, path, nil)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d body=%s", path, rr.Code, rr.Body.String())
		}
	}
}

func TestAPITaskStatsUsesPresentationStatusContract(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	first := fixture.enqueue(t, model.TaskTypeUploadPlan, "one", time.Now(), "", "")
	second := fixture.enqueue(t, model.TaskTypeUploadPlan, "two", time.Now(), "", "")
	fixture.enqueue(t, model.TaskTypeUploadPlan, "three", time.Now(), "", "")
	fixture.transition(t, first.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("provider_error"), LastError: new("provider unavailable"),
	})
	fixture.transition(t, second.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("old_error"), LastError: new("old failure"),
	})
	if err := fixture.repos.Tasks.AcknowledgeFailed(t.Context(), second.ID); err != nil {
		t.Fatalf("AcknowledgeFailed: %v", err)
	}

	rr := fixture.request(http.MethodGet, "/api/v1/tasks/stats", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body []taskStatsItem
	decodeJSON(t, rr, &body)
	counts := make(map[[2]string]int64, len(body))
	for _, item := range body {
		counts[[2]string{item.Type, item.Status}] = item.Count
	}
	if counts[[2]string{string(model.TaskTypeUploadPlan), string(model.TaskStatusPending)}] != 1 ||
		counts[[2]string{string(model.TaskTypeUploadPlan), string(model.TaskStatusFailed)}] != 1 {
		t.Fatalf("counts = %#v", counts)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks/stats?scope=history", nil)
	decodeJSON(t, rr, &body)
	if rr.Code != http.StatusOK || len(body) != 1 || body[0].Status != "failed" || body[0].Count != 1 {
		t.Fatalf("history counts = %d %s", rr.Code, rr.Body.String())
	}
}

func TestAPITasksAcknowledgementPreservesFailedStatus(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	failed := fixture.enqueue(t, model.TaskTypeUploadPlan, "unread-failure", time.Now(), "", "")
	viewed := fixture.enqueue(t, model.TaskTypeUploadPlan, "viewed-failure", time.Now(), "", "")
	for _, row := range []*model.Task{failed, viewed} {
		fixture.transition(t, row.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
	}
	if err := fixture.repos.Tasks.AcknowledgeFailed(t.Context(), viewed.ID); err != nil {
		t.Fatal(err)
	}
	rr := fixture.request(http.MethodGet, "/api/v1/tasks?status=failed", nil)
	var page taskListResponse
	decodeJSON(t, rr, &page)
	if rr.Code != 200 || len(page.Tasks) != 1 || page.Tasks[0].ID != failed.ID {
		t.Fatalf("work failures = %d %s", rr.Code, rr.Body.String())
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks?scope=history&status=failed", nil)
	decodeJSON(t, rr, &page)
	if rr.Code != 200 || len(page.Tasks) != 1 || page.Tasks[0].ID != viewed.ID || page.Tasks[0].Status != "failed" || page.Tasks[0].AcknowledgedAt == nil || !page.Tasks[0].Retryable {
		t.Fatalf("acknowledged history = %d %s", rr.Code, rr.Body.String())
	}
	if rr := fixture.request(http.MethodGet, "/api/v1/tasks?status=dismissed", nil); rr.Code != 400 {
		t.Fatalf("removed status response = %d", rr.Code)
	}
}

type commitAttentionTaskRepo struct {
	repository.StorageContentRepository
	records []storagecommit.AttentionRecord
	err     error
}

func (r *commitAttentionTaskRepo) ListCommitAttentionForTasks(context.Context, []int64) ([]storagecommit.AttentionRecord, error) {
	return r.records, r.err
}

type heldAcknowledgeTaskRepo struct {
	repository.TaskRepository
}

type contendedTaskRepo struct {
	repository.TaskRepository
	err error
}

func (r contendedTaskRepo) GetDirectSuccessor(context.Context, int64) (*model.Task, error) {
	return nil, r.err
}

func TestAPITaskRetryReportsTemporaryContention(t *testing.T) {
	for _, contention := range []error{repository.ErrTaskIdentityContended, repository.ErrRepositoryContended} {
		t.Run(contention.Error(), func(t *testing.T) {
			f := newAdminTaskFixture(t)
			source := f.enqueue(t, model.TaskTypeUploadPlan, "contended-retry", time.Now(), "bucket", "1")
			f.transition(t, source.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
			f.repos.Tasks = contendedTaskRepo{TaskRepository: f.repos.Tasks, err: contention}
			rr := f.request(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(source.ID, 10)+"/retry", nil)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("contention = %d %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func (heldAcknowledgeTaskRepo) AcknowledgeFailed(context.Context, int64) error {
	return repository.ErrConflict
}

// A stopped Submit batch task carries the confirmation it holds and offers
// Retry instead of a dismissal, which is refused.
func TestAPITasksShowStoppedStorageConfirmation(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	stopped := fixture.enqueue(t, model.TaskTypeStorageCommit, "stopped-commit", time.Now(), "", "")
	fixture.transition(t, stopped.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("submission_mismatch"), LastError: new("batch requires attention"),
	})
	now := time.Date(2026, 9, 30, 19, 53, 39, 0, time.UTC)
	fixture.repos.Contents = &commitAttentionTaskRepo{StorageContentRepository: fixture.repos.Contents, records: []storagecommit.AttentionRecord{{
		RequestID: "request-447", TaskID: &stopped.ID, ProviderID: "32", DataSetID: "39911", PieceCIDs: []string{"piece-1"},
		TransactionID: "0xcommit", SubmitError: "provider returned HTTP 500: piece not found",
		Code: storagecommit.AttentionSubmissionMismatch, SubmittedAt: now.Add(-6 * time.Second), AttentionAt: now,
	}}}

	rr := fixture.request(http.MethodGet, "/api/v1/tasks?type=storage_commit&status=failed", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var page taskListResponse
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 1 || page.Tasks[0].Acknowledgeable || !page.Tasks[0].Retryable || page.Tasks[0].StorageConfirmation == nil {
		t.Fatalf("page = %#v, want the stopped task with its confirmation, Retry, and no dismissal", page)
	}
	if confirmation := page.Tasks[0].StorageConfirmation; confirmation.RequestID != "request-447" ||
		confirmation.SubmitError != "provider returned HTTP 500: piece not found" ||
		confirmation.ProviderID != "32" || confirmation.DataSetID != "39911" ||
		confirmation.PieceCount != 1 || confirmation.PieceCIDs[0] != "piece-1" || confirmation.TransactionID != "0xcommit" {
		t.Fatalf("confirmation = %#v, want the confirmation with the provider reply", confirmation)
	}
	path := "/api/v1/tasks/" + strconv.FormatInt(stopped.ID, 10)
	rr = fixture.request(http.MethodGet, path, nil)
	var detail struct {
		Task taskListItem `json:"task"`
	}
	decodeJSON(t, rr, &detail)
	if rr.Code != http.StatusOK || detail.Task.Acknowledgeable || detail.Task.StorageConfirmation == nil {
		t.Fatalf("details = %d %s", rr.Code, rr.Body.String())
	}
	rr = fixture.request(http.MethodGet, path+"/history", nil)
	decodeJSON(t, rr, &page)
	if rr.Code != http.StatusOK || len(page.Tasks) != 1 || page.Tasks[0].Acknowledgeable || page.Tasks[0].StorageConfirmation == nil {
		t.Fatalf("history = %d %s", rr.Code, rr.Body.String())
	}
	attentionRepo := fixture.repos.Contents.(*commitAttentionTaskRepo)
	attentionRepo.err = errors.New("attention lookup failed")
	for _, endpoint := range []string{"/api/v1/tasks", path, path + "/history"} {
		rr = fixture.request(http.MethodGet, endpoint, nil)
		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("%s lookup error = %d %s", endpoint, rr.Code, rr.Body.String())
		}
	}
	attentionRepo.err = nil

	fixture.repos.Tasks = heldAcknowledgeTaskRepo{TaskRepository: fixture.repos.Tasks}
	rr = fixture.request(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(stopped.ID, 10)+"/acknowledge", nil)
	if rr.Code != http.StatusConflict {
		t.Fatalf("dismiss status = %d body=%s, want conflict", rr.Code, rr.Body.String())
	}
}

func TestAPITaskRetryCandidatesKeepDomainValidationAtDetailsAndAction(t *testing.T) {
	for _, tc := range []struct {
		name         string
		err          error
		detailStatus int
		actionStatus int
		reason       string
	}{
		{"unsafe recovery", &repository.CopyRetryBlockedError{Block: storagepipeline.CopyRetryRecoveryRequiresAttention}, http.StatusOK, http.StatusConflict, "Previous transfer needs recovery."},
		{"stale ownership", repository.ErrConflict, http.StatusOK, http.StatusConflict, "Retry is unavailable for this task."},
		{"missing domain owner", repository.ErrNotFound, http.StatusOK, http.StatusConflict, "Retry is unavailable for this task."},
		{"database failure", errors.New("private database diagnostics"), http.StatusInternalServerError, http.StatusInternalServerError, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAdminTaskFixture(t)
			inspections := 0
			f.service = newAdminTestTaskService(t, f.repos, func(definition *taskengine.Definition) {
				if definition.Type == model.TaskTypeUploadPlan {
					definition.InspectRetry = func(context.Context, *repository.Repositories, *model.Task) error {
						inspections++
						return tc.err
					}
				}
			})
			f.server.WithTaskService(f.service)
			source := f.enqueue(t, model.TaskTypeUploadPlan, "retry-candidate", time.Now(), "bucket", "1")
			f.transition(t, source.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
			path := "/api/v1/tasks/" + strconv.FormatInt(source.ID, 10)
			for _, endpoint := range []string{"/api/v1/tasks", path + "/history"} {
				rr := f.request(http.MethodGet, endpoint, nil)
				var page taskListResponse
				decodeJSON(t, rr, &page)
				if rr.Code != http.StatusOK || len(page.Tasks) != 1 || !page.Tasks[0].Retryable || inspections != 0 {
					t.Fatalf("candidate list %s = %d %s, inspections=%d", endpoint, rr.Code, rr.Body.String(), inspections)
				}
			}
			rr := f.request(http.MethodGet, path, nil)
			if rr.Code != tc.detailStatus || inspections != 1 || strings.Contains(rr.Body.String(), "private database diagnostics") {
				t.Fatalf("details = %d %s, inspections=%d", rr.Code, rr.Body.String(), inspections)
			}
			if tc.reason != "" {
				var detail struct {
					Task taskListItem `json:"task"`
				}
				decodeJSON(t, rr, &detail)
				if detail.Task.Retryable || detail.Task.RetryTaskID != nil || detail.Task.RetryUnavailableReason != tc.reason {
					t.Fatalf("unsafe details = %#v", detail.Task)
				}
			}
			rr = f.request(http.MethodPost, path+"/retry", nil)
			if rr.Code != tc.actionStatus || strings.Contains(rr.Body.String(), "private database diagnostics") {
				t.Fatalf("action = %d %s", rr.Code, rr.Body.String())
			}
			if tc.reason != "" {
				var actionError map[string]string
				if err := json.Unmarshal(rr.Body.Bytes(), &actionError); err != nil || actionError["error"] != tc.reason {
					t.Fatalf("action did not explain refusal: %s %v", rr.Body.String(), err)
				}
			}
			child, err := f.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
			original, getErr := f.repos.Tasks.GetByID(t.Context(), source.ID)
			if err != nil || getErr != nil || child != nil || original.SupersededAt != nil {
				t.Fatalf("rejected retry changed source: child=%#v original=%#v errors=%v/%v", child, original, err, getErr)
			}
		})
	}
}

func TestAPITasksShowsRecurringWorkAndArchivesAcknowledgedFailures(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	healthy := fixture.enqueue(t, model.TaskTypeCacheCapacityReconcile, "healthy-system", time.Now().Add(time.Hour), "system", "cache-capacity")
	failed := fixture.enqueue(t, model.TaskTypeObservabilityRefresh, "failed-system", time.Now(), "system", "observability")
	fixture.transition(t, failed.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("refresh_failed"), LastError: new("health refresh failed"),
	})
	rr := fixture.request(http.MethodGet, "/api/v1/tasks", nil)
	var page taskListResponse
	decodeJSON(t, rr, &page)
	if rr.Code != http.StatusOK || len(page.Tasks) != 2 || page.Tasks[0].ID != failed.ID || page.Tasks[1].ID != healthy.ID {
		t.Fatalf("recurring work = %d %s", rr.Code, rr.Body.String())
	}
	if err := fixture.repos.Tasks.AcknowledgeFailed(t.Context(), failed.ID); err != nil {
		t.Fatal(err)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks?status=failed", nil)
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 0 {
		t.Fatalf("acknowledged failure remained in work: %#v", page.Tasks)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks?scope=history&status=failed", nil)
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 1 || page.Tasks[0].ID != failed.ID || page.Tasks[0].Presentation != "failed" {
		t.Fatalf("recurring history = %#v", page.Tasks)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks/stats", nil)
	var stats []taskStatsItem
	decodeJSON(t, rr, &stats)
	if len(stats) != 1 || stats[0].Type != string(model.TaskTypeCacheCapacityReconcile) || stats[0].Status != "pending" {
		t.Fatalf("recurring work counts = %#v", stats)
	}
}

func TestAPITaskRetryAndAcknowledgeFollowDefinition(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	retryable := fixture.enqueue(t, model.TaskTypeUploadPlan, "retryable", time.Now(), "", "")
	fixture.transition(t, retryable.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("temporary"), LastError: new("temporary failure"), IncrementRetry: true,
	})
	nonRetryable := fixture.enqueue(t, model.TaskTypeWalletOperation, "wallet", time.Now(), "", "")
	fixture.transition(t, nonRetryable.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("unknown_outcome"), LastError: new("transaction outcome unknown"),
	})

	rr := fixture.request(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(retryable.ID, 10)+"/retry", nil)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d body=%s", rr.Code, rr.Body.String())
	}
	var created struct {
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, rr, &created)
	got, err := fixture.repos.Tasks.GetByID(t.Context(), created.TaskID)
	if err != nil || got == nil || got.ID == retryable.ID || got.Status != model.TaskStatusPending || got.RetryCount != 0 || got.RetryOfTaskID == nil || *got.RetryOfTaskID != retryable.ID {
		t.Fatalf("successor = %#v err=%v", got, err)
	}
	old, err := fixture.repos.Tasks.GetByID(t.Context(), retryable.ID)
	if err != nil || old.Status != model.TaskStatusFailed || old.RetryCount != 1 || old.SupersededAt == nil {
		t.Fatalf("old changed: %#v %v", old, err)
	}
	replay := fixture.request(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(retryable.ID, 10)+"/retry", nil)
	var repeated struct {
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, replay, &repeated)
	if replay.Code != 202 || repeated.TaskID != created.TaskID {
		t.Fatalf("replay = %d %#v", replay.Code, repeated)
	}

	rr = fixture.request(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(nonRetryable.ID, 10)+"/retry", nil)
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), `"code":"task_retry_unsupported"`) {
		t.Fatalf("unsupported retry status = %d body=%s", rr.Code, rr.Body.String())
	}

	rr = fixture.request(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(nonRetryable.ID, 10)+"/acknowledge", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("acknowledge status = %d body=%s", rr.Code, rr.Body.String())
	}
	got, err = fixture.repos.Tasks.GetByID(t.Context(), nonRetryable.ID)
	if err != nil || got == nil || got.AcknowledgedAt == nil {
		t.Fatalf("acknowledged task = %#v err=%v", got, err)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks?status=failed", nil)
	var body taskListResponse
	decodeJSON(t, rr, &body)
	for _, item := range body.Tasks {
		if item.ID == nonRetryable.ID && item.Presentation != "failed" {
			t.Fatalf("acknowledged task presentation = %q, want dismissed", item.Presentation)
		}
	}
}

func TestAPITaskFlagsComeFromRegistry(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	retryable := fixture.enqueue(t, model.TaskTypeUploadPlan, "flags-retry", time.Now(), "", "")
	fixture.transition(t, retryable.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
	wallet := fixture.enqueue(t, model.TaskTypeWalletOperation, "flags-wallet", time.Now(), "", "")
	fixture.transition(t, wallet.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
	replacement := fixture.enqueue(t, model.TaskTypeProviderReplacementCoordinate, "flags-replacement", time.Now(), "", "")
	fixture.transition(t, replacement.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
	retirement := fixture.enqueue(t, model.TaskTypeStorageDataSetRetire, "flags-retirement", time.Now(), "", "")
	fixture.transition(t, retirement.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("termination_outcome_unknown"),
	})

	rr := fixture.request(http.MethodGet, "/api/v1/tasks?status=failed", nil)
	var body taskListResponse
	decodeJSON(t, rr, &body)
	if len(body.Tasks) != 4 {
		t.Fatalf("tasks = %#v", body.Tasks)
	}
	byType := make(map[string]taskListItem, len(body.Tasks))
	for _, item := range body.Tasks {
		byType[item.Type] = item
	}
	if !byType[string(model.TaskTypeUploadPlan)].Retryable || !byType[string(model.TaskTypeUploadPlan)].Acknowledgeable {
		t.Fatalf("upload flags = %#v", byType[string(model.TaskTypeUploadPlan)])
	}
	if byType[string(model.TaskTypeWalletOperation)].Retryable || !byType[string(model.TaskTypeWalletOperation)].Acknowledgeable {
		t.Fatalf("wallet flags = %#v", byType[string(model.TaskTypeWalletOperation)])
	}
	if byType[string(model.TaskTypeProviderReplacementCoordinate)].Retryable ||
		!byType[string(model.TaskTypeProviderReplacementCoordinate)].Acknowledgeable {
		t.Fatalf("replacement flags = %#v", byType[string(model.TaskTypeProviderReplacementCoordinate)])
	}
	if byType[string(model.TaskTypeStorageDataSetRetire)].Retryable ||
		!byType[string(model.TaskTypeStorageDataSetRetire)].Acknowledgeable {
		t.Fatalf("retirement flags = %#v", byType[string(model.TaskTypeStorageDataSetRetire)])
	}
	rr = fixture.request(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(retirement.ID, 10)+"/retry", nil)
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), `"code":"task_retry_unsupported"`) {
		t.Fatalf("retirement retry status = %d body=%s", rr.Code, rr.Body.String())
	}
}

type adminTaskFixture struct {
	t       *testing.T
	db      *bun.DB
	repos   *repository.Repositories
	service *taskengine.Service
	server  *Server
}

// TestAPITaskBulkAcknowledgeDismissesTheSelectedBacklog checks that a bulk
// dismissal covers the selected operation only, and reports how many it
// dismissed.
func TestAPITaskBulkAcknowledgeDismissesTheSelectedBacklog(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	now := time.Now()
	fail := func(taskType model.TaskType, key string) *model.Task {
		t.Helper()
		row := fixture.enqueue(t, taskType, key, now, "storage_copy", key)
		fixture.transition(t, row.ID, repository.TaskTransition{
			Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
			FailureReason: new("store_not_started"), LastError: new("provider unavailable"),
		})
		return row
	}
	stored := fail(model.TaskTypeStorageStore, "bulk-store")
	evicted := fail(model.TaskTypeCacheEvict, "bulk-evict")

	rr := fixture.request(http.MethodPost, "/api/v1/tasks/acknowledge", strings.NewReader(`{"type":"storage_store"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var body struct {
		Acknowledged int `json:"acknowledged"`
	}
	decodeJSON(t, rr, &body)
	if body.Acknowledged != 1 {
		t.Fatalf("acknowledged = %d, want 1", body.Acknowledged)
	}
	dismissed, err := fixture.repos.Tasks.GetByID(t.Context(), stored.ID)
	if err != nil || dismissed == nil || dismissed.AcknowledgedAt == nil {
		t.Fatalf("dismissed task = %#v, err=%v", dismissed, err)
	}
	untouched, err := fixture.repos.Tasks.GetByID(t.Context(), evicted.ID)
	if err != nil || untouched == nil || untouched.AcknowledgedAt != nil {
		t.Fatalf("task of another operation = %#v, err=%v", untouched, err)
	}

	rr = fixture.request(http.MethodPost, "/api/v1/tasks/acknowledge", strings.NewReader(`{"type":"not-an-operation"}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unknown type status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	rr = fixture.request(http.MethodPost, "/api/v1/tasks/acknowledge", strings.NewReader(`{"failed_before":"yesterday"}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid cutoff status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

// The number an operator confirms has to be the number that is dismissed. The
// preview counts and reports the cutoff it counted at; confirming with that
// cutoff leaves anything that failed in between visible.
func TestAPITaskBulkAcknowledgePreviewFreezesWhatIsDismissed(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	now := time.Now()
	fail := func(taskType model.TaskType, key string) *model.Task {
		t.Helper()
		row := fixture.enqueue(t, taskType, key, now, "storage_copy", key)
		fixture.transition(t, row.ID, repository.TaskTransition{
			Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
			FailureReason: new("store_not_started"), LastError: new("provider unavailable"),
		})
		return row
	}
	seen := fail(model.TaskTypeStorageStore, "preview-seen")
	fail(model.TaskTypeCacheEvict, "preview-other-operation")

	rr := fixture.request(http.MethodGet, "/api/v1/tasks/acknowledge/preview?type=storage_store", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var preview struct {
		Count int    `json:"count"`
		AsOf  string `json:"as_of"`
	}
	decodeJSON(t, rr, &preview)
	if preview.Count != 1 || preview.AsOf == "" {
		t.Fatalf("preview = %#v, want one dismissable failure and a cutoff", preview)
	}

	// A failure recorded after the operator looked must survive the dismissal.
	later := fail(model.TaskTypeStorageStore, "preview-unseen")
	asOf, err := time.Parse(time.RFC3339Nano, preview.AsOf)
	if err != nil {
		t.Fatalf("parse preview cutoff: %v", err)
	}
	if _, err := fixture.db.NewUpdate().Model((*model.Task)(nil)).
		Set("finished_at = ?", asOf.Add(time.Minute)).Where("id = ?", later.ID).Exec(t.Context()); err != nil {
		t.Fatalf("record a later failure: %v", err)
	}

	rr = fixture.request(http.MethodPost, "/api/v1/tasks/acknowledge",
		strings.NewReader(`{"type":"storage_store","failed_before":"`+preview.AsOf+`"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var body struct {
		Acknowledged int `json:"acknowledged"`
	}
	decodeJSON(t, rr, &body)
	if body.Acknowledged != preview.Count {
		t.Fatalf("acknowledged = %d, want the previewed %d", body.Acknowledged, preview.Count)
	}
	dismissed, err := fixture.repos.Tasks.GetByID(t.Context(), seen.ID)
	if err != nil || dismissed == nil || dismissed.AcknowledgedAt == nil {
		t.Fatalf("previewed failure = %#v, err=%v, want it dismissed", dismissed, err)
	}
	kept, err := fixture.repos.Tasks.GetByID(t.Context(), later.ID)
	if err != nil || kept == nil || kept.AcknowledgedAt != nil {
		t.Fatalf("failure recorded after the preview = %#v, err=%v, want it still visible", kept, err)
	}

	rr = fixture.request(http.MethodGet, "/api/v1/tasks/acknowledge/preview?type=not-an-operation", nil)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unknown preview type status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

// A body that does not decode cleanly is refused rather than partially applied:
// a mistyped field would otherwise dismiss every operation instead of the one
// the operator selected.
func TestAPITaskBulkAcknowledgeRefusesUnusableRequests(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	now := time.Now()
	row := fixture.enqueue(t, model.TaskTypeStorageStore, "strict-decode", now, "storage_copy", "strict-decode")
	fixture.transition(t, row.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("store_not_started"), LastError: new("provider unavailable"),
	})

	bodies := map[string]string{
		"misspelled field": `{"typ":"storage_store"}`,
		"wrong value type": `{"type":123}`,
		"trailing content": `{"type":"storage_store"}{"type":"cache_evict"}`,
		"oversized body":   `{"type":"` + strings.Repeat("x", taskAcknowledgeMaxBodyBytes) + `"}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			rr := fixture.request(http.MethodPost, "/api/v1/tasks/acknowledge", strings.NewReader(body))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
		})
	}
	kept, err := fixture.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || kept == nil || kept.AcknowledgedAt != nil {
		t.Fatalf("failure = %#v, err=%v, want it untouched by every refused request", kept, err)
	}
}

// Scripts and proxies may stream a request without declaring its length. An
// empty body still selects every failure recorded before now.
func TestAPITaskBulkAcknowledgeAcceptsAnEmptyBodyOfUnknownLength(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	row := fixture.enqueue(t, model.TaskTypeStorageStore, "streamed-empty", time.Now(), "storage_copy", "streamed-empty")
	fixture.transition(t, row.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("store_not_started"), LastError: new("provider unavailable"),
	})

	// httptest reports a reader it cannot measure as ContentLength -1, which is
	// how a chunked request arrives.
	rr := fixture.request(http.MethodPost, "/api/v1/tasks/acknowledge", io.MultiReader())
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var body struct {
		Acknowledged int `json:"acknowledged"`
	}
	decodeJSON(t, rr, &body)
	if body.Acknowledged != 1 {
		t.Fatalf("acknowledged = %d, want 1", body.Acknowledged)
	}
}

func newAdminTaskFixture(t *testing.T) *adminTaskFixture {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	service := newAdminTestTaskService(t, repos)
	server := newTestServer(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).WithTaskService(service)
	return &adminTaskFixture{t: t, db: db, repos: repos, service: service, server: server}
}

func (f *adminTaskFixture) enqueue(t *testing.T, taskType model.TaskType, key string, availableAt time.Time, subjectType, subjectKey string) *model.Task {
	t.Helper()
	row, _, err := f.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: taskType, IdempotencyKey: key, Input: map[string]any{"key": key},
		AvailableAt: availableAt, SubjectType: subjectType, SubjectKey: subjectKey,
	})
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", key, err)
	}
	return row
}

func (f *adminTaskFixture) transition(t *testing.T, id int64, transition repository.TaskTransition) {
	t.Helper()
	claimed, err := f.repos.Tasks.ClaimNext(t.Context(), time.Minute, repository.TaskClaimFilter{})
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimed == nil || claimed.ID != id {
		t.Fatalf("claimed = %#v, want task %d", claimed, id)
	}
	if err := f.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, transition); err != nil {
		t.Fatalf("Settle(%d): %v", id, err)
	}
}

func (f *adminTaskFixture) request(method, path string, body io.Reader) *httptest.ResponseRecorder {
	f.t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", f.server.handleAPITasks)
	mux.HandleFunc("GET /api/v1/tasks/{id}", f.server.handleAPIGetTask)
	mux.HandleFunc("GET /api/v1/tasks/{id}/history", f.server.handleAPITaskHistory)
	mux.HandleFunc("GET /api/v1/tasks/{id}/events", f.server.handleAPITaskEvents)
	mux.HandleFunc("GET /api/v1/task-subjects/{subject_type}/{subject_key}", f.server.handleAPITaskSubject)
	mux.HandleFunc("GET /api/v1/tasks/stats", f.server.handleAPITaskStats)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", f.server.handleAPITaskRetry)
	mux.HandleFunc("POST /api/v1/tasks/{id}/acknowledge", f.server.handleAPITaskAcknowledge)
	mux.HandleFunc("GET /api/v1/tasks/acknowledge/preview", f.server.handleAPITaskAcknowledgePreview)
	mux.HandleFunc("POST /api/v1/tasks/acknowledge", f.server.handleAPITaskAcknowledgeMatching)
	if body == nil {
		body = strings.NewReader("")
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(method, path, body))
	return rr
}

func decodeJSON(t *testing.T, rr *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.NewDecoder(rr.Body).Decode(target); err != nil {
		t.Fatalf("Decode: %v; body=%s", err, rr.Body.String())
	}
}

func TestAPITaskDetailsAndHistoryPreserveExecutions(t *testing.T) {
	f := newAdminTaskFixture(t)
	first := f.enqueue(t, model.TaskTypeUploadPlan, "task-history", time.Now(), "bucket", "1")
	f.transition(t, first.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, LastError: new("first failure")})
	second, err := f.service.Retry(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.transition(t, second.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, LastError: new("second failure")})
	third, err := f.service.Retry(t.Context(), second.ID)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/tasks/" + strconv.FormatInt(first.ID, 10)
	rr := f.request(http.MethodGet, path, nil)
	var detail struct {
		Task   taskListItem    `json:"task"`
		Policy *taskPolicyView `json:"policy"`
	}
	decodeJSON(t, rr, &detail)
	if rr.Code != http.StatusOK || detail.Task.Status != "failed" || (detail.Task.LastError == nil || *detail.Task.LastError != "first failure") || detail.Policy == nil || detail.Policy.MaxAttempts == nil || *detail.Policy.MaxAttempts != 6 {
		t.Fatalf("original execution details: %d %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "checkpoint") || strings.Contains(rr.Body.String(), "input") {
		t.Fatalf("details exposed execution payload: %s", rr.Body.String())
	}
	var cursor *int64
	for _, want := range []int64{third.ID, second.ID, first.ID} {
		url := path + "/history?limit=1"
		if cursor != nil {
			url += "&cursor=" + strconv.FormatInt(*cursor, 10)
		}
		rr = f.request(http.MethodGet, url, nil)
		var page taskListResponse
		decodeJSON(t, rr, &page)
		if rr.Code != http.StatusOK || len(page.Tasks) != 1 || page.Tasks[0].ID != want {
			t.Fatalf("history page: %d %s, want %d", rr.Code, rr.Body.String(), want)
		}
		cursor = page.NextCursor
	}
	if cursor != nil {
		t.Fatalf("last history page retained cursor: %d", *cursor)
	}
}

func TestAPITaskEventsExposeOnlyDiagnosticFields(t *testing.T) {
	f := newAdminTaskFixture(t)
	row := f.enqueue(t, model.TaskTypeUploadPlan, "task-events", time.Now(), "bucket", "1")
	for _, details := range []string{`{"attempt":1,"operation_key":"private-effect","checkpoint":{"secret":"private-payload"}}`, `{"next_attempt":2,"failure_reason":"attempts_exhausted"}`} {
		if err := f.repos.Tasks.AppendEvent(t.Context(), row.ID, "retry", json.RawMessage(details)); err != nil {
			t.Fatal(err)
		}
	}
	path := "/api/v1/tasks/" + strconv.FormatInt(row.ID, 10) + "/events?limit=1"
	rr := f.request(http.MethodGet, path, nil)
	var page struct {
		Events     []taskEventView `json:"events"`
		NextCursor *int64          `json:"next_cursor"`
	}
	decodeJSON(t, rr, &page)
	if rr.Code != http.StatusOK || len(page.Events) != 1 || page.Events[0].NextAttempt == nil || *page.Events[0].NextAttempt != 2 || page.NextCursor == nil {
		t.Fatalf("event page: %d %s", rr.Code, rr.Body.String())
	}
	rr = f.request(http.MethodGet, path+"&cursor="+strconv.FormatInt(*page.NextCursor, 10), nil)
	decodeJSON(t, rr, &page)
	if rr.Code != http.StatusOK || len(page.Events) != 1 || page.Events[0].Attempt == nil || *page.Events[0].Attempt != 1 || strings.Contains(rr.Body.String(), "private-") || strings.Contains(rr.Body.String(), "checkpoint") {
		t.Fatalf("unsafe event page: %d %s", rr.Code, rr.Body.String())
	}
}

func TestAPITaskLegacyPolicyIsReadableAndRetryRequiresEmptyBody(t *testing.T) {
	f := newAdminTaskFixture(t)
	row := f.enqueue(t, model.TaskTypeUploadPlan, "legacy-detail", time.Now(), "bucket", "1")
	f.transition(t, row.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
	if _, err := f.db.NewRaw("UPDATE tasks SET policy_json = ? WHERE id = ?", `{"version":0,"legacy":true,"max_attempts":6,"backoff":{"initial_delay":10000000000,"multiplier":2,"maximum_delay":300000000000,"jitter":0.2}}`, row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/tasks/" + strconv.FormatInt(row.ID, 10)
	rr := f.request(http.MethodGet, path, nil)
	var detail struct {
		Policy *taskPolicyView `json:"policy"`
	}
	decodeJSON(t, rr, &detail)
	if rr.Code != http.StatusOK || detail.Policy == nil || !detail.Policy.Legacy || detail.Policy.MaxAttempts == nil || *detail.Policy.MaxAttempts != 6 {
		t.Fatalf("legacy policy unreadable: %d %s", rr.Code, rr.Body.String())
	}
	rr = f.request(http.MethodPost, path+"/retry", strings.NewReader(`{}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("retry accepted body: %d %s", rr.Code, rr.Body.String())
	}
	child, err := f.repos.Tasks.GetDirectSuccessor(t.Context(), row.ID)
	if err != nil || child != nil {
		t.Fatalf("invalid request created a successor: %#v %v", child, err)
	}
}

func TestAPITaskScopesKeepAllTerminalRoundsAndSeparateWork(t *testing.T) {
	f := newAdminTaskFixture(t)
	completed := f.enqueue(t, model.TaskTypeObservabilityRefresh, "completed-periodic", time.Now(), "system", "observability")
	f.transition(t, completed.ID, repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute})
	cancelled := f.enqueue(t, model.TaskTypeUploadPlan, "cancelled-plan", time.Now(), "bucket", "1")
	f.transition(t, cancelled.ID, repository.TaskTransition{Status: model.TaskStatusCancelled, ResumeMode: model.TaskResumeModeExecute})
	current := f.enqueue(t, model.TaskTypeUploadPlan, "current-plan", time.Now().Add(time.Hour), "bucket", "1")
	var page taskListResponse
	rr := f.request(http.MethodGet, "/api/v1/tasks", nil)
	decodeJSON(t, rr, &page)
	if rr.Code != http.StatusOK || len(page.Tasks) != 1 || page.Tasks[0].ID != current.ID {
		t.Fatalf("work = %d %s", rr.Code, rr.Body.String())
	}
	rr = f.request(http.MethodGet, "/api/v1/tasks?scope=history&limit=1", nil)
	decodeJSON(t, rr, &page)
	if rr.Code != http.StatusOK || len(page.Tasks) != 1 || page.Tasks[0].ID != cancelled.ID || page.NextCursor == nil {
		t.Fatalf("history first page = %d %s", rr.Code, rr.Body.String())
	}
	rr = f.request(http.MethodGet, "/api/v1/tasks?scope=history&limit=1&cursor="+strconv.FormatInt(*page.NextCursor, 10), nil)
	page = taskListResponse{}
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 1 || page.Tasks[0].ID != completed.ID || page.NextCursor != nil || page.Tasks[0].MaxAttempts == nil || *page.Tasks[0].MaxAttempts != 6 {
		t.Fatalf("periodic history = %d %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "retry_limit") || strings.Contains(rr.Body.String(), "events_json") || strings.Contains(rr.Body.String(), "checkpoint") {
		t.Fatalf("list exposes internal payload: %s", rr.Body.String())
	}
}

func TestAPITaskRetryReplaysArchivedSuccessorAfterAcknowledgement(t *testing.T) {
	f := newAdminTaskFixture(t)
	first := f.enqueue(t, model.TaskTypeUploadPlan, "acknowledged-retry", time.Now(), "bucket", "1")
	f.transition(t, first.ID, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, LastError: new("failed")})
	if err := f.service.Acknowledge(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/tasks/" + strconv.FormatInt(first.ID, 10) + "/retry"
	rr := f.request(http.MethodPost, path, nil)
	var response struct {
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, rr, &response)
	if rr.Code != http.StatusAccepted || response.TaskID <= first.ID {
		t.Fatalf("retry = %d %s", rr.Code, rr.Body.String())
	}
	f.transition(t, response.TaskID, repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute})
	rr = f.request(http.MethodPost, path, nil)
	var replay struct {
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, rr, &replay)
	if rr.Code != http.StatusAccepted || replay.TaskID != response.TaskID {
		t.Fatalf("archived replay = %d %s", rr.Code, rr.Body.String())
	}
}
