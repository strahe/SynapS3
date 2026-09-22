package admin

import (
	"context"
	"encoding/json"
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
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
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

func newAdminTestTaskService(t *testing.T, repos *repository.Repositories) *taskengine.Service {
	t.Helper()

	registry := taskengine.NewRegistry()
	limit := 5
	for _, taskType := range []model.TaskType{
		model.TaskTypeBucketProvision,
		model.TaskTypeUploadPlan,
		model.TaskTypeStorageDataSetEnsure,
		model.TaskTypeStorageTransferPlan,
		model.TaskTypeStorageStore,
		model.TaskTypeStoragePull,
		model.TaskTypeStorageCommitCoordinate,
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
			InputVersion: 1,
			Codec:        taskengine.StrictJSONCodec[map[string]any](nil),
			RetryLimit:   &limit,
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
		if err := registry.Register(adminTaskHandler{definition: definition}); err != nil {
			t.Fatalf("Register(%s): %v", definition.Type, err)
		}
	}
	service, err := taskengine.NewService(registry, repos, 7*24*time.Hour)
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

func TestAPITasksRejectsRemovedAndInvalidFilters(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	for _, path := range []string{
		"/api/v1/tasks?category=upload",
		"/api/v1/tasks?stage=store",
		"/api/v1/tasks?offset=10",
		"/api/v1/tasks?type=future_task_type",
		"/api/v1/tasks?status=waiting",
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
	if err := fixture.repos.Tasks.AcknowledgeFailed(t.Context(), second.ID, time.Hour); err != nil {
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
		counts[[2]string{string(model.TaskTypeUploadPlan), string(model.TaskStatusFailed)}] != 1 ||
		counts[[2]string{string(model.TaskTypeUploadPlan), "dismissed"}] != 1 {
		t.Fatalf("counts = %#v", counts)
	}
}

func TestAPITasksSeparatesFailedAndDismissedFilters(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	failed := fixture.enqueue(t, model.TaskTypeUploadPlan, "visible-failure", time.Now(), "", "")
	dismissed := fixture.enqueue(t, model.TaskTypeUploadPlan, "dismissed-failure", time.Now(), "", "")
	for _, taskRow := range []*model.Task{failed, dismissed} {
		fixture.transition(t, taskRow.ID, repository.TaskTransition{
			Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
			FailureReason: new("provider_error"), LastError: new("provider unavailable"),
		})
	}
	if err := fixture.repos.Tasks.AcknowledgeFailed(t.Context(), dismissed.ID, time.Hour); err != nil {
		t.Fatalf("AcknowledgeFailed: %v", err)
	}

	for _, tt := range []struct {
		status       string
		wantID       int64
		presentation string
	}{
		{status: "failed", wantID: failed.ID, presentation: "failed"},
		{status: "dismissed", wantID: dismissed.ID, presentation: "dismissed"},
	} {
		rr := fixture.request(http.MethodGet, "/api/v1/tasks?status="+tt.status, nil)
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%s response = %d %s", tt.status, rr.Code, rr.Body.String())
		}
		var page taskListResponse
		decodeJSON(t, rr, &page)
		if len(page.Tasks) != 1 || page.Tasks[0].ID != tt.wantID || page.Tasks[0].Status != string(model.TaskStatusFailed) || page.Tasks[0].Presentation != tt.presentation {
			t.Fatalf("status=%s page = %#v", tt.status, page)
		}
	}
}

func TestAPITasksHideHealthyRecurringSystemWorkButKeepFailures(t *testing.T) {
	fixture := newAdminTaskFixture(t)
	healthy := fixture.enqueue(t, model.TaskTypeCacheCapacityReconcile, "healthy-system", time.Now().Add(time.Hour), "system", "cache-capacity")
	failed := fixture.enqueue(t, model.TaskTypeObservabilityRefresh, "failed-system", time.Now(), "system", "observability")
	fixture.transition(t, failed.ID, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("refresh_failed"), LastError: new("health refresh failed"),
	})

	rr := fixture.request(http.MethodGet, "/api/v1/tasks", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var page taskListResponse
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 1 || page.Tasks[0].ID != failed.ID {
		t.Fatalf("visible tasks = %#v; healthy task %d should be hidden", page.Tasks, healthy.ID)
	}

	rr = fixture.request(http.MethodGet, "/api/v1/tasks/stats", nil)
	var stats []taskStatsItem
	decodeJSON(t, rr, &stats)
	if len(stats) != 1 || stats[0].Type != string(model.TaskTypeObservabilityRefresh) || stats[0].Status != string(model.TaskStatusFailed) {
		t.Fatalf("visible task stats = %#v", stats)
	}
	if err := fixture.repos.Tasks.AcknowledgeFailed(t.Context(), failed.ID, time.Hour); err != nil {
		t.Fatalf("acknowledge recurring failure: %v", err)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks?status=dismissed", nil)
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 1 || page.Tasks[0].ID != failed.ID || page.Tasks[0].Presentation != "dismissed" {
		t.Fatalf("dismissed recurring tasks = %#v", page.Tasks)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks/stats", nil)
	decodeJSON(t, rr, &stats)
	if len(stats) != 1 || stats[0].Type != string(model.TaskTypeObservabilityRefresh) || stats[0].Status != "dismissed" {
		t.Fatalf("dismissed recurring task stats = %#v", stats)
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
	if rr.Code != http.StatusOK {
		t.Fatalf("retry status = %d body=%s", rr.Code, rr.Body.String())
	}
	got, err := fixture.repos.Tasks.GetByID(t.Context(), retryable.ID)
	if err != nil || got == nil || got.Status != model.TaskStatusPending || got.ResumeMode != model.TaskResumeModeRecover || got.RetryCount != 0 {
		t.Fatalf("retried task = %#v err=%v", got, err)
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
	if err != nil || got == nil || got.AcknowledgedAt == nil || got.RetentionUntil == nil {
		t.Fatalf("acknowledged task = %#v err=%v", got, err)
	}
	rr = fixture.request(http.MethodGet, "/api/v1/tasks?status=failed", nil)
	var body taskListResponse
	decodeJSON(t, rr, &body)
	for _, item := range body.Tasks {
		if item.ID == nonRetryable.ID && item.Presentation != "dismissed" {
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
	if err != nil || dismissed == nil || dismissed.AcknowledgedAt == nil || dismissed.RetentionUntil == nil {
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
	claimed, err := f.repos.Tasks.ClaimNext(t.Context(), time.Minute)
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
