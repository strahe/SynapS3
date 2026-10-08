package admin

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type copyRetryAPIFixture struct {
	server  *Server
	copy    *model.StorageCopy
	version *model.ObjectVersion
	oldTask *model.Task
	mux     *http.ServeMux
}

func newCopyRetryAPIFixture(t *testing.T) copyRetryAPIFixture {
	t.Helper()
	server, repos := newBucketAPITestServer(t)
	ctx := t.Context()
	bucket := &model.Bucket{Name: "retry-api", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{BucketID: bucket.ID, ContentSize: 10, Checksum: testutil.StorageChecksum("retry-api"), RequestedCopies: 1})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "retry.bin", ContentID: &content.ID, Size: 10, ETag: "retry", ContentType: "application/octet-stream"}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatal(err)
	}
	if err := repos.Objects.SetVersionCachePresence(ctx, version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	dataSet, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: dataSet.ID, DataSetID: onChainID(t, "201")}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{StorageDataSetID: dataSet.ID, CopyIndex: 0, ProviderID: dataSet.ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress}}); err != nil {
		t.Fatal(err)
	}
	copyRow, err := repos.Contents.GetUploadCopy(ctx, content.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{StorageCopyID: copyRow.ID, ContentID: content.ID, CopyIndex: 0, LastError: "transfer failed"}); err != nil {
		t.Fatal(err)
	}
	old, _, err := server.taskService.Enqueue(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeStorageStore, IdempotencyKey: storagepipeline.StoreKey(copyRow.ID, 1), Input: storagepipeline.CopyGenerationInput{CopyID: copyRow.ID, Generation: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("work_generation = ?", 1).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := server.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Set("retry_count = ?", 5).Set("failure_reason = ?", "store_failed").Where("id = ?", old.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	old, err = repos.Tasks.GetByID(ctx, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	mux := newBucketAPIMux(server)
	mux.HandleFunc("GET /api/v1/tasks", server.handleAPITasks)
	return copyRetryAPIFixture{server: server, copy: copyRow, version: version, oldTask: old, mux: mux}
}

func (f copyRetryAPIFixture) request(method, path string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	f.mux.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
	return rr
}

func TestAPIStorageCopyUsesTaskRetryAndPreservesHistory(t *testing.T) {
	f := newCopyRetryAPIFixture(t)
	rr := f.request(http.MethodGet, "/api/v1/tasks?status=failed")
	var page taskListResponse
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 1 || !page.Tasks[0].Retryable || page.Tasks[0].RetryTaskID == nil || *page.Tasks[0].RetryTaskID != f.oldTask.ID {
		t.Fatalf("source action = %#v", page.Tasks)
	}
	path := fmt.Sprintf("/api/v1/tasks/%d/retry", f.oldTask.ID)
	rr = f.request(http.MethodPost, path)
	var result struct {
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, rr, &result)
	if rr.Code != 202 || result.TaskID == f.oldTask.ID || result.TaskID == 0 {
		t.Fatalf("retry = %d %s", rr.Code, rr.Body.String())
	}
	next, err := f.server.repos.Tasks.GetByID(t.Context(), result.TaskID)
	if err != nil || next == nil || next.Type != model.TaskTypeStorageTransferPlan || next.RetryCount != 0 || next.RetryOfTaskID == nil || *next.RetryOfTaskID != f.oldTask.ID {
		t.Fatalf("new round = %#v %v", next, err)
	}
	old, err := f.server.repos.Tasks.GetByID(t.Context(), f.oldTask.ID)
	if err != nil || old.Status != model.TaskStatusFailed || old.RetryCount != 5 || old.AcknowledgedAt != nil || old.SupersededAt == nil {
		t.Fatalf("old = %#v %v", old, err)
	}
	rr = f.request(http.MethodPost, path)
	var repeat struct {
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, rr, &repeat)
	if rr.Code != 202 || repeat.TaskID != result.TaskID {
		t.Fatalf("replay = %d %#v", rr.Code, repeat)
	}
	if rr = f.request(http.MethodPost, fmt.Sprintf("/api/v1/storage-copies/%d/retry", f.copy.ID)); rr.Code != 404 {
		t.Fatalf("removed route = %d", rr.Code)
	}
}

func TestAPIStorageCopyTaskRetryRechecksAdmission(t *testing.T) {
	f := newCopyRetryAPIFixture(t)
	if err := f.server.repos.Objects.SetVersionCachePresence(t.Context(), f.version.VersionID, false); err != nil {
		t.Fatal(err)
	}
	rr := f.request(http.MethodPost, fmt.Sprintf("/api/v1/tasks/%d/retry", f.oldTask.ID))
	if rr.Code != 409 {
		t.Fatalf("missing source = %d %s", rr.Code, rr.Body.String())
	}
	old, err := f.server.repos.Tasks.GetByID(t.Context(), f.oldTask.ID)
	if err != nil || old.SupersededAt != nil {
		t.Fatalf("rejected retry changed source = %#v %v", old, err)
	}
}
