package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
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
	subject, key := "storage_copy", fmt.Sprint(copyRow.ID)
	finished := time.Now()
	old, _, err := repos.Tasks.Enqueue(ctx, &model.Task{Type: model.TaskTypeStoragePull, IdempotencyKey: "old-pull", InputVersion: 1, Input: []byte(`{}`), InputHash: "old", SubjectType: &subject, SubjectKey: &key, Status: model.TaskStatusFailed, FinishedAt: &finished})
	if err != nil {
		t.Fatal(err)
	}
	mux := newBucketAPIMux(server)
	mux.HandleFunc("POST /api/v1/storage-copies/{id}/retry", server.handleAPIRetryStorageCopy)
	mux.HandleFunc("GET /api/v1/tasks", server.handleAPITasks)
	return copyRetryAPIFixture{server: server, copy: copyRow, version: version, oldTask: old, mux: mux}
}

func (f copyRetryAPIFixture) request(method, path string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	f.mux.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
	return rr
}

func TestAPIStorageCopyRetryAndPresentation(t *testing.T) {
	f := newCopyRetryAPIFixture(t)
	for _, dismissed := range []bool{false, true} {
		status := "failed"
		if dismissed {
			status = "dismissed"
			if err := f.server.taskService.Acknowledge(t.Context(), f.oldTask.ID); err != nil {
				t.Fatal(err)
			}
		}
		rr := f.request("GET", "/api/v1/tasks?status="+status)
		var page taskListResponse
		decodeJSON(t, rr, &page)
		if rr.Code != 200 || len(page.Tasks) != 1 || page.Tasks[0].CopyRetry == nil || !page.Tasks[0].CopyRetry.Available || page.Tasks[0].Retryable {
			t.Fatalf("tasks=%+v response=%s", page, rr.Body)
		}
	}
	rr := f.request("GET", "/api/v1/buckets/retry-api/objects/provenance?version_id="+f.version.VersionID)
	var provenance objectProvenanceResponse
	decodeJSON(t, rr, &provenance)
	if rr.Code != 200 || len(provenance.Copies) != 1 || provenance.Copies[0].CopyID != f.copy.ID || provenance.Copies[0].Retry == nil || !provenance.Copies[0].Retry.Available || provenance.Copies[0].LastError == nil {
		t.Fatalf("provenance=%+v response=%s", provenance, rr.Body)
	}
	path := fmt.Sprintf("/api/v1/storage-copies/%d/retry", f.copy.ID)
	rr = f.request("POST", path)
	var accepted struct {
		CopyID int64 `json:"copy_id"`
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, rr, &accepted)
	if rr.Code != 202 || accepted.CopyID != f.copy.ID || accepted.TaskID == f.oldTask.ID || accepted.TaskID == 0 {
		t.Fatalf("retry=%s status=%d", rr.Body, rr.Code)
	}
	row, err := f.server.repos.Contents.GetUploadCopyByID(t.Context(), f.copy.ID)
	if err != nil || row.ActiveTaskID == nil || *row.ActiveTaskID != accepted.TaskID || row.WorkGeneration != 1 {
		t.Fatalf("copy=%+v err=%v", row, err)
	}
	work, err := f.server.repos.Tasks.GetByID(t.Context(), accepted.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(work.Input, &input); err != nil {
		t.Fatal(err)
	}
	if work.Type != model.TaskTypeStorageTransferPlan || work.IdempotencyKey != storagepipeline.TransferPlanKey(f.copy.ID, row.WorkGeneration) ||
		work.SubjectType == nil || *work.SubjectType != "storage_copy" || work.SubjectKey == nil || *work.SubjectKey != fmt.Sprint(f.copy.ID) ||
		input.CopyID != f.copy.ID || input.Generation != row.WorkGeneration {
		t.Fatalf("recovery work=%+v input=%+v", work, input)
	}
	history, err := f.server.repos.Tasks.GetByID(t.Context(), f.oldTask.ID)
	if err != nil || history.AcknowledgedAt == nil || history.Status != model.TaskStatusFailed {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	if rr := f.request("POST", path); rr.Code != 409 {
		t.Fatalf("duplicate=%d %s", rr.Code, rr.Body)
	}
	rr = f.request("GET", "/api/v1/tasks?status=dismissed")
	var page taskListResponse
	decodeJSON(t, rr, &page)
	if len(page.Tasks) != 1 || page.Tasks[0].CopyRetry != nil {
		t.Fatalf("retry shown for pending copy: %+v", page)
	}
}

func TestAPIStorageCopyRetryAdmissionErrors(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		status     int
		change     func(*testing.T, copyRetryAPIFixture)
	}{
		{name: "no source", code: "no_source", status: 409, change: func(t *testing.T, f copyRetryAPIFixture) {
			if err := f.server.repos.Objects.SetVersionCachePresence(t.Context(), f.version.VersionID, false); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unavailable", code: "storage_service_unavailable", status: 409, change: func(t *testing.T, f copyRetryAPIFixture) {
			if _, err := f.server.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("is_current = ?", false).Set("status = ?", model.StorageDataSetStatusDraining).Where("id = ?", f.copy.StorageDataSetID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "deleted", code: "object_deleted", status: 409, change: func(t *testing.T, f copyRetryAPIFixture) {
			if _, err := f.server.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("cleanup_task_id = ?", f.oldTask.ID).Where("id = ?", f.copy.ContentID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "replacement", code: "replacement_in_progress", status: 409, change: func(t *testing.T, f copyRetryAPIFixture) {
			if _, _, err := f.server.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: f.copy.BucketID, SourceDataSetID: f.copy.StorageDataSetID, TargetProviderID: onChainID(t, "102"), ClientRequestID: "replace", SelectionMode: storagereplacement.SelectionModeManual, PriceListFingerprint: "prices"}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unresolved", code: "recovery_requires_attention", status: 409, change: func(t *testing.T, f copyRetryAPIFixture) {
			row := &storagepull.Attempt{AttemptID: "unresolved", ContentID: f.copy.ContentID, StorageDataSetID: f.copy.StorageDataSetID, Status: storagepull.AttemptStatusAttempted, SourceProviderID: onChainID(t, "102"), SourceDataSetID: onChainID(t, "202"), SourcePieceID: onChainID(t, "0"), SourcePieceCID: "piece", SourceRetrievalURL: "https://source.example", ExtraDataHex: "abcd", AttemptedAt: time.Now()}
			if _, err := f.server.db.NewInsert().Model(row).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "service missing", status: 503, change: func(_ *testing.T, f copyRetryAPIFixture) { f.server.taskService = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCopyRetryAPIFixture(t)
			tt.change(t, f)
			rr := f.request("POST", fmt.Sprintf("/api/v1/storage-copies/%d/retry", f.copy.ID))
			var body map[string]string
			decodeJSON(t, rr, &body)
			if rr.Code != tt.status || body["code"] != tt.code {
				t.Fatalf("response=%d %+v", rr.Code, body)
			}
		})
	}
	f := newCopyRetryAPIFixture(t)
	for _, tt := range []struct {
		id     string
		status int
	}{{"bad", 400}, {"0", 400}, {"99999", 404}} {
		if rr := f.request("POST", "/api/v1/storage-copies/"+tt.id+"/retry"); rr.Code != tt.status {
			t.Fatalf("id=%s status=%d", tt.id, rr.Code)
		}
	}
}

func TestAPIStorageCopyRetryRollsBackWhenEnqueueFails(t *testing.T) {
	f := newCopyRetryAPIFixture(t)
	// Existing identity is terminal, so it cannot be bound as new live work.
	finished := time.Now()
	retention := finished.Add(time.Hour)
	_, _, err := f.server.repos.Tasks.Enqueue(t.Context(), &model.Task{Type: model.TaskTypeStorageTransferPlan, IdempotencyKey: storagepipeline.TransferPlanKey(f.copy.ID, 1), InputVersion: 1, Input: []byte(`{}`), InputHash: "different", Status: model.TaskStatusCompleted, FinishedAt: &finished, RetentionUntil: &retention})
	if err != nil {
		t.Fatal(err)
	}
	rr := f.request("POST", fmt.Sprintf("/api/v1/storage-copies/%d/retry", f.copy.ID))
	if rr.Code == 202 {
		t.Fatal("accepted incompatible task identity")
	}
	copyRow, err := f.server.repos.Contents.GetUploadCopyByID(t.Context(), f.copy.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusFailed || copyRow.ActiveTaskID != nil {
		t.Fatalf("copy=%+v err=%v", copyRow, err)
	}
	history, err := f.server.repos.Tasks.GetByID(t.Context(), f.oldTask.ID)
	if err != nil || history.AcknowledgedAt != nil {
		t.Fatalf("history=%+v err=%v", history, err)
	}
}
