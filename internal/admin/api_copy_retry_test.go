package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
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
	for _, replacement := range []bool{false, true} {
		name, reason := "missing source", ""
		if replacement {
			name, reason = "replacement", "Provider replacement is in progress."
		}
		t.Run(name, func(t *testing.T) {
			f := newCopyRetryAPIFixture(t)
			if replacement {
				if err := f.startReplacement(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else if err := f.server.repos.Objects.SetVersionCachePresence(t.Context(), f.version.VersionID, false); err != nil {
				t.Fatal(err)
			}
			provenance := f.request(http.MethodGet, "/api/v1/buckets/retry-api/objects/provenance?version_id="+url.QueryEscape(f.version.VersionID))
			var detail objectProvenanceResponse
			decodeJSON(t, provenance, &detail)
			wantRetryable := !replacement
			if provenance.Code != http.StatusOK || len(detail.Copies) != 1 || detail.Copies[0].Retryable != wantRetryable || (detail.Copies[0].RetryTaskID != nil) != wantRetryable || detail.Copies[0].RetryUnavailableReason != reason {
				t.Fatalf("recovery admission = %d %#v", provenance.Code, detail.Copies)
			}
			rr := f.request(http.MethodPost, fmt.Sprintf("/api/v1/tasks/%d/retry", f.oldTask.ID))
			if !replacement {
				if rr.Code != http.StatusAccepted || *detail.Copies[0].RetryTaskID != f.oldTask.ID {
					t.Fatalf("source waiting recovery = %d %s", rr.Code, rr.Body.String())
				}
				next, err := f.server.repos.Tasks.GetDirectSuccessor(t.Context(), f.oldTask.ID)
				if err != nil || next == nil || next.Type != model.TaskTypeStorageTransferPlan || next.Status != model.TaskStatusPending || next.RetryCount != 0 {
					t.Fatalf("source waiting successor = %#v %v", next, err)
				}
				copyRow, err := f.server.repos.Contents.GetUploadCopyByID(t.Context(), f.copy.ID)
				if err != nil || copyRow == nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != next.ID || copyRow.WorkGeneration != 2 {
					t.Fatalf("source waiting ownership = %#v %v", copyRow, err)
				}
				old, err := f.server.repos.Tasks.GetByID(t.Context(), f.oldTask.ID)
				if err != nil || old.SupersededAt == nil || old.Status != model.TaskStatusFailed {
					t.Fatalf("accepted retry source = %#v %v", old, err)
				}
				return
			}
			if rr.Code != http.StatusConflict {
				t.Fatalf("blocked retry = %d %s", rr.Code, rr.Body.String())
			}
			old, err := f.server.repos.Tasks.GetByID(t.Context(), f.oldTask.ID)
			if err != nil || old.SupersededAt != nil {
				t.Fatalf("rejected retry changed source = %#v %v", old, err)
			}
		})
	}
}

func (f copyRetryAPIFixture) startReplacement(ctx context.Context) error {
	return f.server.repos.WithTx(ctx, func(repos *repository.Repositories) error {
		replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
			BucketID: f.version.BucketID, SourceDataSetID: f.copy.StorageDataSetID,
			SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainIDValue("202"), ClientRequestID: "copy-retry-blocker",
		})
		if err != nil {
			return err
		}
		row, _, err := f.server.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
			Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: storagereplacement.CoordinateTaskKey(replacement.ID, replacement.TaskGeneration),
			Input: storagereplacement.CoordinateInput{ReplacementID: replacement.ID, Generation: replacement.TaskGeneration},
		})
		if err != nil {
			return err
		}
		return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, row.ID)
	})
}

type copyRetryFactsRepository struct {
	repository.StorageContentRepository
	read func(context.Context, []int64) (map[int64]repository.CopyRetryState, error)
}

func (r *copyRetryFactsRepository) CopyRetryStates(ctx context.Context, ids []int64) (map[int64]repository.CopyRetryState, error) {
	return r.read(ctx, ids)
}

func TestAPIStorageCopyRetryRechecksReplacementInsideTransaction(t *testing.T) {
	f := newCopyRetryAPIFixture(t)
	contents := f.server.repos.Contents
	f.server.repos.Contents = &copyRetryFactsRepository{StorageContentRepository: contents, read: func(ctx context.Context, ids []int64) (map[int64]repository.CopyRetryState, error) {
		states, err := contents.CopyRetryStates(ctx, ids)
		if err != nil {
			return nil, err
		}
		if !states[f.copy.ID].Available {
			t.Fatal("retry was blocked before replacement started")
		}
		return states, f.startReplacement(ctx)
	}}
	rr := f.request(http.MethodPost, fmt.Sprintf("/api/v1/tasks/%d/retry", f.oldTask.ID))
	if rr.Code != http.StatusConflict {
		t.Fatalf("replacement admitted after inspection = %d %s", rr.Code, rr.Body.String())
	}
	old, err := f.server.repos.Tasks.GetByID(t.Context(), f.oldTask.ID)
	if err != nil || old.SupersededAt != nil {
		t.Fatalf("rejected retry changed source = %#v %v", old, err)
	}
	if next, err := f.server.repos.Tasks.GetDirectSuccessor(t.Context(), f.oldTask.ID); err != nil || next != nil {
		t.Fatalf("rejected retry created a successor = %#v %v", next, err)
	}
}

func TestAPIStorageCopyRetryPropagatesAdmissionErrors(t *testing.T) {
	f := newCopyRetryAPIFixture(t)
	failure := errors.New("copy retry facts unavailable")
	f.server.repos.Contents = &copyRetryFactsRepository{StorageContentRepository: f.server.repos.Contents, read: func(context.Context, []int64) (map[int64]repository.CopyRetryState, error) {
		return nil, failure
	}}
	id, reason, err := f.server.retryTaskForSubject(t.Context(), model.TaskSubjectStorageCopy, fmt.Sprint(f.copy.ID), model.TaskTypeStorageStore)
	if !errors.Is(err, failure) || id != nil || reason != "" {
		t.Fatalf("admission error was hidden = id=%v reason=%q error=%v", id, reason, err)
	}
	for _, request := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/buckets/retry-api/objects/provenance?version_id=" + url.QueryEscape(f.version.VersionID)},
		{http.MethodPost, fmt.Sprintf("/api/v1/tasks/%d/retry", f.oldTask.ID)},
	} {
		rr := f.request(request.method, request.path)
		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("admission error returned %d: %s", rr.Code, rr.Body.String())
		}
	}
}
