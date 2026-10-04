package admin

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
)

func TestAPICommitBatchesPagingAndSealIntent(t *testing.T) {
	f := newAdminTaskFixture(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/commit-batches", f.server.handleAPIListCommitBatches)
	mux.HandleFunc("GET /api/v1/commit-batches/{id}", f.server.handleAPIGetCommitBatch)
	mux.HandleFunc("POST /api/v1/commit-batches/{id}/seal", f.server.handleAPISealCommitBatch)
	request := func(method, path string, body io.Reader) *httptest.ResponseRecorder {
		t.Helper()
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(method, path, body))
		return rr
	}
	created := time.Now().UTC().Truncate(time.Second)
	var copyRow *model.StorageCopy
	var owner int64
	for i, id := range []string{"a", "b", "c"} {
		bucket := &model.Bucket{Name: "batch-" + id, Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
		if err := f.repos.Buckets.Create(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
		content, err := f.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{BucketID: bucket.ID, ContentSize: 10, Checksum: testutil.StorageChecksum(id), RequestedCopies: 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.repos.Objects.CreateVersionAndSetCurrent(t.Context(), &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: id, ContentID: &content.ID, Size: 10, ETag: id, ContentType: "application/octet-stream"}); err != nil {
			t.Fatal(err)
		}
		provider, err := idtypes.ParseOnChainID("provider", "701")
		if err != nil {
			t.Fatal(err)
		}
		dataSet, err := f.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: provider, CreatedByContentID: content.ID})
		if err != nil {
			t.Fatal(err)
		}
		chainID, err := idtypes.ParseOnChainID("data set", fmt.Sprint(801+i))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: dataSet.ID, DataSetID: chainID}); err != nil {
			t.Fatal(err)
		}
		if err := f.repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{StorageDataSetID: dataSet.ID, ProviderID: provider, TransferMethod: model.StorageCopyTransferMethodIngress}}); err != nil {
			t.Fatal(err)
		}
		copyRow, err = f.repos.Contents.GetUploadCopy(t.Context(), content.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.repos.Contents.MarkUploadCopyPieceReady(t.Context(), repository.MarkUploadCopyPieceReadyInput{StorageCopyID: copyRow.ID, ContentID: content.ID, PieceCID: "piece-" + id, RequireEligibleCopy: true}); err != nil {
			t.Fatal(err)
		}
		task := f.enqueue(t, model.TaskTypeStorageCommit, "batch-"+id, created.Add(time.Hour), model.TaskSubjectStorageCommitRequest, id)
		owner = task.ID
		if err := f.repos.Contents.CreateCollectingCommitRequest(t.Context(), repository.CreateCommitRequestInput{RequestID: id, TaskID: task.ID, StorageDataSetID: dataSet.ID, CopyIDs: []int64{copyRow.ID}, Now: created}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.repos.Objects.CreateVersionAndSetCurrent(t.Context(), &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: copyRow.BucketID, Key: "c-alias", ContentID: &copyRow.ContentID, Size: 10, ETag: "c", ContentType: "application/octet-stream", CreatedAt: created.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	var page struct {
		Batches    []commitBatchResponse `json:"batches"`
		NextCursor string                `json:"next_cursor"`
	}
	rr := request(http.MethodGet, "/api/v1/commit-batches?status=collecting&limit=2", nil)
	decodeJSON(t, rr, &page)
	if rr.Code != 200 || len(page.Batches) != 2 || page.Batches[0].RequestID != "c" || !page.Batches[0].CanSeal || page.NextCursor == "" {
		t.Fatalf("page = %#v (%d)", page, rr.Code)
	}
	rr = request(http.MethodGet, "/api/v1/commit-batches?limit=2&cursor="+page.NextCursor, nil)
	decodeJSON(t, rr, &page)
	if rr.Code != 200 || len(page.Batches) != 1 || page.Batches[0].RequestID != "a" {
		t.Fatalf("next page = %#v (%d)", page, rr.Code)
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=oops", "status=unknown", "cursor=bad"} {
		if rr := request(http.MethodGet, "/api/v1/commit-batches?"+query, nil); rr.Code != 400 {
			t.Fatalf("%s = %d", query, rr.Code)
		}
	}
	rr = request(http.MethodGet, "/api/v1/commit-batches/c", nil)
	var detail commitBatchDetailResponse
	decodeJSON(t, rr, &detail)
	if rr.Code != 200 || len(detail.Members) != 1 || detail.Members[0].ContentID != copyRow.ContentID || detail.Members[0].PieceCID != "piece-c" || detail.Members[0].Size == nil || *detail.Members[0].Size != 10 || detail.Members[0].File == nil || detail.Members[0].File.Key != "c-alias" || detail.Members[0].File.Source != "current" || detail.Members[0].File.OtherVersions != 1 {
		t.Fatalf("member detail = %#v (%d)", detail, rr.Code)
	}
	for range 2 {
		rr := request(http.MethodPost, "/api/v1/commit-batches/c/seal", nil)
		var batch commitBatchResponse
		decodeJSON(t, rr, &batch)
		if rr.Code != 202 || batch.SealRequestedAt == nil || batch.CanSeal || batch.Task == nil || batch.Status != storagecommit.RequestStatusCollecting {
			t.Fatalf("intent = %#v (%d): %s", batch, rr.Code, rr.Body.String())
		}
	}
	if _, err := f.repos.Contents.SealCommitRequest(t.Context(), repository.SealCommitRequestInput{RequestID: "c", TaskID: owner, ExtraDataHex: "abcd", Members: []repository.SealMember{{CopyID: copyRow.ID, ContentID: copyRow.ContentID, PieceCID: "piece-c"}}}); err != nil {
		t.Fatal(err)
	}
	if rr := request(http.MethodPost, "/api/v1/commit-batches/c/seal", nil); rr.Code != 200 {
		t.Fatalf("sealed replay = %d: %s", rr.Code, rr.Body.String())
	}
	if _, err := f.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("piece_cid = ?", "changed-live-cid").Where("id = ?", copyRow.ContentID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	rr = request(http.MethodGet, "/api/v1/commit-batches/c", nil)
	decodeJSON(t, rr, &detail)
	if rr.Code != 200 || len(detail.Members) != 1 || detail.Members[0].PieceCID != "piece-c" || detail.SealedAt == nil {
		t.Fatalf("signed member detail = %#v (%d)", detail, rr.Code)
	}
	if rr := request(http.MethodPost, "/api/v1/commit-batches/c/seal", strings.NewReader("{}")); rr.Code != 400 {
		t.Fatal(rr.Code)
	}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		path := "/api/v1/commit-batches/absent"
		if method == http.MethodPost {
			path += "/seal"
		}
		if rr := request(method, path, nil); rr.Code != 404 {
			t.Fatalf("%s = %d", method, rr.Code)
		}
	}
	// Qualification and HTTP conflict come from the same stopped task state.
	row, err := f.repos.Contents.GetCommitRequest(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Set("failure_reason = ?", "handler_panic").Set("last_error = ?", "stopped").Where("id = ?", *row.TaskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	rr = request(http.MethodGet, "/api/v1/commit-batches/b", nil)
	var stopped commitBatchResponse
	decodeJSON(t, rr, &stopped)
	if stopped.CanSeal || stopped.Task == nil || !stopped.Task.Retryable {
		t.Fatalf("stopped = %#v", stopped)
	}
	if rr := request(http.MethodPost, "/api/v1/commit-batches/b/seal", nil); rr.Code != 409 {
		t.Fatalf("stopped request = %d: %s", rr.Code, rr.Body.String())
	}
}
