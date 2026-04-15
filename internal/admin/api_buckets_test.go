package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func newBucketAPITestServer(t *testing.T) (*Server, *repository.Repositories) {
	t.Helper()

	db := testutil.NewTestDB(t)
	localCache, err := cache.NewFilesystem(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("creating test cache: %v", err)
	}

	repos := repository.NewRepositories(db)
	srv := New(":0", db, localCache, 1<<20, repos, nil, nil, testLogger())
	return srv, repos
}

func newBucketAPIMux(srv *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/buckets", srv.handleAPIListBuckets)
	mux.HandleFunc("POST /api/v1/buckets", srv.handleAPICreateBucket)
	mux.HandleFunc("GET /api/v1/buckets/{name}", srv.handleAPIGetBucket)
	mux.HandleFunc("DELETE /api/v1/buckets/{name}", srv.handleAPIDeleteBucket)
	mux.HandleFunc("GET /api/v1/buckets/{name}/objects", srv.handleAPIBucketObjects)
	return mux
}

func TestHandleAPIBuckets_CreateBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"admin-create-bucket"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPICreateBucket(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusCreated, rr.Body.String())
	}

	ctx := context.Background()
	bucket, err := repos.Buckets.GetByName(ctx, "admin-create-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if bucket == nil {
		t.Fatal("expected bucket to be created")
	}
	if bucket.Status != model.BucketStatusCreating {
		t.Fatalf("bucket status = %s, want %s", bucket.Status, model.BucketStatusCreating)
	}

	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeCreateProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List: %v", err)
	}
	if total != 1 {
		t.Fatalf("create_proof_set total = %d, want 1", total)
	}
	if len(tasks) != 1 {
		t.Fatalf("returned tasks len = %d, want 1", len(tasks))
	}
	if tasks[0].RefType != "bucket" || tasks[0].RefID != bucket.ID {
		t.Fatalf("task refs = %s/%d, want bucket/%d", tasks[0].RefType, tasks[0].RefID, bucket.ID)
	}
}

func TestAPIBucketDetail(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "detail-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	for _, tc := range []struct {
		key  string
		size int64
	}{
		{key: "a.txt", size: 5},
		{key: "b.txt", size: 7},
	} {
		_, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, &model.Object{
			BucketID:    bucket.ID,
			Key:         tc.key,
			Size:        tc.size,
			ETag:        tc.key,
			Checksum:    tc.key,
			ContentType: "text/plain",
			CachePath:   "/cache/" + tc.key,
			State:       model.ObjectStateCached,
			MaxRetries:  5,
		})
		if err != nil {
			t.Fatalf("Objects.UpsertAndBumpGeneration(%s): %v", tc.key, err)
		}
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/detail-bucket")
	if err != nil {
		t.Fatalf("GET bucket detail: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body struct {
		ID             int64  `json:"id"`
		Name           string `json:"name"`
		Status         string `json:"status"`
		ObjectCount    int64  `json:"object_count"`
		TotalSizeBytes int64  `json:"total_size_bytes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if body.ID != bucket.ID {
		t.Fatalf("id = %d, want %d", body.ID, bucket.ID)
	}
	if body.Name != bucket.Name {
		t.Fatalf("name = %q, want %q", body.Name, bucket.Name)
	}
	if body.Status != string(bucket.Status) {
		t.Fatalf("status = %q, want %q", body.Status, bucket.Status)
	}
	if body.ObjectCount != 2 {
		t.Fatalf("object_count = %d, want 2", body.ObjectCount)
	}
	if body.TotalSizeBytes != 12 {
		t.Fatalf("total_size_bytes = %d, want 12", body.TotalSizeBytes)
	}
}

func TestAPIBuckets_ListIncludesFailedBuckets(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	for _, bucket := range []*model.Bucket{
		{Name: "active-bucket", Status: model.BucketStatusActive},
		{Name: "create-failed-bucket", Status: model.BucketStatusCreateFailed},
		{Name: "delete-failed-bucket", Status: model.BucketStatusDeleteFailed},
		{Name: "deleted-bucket", Status: model.BucketStatusDeleted},
	} {
		if err := repos.Buckets.Create(ctx, bucket); err != nil {
			t.Fatalf("Buckets.Create(%s): %v", bucket.Name, err)
		}
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets")
	if err != nil {
		t.Fatalf("GET buckets: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	got := make(map[string]string, len(body))
	for _, item := range body {
		got[item.Name] = item.Status
	}

	if got["active-bucket"] != string(model.BucketStatusActive) {
		t.Fatalf("active-bucket status = %q, want %q", got["active-bucket"], model.BucketStatusActive)
	}
	if got["create-failed-bucket"] != string(model.BucketStatusCreateFailed) {
		t.Fatalf("create-failed-bucket status = %q, want %q", got["create-failed-bucket"], model.BucketStatusCreateFailed)
	}
	if got["delete-failed-bucket"] != string(model.BucketStatusDeleteFailed) {
		t.Fatalf("delete-failed-bucket status = %q, want %q", got["delete-failed-bucket"], model.BucketStatusDeleteFailed)
	}
	if _, exists := got["deleted-bucket"]; exists {
		t.Fatal("deleted bucket should be hidden from admin bucket list")
	}
}

func TestAPIBucketDetail_AllowsFailedBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "failed-detail-bucket", Status: model.BucketStatusCreateFailed}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/failed-detail-bucket")
	if err != nil {
		t.Fatalf("GET bucket detail: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if body.Name != bucket.Name {
		t.Fatalf("name = %q, want %q", body.Name, bucket.Name)
	}
	if body.Status != string(model.BucketStatusCreateFailed) {
		t.Fatalf("status = %q, want %q", body.Status, model.BucketStatusCreateFailed)
	}
}

func TestAPIBucketObjects_AllowsFailedBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "failed-objects-bucket", Status: model.BucketStatusDeleteFailed}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, &model.Object{
		BucketID:    bucket.ID,
		Key:         "kept.txt",
		Size:        4,
		ETag:        "etag-kept",
		Checksum:    "checksum-kept",
		ContentType: "text/plain",
		CachePath:   "/cache/kept.txt",
		State:       model.ObjectStateOnChained,
		MaxRetries:  5,
	}); err != nil {
		t.Fatalf("Objects.UpsertAndBumpGeneration: %v", err)
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/failed-objects-bucket/objects")
	if err != nil {
		t.Fatalf("GET bucket objects: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body objectListResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(body.Objects) != 1 {
		t.Fatalf("objects len = %d, want 1", len(body.Objects))
	}
	if body.Objects[0].Key != "kept.txt" {
		t.Fatalf("key = %q, want %q", body.Objects[0].Key, "kept.txt")
	}
}

func TestAPIBucket_DeleteEmptyBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "delete-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/delete-bucket", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to remain visible in deleting state")
	}
	if updated.Status != model.BucketStatusDeleting {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusDeleting)
	}

	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeDeleteProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List: %v", err)
	}
	if total != 1 {
		t.Fatalf("delete_proof_set total = %d, want 1", total)
	}
	if len(tasks) != 1 {
		t.Fatalf("returned tasks len = %d, want 1", len(tasks))
	}
	if tasks[0].RefType != "bucket" || tasks[0].RefID != bucket.ID {
		t.Fatalf("task refs = %s/%d, want bucket/%d", tasks[0].RefType, tasks[0].RefID, bucket.ID)
	}
}

func TestAPIBucket_DeleteRecursiveSafeBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "recursive-safe-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	cached, err := srv.cache.Put(ctx, bucket.Name, "safe.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("cache.Put: %v", err)
	}
	pieceCID := "baga-piece"
	for _, obj := range []*model.Object{
		{
			BucketID:    bucket.ID,
			Key:         "safe.txt",
			Size:        5,
			ETag:        "etag-safe",
			Checksum:    "checksum-safe",
			ContentType: "text/plain",
			CachePath:   cached.Path,
			PieceCID:    &pieceCID,
			State:       model.ObjectStateOnChained,
			MaxRetries:  5,
		},
		{
			BucketID:    bucket.ID,
			Key:         "failed.txt",
			Size:        3,
			ETag:        "etag-failed",
			Checksum:    "checksum-failed",
			ContentType: "text/plain",
			CachePath:   "/cache/failed.txt",
			State:       model.ObjectStateFailed,
			MaxRetries:  5,
		},
		{
			BucketID:    bucket.ID,
			Key:         "cached.txt",
			Size:        4,
			ETag:        "etag-cached",
			Checksum:    "checksum-cached",
			ContentType: "text/plain",
			CachePath:   "/cache/cached.txt",
			State:       model.ObjectStateCached,
			MaxRetries:  5,
		},
	} {
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("Objects.UpsertAndBumpGeneration(%s): %v", obj.Key, err)
		}
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/recursive-safe-bucket?recursive=true", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to remain visible in deleting state")
	}
	if updated.Status != model.BucketStatusDeleting {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusDeleting)
	}

	objects, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("Objects.ListByBucket: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("visible objects len = %d, want 0", len(objects))
	}
	if srv.cache.Exists(ctx, bucket.Name, "safe.txt") {
		t.Fatal("expected cached object to be removed during recursive delete")
	}
}

func TestAPIBucket_DeleteRecursiveBlockedByInFlightObject(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "recursive-blocked-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, &model.Object{
		BucketID:    bucket.ID,
		Key:         "uploading.txt",
		Size:        9,
		ETag:        "etag-uploading",
		Checksum:    "checksum-uploading",
		ContentType: "text/plain",
		CachePath:   "/cache/uploading.txt",
		State:       model.ObjectStateUploaded,
		MaxRetries:  5,
	}); err != nil {
		t.Fatalf("Objects.UpsertAndBumpGeneration: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/recursive-blocked-bucket?recursive=true", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusConflict, rr.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to remain present")
	}
	if updated.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusActive)
	}

	objects, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("Objects.ListByBucket: %v", err)
	}
	if len(objects) != 1 {
		t.Fatalf("visible objects len = %d, want 1", len(objects))
	}

	_, total, err := repos.Tasks.List(ctx, string(model.TaskTypeDeleteProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List: %v", err)
	}
	if total != 0 {
		t.Fatalf("delete_proof_set total = %d, want 0", total)
	}
}

func TestAPIBucket_DeleteRecursiveBlockedByInFlightTask(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "recursive-task-blocked-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	objectID, generation, err := repos.Objects.UpsertAndBumpGeneration(ctx, &model.Object{
		BucketID:    bucket.ID,
		Key:         "stable.txt",
		Size:        9,
		ETag:        "etag-stable",
		Checksum:    "checksum-stable",
		ContentType: "text/plain",
		CachePath:   "/cache/stable.txt",
		State:       model.ObjectStateOnChained,
		MaxRetries:  5,
	})
	if err != nil {
		t.Fatalf("Objects.UpsertAndBumpGeneration: %v", err)
	}

	if err := repos.Tasks.Create(ctx, &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objectID,
		RefGeneration:  generation,
		IdempotencyKey: "evict-cache-blocker",
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}); err != nil {
		t.Fatalf("Tasks.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/recursive-task-blocked-bucket?recursive=true", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusConflict, rr.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to remain present")
	}
	if updated.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusActive)
	}

	objects, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("Objects.ListByBucket: %v", err)
	}
	if len(objects) != 1 {
		t.Fatalf("visible objects len = %d, want 1", len(objects))
	}

	_, total, err := repos.Tasks.List(ctx, string(model.TaskTypeDeleteProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List: %v", err)
	}
	if total != 0 {
		t.Fatalf("delete_proof_set total = %d, want 0", total)
	}
}

func TestAPIBucket_DeleteBlockedByBucketLifecycleTask(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "lifecycle-blocked-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	if err := repos.Tasks.Create(ctx, &model.Task{
		Type:           model.TaskTypeCreateProofSet,
		RefType:        "bucket",
		RefID:          bucket.ID,
		IdempotencyKey: "create-ps-blocker",
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}); err != nil {
		t.Fatalf("Tasks.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/lifecycle-blocked-bucket", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusConflict, rr.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusActive)
	}
}

func TestAPIBucket_DeleteCreatingBucketRejected(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"creating-delete-bucket"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	srv.handleAPICreateBucket(createRR, createReq)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d, body=%s", createRR.Code, http.StatusCreated, createRR.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/creating-delete-bucket", nil)
	deleteReq.SetPathValue("name", "creating-delete-bucket")
	deleteRR := httptest.NewRecorder()
	srv.handleAPIDeleteBucket(deleteRR, deleteReq)

	if deleteRR.Code != http.StatusConflict {
		t.Fatalf("delete status = %d, want %d, body=%s", deleteRR.Code, http.StatusConflict, deleteRR.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, "creating-delete-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to remain present")
	}
	if updated.Status != model.BucketStatusCreating {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusCreating)
	}

	createTasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeCreateProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List(create_proof_set): %v", err)
	}
	if total != 1 || len(createTasks) != 1 {
		t.Fatalf("create_proof_set tasks = total %d len %d, want 1/1", total, len(createTasks))
	}

	_, deleteTotal, err := repos.Tasks.List(ctx, string(model.TaskTypeDeleteProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List(delete_proof_set): %v", err)
	}
	if deleteTotal != 0 {
		t.Fatalf("delete_proof_set total = %d, want 0", deleteTotal)
	}
}

func TestAPIBucket_DeleteRecursiveBlockedBySoftDeletedObjectTask(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "soft-deleted-task-blocked-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	objectID, generation, err := repos.Objects.UpsertAndBumpGeneration(ctx, &model.Object{
		BucketID:    bucket.ID,
		Key:         "ghost.txt",
		Size:        5,
		ETag:        "etag-ghost",
		Checksum:    "checksum-ghost",
		ContentType: "text/plain",
		CachePath:   "/cache/ghost.txt",
		State:       model.ObjectStateCached,
		MaxRetries:  5,
	})
	if err != nil {
		t.Fatalf("Objects.UpsertAndBumpGeneration: %v", err)
	}
	if err := repos.Tasks.Create(ctx, &model.Task{
		Type:           model.TaskTypeUploadToSP,
		RefType:        "object",
		RefID:          objectID,
		RefGeneration:  generation,
		IdempotencyKey: "soft-deleted-object-blocker",
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}); err != nil {
		t.Fatalf("Tasks.Create: %v", err)
	}
	if err := repos.Objects.SoftDelete(ctx, objectID); err != nil {
		t.Fatalf("Objects.SoftDelete: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/soft-deleted-task-blocked-bucket?recursive=true", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()
	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusConflict, rr.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to remain present")
	}
	if updated.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusActive)
	}

	_, deleteTotal, err := repos.Tasks.List(ctx, string(model.TaskTypeDeleteProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List(delete_proof_set): %v", err)
	}
	if deleteTotal != 0 {
		t.Fatalf("delete_proof_set total = %d, want 0", deleteTotal)
	}
}

func TestAPIBucket_DeleteBlockedByActiveMultipartUpload(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "multipart-blocked-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	if err := repos.Multiparts.Create(ctx, &model.MultipartUpload{
		BucketID: bucket.ID,
		Key:      "large.bin",
		UploadID: "multipart-blocker",
		Status:   model.MultipartStatusInitiated,
	}); err != nil {
		t.Fatalf("Multiparts.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/multipart-blocked-bucket", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()
	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusConflict, rr.Body.String())
	}

	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to remain present")
	}
	if updated.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusActive)
	}

	_, deleteTotal, err := repos.Tasks.List(ctx, string(model.TaskTypeDeleteProofSet), "", 10, 0)
	if err != nil {
		t.Fatalf("Tasks.List(delete_proof_set): %v", err)
	}
	if deleteTotal != 0 {
		t.Fatalf("delete_proof_set total = %d, want 0", deleteTotal)
	}
}
