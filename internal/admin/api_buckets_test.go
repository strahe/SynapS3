package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	if bucket.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", bucket.Status, model.BucketStatusActive)
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

func TestAPIBuckets_ListAllBuckets(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	for _, bucket := range []*model.Bucket{
		{Name: "alpha-bucket", Status: model.BucketStatusActive},
		{Name: "beta-bucket", Status: model.BucketStatusActive},
		{Name: "gamma-bucket", Status: model.BucketStatusActive},
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

	for _, name := range []string{"alpha-bucket", "beta-bucket", "gamma-bucket"} {
		if got[name] != string(model.BucketStatusActive) {
			t.Fatalf("%s status = %q, want %q", name, got[name], model.BucketStatusActive)
		}
	}
}

func TestAPIBucketDetail_ActiveBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "active-detail-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/active-detail-bucket")
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
	if body.Status != string(model.BucketStatusActive) {
		t.Fatalf("status = %q, want %q", body.Status, model.BucketStatusActive)
	}
}

func TestAPIBucketObjects_ActiveBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "objects-bucket", Status: model.BucketStatusActive}
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
		State:       model.ObjectStateStored,
		MaxRetries:  5,
	}); err != nil {
		t.Fatalf("Objects.UpsertAndBumpGeneration: %v", err)
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/objects-bucket/objects")
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

func TestAPIBucket_DeleteReturnsNotImplemented(t *testing.T) {
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

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusNotImplemented, rr.Body.String())
	}

	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if body["error"] == "" {
		t.Fatal("expected error message in response")
	}

	// Bucket should remain unchanged.
	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to still exist")
	}
	if updated.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", updated.Status, model.BucketStatusActive)
	}
}

func TestAPIBucket_DeleteRecursiveReturnsNotImplemented(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "recursive-delete-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, &model.Object{
		BucketID:    bucket.ID,
		Key:         "file.txt",
		Size:        5,
		ETag:        "etag-file",
		Checksum:    "checksum-file",
		ContentType: "text/plain",
		CachePath:   "/cache/file.txt",
		State:       model.ObjectStateStored,
		MaxRetries:  5,
	}); err != nil {
		t.Fatalf("Objects.UpsertAndBumpGeneration: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/recursive-delete-bucket?recursive=true", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIDeleteBucket(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusNotImplemented, rr.Body.String())
	}

	// Bucket and objects should remain unchanged.
	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("expected bucket to still exist")
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
}
