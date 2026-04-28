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
	"github.com/strahe/synaps3/internal/s3iam"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/versity/versitygw/auth"
)

func newBucketAPITestServer(t *testing.T) (*Server, *repository.Repositories) {
	t.Helper()

	db := testutil.NewTestDB(t)
	localCache, err := cache.NewFilesystem(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("creating test cache: %v", err)
	}

	repos := repository.NewRepositories(db)
	srv := New("127.0.0.1:0", db, localCache, 1<<20, repos, nil, nil, testLogger())
	return srv, repos
}

func newBucketAPITestServerWithS3Users(t *testing.T, accessKeys ...string) (*Server, *repository.Repositories) {
	t.Helper()

	srv, repos := newBucketAPITestServer(t)
	iamSvc := s3iam.NewService(repos)
	root, err := iamSvc.EnsureRootAccount(t.Context())
	if err != nil {
		t.Fatalf("EnsureRootAccount: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })
	for _, accessKey := range accessKeys {
		if err := iamSvc.CreateAccount(auth.Account{Access: accessKey, Secret: "secret-" + accessKey, Role: auth.RoleUserPlus}); err != nil {
			t.Fatalf("CreateAccount(%s): %v", accessKey, err)
		}
	}
	srv.WithS3IAM(iamSvc, root.Access)
	return srv, repos
}

func newBucketAPIMux(srv *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/buckets", srv.handleAPIListBuckets)
	mux.HandleFunc("POST /api/v1/buckets", srv.handleAPICreateBucket)
	mux.HandleFunc("GET /api/v1/buckets/{name}", srv.handleAPIGetBucket)
	mux.HandleFunc("PUT /api/v1/buckets/{name}/owner", srv.handleAPIUpdateBucketOwner)
	mux.HandleFunc("DELETE /api/v1/buckets/{name}", srv.handleAPIDeleteBucket)
	mux.HandleFunc("GET /api/v1/buckets/{name}/objects", srv.handleAPIBucketObjects)
	return mux
}

func setBucketWriteHeaders(req *http.Request) {
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
}

func TestHandleAPIBuckets_CreateBucket(t *testing.T) {
	srv, repos := newBucketAPITestServerWithS3Users(t, "owner-access")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"admin-create-bucket","owner_access_key":"owner-access"}`))
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
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
	acl, err := auth.ParseACL(bucket.ACL)
	if err != nil {
		t.Fatalf("ParseACL: %v", err)
	}
	if acl.Owner != "owner-access" {
		t.Fatalf("owner = %q, want owner-access", acl.Owner)
	}

	var body struct {
		Name           string  `json:"name"`
		OwnerAccessKey *string `json:"owner_access_key"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if body.OwnerAccessKey == nil || *body.OwnerAccessKey != "owner-access" {
		t.Fatalf("owner_access_key = %v, want owner-access", body.OwnerAccessKey)
	}
}

func TestHandleAPIBuckets_CreateBucketAllowsInternalRootOwner(t *testing.T) {
	srv, repos := newBucketAPITestServerWithS3Users(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"root-owned-bucket","owner_access_key":"`+internalRootOwnerAccessKey+`"}`))
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
	rr := httptest.NewRecorder()

	srv.handleAPICreateBucket(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusCreated, rr.Body.String())
	}
	bucket, err := repos.Buckets.GetByName(context.Background(), "root-owned-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if bucket.OwnerAccessKey == nil || *bucket.OwnerAccessKey != srv.s3RootAccess {
		t.Fatalf("stored owner = %v, want root access", bucket.OwnerAccessKey)
	}
	acl, err := auth.ParseACL(bucket.ACL)
	if err != nil {
		t.Fatalf("ParseACL: %v", err)
	}
	if acl.Owner != srv.s3RootAccess {
		t.Fatalf("ACL owner = %q, want root access", acl.Owner)
	}
	var body struct {
		OwnerAccessKey *string `json:"owner_access_key"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if body.OwnerAccessKey == nil || *body.OwnerAccessKey != internalRootOwnerAccessKey {
		t.Fatalf("response owner = %v, want internal root token", body.OwnerAccessKey)
	}
}

func TestHandleAPIBuckets_CreateBucketRequiresOwner(t *testing.T) {
	srv, _ := newBucketAPITestServerWithS3Users(t, "owner-access")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"ownerless-bucket"}`))
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
	rr := httptest.NewRecorder()

	srv.handleAPICreateBucket(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
}

func TestHandleAPIBuckets_CreateBucketRejectsUnknownOwner(t *testing.T) {
	srv, _ := newBucketAPITestServerWithS3Users(t, "owner-access")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"unknown-owner-bucket","owner_access_key":"missing-owner"}`))
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
	rr := httptest.NewRecorder()

	srv.handleAPICreateBucket(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
}

func TestHandleAPIBuckets_CreateBucketRejectsMalformedStrictJSON(t *testing.T) {
	srv, _ := newBucketAPITestServerWithS3Users(t, "owner-access")

	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"name":"strict-bucket","owner_access_key":"owner-access","extra":true}`},
		{name: "trailing object", body: `{"name":"strict-bucket","owner_access_key":"owner-access"} {}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			setBucketWriteHeaders(req)
			rr := httptest.NewRecorder()

			srv.handleAPICreateBucket(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
		})
	}
}

func TestHandleAPIBuckets_CreateBucketRequiresWriteHeader(t *testing.T) {
	srv, _ := newBucketAPITestServerWithS3Users(t, "owner-access")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"guarded-bucket","owner_access_key":"owner-access"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPICreateBucket(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
}

func TestHandleAPIBuckets_CreateBucketRequiresLoopbackBinding(t *testing.T) {
	srv, _ := newBucketAPITestServerWithS3Users(t, "owner-access")
	srv.addr = "0.0.0.0:9090"

	req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", strings.NewReader(`{"name":"guarded-bucket","owner_access_key":"owner-access"}`))
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
	rr := httptest.NewRecorder()

	srv.handleAPICreateBucket(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusForbidden, rr.Body.String())
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

func TestAPIBuckets_ListAndDetailIncludeOwnerAccessKey(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	ownedACL, err := json.Marshal(auth.ACL{Owner: "owner-access"})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}
	ownerAccess := "owner-access"
	if err := repos.S3Accounts.Create(ctx, &model.S3Account{AccessKey: ownerAccess, SecretKey: "owner-secret", Role: auth.RoleUserPlus}); err != nil {
		t.Fatalf("S3Accounts.Create: %v", err)
	}
	for _, bucket := range []*model.Bucket{
		{Name: "owned-bucket", Status: model.BucketStatusActive, ACL: ownedACL, OwnerAccessKey: &ownerAccess},
		{Name: "unassigned-bucket", Status: model.BucketStatusActive},
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
		t.Fatalf("list status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var listBody []struct {
		Name           string  `json:"name"`
		OwnerAccessKey *string `json:"owner_access_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listBody); err != nil {
		t.Fatalf("Decode list: %v", err)
	}
	owners := make(map[string]*string, len(listBody))
	for _, item := range listBody {
		owners[item.Name] = item.OwnerAccessKey
	}
	if owners["owned-bucket"] == nil || *owners["owned-bucket"] != "owner-access" {
		t.Fatalf("owned-bucket owner = %v, want owner-access", owners["owned-bucket"])
	}
	if owners["unassigned-bucket"] != nil {
		t.Fatalf("unassigned-bucket owner = %v, want nil", *owners["unassigned-bucket"])
	}

	detailResp, err := http.Get(ts.URL + "/api/v1/buckets/owned-bucket")
	if err != nil {
		t.Fatalf("GET bucket detail: %v", err)
	}
	defer func() { _ = detailResp.Body.Close() }()
	if detailResp.StatusCode != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", detailResp.StatusCode, http.StatusOK)
	}
	var detailBody struct {
		OwnerAccessKey *string `json:"owner_access_key"`
	}
	if err := json.NewDecoder(detailResp.Body).Decode(&detailBody); err != nil {
		t.Fatalf("Decode detail: %v", err)
	}
	if detailBody.OwnerAccessKey == nil || *detailBody.OwnerAccessKey != "owner-access" {
		t.Fatalf("detail owner = %v, want owner-access", detailBody.OwnerAccessKey)
	}
}

func TestAPIBucketOwner_UpdateAssignsExistingS3User(t *testing.T) {
	srv, repos := newBucketAPITestServerWithS3Users(t, "owner-access")
	ctx := context.Background()
	bucket := &model.Bucket{Name: "assign-owner-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/assign-owner-bucket/owner", strings.NewReader(`{"owner_access_key":"owner-access"}`))
	req.SetPathValue("name", bucket.Name)
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateBucketOwner(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	acl, err := auth.ParseACL(updated.ACL)
	if err != nil {
		t.Fatalf("ParseACL: %v", err)
	}
	if acl.Owner != "owner-access" {
		t.Fatalf("owner = %q, want owner-access", acl.Owner)
	}
	var body struct {
		OwnerAccessKey *string `json:"owner_access_key"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if body.OwnerAccessKey == nil || *body.OwnerAccessKey != "owner-access" {
		t.Fatalf("response owner = %v, want owner-access", body.OwnerAccessKey)
	}
}

func TestAPIBucketOwner_UpdateAllowsInternalRootOwner(t *testing.T) {
	srv, repos := newBucketAPITestServerWithS3Users(t, "owner-access")
	ctx := context.Background()
	owner := "owner-access"
	acl, err := json.Marshal(auth.ACL{Owner: owner})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}
	bucket := &model.Bucket{Name: "root-transfer-bucket", Status: model.BucketStatusActive, OwnerAccessKey: &owner, ACL: acl}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/root-transfer-bucket/owner", strings.NewReader(`{"owner_access_key":"`+internalRootOwnerAccessKey+`"}`))
	req.SetPathValue("name", bucket.Name)
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateBucketOwner(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	updated, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated.OwnerAccessKey == nil || *updated.OwnerAccessKey != srv.s3RootAccess {
		t.Fatalf("stored owner = %v, want root access", updated.OwnerAccessKey)
	}
	updatedACL, err := auth.ParseACL(updated.ACL)
	if err != nil {
		t.Fatalf("ParseACL: %v", err)
	}
	if updatedACL.Owner != srv.s3RootAccess {
		t.Fatalf("ACL owner = %q, want root access", updatedACL.Owner)
	}
	var body struct {
		OwnerAccessKey *string `json:"owner_access_key"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if body.OwnerAccessKey == nil || *body.OwnerAccessKey != internalRootOwnerAccessKey {
		t.Fatalf("response owner = %v, want internal root token", body.OwnerAccessKey)
	}
}

func TestAPIBucketOwner_UpdateRejectsUnknownS3User(t *testing.T) {
	srv, repos := newBucketAPITestServerWithS3Users(t, "owner-access")
	ctx := context.Background()
	bucket := &model.Bucket{Name: "unknown-owner-target", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/unknown-owner-target/owner", strings.NewReader(`{"owner_access_key":"missing-owner"}`))
	req.SetPathValue("name", bucket.Name)
	req.Header.Set("Content-Type", "application/json")
	setBucketWriteHeaders(req)
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateBucketOwner(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusNotFound, rr.Body.String())
	}
}

func TestAPIBucketOwner_UpdateRejectsMalformedStrictJSON(t *testing.T) {
	srv, repos := newBucketAPITestServerWithS3Users(t, "owner-access")
	ctx := context.Background()
	bucket := &model.Bucket{Name: "strict-owner-target", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"owner_access_key":"owner-access","extra":true}`},
		{name: "trailing object", body: `{"owner_access_key":"owner-access"} {}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/strict-owner-target/owner", strings.NewReader(tc.body))
			req.SetPathValue("name", bucket.Name)
			req.Header.Set("Content-Type", "application/json")
			setBucketWriteHeaders(req)
			rr := httptest.NewRecorder()

			srv.handleAPIUpdateBucketOwner(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
		})
	}
}

func TestAPIBucketOwner_UpdateRequiresWriteHeader(t *testing.T) {
	srv, repos := newBucketAPITestServerWithS3Users(t, "owner-access")
	ctx := context.Background()
	bucket := &model.Bucket{Name: "guarded-owner-target", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/guarded-owner-target/owner", strings.NewReader(`{"owner_access_key":"owner-access"}`))
	req.SetPathValue("name", bucket.Name)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateBucketOwner(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
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
