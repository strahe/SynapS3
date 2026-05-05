package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/s3iam"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
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
	mux.HandleFunc("GET /api/v1/buckets/{name}/objects/status-detail", srv.handleAPIBucketObjectStatusDetail)
	mux.HandleFunc("GET /api/v1/buckets/{name}/objects/versions", srv.handleAPIBucketObjectVersions)
	mux.HandleFunc("GET /api/v1/buckets/{name}/objects/download", srv.handleAPIDownloadObject)
	return mux
}

func setBucketWriteHeaders(req *http.Request) {
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
}

type writeDeadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (r *writeDeadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	r.deadlines = append(r.deadlines, deadline)
	return nil
}

type storageUploadSelectCounter struct {
	selects atomic.Int32
}

func (c *storageUploadSelectCounter) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	query := strings.ToLower(strings.TrimSpace(event.Query))
	if strings.HasPrefix(query, "select") && (strings.Contains(query, `from "storage_uploads"`) || strings.Contains(query, "from storage_uploads")) {
		c.selects.Add(1)
	}
	return ctx
}

func (c *storageUploadSelectCounter) AfterQuery(context.Context, *bun.QueryEvent) {}

func seedAdminObjectVersion(t *testing.T, repos *repository.Repositories, bucket *model.Bucket, key string, size int64, etag, checksum, contentType, cacheKey string, state model.ObjectState) (int64, string) {
	t.Helper()
	versionID := model.NewVersionID()
	if cacheKey == "" {
		cacheKey = ".versions/" + versionID
	}
	createState := state
	if state == model.ObjectStateStored || state == model.ObjectStateCacheEvicted {
		createState = model.ObjectStateUploading
	}
	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         key,
		Size:        size,
		ETag:        etag,
		Checksum:    checksum,
		ContentType: contentType,
		CacheKey:    cacheKey,
		State:       createState,
	}
	objID, err := repos.Objects.CreateVersionAndSetCurrent(context.Background(), version)
	if err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	if state == model.ObjectStateStored || state == model.ObjectStateCacheEvicted {
		acceptAdminVersionUpload(t, repos, versionID, "piece-"+versionID, "https://provider.example/piece/"+versionID)
		if state == model.ObjectStateCacheEvicted {
			if err := repos.Objects.UpdateVersionState(context.Background(), versionID, model.ObjectStateStored, model.ObjectStateCacheEvicted); err != nil {
				t.Fatalf("Objects.UpdateVersionState cache_evicted: %v", err)
			}
		}
	}
	return objID, versionID
}

func acceptAdminVersionUpload(t *testing.T, repos *repository.Repositories, versionID string, pieceCID string, retrievalURL string) *model.StorageUpload {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for upload accept: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	providerID := onChainIDPtr(t, "101")
	dataSetID := onChainIDPtr(t, "1001")
	pieceID := onChainIDPtr(t, "1")
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        &pieceCID,
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{{
			ProviderID:   providerID,
			DataSetID:    dataSetID,
			PieceID:      pieceID,
			Role:         "primary",
			RetrievalURL: &retrievalURL,
		}},
	}); err != nil {
		t.Fatalf("record upload result: %v", err)
	}
	if _, err := repos.Uploads.AcceptCompleteUploadForContent(ctx, repository.AcceptCompleteUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("accept upload result: %v", err)
	}
	return upload
}

func bindAdminPartialUpload(t *testing.T, repos *repository.Repositories, versionID string) *model.StorageUpload {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for partial upload: version=%v err=%v", version, err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("partial uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("partial committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start partial upload attempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: version.BucketID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: version.BucketID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("secondary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("primary dataset ready: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: secondary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1002"), ClientDataSetID: onChainIDPtr(t, "9002")}); err != nil {
		t.Fatalf("secondary dataset ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, Role: "secondary", ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("create upload copies: %v", err)
	}
	pieceCID := "piece-partial-" + versionID
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://primary.example/piece/" + versionID,
	}); err != nil {
		t.Fatalf("primary committed: %v", err)
	}
	if _, err := repos.Uploads.BindPrimaryCommittedUploadForContent(ctx, repository.BindPrimaryCommittedUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("bind primary committed upload: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 1, "secondary pull: timeout"); err != nil {
		t.Fatalf("mark secondary failed: %v", err)
	}
	return upload
}

func markAdminStoredOnPrimaryUpload(t *testing.T, repos *repository.Repositories, versionID string) *model.StorageUpload {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for stored-on-primary upload: version=%v err=%v", version, err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("stored-on-primary uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("stored-on-primary committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start stored-on-primary upload attempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: version.BucketID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("primary dataset ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("create upload copy: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-primary-" + versionID,
		RetrievalURL: "https://primary.example/piece/" + versionID,
	}); err != nil {
		t.Fatalf("mark primary piece ready: %v", err)
	}
	return upload
}

func markAdminFailedUpload(t *testing.T, repos *repository.Repositories, versionID string, message string) *model.StorageUpload {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for failed upload: version=%v err=%v", version, err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("failed upload state: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start failed upload attempt: %v", err)
	}
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        false,
		RequestedCopies: 2,
		ErrorMessage:    &message,
	}); err != nil {
		t.Fatalf("record failed upload result: %v", err)
	}
	return upload
}

func seedCachedDownloadObject(t *testing.T, srv *Server, repos *repository.Repositories, bucketName, key, body string) *cache.ObjectInfo {
	t.Helper()
	ctx := context.Background()
	bucket := &model.Bucket{Name: bucketName, Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	versionID := model.NewVersionID()
	cacheKey := ".versions/" + versionID
	info, err := srv.cache.Put(ctx, bucket.Name, cacheKey, strings.NewReader(body))
	if err != nil {
		t.Fatalf("cache.Put: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         key,
		Size:        info.Size,
		ETag:        info.ETag,
		Checksum:    info.Checksum,
		ContentType: "text/plain",
		CacheKey:    cacheKey,
		State:       model.ObjectStateCached,
	}); err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	return info
}

type recordingObjectListRepo struct {
	repository.ObjectRepository

	keys           []string
	listCalls      int
	atOrAfterCalls int
}

func (r *recordingObjectListRepo) scanCalls() int {
	return r.listCalls + r.atOrAfterCalls
}

func (r *recordingObjectListRepo) ListCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.ObjectVersion, error) {
	r.listCalls++
	return r.list(prefix, func(key string) bool {
		return afterKey == "" || key > afterKey
	}, maxKeys), nil
}

func (r *recordingObjectListRepo) ListCurrentVersionsByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix string, fromKey string, maxKeys int) ([]model.ObjectVersion, error) {
	r.atOrAfterCalls++
	return r.list(prefix, func(key string) bool {
		return fromKey == "" || key >= fromKey
	}, maxKeys), nil
}

func (r *recordingObjectListRepo) list(prefix string, include func(string) bool, maxKeys int) []model.ObjectVersion {
	objects := make([]model.ObjectVersion, 0)
	for _, key := range r.keys {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if !include(key) {
			continue
		}
		objects = append(objects, model.ObjectVersion{Key: key})
		if maxKeys > 0 && len(objects) >= maxKeys {
			break
		}
	}
	return objects
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
		seedAdminObjectVersion(t, repos, bucket, tc.key, tc.size, tc.key, tc.key, "text/plain", "", model.ObjectStateCached)
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
		Name               string `json:"name"`
		Status             string `json:"status"`
		VersioningStatus   string `json:"versioning_status"`
		VersioningEnforced bool   `json:"versioning_enforced"`
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
	if body.VersioningStatus != "Enabled" || !body.VersioningEnforced {
		t.Fatalf("versioning = %q enforced=%v, want Enabled/enforced", body.VersioningStatus, body.VersioningEnforced)
	}
}

func TestAPIBucketObjects_ActiveBucket(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "objects-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	_, versionID := seedAdminObjectVersion(t, repos, bucket, "kept.txt", 4, "etag-kept", "checksum-kept", "text/plain", "", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	acceptAdminVersionUpload(t, repos, versionID, "piece-kept", "https://provider.example/kept")

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

	var body struct {
		Objects []struct {
			Key              string         `json:"key"`
			CurrentVersionID string         `json:"current_version_id"`
			State            string         `json:"state"`
			Status           string         `json:"status"`
			Location         objectLocation `json:"location"`
		} `json:"objects"`
		Folders []objectFolderItem `json:"folders"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(body.Objects) != 1 {
		t.Fatalf("objects len = %d, want 1", len(body.Objects))
	}
	if body.Objects[0].Key != "kept.txt" {
		t.Fatalf("key = %q, want %q", body.Objects[0].Key, "kept.txt")
	}
	if body.Objects[0].CurrentVersionID == "" {
		t.Fatal("expected current version id")
	}
	if body.Objects[0].Status != "success" {
		t.Fatalf("status = %q, want success", body.Objects[0].Status)
	}
	if body.Objects[0].State != string(model.ObjectStateStored) {
		t.Fatalf("state = %q, want stored", body.Objects[0].State)
	}
	if !body.Objects[0].Location.Cache || !body.Objects[0].Location.Filecoin {
		t.Fatalf("location = %#v, want cache and filecoin", body.Objects[0].Location)
	}
	if len(body.Folders) != 0 {
		t.Fatalf("folders len = %d, want 0 for flat object list", len(body.Folders))
	}
	if body.Folders == nil {
		t.Fatal("folders should be an empty array, not null")
	}

	resp, err = http.Get(ts.URL + "/api/v1/buckets/objects-bucket/objects")
	if err != nil {
		t.Fatalf("GET bucket objects raw: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var raw struct {
		Objects []map[string]any `json:"objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("Decode raw: %v", err)
	}
	if raw.Objects[0]["state"] != string(model.ObjectStateStored) {
		t.Fatalf("object list state = %#v, want stored", raw.Objects[0]["state"])
	}
	if _, ok := raw.Objects[0]["storage"]; ok {
		t.Fatal("object list exposed storage instead of location")
	}
	if _, ok := raw.Objects[0]["attention"]; ok {
		t.Fatal("object list exposed attention")
	}
}

func TestAPIBucketObjectsDelimiterReturnsCurrentLevelFoldersAndFiles(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "folder-list-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	seedAdminObjectVersion(t, repos, bucket, "docs/guide.md", 4, "etag-doc", "checksum-doc", "text/markdown", "", model.ObjectStateStored)
	seedAdminObjectVersion(t, repos, bucket, "photos/", 0, "etag-marker", "checksum-marker", "application/x-directory", "", model.ObjectStateCached)
	seedAdminObjectVersion(t, repos, bucket, "photos/2026/a.jpg", 7, "etag-photo", "checksum-photo", "image/jpeg", "", model.ObjectStateStored)
	seedAdminObjectVersion(t, repos, bucket, "root.txt", 5, "etag-root", "checksum-root", "text/plain", "", model.ObjectStateStored)

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/folder-list-bucket/objects?delimiter=/")
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
	if len(body.Folders) != 2 {
		t.Fatalf("folders len = %d, want 2: %#v", len(body.Folders), body.Folders)
	}
	if body.Folders[0].Name != "docs" || body.Folders[0].Prefix != "docs/" {
		t.Fatalf("first folder = %#v, want docs/", body.Folders[0])
	}
	if body.Folders[1].Name != "photos" || body.Folders[1].Prefix != "photos/" {
		t.Fatalf("second folder = %#v, want photos/", body.Folders[1])
	}
	if len(body.Objects) != 1 || body.Objects[0].Key != "root.txt" {
		t.Fatalf("objects = %#v, want root.txt only", body.Objects)
	}
}

func TestAPIBucketObjectsDelimiterPrefixReturnsNestedLevel(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "nested-folder-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	seedAdminObjectVersion(t, repos, bucket, "photos/", 0, "etag-marker", "checksum-marker", "application/x-directory", "", model.ObjectStateCached)
	seedAdminObjectVersion(t, repos, bucket, "photos/cover.jpg", 5, "etag-cover", "checksum-cover", "image/jpeg", "", model.ObjectStateStored)
	seedAdminObjectVersion(t, repos, bucket, "photos/2026/a.jpg", 7, "etag-photo", "checksum-photo", "image/jpeg", "", model.ObjectStateStored)

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/nested-folder-bucket/objects?prefix=" + url.QueryEscape("photos/") + "&delimiter=/")
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
	if len(body.Folders) != 1 || body.Folders[0].Name != "2026" || body.Folders[0].Prefix != "photos/2026/" {
		t.Fatalf("folders = %#v, want photos/2026/", body.Folders)
	}
	if len(body.Objects) != 1 || body.Objects[0].Key != "photos/cover.jpg" {
		t.Fatalf("objects = %#v, want photos/cover.jpg only", body.Objects)
	}
}

func TestAPIBucketObjectsDelimiterPreservesSlashOnlyFolderName(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "slash-folder-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	seedAdminObjectVersion(t, repos, bucket, "a//child.txt", 1, "etag-slash", "checksum-slash", "text/plain", "", model.ObjectStateStored)

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/slash-folder-bucket/objects?prefix=" + url.QueryEscape("a/") + "&delimiter=/")
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
	if len(body.Folders) != 1 || body.Folders[0].Name != "/" || body.Folders[0].Prefix != "a//" {
		t.Fatalf("folders = %#v, want slash-only folder a//", body.Folders)
	}
}

func TestAPIBucketObjectsRejectsUnsupportedDelimiter(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "unsupported-delimiter-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/unsupported-delimiter-bucket/objects?delimiter=:")
	if err != nil {
		t.Fatalf("GET bucket objects: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestAPIBucketObjectsDelimiterPaginationSkipsDuplicateFolders(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "folder-page-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	seedAdminObjectVersion(t, repos, bucket, "a/1.txt", 1, "etag-a1", "checksum-a1", "text/plain", "", model.ObjectStateStored)
	seedAdminObjectVersion(t, repos, bucket, "a/2.txt", 1, "etag-a2", "checksum-a2", "text/plain", "", model.ObjectStateStored)
	seedAdminObjectVersion(t, repos, bucket, "b/1.txt", 1, "etag-b1", "checksum-b1", "text/plain", "", model.ObjectStateStored)

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/folder-page-bucket/objects?delimiter=/&limit=1")
	if err != nil {
		t.Fatalf("GET page 1: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var page1 objectListResponse
	if err := json.NewDecoder(resp.Body).Decode(&page1); err != nil {
		t.Fatalf("Decode page 1: %v", err)
	}
	if len(page1.Folders) != 1 || page1.Folders[0].Prefix != "a/" || len(page1.Objects) != 0 || !page1.HasMore || page1.NextMarker == "" {
		t.Fatalf("page 1 = %#v, want a/ folder and next marker", page1)
	}

	resp2, err := http.Get(ts.URL + "/api/v1/buckets/folder-page-bucket/objects?delimiter=/&limit=1&after=" + url.QueryEscape(page1.NextMarker))
	if err != nil {
		t.Fatalf("GET page 2: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	var page2 objectListResponse
	if err := json.NewDecoder(resp2.Body).Decode(&page2); err != nil {
		t.Fatalf("Decode page 2: %v", err)
	}
	if len(page2.Folders) != 1 || page2.Folders[0].Prefix != "b/" || len(page2.Objects) != 0 {
		t.Fatalf("page 2 = %#v, want b/ folder only", page2)
	}
}

func TestListBucketObjectEntriesSkipsEmittedFolderSubtree(t *testing.T) {
	keys := make([]string, adminObjectListingBatchSize*2+1)
	for i := range keys {
		keys[i] = fmt.Sprintf("a/%04d.txt", i)
	}
	objects := &recordingObjectListRepo{keys: keys}
	srv := &Server{repos: &repository.Repositories{Objects: objects}}

	folders, files, hasMore, nextMarker, err := srv.listBucketObjectEntries(t.Context(), 1, "", "/", "", 50)
	if err != nil {
		t.Fatalf("listBucketObjectEntries: %v", err)
	}

	if len(folders) != 1 || folders[0].Prefix != "a/" || len(files) != 0 || hasMore || nextMarker != "" {
		t.Fatalf("listing = folders:%#v files:%#v hasMore:%v nextMarker:%q, want a/ folder only", folders, files, hasMore, nextMarker)
	}
	if objects.scanCalls() > 2 {
		t.Fatalf("object list scans = %d, want at most 2 without walking every child batch", objects.scanCalls())
	}
}

func TestListBucketObjectEntriesKeepsCurrentBatchAcrossSiblingFolders(t *testing.T) {
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("dir-%02d/file.txt", i)
	}
	objects := &recordingObjectListRepo{keys: keys}
	srv := &Server{repos: &repository.Repositories{Objects: objects}}

	folders, files, hasMore, nextMarker, err := srv.listBucketObjectEntries(t.Context(), 1, "", "/", "", 50)
	if err != nil {
		t.Fatalf("listBucketObjectEntries: %v", err)
	}

	if len(folders) != 50 || len(files) != 0 || hasMore || nextMarker != "" {
		t.Fatalf("listing = folders:%d files:%#v hasMore:%v nextMarker:%q, want 50 folders only", len(folders), files, hasMore, nextMarker)
	}
	if objects.scanCalls() != 1 {
		t.Fatalf("object list scans = %d, want 1 for sibling folders in one batch", objects.scanCalls())
	}
}

func TestListBucketObjectEntriesSkipsDuplicateRowsBeforeSiblingFolders(t *testing.T) {
	keys := make([]string, 0, 100)
	for i := 0; i < 50; i++ {
		keys = append(keys, fmt.Sprintf("dir-%02d/a.txt", i), fmt.Sprintf("dir-%02d/b.txt", i))
	}
	objects := &recordingObjectListRepo{keys: keys}
	srv := &Server{repos: &repository.Repositories{Objects: objects}}

	folders, files, hasMore, nextMarker, err := srv.listBucketObjectEntries(t.Context(), 1, "", "/", "", 50)
	if err != nil {
		t.Fatalf("listBucketObjectEntries: %v", err)
	}

	if len(folders) != 50 || len(files) != 0 || hasMore || nextMarker != "" {
		t.Fatalf("listing = folders:%d files:%#v hasMore:%v nextMarker:%q, want 50 folders only", len(folders), files, hasMore, nextMarker)
	}
	if objects.scanCalls() != 1 {
		t.Fatalf("object list scans = %d, want 1 while duplicate folder rows fit in one batch", objects.scanCalls())
	}
}

func TestAPIBucketObjectVersions(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "versions-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	_, oldVersionID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 4, "etag-old", "checksum-old", "text/plain", "", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, oldVersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("old uploading: %v", err)
	}
	acceptAdminVersionUpload(t, repos, oldVersionID, "piece-old", "https://provider.example/old")
	_, currentVersionID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 7, "etag-current", "checksum-current", "text/plain", "", model.ObjectStateCached)

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/versions-bucket/objects/versions?key=" + url.QueryEscape("file.txt"))
	if err != nil {
		t.Fatalf("GET object versions: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.StatusCode, http.StatusOK, readBody(t, resp.Body))
	}

	var body struct {
		Versions []struct {
			VersionID string         `json:"version_id"`
			State     string         `json:"state"`
			Status    string         `json:"status"`
			Location  objectLocation `json:"location"`
			IsCurrent bool           `json:"is_current"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(body.Versions) != 2 {
		t.Fatalf("versions len = %d, want 2", len(body.Versions))
	}
	if body.Versions[0].VersionID != currentVersionID || !body.Versions[0].IsCurrent {
		t.Fatalf("first version = %#v, want current %s", body.Versions[0], currentVersionID)
	}
	if body.Versions[1].VersionID != oldVersionID || body.Versions[1].IsCurrent {
		t.Fatalf("second version = %#v, want old %s", body.Versions[1], oldVersionID)
	}
	if body.Versions[0].Status != "uploading" {
		t.Fatalf("current version status = %q, want uploading", body.Versions[0].Status)
	}
	if body.Versions[0].State != string(model.ObjectStateCached) {
		t.Fatalf("current version state = %q, want cached", body.Versions[0].State)
	}
	if !body.Versions[0].Location.Cache || body.Versions[0].Location.Filecoin {
		t.Fatalf("current version location = %#v, want cache only", body.Versions[0].Location)
	}
	if body.Versions[1].Status != "success" {
		t.Fatalf("old version status = %q, want success", body.Versions[1].Status)
	}
	if body.Versions[1].State != string(model.ObjectStateStored) {
		t.Fatalf("old version state = %q, want stored", body.Versions[1].State)
	}
	if !body.Versions[1].Location.Cache || !body.Versions[1].Location.Filecoin {
		t.Fatalf("old version location = %#v, want cache and filecoin", body.Versions[1].Location)
	}

	resp, err = http.Get(ts.URL + "/api/v1/buckets/versions-bucket/objects/versions?key=" + url.QueryEscape("file.txt"))
	if err != nil {
		t.Fatalf("GET object versions raw: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var raw struct {
		Versions []map[string]any `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("Decode raw: %v", err)
	}
	if raw.Versions[0]["state"] != string(model.ObjectStateCached) {
		t.Fatalf("version list state = %#v, want cached", raw.Versions[0]["state"])
	}
	if _, ok := raw.Versions[0]["storage"]; ok {
		t.Fatal("version list exposed storage instead of location")
	}
}

func TestAPIBucketObjects_StatusMappingAndDetail(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "status-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	_, warningVersionID := seedAdminObjectVersion(t, repos, bucket, "warning.txt", 4, "etag-warning", "checksum-warning", "text/plain", "", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, warningVersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("warning uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, warningVersionID, model.ObjectStateUploading, "provider rejected piece"); err != nil {
		t.Fatalf("warning failed: %v", err)
	}

	unavailable := &model.ObjectVersion{
		VersionID:   "01J000000000000000UNAVAIL",
		BucketID:    bucket.ID,
		Key:         "unavailable.txt",
		Size:        1,
		ETag:        "etag-unavailable",
		Checksum:    "checksum-unavailable",
		ContentType: "text/plain",
		CacheKey:    "cache-unavailable",
		State:       model.ObjectStateCached,
		InCache:     false,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, unavailable); err != nil {
		t.Fatalf("unavailable version: %v", err)
	}
	if err := repos.Objects.SetVersionCachePresence(ctx, unavailable.VersionID, false); err != nil {
		t.Fatalf("unavailable cache presence: %v", err)
	}
	_, storedOnPrimaryVersionID := seedAdminObjectVersion(t, repos, bucket, "stored-primary.txt", 2, "etag-primary", "checksum-primary", "text/plain", "", model.ObjectStateCached)
	markAdminStoredOnPrimaryUpload(t, repos, storedOnPrimaryVersionID)
	_, partialVersionID := seedAdminObjectVersion(t, repos, bucket, "partial.txt", 3, "etag-partial", "checksum-partial", "text/plain", "", model.ObjectStateCached)
	bindAdminPartialUpload(t, repos, partialVersionID)
	_, failedUploadVersionID := seedAdminObjectVersion(t, repos, bucket, "failed-upload.txt", 5, "etag-failed-upload", "checksum-failed-upload", "text/plain", "", model.ObjectStateCached)
	markAdminFailedUpload(t, repos, failedUploadVersionID, "provider rejected piece")

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/status-bucket/objects")
	if err != nil {
		t.Fatalf("GET bucket objects: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var list struct {
		Objects []struct {
			Key          string `json:"key"`
			Status       string `json:"status"`
			UploadStatus string `json:"upload_status"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("Decode list: %v", err)
	}

	statusByKey := map[string]string{}
	uploadStatusByKey := map[string]string{}
	for _, object := range list.Objects {
		statusByKey[object.Key] = object.Status
		uploadStatusByKey[object.Key] = object.UploadStatus
	}
	if statusByKey["warning.txt"] != "warning" {
		t.Fatalf("warning status = %q, want warning", statusByKey["warning.txt"])
	}
	if statusByKey["unavailable.txt"] != "unavailable" {
		t.Fatalf("unavailable status = %q, want unavailable", statusByKey["unavailable.txt"])
	}
	if uploadStatusByKey["stored-primary.txt"] != string(model.StorageUploadStatusStoredOnPrimary) {
		t.Fatalf("stored-primary upload_status = %q, want stored_on_primary", uploadStatusByKey["stored-primary.txt"])
	}
	if statusByKey["partial.txt"] != "warning" || uploadStatusByKey["partial.txt"] != string(model.StorageUploadStatusPartial) {
		t.Fatalf("partial status/upload_status = %q/%q, want warning/partial", statusByKey["partial.txt"], uploadStatusByKey["partial.txt"])
	}
	if statusByKey["failed-upload.txt"] != "warning" || uploadStatusByKey["failed-upload.txt"] != string(model.StorageUploadStatusFailed) {
		t.Fatalf("failed upload status/upload_status = %q/%q, want warning/failed", statusByKey["failed-upload.txt"], uploadStatusByKey["failed-upload.txt"])
	}

	resp, err = http.Get(ts.URL + "/api/v1/buckets/status-bucket/objects/status-detail?version_id=" + url.QueryEscape(warningVersionID))
	if err != nil {
		t.Fatalf("GET status detail: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status detail code = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var detail objectStatusDetailResponse
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("Decode detail: %v", err)
	}
	if detail.VersionID != warningVersionID || detail.Status != "warning" {
		t.Fatalf("detail = %#v, want version %s warning", detail, warningVersionID)
	}
	if detail.State != string(model.ObjectStateFailed) {
		t.Fatalf("detail state = %q, want failed", detail.State)
	}
	if detail.FailedAtState == nil || *detail.FailedAtState != string(model.ObjectStateUploading) {
		t.Fatalf("failed_at_state = %#v, want uploading", detail.FailedAtState)
	}
	if detail.Message == nil || *detail.Message != "provider rejected piece" {
		t.Fatalf("message = %#v, want provider rejected piece", detail.Message)
	}

	resp, err = http.Get(ts.URL + "/api/v1/buckets/status-bucket/objects/status-detail?version_id=" + url.QueryEscape(partialVersionID))
	if err != nil {
		t.Fatalf("GET partial status detail: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var partialDetail struct {
		VersionID    string `json:"version_id"`
		Status       string `json:"status"`
		UploadStatus string `json:"upload_status"`
		Message      string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&partialDetail); err != nil {
		t.Fatalf("Decode partial detail: %v", err)
	}
	if partialDetail.VersionID != partialVersionID || partialDetail.Status != "warning" || partialDetail.UploadStatus != string(model.StorageUploadStatusPartial) {
		t.Fatalf("partial detail = %#v, want partial warning", partialDetail)
	}
	if partialDetail.Message != "secondary pull: timeout" {
		t.Fatalf("partial message = %q, want secondary pull: timeout", partialDetail.Message)
	}

	resp, err = http.Get(ts.URL + "/api/v1/buckets/status-bucket/objects/status-detail?version_id=" + url.QueryEscape(failedUploadVersionID))
	if err != nil {
		t.Fatalf("GET failed upload status detail: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var failedUploadDetail struct {
		UploadStatus string `json:"upload_status"`
		Message      string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&failedUploadDetail); err != nil {
		t.Fatalf("Decode failed upload detail: %v", err)
	}
	if failedUploadDetail.UploadStatus != string(model.StorageUploadStatusFailed) || failedUploadDetail.Message != "provider rejected piece" {
		t.Fatalf("failed upload detail = %#v, want failed/provider rejected piece", failedUploadDetail)
	}

	otherBucket := &model.Bucket{Name: "other-status-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, otherBucket); err != nil {
		t.Fatalf("other bucket: %v", err)
	}
	resp, err = http.Get(ts.URL + "/api/v1/buckets/other-status-bucket/objects/status-detail?version_id=" + url.QueryEscape(warningVersionID))
	if err != nil {
		t.Fatalf("GET status detail wrong bucket: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("wrong bucket status detail code = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestAPIBucketObjects_LoadsUploadStatusInBatches(t *testing.T) {
	db := testutil.NewTestDB(t)
	counter := &storageUploadSelectCounter{}
	db.AddQueryHook(counter)
	localCache, err := cache.NewFilesystem(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("creating test cache: %v", err)
	}
	repos := repository.NewRepositories(db)
	srv := New("127.0.0.1:0", db, localCache, 1<<20, repos, nil, nil, testLogger())
	ctx := context.Background()
	bucket := &model.Bucket{Name: "batched-upload-status-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	seedAdminObjectVersion(t, repos, bucket, "stored.txt", 2, "etag-stored", "checksum-stored", "text/plain", "", model.ObjectStateStored)
	_, primaryVersionID := seedAdminObjectVersion(t, repos, bucket, "primary.txt", 3, "etag-primary", "checksum-primary", "text/plain", "", model.ObjectStateCached)
	markAdminStoredOnPrimaryUpload(t, repos, primaryVersionID)
	_, partialVersionID := seedAdminObjectVersion(t, repos, bucket, "partial.txt", 4, "etag-partial", "checksum-partial", "text/plain", "", model.ObjectStateCached)
	bindAdminPartialUpload(t, repos, partialVersionID)
	_, failedVersionID := seedAdminObjectVersion(t, repos, bucket, "failed.txt", 5, "etag-failed", "checksum-failed", "text/plain", "", model.ObjectStateCached)
	markAdminFailedUpload(t, repos, failedVersionID, "provider rejected piece")

	counter.selects.Store(0)
	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/api/v1/buckets/batched-upload-status-bucket/objects")
	if err != nil {
		t.Fatalf("GET bucket objects: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.StatusCode, http.StatusOK, readBody(t, resp.Body))
	}
	if got := counter.selects.Load(); got > 2 {
		t.Fatalf("storage upload selects = %d, want batched lookups no more than 2", got)
	}
}

func TestAPIBucketObjectVersions_LoadsUploadStatusInBatches(t *testing.T) {
	db := testutil.NewTestDB(t)
	counter := &storageUploadSelectCounter{}
	db.AddQueryHook(counter)
	localCache, err := cache.NewFilesystem(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("creating test cache: %v", err)
	}
	repos := repository.NewRepositories(db)
	srv := New("127.0.0.1:0", db, localCache, 1<<20, repos, nil, nil, testLogger())
	ctx := context.Background()
	bucket := &model.Bucket{Name: "batched-version-status-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	seedAdminObjectVersion(t, repos, bucket, "file.txt", 2, "etag-stored", "checksum-stored", "text/plain", "", model.ObjectStateStored)
	_, primaryVersionID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 3, "etag-primary", "checksum-primary", "text/plain", "", model.ObjectStateCached)
	markAdminStoredOnPrimaryUpload(t, repos, primaryVersionID)
	_, partialVersionID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 4, "etag-partial", "checksum-partial", "text/plain", "", model.ObjectStateCached)
	bindAdminPartialUpload(t, repos, partialVersionID)
	_, failedVersionID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 5, "etag-failed", "checksum-failed", "text/plain", "", model.ObjectStateCached)
	markAdminFailedUpload(t, repos, failedVersionID, "provider rejected piece")

	counter.selects.Store(0)
	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/api/v1/buckets/batched-version-status-bucket/objects/versions?key=" + url.QueryEscape("file.txt"))
	if err != nil {
		t.Fatalf("GET object versions: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.StatusCode, http.StatusOK, readBody(t, resp.Body))
	}
	if got := counter.selects.Load(); got > 2 {
		t.Fatalf("storage upload selects = %d, want batched lookups no more than 2", got)
	}
}

func TestAPIBucketObjectDownload_FromCache(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	info := seedCachedDownloadObject(t, srv, repos, "download-bucket", "folder/report.txt", "hello admin")

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/buckets/download-bucket/objects/download?key=" + url.QueryEscape("folder/report.txt"))
	if err != nil {
		t.Fatalf("GET object download: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.StatusCode, http.StatusOK, readBody(t, resp.Body))
	}
	body := readBody(t, resp.Body)
	if body != "hello admin" {
		t.Fatalf("body = %q, want %q", body, "hello admin")
	}
	if got := resp.Header.Get("Content-Type"); got != "text/plain" {
		t.Fatalf("Content-Type = %q, want text/plain", got)
	}
	if got := resp.Header.Get("Content-Length"); got != "11" {
		t.Fatalf("Content-Length = %q, want 11", got)
	}
	if got := resp.Header.Get("ETag"); got != `"`+info.ETag+`"` {
		t.Fatalf("ETag = %q, want quoted object etag", got)
	}
	if got := resp.Header.Get("Content-Disposition"); !strings.Contains(got, "attachment") || !strings.Contains(got, "report.txt") {
		t.Fatalf("Content-Disposition = %q, want attachment filename", got)
	}
}

func TestAPIBucketObjectDownload_WithVersionID(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "download-version-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	oldVersionID := model.NewVersionID()
	oldCacheKey := ".versions/" + oldVersionID
	oldInfo, err := srv.cache.Put(ctx, bucket.Name, oldCacheKey, strings.NewReader("old admin"))
	if err != nil {
		t.Fatalf("cache.Put old: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
		VersionID:   oldVersionID,
		BucketID:    bucket.ID,
		Key:         "folder/report.txt",
		Size:        oldInfo.Size,
		ETag:        oldInfo.ETag,
		Checksum:    oldInfo.Checksum,
		ContentType: "text/plain",
		CacheKey:    oldCacheKey,
		State:       model.ObjectStateCached,
	}); err != nil {
		t.Fatalf("create old version: %v", err)
	}
	newVersionID := model.NewVersionID()
	newCacheKey := ".versions/" + newVersionID
	newInfo, err := srv.cache.Put(ctx, bucket.Name, newCacheKey, strings.NewReader("new admin"))
	if err != nil {
		t.Fatalf("cache.Put new: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
		VersionID:   newVersionID,
		BucketID:    bucket.ID,
		Key:         "folder/report.txt",
		Size:        newInfo.Size,
		ETag:        newInfo.ETag,
		Checksum:    newInfo.Checksum,
		ContentType: "text/plain",
		CacheKey:    newCacheKey,
		State:       model.ObjectStateCached,
	}); err != nil {
		t.Fatalf("create new version: %v", err)
	}

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()

	target := ts.URL + "/api/v1/buckets/download-version-bucket/objects/download?key=" + url.QueryEscape("folder/report.txt") + "&version_id=" + url.QueryEscape(oldVersionID)
	resp, err := http.Get(target)
	if err != nil {
		t.Fatalf("GET object download version: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.StatusCode, http.StatusOK, readBody(t, resp.Body))
	}
	if body := readBody(t, resp.Body); body != "old admin" {
		t.Fatalf("body = %q, want old admin", body)
	}
}

func TestAPIBucketObjectDownload_RequiresLoopbackBinding(t *testing.T) {
	srv, _ := newBucketAPITestServer(t)
	srv.addr = "0.0.0.0:9090"

	req := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/download-bucket/objects/download?key=file.txt", nil)
	req.SetPathValue("name", "download-bucket")
	rr := httptest.NewRecorder()

	srv.handleAPIDownloadObject(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusForbidden, rr.Body.String())
	}
}

func TestAPIBucketObjectDownload_ClearsWriteDeadline(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "deadline-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	seedAdminObjectVersion(t, repos, bucket, "folder/report.txt", 11, "etag", "checksum", "text/plain", "", model.ObjectStateCached)
	var rr *writeDeadlineRecorder
	mockCache := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			if rr == nil || len(rr.deadlines) != 1 || !rr.deadlines[0].IsZero() {
				t.Fatalf("write deadline was not cleared before object read: %#v", rr)
			}
			return io.NopCloser(strings.NewReader("hello admin")), &cache.ObjectInfo{
				Path: "/cache/report.txt", Size: 11, ETag: "etag", Checksum: "checksum",
			}, nil
		},
	}
	srv.cache = mockCache
	srv.objectReader = objectreader.New(repos, mockCache, nil, testLogger())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/deadline-bucket/objects/download?key="+url.QueryEscape("folder/report.txt"), nil)
	req.SetPathValue("name", "deadline-bucket")
	rr = &writeDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}

	srv.handleAPIDownloadObject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	if len(rr.deadlines) != 1 {
		t.Fatalf("write deadline calls = %d, want 1", len(rr.deadlines))
	}
	if !rr.deadlines[0].IsZero() {
		t.Fatalf("write deadline = %v, want zero deadline", rr.deadlines[0])
	}
}

func TestAPIBucketObjectDownload_NotFound(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "download-missing-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/download-missing-bucket/objects/download?key=missing.txt", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIDownloadObject(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusNotFound, rr.Body.String())
	}
}

func TestAPIBucketObjectDownload_RejectsInvalidRequest(t *testing.T) {
	srv, _ := newBucketAPITestServer(t)

	for _, tc := range []struct {
		name       string
		bucketName string
		target     string
	}{
		{name: "invalid bucket", bucketName: "BadBucket", target: "/api/v1/buckets/BadBucket/objects/download?key=file.txt"},
		{name: "missing key", bucketName: "valid-bucket", target: "/api/v1/buckets/valid-bucket/objects/download"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.target, nil)
			req.SetPathValue("name", tc.bucketName)
			rr := httptest.NewRecorder()

			srv.handleAPIDownloadObject(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
		})
	}
}

func readBody(t *testing.T, r io.Reader) string {
	t.Helper()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return string(data)
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

	seedAdminObjectVersion(t, repos, bucket, "file.txt", 5, "etag-file", "checksum-file", "text/plain", "", model.ObjectStateStored)

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

	objects, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("Objects.ListCurrentVersionsByBucket: %v", err)
	}
	if len(objects) != 1 {
		t.Fatalf("visible objects len = %d, want 1", len(objects))
	}
}
