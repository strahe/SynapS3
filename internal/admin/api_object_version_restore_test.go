package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type recordingObjectVersionRestorer struct {
	calls                    int
	bucket                   string
	key                      string
	sourceVersionID          string
	expectedCurrentVersionID string
	deadline                 time.Time
	versionID                string
	err                      error
}

type missingCurrentObjectRepository struct {
	repository.ObjectRepository
}

func (r missingCurrentObjectRepository) GetCurrentVersionByBucketAndKey(context.Context, int64, string) (*model.ObjectVersion, error) {
	return nil, nil
}

func (r *recordingObjectVersionRestorer) RestoreObjectVersion(ctx context.Context, bucketName, key, sourceVersionID, expectedCurrentVersionID string) (string, error) {
	r.calls++
	r.bucket = bucketName
	r.key = key
	r.sourceVersionID = sourceVersionID
	r.expectedCurrentVersionID = expectedCurrentVersionID
	r.deadline, _ = ctx.Deadline()
	return r.versionID, r.err
}

func TestAPIObjectVersionRestorePassesCASRequest(t *testing.T) {
	srv, _ := newBucketAPITestServer(t)
	restorer := &recordingObjectVersionRestorer{versionID: "01J0000000000000000000AR03"}
	srv.WithObjectVersionRestorer(restorer)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/buckets/restore-api-bucket/objects/versions/restore",
		strings.NewReader(`{"key":"folder/file.txt","version_id":"01J0000000000000000000AR01","expected_current_version_id":"01J0000000000000000000AR02"}`),
	)
	req.SetPathValue("name", "restore-api-bucket")
	rr := &writeDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	start := time.Now()

	srv.handleAPIRestoreObjectVersion(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	if restorer.calls != 1 || restorer.bucket != "restore-api-bucket" || restorer.key != "folder/file.txt" {
		t.Fatalf("restorer request = %#v", restorer)
	}
	if restorer.sourceVersionID != "01J0000000000000000000AR01" || restorer.expectedCurrentVersionID != "01J0000000000000000000AR02" {
		t.Fatalf("restorer versions = source:%q current:%q", restorer.sourceVersionID, restorer.expectedCurrentVersionID)
	}
	if restorer.deadline.Before(start.Add(time.Hour-time.Second)) || restorer.deadline.After(time.Now().Add(time.Hour+time.Second)) {
		t.Fatalf("context deadline = %v, want about one hour", restorer.deadline)
	}
	if len(rr.deadlines) != 1 || rr.deadlines[0].Before(start.Add(time.Hour-time.Second)) || rr.deadlines[0].After(time.Now().Add(time.Hour+time.Second)) {
		t.Fatalf("write deadlines = %#v, want one deadline about one hour", rr.deadlines)
	}
	var body restoreObjectVersionResponse
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if body.Key != "folder/file.txt" || body.SourceVersionID != restorer.sourceVersionID || body.VersionID != restorer.versionID {
		t.Fatalf("response = %#v", body)
	}
}

func TestAPIObjectVersionRestoreRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"key":"file.txt","version_id":"source","expected_current_version_id":"current","extra":true}`},
		{name: "trailing object", body: `{"key":"file.txt","version_id":"source","expected_current_version_id":"current"} {}`},
		{name: "missing key", body: `{"version_id":"source","expected_current_version_id":"current"}`},
		{name: "missing source version", body: `{"key":"file.txt","expected_current_version_id":"current"}`},
		{name: "missing current version", body: `{"key":"file.txt","version_id":"source"}`},
		{name: "invalid key", body: "{\"key\":\"folder/\\u0000file.txt\",\"version_id\":\"source\",\"expected_current_version_id\":\"current\"}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newBucketAPITestServer(t)
			restorer := &recordingObjectVersionRestorer{}
			srv.WithObjectVersionRestorer(restorer)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets/restore-api-bucket/objects/versions/restore", strings.NewReader(tt.body))
			req.SetPathValue("name", "restore-api-bucket")
			rr := httptest.NewRecorder()

			srv.handleAPIRestoreObjectVersion(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
			if restorer.calls != 0 {
				t.Fatalf("restorer calls = %d, want 0", restorer.calls)
			}
		})
	}
}

func TestAPIObjectVersionRestoreMapsErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "invalid", err: repository.ErrInvalidInput, wantStatus: http.StatusBadRequest},
		{name: "missing", err: repository.ErrNotFound, wantStatus: http.StatusNotFound},
		{name: "already current", err: repository.ErrAlreadyCurrent, wantStatus: http.StatusConflict, wantCode: objectVersionAlreadyCurrentCode},
		{name: "conflict", err: repository.ErrConflict, wantStatus: http.StatusConflict},
		{name: "cache full", err: fmt.Errorf("staging restore: %w", cache.ErrCacheFull), wantStatus: http.StatusInsufficientStorage},
		{name: "internal", err: errors.New("provider read failed"), wantStatus: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newBucketAPITestServer(t)
			srv.WithObjectVersionRestorer(&recordingObjectVersionRestorer{err: tt.err})
			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/buckets/restore-api-bucket/objects/versions/restore",
				strings.NewReader(`{"key":"file.txt","version_id":"source","expected_current_version_id":"current"}`),
			)
			req.SetPathValue("name", "restore-api-bucket")
			rr := httptest.NewRecorder()

			srv.handleAPIRestoreObjectVersion(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			var body map[string]string
			if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if body["error"] == "" {
				t.Fatal("error response is empty")
			}
			if body["code"] != tt.wantCode {
				t.Fatalf("error code = %q, want %q", body["code"], tt.wantCode)
			}
		})
	}
}

func TestAPIObjectVersionsReturnsCurrentVersionIDOnEveryPage(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "version-token-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	_, firstID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 1, "etag-1", "checksum-1", "text/plain", "", model.ObjectStateCached)
	_, secondID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 2, "etag-2", "checksum-2", "text/plain", "", model.ObjectStateCached)
	_, currentID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 3, "etag-3", "checksum-3", "text/plain", "", model.ObjectStateCached)

	ts := httptest.NewServer(newBucketAPIMux(srv))
	defer ts.Close()
	firstPageURL := ts.URL + "/api/v1/buckets/" + bucket.Name + "/objects/versions?key=" + url.QueryEscape("file.txt") + "&limit=1"
	resp, err := http.Get(firstPageURL)
	if err != nil {
		t.Fatalf("GET first page: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var firstPage objectVersionListResponse
	if err := json.NewDecoder(resp.Body).Decode(&firstPage); err != nil {
		t.Fatalf("Decode first page: %v", err)
	}
	if resp.StatusCode != http.StatusOK || firstPage.CurrentVersionID != currentID || len(firstPage.Versions) != 1 || !firstPage.HasMore || firstPage.NextVersionMarker == "" {
		t.Fatalf("first page status/body = %d/%#v", resp.StatusCode, firstPage)
	}

	secondPageURL := firstPageURL + "&version_marker=" + url.QueryEscape(firstPage.NextVersionMarker)
	resp, err = http.Get(secondPageURL)
	if err != nil {
		t.Fatalf("GET second page: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var secondPage objectVersionListResponse
	if err := json.NewDecoder(resp.Body).Decode(&secondPage); err != nil {
		t.Fatalf("Decode second page: %v", err)
	}
	if resp.StatusCode != http.StatusOK || secondPage.CurrentVersionID != currentID || len(secondPage.Versions) != 1 {
		t.Fatalf("second page status/body = %d/%#v", resp.StatusCode, secondPage)
	}
	if secondPage.Versions[0].VersionID != secondID && secondPage.Versions[0].VersionID != firstID {
		t.Fatalf("second page version = %s, want historical version", secondPage.Versions[0].VersionID)
	}

	emptyResp, err := http.Get(ts.URL + "/api/v1/buckets/" + bucket.Name + "/objects/versions?key=" + url.QueryEscape("missing.txt"))
	if err != nil {
		t.Fatalf("GET empty history: %v", err)
	}
	defer func() { _ = emptyResp.Body.Close() }()
	var emptyBody map[string]any
	if err := json.NewDecoder(emptyResp.Body).Decode(&emptyBody); err != nil {
		t.Fatalf("Decode empty history: %v", err)
	}
	if _, exists := emptyBody["current_version_id"]; exists {
		t.Fatalf("empty history includes current_version_id: %#v", emptyBody)
	}
}

func TestAPIObjectVersionsFallsBackToCurrentVersionInListedPage(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "version-token-race-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	_, currentID := seedAdminObjectVersion(t, repos, bucket, "file.txt", 1, "etag", "checksum", "text/plain", "", model.ObjectStateCached)
	repos.Objects = missingCurrentObjectRepository{ObjectRepository: repos.Objects}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/"+bucket.Name+"/objects/versions?key=file.txt", nil)
	req.SetPathValue("name", bucket.Name)
	rr := httptest.NewRecorder()

	srv.handleAPIBucketObjectVersions(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var body objectVersionListResponse
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if body.CurrentVersionID != currentID || len(body.Versions) != 1 || !body.Versions[0].IsCurrent {
		t.Fatalf("response = %#v, want listed current version %s", body, currentID)
	}
}
