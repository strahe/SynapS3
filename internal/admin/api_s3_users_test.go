package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/config"
	"github.com/versity/versitygw/auth"
)

func newS3UsersAPITestServer(t *testing.T, addr string) (*Server, auth.IAMService) {
	t.Helper()
	iamDir := t.TempDir()
	iamSvc, err := auth.NewInternal(auth.Account{
		Access: "root-access",
		Secret: "root-secret",
		Role:   auth.RoleAdmin,
	}, iamDir)
	if err != nil {
		t.Fatalf("NewInternal: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })

	srv := &Server{
		addr:         addr,
		s3IAM:        iamSvc,
		s3RootAccess: "root-access",
		s3IAMDir:     iamDir,
		logger:       testLogger(),
	}
	return srv, iamSvc
}

func TestS3UsersCreateDefaultsToUserPlusAndListDoesNotLeakSecrets(t *testing.T) {
	srv, _ := newS3UsersAPITestServer(t, "127.0.0.1:9090")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/s3-users", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()
	srv.handleAPICreateS3User(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d, body=%s", rr.Code, http.StatusCreated, rr.Body.String())
	}
	var created s3UserCredentialsResponse
	if err := json.NewDecoder(rr.Body).Decode(&created); err != nil {
		t.Fatalf("Decode create response: %v", err)
	}
	if created.AccessKey == "" || created.SecretKey == "" {
		t.Fatalf("created credentials must include access and one-time secret: %#v", created)
	}
	if created.Role != string(auth.RoleUserPlus) {
		t.Fatalf("role = %q, want %q", created.Role, auth.RoleUserPlus)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/s3-users", nil)
	listRR := httptest.NewRecorder()
	srv.handleAPIListS3Users(listRR, listReq)

	if listRR.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200, body=%s", listRR.Code, listRR.Body.String())
	}
	if strings.Contains(listRR.Body.String(), created.SecretKey) {
		t.Fatalf("list response leaked secret: %s", listRR.Body.String())
	}
	if !strings.Contains(listRR.Body.String(), created.AccessKey) {
		t.Fatalf("list response missing created access key: %s", listRR.Body.String())
	}
}

func TestS3UsersCreateRejectsInvalidRole(t *testing.T) {
	srv, _ := newS3UsersAPITestServer(t, "127.0.0.1:9090")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/s3-users", strings.NewReader(`{"role":"owner"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()
	srv.handleAPICreateS3User(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
}

func TestS3UsersUpdateRotateAndDelete(t *testing.T) {
	srv, iamSvc := newS3UsersAPITestServer(t, "127.0.0.1:9090")

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/s3-users", strings.NewReader(`{"role":"user"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	createRR := httptest.NewRecorder()
	srv.handleAPICreateS3User(createRR, createReq)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201, body=%s", createRR.Code, createRR.Body.String())
	}
	var created s3UserCredentialsResponse
	if err := json.NewDecoder(createRR.Body).Decode(&created); err != nil {
		t.Fatalf("Decode create response: %v", err)
	}

	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/s3-users/"+created.AccessKey, strings.NewReader(`{"role":"admin"}`))
	updateReq.SetPathValue("accessKey", created.AccessKey)
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	updateRR := httptest.NewRecorder()
	srv.handleAPIUpdateS3User(updateRR, updateReq)
	if updateRR.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200, body=%s", updateRR.Code, updateRR.Body.String())
	}
	acct, err := iamSvc.GetUserAccount(created.AccessKey)
	if err != nil {
		t.Fatalf("GetUserAccount after role update: %v", err)
	}
	if acct.Role != auth.RoleAdmin {
		t.Fatalf("role = %q, want admin", acct.Role)
	}

	rotateReq := httptest.NewRequest(http.MethodPost, "/api/v1/s3-users/"+created.AccessKey+"/secret", strings.NewReader(`{}`))
	rotateReq.SetPathValue("accessKey", created.AccessKey)
	rotateReq.Header.Set("Content-Type", "application/json")
	rotateReq.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rotateRR := httptest.NewRecorder()
	srv.handleAPIRotateS3UserSecret(rotateRR, rotateReq)
	if rotateRR.Code != http.StatusOK {
		t.Fatalf("rotate status = %d, want 200, body=%s", rotateRR.Code, rotateRR.Body.String())
	}
	var rotated s3UserCredentialsResponse
	if err := json.NewDecoder(rotateRR.Body).Decode(&rotated); err != nil {
		t.Fatalf("Decode rotate response: %v", err)
	}
	if rotated.SecretKey == "" || rotated.SecretKey == created.SecretKey {
		t.Fatalf("rotated secret = %q, want non-empty value different from original", rotated.SecretKey)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/s3-users/"+created.AccessKey, nil)
	deleteReq.SetPathValue("accessKey", created.AccessKey)
	deleteReq.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	deleteRR := httptest.NewRecorder()
	srv.handleAPIDeleteS3User(deleteRR, deleteReq)
	if deleteRR.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204, body=%s", deleteRR.Code, deleteRR.Body.String())
	}
	if _, err := iamSvc.GetUserAccount(created.AccessKey); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Fatalf("GetUserAccount after delete error = %v, want ErrNoSuchUser", err)
	}
}

func TestS3UsersDeleteMissingUserReturnsNotFound(t *testing.T) {
	srv, _ := newS3UsersAPITestServer(t, "127.0.0.1:9090")

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/s3-users/missing-user", nil)
	req.SetPathValue("accessKey", "missing-user")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()
	srv.handleAPIDeleteS3User(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("delete missing status = %d, want 404, body=%s", rr.Code, rr.Body.String())
	}
}

func TestS3UsersRejectRootMutations(t *testing.T) {
	srv, _ := newS3UsersAPITestServer(t, "127.0.0.1:9090")

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
		call   func(http.ResponseWriter, *http.Request)
	}{
		{name: "update", method: http.MethodPut, path: "/api/v1/s3-users/root-access", body: `{"role":"user"}`, call: srv.handleAPIUpdateS3User},
		{name: "rotate", method: http.MethodPost, path: "/api/v1/s3-users/root-access/secret", body: `{}`, call: srv.handleAPIRotateS3UserSecret},
		{name: "delete", method: http.MethodDelete, path: "/api/v1/s3-users/root-access", call: srv.handleAPIDeleteS3User},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.SetPathValue("accessKey", "root-access")
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
			rr := httptest.NewRecorder()
			tc.call(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestS3UsersRequireLoopbackAndWriteHeader(t *testing.T) {
	nonLoopbackSrv, _ := newS3UsersAPITestServer(t, "0.0.0.0:9090")
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/s3-users", nil)
	getRR := httptest.NewRecorder()
	nonLoopbackSrv.handleAPIListS3Users(getRR, getReq)
	if getRR.Code != http.StatusForbidden {
		t.Fatalf("non-loopback GET status = %d, want 403", getRR.Code)
	}

	srv, _ := newS3UsersAPITestServer(t, "127.0.0.1:9090")
	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/s3-users", strings.NewReader(`{}`))
	postReq.Header.Set("Content-Type", "application/json")
	postRR := httptest.NewRecorder()
	srv.handleAPICreateS3User(postRR, postReq)
	if postRR.Code != http.StatusBadRequest {
		t.Fatalf("missing-header POST status = %d, want 400", postRR.Code)
	}
}

func TestS3UsersUnavailableAfterIAMDirChangeUntilRestart(t *testing.T) {
	cfg := validSettingsConfig(t)
	cfg.S3.IAMDir = t.TempDir()
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml"), Exists: true}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	settingsSvc, err := NewSettingsService(cfg, source)
	if err != nil {
		t.Fatalf("NewSettingsService: %v", err)
	}

	iamSvc, err := auth.NewInternal(auth.Account{
		Access: cfg.S3.AccessKey,
		Secret: cfg.S3.SecretKey,
		Role:   auth.RoleAdmin,
	}, cfg.S3.IAMDir)
	if err != nil {
		t.Fatalf("NewInternal: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })
	srv := &Server{
		addr:         "127.0.0.1:9090",
		settings:     settingsSvc,
		s3IAM:        iamSvc,
		s3RootAccess: cfg.S3.AccessKey,
		s3IAMDir:     cfg.S3.IAMDir,
		logger:       testLogger(),
	}
	if status := srv.decorateSettingsResponse(settingsSvc.Snapshot(true)).S3Users; !status.Available {
		t.Fatalf("initial S3Users = %#v, want available", status)
	}

	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"s3":{"iam_dir":"`+t.TempDir()+`"}}`))
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	updateRR := httptest.NewRecorder()
	srv.handleAPIUpdateSettings(updateRR, updateReq)
	if updateRR.Code != http.StatusOK {
		t.Fatalf("settings update status = %d, want 200, body=%s", updateRR.Code, updateRR.Body.String())
	}
	var updateResp settingsResponse
	if err := json.Unmarshal(updateRR.Body.Bytes(), &updateResp); err != nil {
		t.Fatalf("Unmarshal settings update response: %v", err)
	}
	if updateResp.S3Users.Available || strings.TrimSpace(updateResp.S3Users.Reason) == "" {
		t.Fatalf("updated S3Users = %#v, want unavailable with reason", updateResp.S3Users)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/s3-users", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()
	srv.handleAPICreateS3User(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("create after IAM dir change status = %d, want 409, body=%s", rr.Code, rr.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/s3-users", nil)
	getRR := httptest.NewRecorder()
	srv.handleAPIListS3Users(getRR, getReq)
	if getRR.Code != http.StatusConflict {
		t.Fatalf("list after IAM dir change status = %d, want 409, body=%s", getRR.Code, getRR.Body.String())
	}
}
