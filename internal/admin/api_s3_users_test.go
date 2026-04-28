package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/s3iam"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/versity/versitygw/auth"
)

type bucketOwnerCountSpy struct {
	repository.BucketRepository
	countByOwnerCalls       int
	aggregateCountsCalls    int
	aggregateCountsByAccess map[string]int
}

func (s *bucketOwnerCountSpy) CountByOwner(ctx context.Context, ownerAccessKey string) (int, error) {
	s.countByOwnerCalls++
	return 0, errors.New("CountByOwner should not be called")
}

func (s *bucketOwnerCountSpy) AggregateCountsByOwner(ctx context.Context) (map[string]int, error) {
	s.aggregateCountsCalls++
	counts := make(map[string]int, len(s.aggregateCountsByAccess))
	for accessKey, count := range s.aggregateCountsByAccess {
		counts[accessKey] = count
	}
	return counts, nil
}

func newS3UsersAPITestServer(t *testing.T, addr string) (*Server, auth.IAMService) {
	t.Helper()
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	iamSvc := s3iam.NewService(repos)
	root, err := iamSvc.EnsureRootAccount(t.Context())
	if err != nil {
		t.Fatalf("EnsureRootAccount: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })

	srv := &Server{
		addr:         addr,
		repos:        repos,
		s3IAM:        iamSvc,
		s3RootAccess: root.Access,
		logger:       testLogger(),
	}
	return srv, iamSvc
}

func newS3UsersAPITestServerWithRepos(t *testing.T, addr string) (*Server, auth.IAMService, *repository.Repositories) {
	t.Helper()
	srv, iamSvc := newS3UsersAPITestServer(t, addr)
	return srv, iamSvc, srv.repos
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
	var updated s3UserListItem
	if err := json.NewDecoder(updateRR.Body).Decode(&updated); err != nil {
		t.Fatalf("Decode update response: %v", err)
	}
	if updated.AccessKey != created.AccessKey {
		t.Fatalf("update response access_key = %q, want %q", updated.AccessKey, created.AccessKey)
	}
	if updated.Role != string(auth.RoleAdmin) {
		t.Fatalf("update response role = %q, want %q", updated.Role, auth.RoleAdmin)
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

func TestS3UsersListIncludesOwnedBucketCount(t *testing.T) {
	srv, iamSvc, repos := newS3UsersAPITestServerWithRepos(t, "127.0.0.1:9090")
	ctx := context.Background()
	for _, accessKey := range []string{"owner-a", "owner-b"} {
		if err := iamSvc.CreateAccount(auth.Account{Access: accessKey, Secret: "secret-" + accessKey, Role: auth.RoleUserPlus}); err != nil {
			t.Fatalf("CreateAccount(%s): %v", accessKey, err)
		}
	}
	for _, seed := range []struct {
		name  string
		owner string
	}{
		{name: "owner-a-one", owner: "owner-a"},
		{name: "owner-a-two", owner: "owner-a"},
		{name: "owner-b-one", owner: "owner-b"},
		{name: "unassigned"},
	} {
		bucket := &model.Bucket{Name: seed.name, Status: model.BucketStatusActive}
		if seed.owner != "" {
			data, err := json.Marshal(auth.ACL{Owner: seed.owner})
			if err != nil {
				t.Fatalf("Marshal ACL: %v", err)
			}
			bucket.ACL = data
			bucket.OwnerAccessKey = &seed.owner
		}
		if err := repos.Buckets.Create(ctx, bucket); err != nil {
			t.Fatalf("Buckets.Create(%s): %v", seed.name, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/s3-users", nil)
	rr := httptest.NewRecorder()
	srv.handleAPIListS3Users(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var users []s3UserListItem
	if err := json.NewDecoder(rr.Body).Decode(&users); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	counts := make(map[string]int, len(users))
	for _, user := range users {
		counts[user.AccessKey] = user.BucketCount
	}
	if counts["owner-a"] != 2 {
		t.Fatalf("owner-a bucket_count = %d, want 2", counts["owner-a"])
	}
	if counts["owner-b"] != 1 {
		t.Fatalf("owner-b bucket_count = %d, want 1", counts["owner-b"])
	}
}

func TestS3UsersListUsesAggregateBucketCounts(t *testing.T) {
	srv, iamSvc, repos := newS3UsersAPITestServerWithRepos(t, "127.0.0.1:9090")
	for _, account := range []auth.Account{
		{Access: "owner-a", Secret: "secret-owner-a", Role: auth.RoleUserPlus},
		{Access: "owner-b", Secret: "secret-owner-b", Role: auth.RoleUserPlus},
	} {
		if err := iamSvc.CreateAccount(account); err != nil {
			t.Fatalf("CreateAccount(%s): %v", account.Access, err)
		}
	}
	srv.repos.Buckets = &bucketOwnerCountSpy{
		BucketRepository: repos.Buckets,
		aggregateCountsByAccess: map[string]int{
			"owner-a": 2,
			"owner-b": 1,
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/s3-users", nil)
	rr := httptest.NewRecorder()
	srv.handleAPIListS3Users(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	spy := srv.repos.Buckets.(*bucketOwnerCountSpy)
	if spy.aggregateCountsCalls != 1 {
		t.Fatalf("AggregateCountsByOwner calls = %d, want 1", spy.aggregateCountsCalls)
	}
	if spy.countByOwnerCalls != 0 {
		t.Fatalf("CountByOwner calls = %d, want 0", spy.countByOwnerCalls)
	}
}

func TestS3UsersUpdateIncludesOwnedBucketCount(t *testing.T) {
	srv, iamSvc, repos := newS3UsersAPITestServerWithRepos(t, "127.0.0.1:9090")
	ctx := context.Background()
	if err := iamSvc.CreateAccount(auth.Account{Access: "owner-access", Secret: "owner-secret", Role: auth.RoleUser}); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	for _, name := range []string{"owned-one", "owned-two"} {
		data, err := json.Marshal(auth.ACL{Owner: "owner-access"})
		if err != nil {
			t.Fatalf("Marshal ACL: %v", err)
		}
		owner := "owner-access"
		if err := repos.Buckets.Create(ctx, &model.Bucket{Name: name, Status: model.BucketStatusActive, OwnerAccessKey: &owner, ACL: data}); err != nil {
			t.Fatalf("Buckets.Create(%s): %v", name, err)
		}
	}

	req := httptest.NewRequest(http.MethodPut, "/api/v1/s3-users/owner-access", strings.NewReader(`{"role":"userplus"}`))
	req.SetPathValue("accessKey", "owner-access")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()
	srv.handleAPIUpdateS3User(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var body s3UserListItem
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if body.BucketCount != 2 {
		t.Fatalf("bucket_count = %d, want 2", body.BucketCount)
	}
}

func TestS3UsersDeleteOwnedUserReturnsConflict(t *testing.T) {
	srv, iamSvc, repos := newS3UsersAPITestServerWithRepos(t, "127.0.0.1:9090")
	if err := iamSvc.CreateAccount(auth.Account{Access: "owner-access", Secret: "owner-secret", Role: auth.RoleUserPlus}); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	data, err := json.Marshal(auth.ACL{Owner: "owner-access"})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}
	owner := "owner-access"
	if err := repos.Buckets.Create(context.Background(), &model.Bucket{Name: "owned-bucket", Status: model.BucketStatusActive, OwnerAccessKey: &owner, ACL: data}); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/s3-users/owner-access", nil)
	req.SetPathValue("accessKey", "owner-access")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()
	srv.handleAPIDeleteS3User(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusConflict, rr.Body.String())
	}
	if _, err := iamSvc.GetUserAccount("owner-access"); err != nil {
		t.Fatalf("GetUserAccount after blocked delete: %v", err)
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

func TestS3UsersDeleteSucceedsAfterBucketsTransferredToRoot(t *testing.T) {
	srv, iamSvc, repos := newS3UsersAPITestServerWithRepos(t, "127.0.0.1:9090")
	if err := iamSvc.CreateAccount(auth.Account{Access: "delete-owner", Secret: "owner-secret", Role: auth.RoleUserPlus}); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	owner := "delete-owner"
	acl, err := json.Marshal(auth.ACL{Owner: owner})
	if err != nil {
		t.Fatalf("Marshal owner ACL: %v", err)
	}
	if err := repos.Buckets.Create(context.Background(), &model.Bucket{
		Name:           "owned-before-transfer",
		Status:         model.BucketStatusActive,
		OwnerAccessKey: &owner,
		ACL:            acl,
	}); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/s3-users/delete-owner", nil)
	req.SetPathValue("accessKey", "delete-owner")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()
	srv.handleAPIDeleteS3User(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want %d, body=%s", rr.Code, http.StatusConflict, rr.Body.String())
	}

	rootACL, err := json.Marshal(auth.ACL{Owner: srv.s3RootAccess})
	if err != nil {
		t.Fatalf("Marshal root ACL: %v", err)
	}
	if err := repos.Buckets.SetOwnerAndACL(context.Background(), "owned-before-transfer", &srv.s3RootAccess, rootACL); err != nil {
		t.Fatalf("SetOwnerAndACL root: %v", err)
	}
	rr = httptest.NewRecorder()
	srv.handleAPIDeleteS3User(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusNoContent, rr.Body.String())
	}
	count, err := srv.bucketOwnerCount(context.Background(), "delete-owner")
	if err != nil {
		t.Fatalf("bucketOwnerCount: %v", err)
	}
	if count != 0 {
		t.Fatalf("bucket count = %d, want 0", count)
	}
}

func TestS3UsersRejectRootMutations(t *testing.T) {
	srv, _ := newS3UsersAPITestServer(t, "127.0.0.1:9090")
	rootAccess := srv.s3RootAccess

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
		call   func(http.ResponseWriter, *http.Request)
	}{
		{name: "update", method: http.MethodPut, path: "/api/v1/s3-users/" + rootAccess, body: `{"role":"user"}`, call: srv.handleAPIUpdateS3User},
		{name: "rotate", method: http.MethodPost, path: "/api/v1/s3-users/" + rootAccess + "/secret", body: `{}`, call: srv.handleAPIRotateS3UserSecret},
		{name: "delete", method: http.MethodDelete, path: "/api/v1/s3-users/" + rootAccess, call: srv.handleAPIDeleteS3User},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.SetPathValue("accessKey", rootAccess)
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
