package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/adminsession"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/testutil"
)

type unavailableSessionStore struct{ adminsession.Store }

type waitingSessionStore struct {
	adminsession.Store
	entered chan time.Time
}

func (s waitingSessionStore) GetActive(ctx context.Context, _ string, _ time.Time) (*adminsession.Session, error) {
	deadline, _ := ctx.Deadline()
	s.entered <- deadline
	<-ctx.Done()
	return nil, ctx.Err()
}

var errSessionStoreUnavailable = errors.New("session database unavailable")

func (unavailableSessionStore) Create(context.Context, *adminsession.Session) error {
	return errSessionStoreUnavailable
}

func (unavailableSessionStore) GetActive(context.Context, string, time.Time) (*adminsession.Session, error) {
	return nil, errSessionStoreUnavailable
}

func (unavailableSessionStore) ExtendActive(context.Context, string, time.Time, time.Time) (bool, error) {
	return false, errSessionStoreUnavailable
}

func (unavailableSessionStore) Delete(context.Context, string) error {
	return errSessionStoreUnavailable
}

func TestAdminAuthDatabaseFailurePreservesCookie(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	token, claims, err := srv.auth.newSession(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	srv.auth.sessions = unavailableSessionStore{Store: srv.auth.sessions}
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	mux.HandleFunc("POST /api/v1/settings", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := srv.withAdminAuth(mux)
	for _, endpoint := range []struct{ method, path, body string }{
		{http.MethodGet, "/api/v1/auth/session", ""},
		{http.MethodPost, "/api/v1/auth/refresh", ""},
		{http.MethodPost, "/api/v1/auth/logout", ""},
		{http.MethodPost, "/api/v1/settings", ""},
		{http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"admin-password"}`},
	} {
		t.Run(endpoint.path, func(t *testing.T) {
			req := httptest.NewRequest(endpoint.method, endpoint.path, strings.NewReader(endpoint.body))
			req.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
			req.Header.Set(adminCSRFHeader, claims.CSRFToken)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable || len(rr.Result().Cookies()) != 0 {
				t.Fatalf("status=%d cookie count=%d; want 503 without Set-Cookie", rr.Code, len(rr.Result().Cookies()))
			}
		})
	}
}

func TestAdminAuthDatabaseValidationHasDeadlineAndHonorsCancellation(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	token, _, err := srv.auth.newSession(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan time.Time, 1)
	srv.auth.sessions = waitingSessionStore{Store: srv.auth.sessions, entered: entered}
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil).WithContext(ctx)
	req.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(done)
		srv.withAdminAuth(mux).ServeHTTP(rr, req)
	}()
	select {
	case deadline := <-entered:
		if deadline.IsZero() || deadline.After(started.Add(adminSessionStoreTimeout+time.Second)) {
			t.Fatalf("session validation deadline=%s", deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("session validation did not enter store")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session validation ignored request cancellation")
	}
	if rr.Code != http.StatusServiceUnavailable || len(rr.Result().Cookies()) != 0 {
		t.Fatalf("cancelled validation status=%d cookies=%v", rr.Code, rr.Result().Cookies())
	}
}

func TestAdminAuthLogoutExpiredTokenDeletesRefreshedFamily(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Now().UTC().Truncate(time.Second)
	srv.auth.now = func() time.Time { return now }
	token, claims, err := srv.auth.newSession(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	now = time.Unix(claims.ExpiresAt-1, 0)
	refreshedToken, _, ok, err := srv.auth.refreshSession(t.Context(), token)
	if err != nil || !ok || refreshedToken == "" {
		t.Fatalf("refresh valid=%t err=%v", ok, err)
	}
	now = time.Unix(claims.ExpiresAt, 0)
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	handler := srv.withAdminAuth(mux)
	logout := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	logout.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
	logout.Header.Set(adminCSRFHeader, claims.CSRFToken)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, logout)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expired signed logout=%d, want 204", rr.Code)
	}
	for range 2 {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, logout.Clone(t.Context()))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("idempotent logout=%d", rr.Code)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
	req.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: refreshedToken})
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("late refreshed cookie after logout=%d", rr.Code)
	}
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil))
	if rr.Code != http.StatusUnauthorized || len(rr.Result().Cookies()) != 0 {
		t.Fatalf("logout without identity=%d cookies=%v", rr.Code, rr.Result().Cookies())
	}
}

func TestAdminAuthSessionPersistenceAcrossServerRestart(t *testing.T) {
	database := testutil.NewTestDB(t)
	first := newTestAuthServer(t, "admin-password")
	first.auth.sessions = repository.NewRepositories(database).AdminSessions
	token, claims, err := first.auth.newSession(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	otherToken, _, err := first.auth.newSession(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	second := newTestAuthServer(t, "admin-password")
	second.auth.sessions = repository.NewRepositories(database).AdminSessions
	request := func(token string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
		r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
		return r
	}
	if _, valid, err := second.auth.sessionFromRequest(request(token)); err != nil || !valid {
		t.Fatalf("session after restart valid=%t err=%v", valid, err)
	}
	if err := second.auth.deleteSession(t.Context(), claims); err != nil {
		t.Fatal(err)
	}
	third := newTestAuthServer(t, "admin-password")
	third.auth.sessions = repository.NewRepositories(database).AdminSessions
	if _, valid, err := third.auth.sessionFromRequest(request(token)); err != nil || valid {
		t.Fatalf("logged out session after restart valid=%t err=%v", valid, err)
	}
	if _, valid, err := third.auth.sessionFromRequest(request(otherToken)); err != nil || !valid {
		t.Fatalf("unrelated session after restart valid=%t err=%v", valid, err)
	}
}

func TestAdminAuthSetupSessionsAreIsolated(t *testing.T) {
	normal := newTestAuthServer(t, "admin-password")
	cfg := config.AdminAuthConfig{Enabled: true, Username: "admin", PasswordHash: string(normal.auth.passwordHash), SessionSecret: "test-admin-session-secret", SessionTTL: time.Hour}
	first, restarted := &Server{setupOnly: true}, &Server{setupOnly: true}
	for _, server := range []*Server{first, restarted} {
		if err := server.WithAuthConfig(cfg); err != nil {
			t.Fatal(err)
		}
	}
	token, _, err := first.auth.newSession(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := first.auth.verifySession(token); !valid {
		t.Fatal("setup token rejected by its issuing process")
	}
	for _, server := range []*Server{restarted, normal} {
		if _, valid := server.auth.verifySession(token); valid {
			t.Fatal("setup token accepted outside its process")
		}
	}
	if err := (&Server{}).WithAuthConfig(cfg); err == nil {
		t.Fatal("normal authentication accepted missing session repository")
	}
}
