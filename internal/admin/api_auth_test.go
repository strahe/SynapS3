package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func TestAdminAuthMiddlewareProtectsAPIAndAllowsHealth(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	handler := srv.withAdminAuth(mux)

	healthRR := httptest.NewRecorder()
	handler.ServeHTTP(healthRR, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if healthRR.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", healthRR.Code)
	}

	apiRR := httptest.NewRecorder()
	handler.ServeHTTP(apiRR, httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil))
	if apiRR.Code != http.StatusUnauthorized {
		t.Fatalf("api status = %d, want 401", apiRR.Code)
	}

	basicReq := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
	basicReq.SetBasicAuth("admin", "admin-password")
	basicRR := httptest.NewRecorder()
	handler.ServeHTTP(basicRR, basicReq)
	if basicRR.Code != http.StatusOK {
		t.Fatalf("basic auth status = %d, want 200; body=%s", basicRR.Code, basicRR.Body.String())
	}
}

func TestAdminAuthMiddlewareProtectsCanonicalAPIPaths(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	handler := srv.withAdminAuth(mux)

	for _, target := range []string{"//api/v1/system/info", "/api//v1/system/info", "/api/v1/../v1/system/info", "/admin/../healthz"} {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s unauth status = %d, want 401", target, rr.Code)
		}
	}

	authReq := httptest.NewRequest(http.MethodGet, "//api/v1/system/info", nil)
	authReq.SetBasicAuth("admin", "admin-password")
	authRR := httptest.NewRecorder()
	handler.ServeHTTP(authRR, authReq)
	if authRR.Code != http.StatusTemporaryRedirect {
		t.Fatalf("authenticated dirty path status = %d, want 307", authRR.Code)
	}
	if got := authRR.Header().Get("Location"); got != "/api/v1/system/info" {
		t.Fatalf("authenticated dirty path location = %q, want /api/v1/system/info", got)
	}
}

func TestAdminAuthLoginSessionAndCSRF(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	loginNow := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return loginNow }
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	mux.HandleFunc("POST /api/v1/settings", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
	})
	handler := srv.withAdminAuth(mux)

	loginBody := strings.NewReader(`{"username":"admin","password":"admin-password"}`)
	loginRR := httptest.NewRecorder()
	handler.ServeHTTP(loginRR, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", loginBody))
	if loginRR.Code != http.StatusOK {
		t.Fatalf("login status = %d, want 200; body=%s", loginRR.Code, loginRR.Body.String())
	}
	var loginResp authSessionResponse
	if err := json.Unmarshal(loginRR.Body.Bytes(), &loginResp); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if loginResp.Username != "admin" || loginResp.CSRFToken == "" {
		t.Fatalf("login response = %#v, want username and csrf token", loginResp)
	}
	if want := loginNow.Add(adminSessionRefreshInterval).Format(time.RFC3339); loginResp.RefreshAfter != want {
		t.Fatalf("login refresh_after = %q, want %q", loginResp.RefreshAfter, want)
	}
	cookies := loginRR.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != adminSessionCookieName || !cookies[0].HttpOnly {
		t.Fatalf("login cookies = %#v, want HttpOnly admin session", cookies)
	}
	if cookies[0].MaxAge != 0 || !cookies[0].Expires.IsZero() {
		t.Fatalf("login cookie = %#v, want a non-persistent browser session cookie", cookies[0])
	}
	loginClaims, ok := srv.auth.verifySession(cookies[0].Value)
	if !ok {
		t.Fatal("standard login cookie did not contain a valid session")
	}
	if loginClaims.Remember || loginClaims.SessionID == "" {
		t.Fatalf("standard login claims = %#v, want standard mode and a session ID", loginClaims)
	}
	if got := sessionLifetime(loginClaims); got != srv.auth.sessionTTL {
		t.Fatalf("standard login lifetime = %s, want %s", got, srv.auth.sessionTTL)
	}
	if want := loginNow.Add(srv.auth.sessionTTL).Format(time.RFC3339); loginResp.ExpiresAt != want {
		t.Fatalf("standard login expires_at = %q, want %q", loginResp.ExpiresAt, want)
	}

	sessionReq := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
	sessionReq.AddCookie(cookies[0])
	sessionRR := httptest.NewRecorder()
	handler.ServeHTTP(sessionRR, sessionReq)
	if sessionRR.Code != http.StatusOK {
		t.Fatalf("session status = %d, want 200; body=%s", sessionRR.Code, sessionRR.Body.String())
	}
	var sessionResp authSessionResponse
	if err := json.Unmarshal(sessionRR.Body.Bytes(), &sessionResp); err != nil {
		t.Fatalf("decode session response: %v", err)
	}
	if sessionResp != loginResp {
		t.Fatalf("session response = %#v, want %#v", sessionResp, loginResp)
	}

	missingCSRFReq := httptest.NewRequest(http.MethodPost, "/api/v1/settings", bytes.NewReader([]byte(`{}`)))
	missingCSRFReq.AddCookie(cookies[0])
	missingCSRFRR := httptest.NewRecorder()
	handler.ServeHTTP(missingCSRFRR, missingCSRFReq)
	if missingCSRFRR.Code != http.StatusForbidden {
		t.Fatalf("missing csrf status = %d, want 403", missingCSRFRR.Code)
	}

	csrfReq := httptest.NewRequest(http.MethodPost, "/api/v1/settings", bytes.NewReader([]byte(`{}`)))
	csrfReq.AddCookie(cookies[0])
	csrfReq.Header.Set(adminCSRFHeader, loginResp.CSRFToken)
	csrfRR := httptest.NewRecorder()
	handler.ServeHTTP(csrfRR, csrfReq)
	if csrfRR.Code != http.StatusOK {
		t.Fatalf("csrf status = %d, want 200; body=%s", csrfRR.Code, csrfRR.Body.String())
	}

	tamperedReq := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
	tampered := *cookies[0]
	tampered.Value += "x"
	tamperedReq.AddCookie(&tampered)
	tamperedRR := httptest.NewRecorder()
	handler.ServeHTTP(tamperedRR, tamperedReq)
	if tamperedRR.Code != http.StatusUnauthorized {
		t.Fatalf("tampered session status = %d, want 401", tamperedRR.Code)
	}
	if cookies := tamperedRR.Result().Cookies(); len(cookies) != 1 || cookies[0].Name != adminSessionCookieName || cookies[0].MaxAge != -1 {
		t.Fatalf("tampered session cookies = %#v, want cleared admin session", cookies)
	}

	logoutNoSessionRR := httptest.NewRecorder()
	handler.ServeHTTP(logoutNoSessionRR, httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil))
	if logoutNoSessionRR.Code != http.StatusUnauthorized {
		t.Fatalf("logout without session status = %d, want 401", logoutNoSessionRR.Code)
	}

	logoutBasicReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	logoutBasicReq.SetBasicAuth("admin", "admin-password")
	logoutBasicRR := httptest.NewRecorder()
	handler.ServeHTTP(logoutBasicRR, logoutBasicReq)
	if logoutBasicRR.Code != http.StatusUnauthorized {
		t.Fatalf("logout basic auth status = %d, want 401", logoutBasicRR.Code)
	}

	logoutMissingCSRFReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	logoutMissingCSRFReq.AddCookie(cookies[0])
	logoutMissingCSRFRR := httptest.NewRecorder()
	handler.ServeHTTP(logoutMissingCSRFRR, logoutMissingCSRFReq)
	if logoutMissingCSRFRR.Code != http.StatusForbidden {
		t.Fatalf("logout missing csrf status = %d, want 403", logoutMissingCSRFRR.Code)
	}

	logoutReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	logoutReq.AddCookie(cookies[0])
	logoutReq.Header.Set(adminCSRFHeader, loginResp.CSRFToken)
	logoutRR := httptest.NewRecorder()
	handler.ServeHTTP(logoutRR, logoutReq)
	if logoutRR.Code != http.StatusNoContent {
		t.Fatalf("logout status = %d, want 204", logoutRR.Code)
	}
	if cookies := logoutRR.Result().Cookies(); len(cookies) != 1 || cookies[0].Name != adminSessionCookieName || cookies[0].MaxAge != -1 {
		t.Fatalf("logout cookies = %#v, want cleared admin session", cookies)
	}

	revokedSessionReq := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
	revokedSessionReq.AddCookie(cookies[0])
	revokedSessionRR := httptest.NewRecorder()
	handler.ServeHTTP(revokedSessionRR, revokedSessionReq)
	if revokedSessionRR.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session status = %d, want 401", revokedSessionRR.Code)
	}
	if cookies := revokedSessionRR.Result().Cookies(); len(cookies) != 1 || cookies[0].Name != adminSessionCookieName || cookies[0].MaxAge != -1 {
		t.Fatalf("revoked session cookies = %#v, want cleared admin session", cookies)
	}

	revokedLogoutReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	revokedLogoutReq.AddCookie(cookies[0])
	revokedLogoutReq.Header.Set(adminCSRFHeader, loginResp.CSRFToken)
	revokedLogoutRR := httptest.NewRecorder()
	handler.ServeHTTP(revokedLogoutRR, revokedLogoutReq)
	if revokedLogoutRR.Code != http.StatusUnauthorized {
		t.Fatalf("revoked logout status = %d, want 401", revokedLogoutRR.Code)
	}
}

func TestAdminAuthRememberedSessionRefreshAndFamilyLogout(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return now }
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	handler := srv.withAdminAuth(mux)

	loginRR := httptest.NewRecorder()
	handler.ServeHTTP(
		loginRR,
		httptest.NewRequest(
			http.MethodPost,
			"/api/v1/auth/login",
			strings.NewReader(`{"username":"admin","password":"admin-password","remember":true}`),
		),
	)
	if loginRR.Code != http.StatusOK {
		t.Fatalf("remembered login status = %d, want 200; body=%s", loginRR.Code, loginRR.Body.String())
	}
	var loginResp authSessionResponse
	if err := json.Unmarshal(loginRR.Body.Bytes(), &loginResp); err != nil {
		t.Fatalf("decode remembered login response: %v", err)
	}
	loginCookies := loginRR.Result().Cookies()
	if len(loginCookies) != 1 {
		t.Fatalf("remembered login cookies = %#v, want one session cookie", loginCookies)
	}
	loginCookie := loginCookies[0]
	if want := int(adminRememberedSessionTTL.Seconds()); loginCookie.MaxAge != want {
		t.Fatalf("remembered login max age = %d, want %d", loginCookie.MaxAge, want)
	}
	if want := now.Add(adminRememberedSessionTTL); !loginCookie.Expires.Equal(want) {
		t.Fatalf("remembered login expires = %s, want %s", loginCookie.Expires, want)
	}
	loginClaims, ok := srv.auth.verifySession(loginCookie.Value)
	if !ok {
		t.Fatal("remembered login cookie did not contain a valid session")
	}
	if !loginClaims.Remember || loginClaims.SessionID == "" {
		t.Fatalf("remembered login claims = %#v, want remember mode and a session ID", loginClaims)
	}
	if got := sessionLifetime(loginClaims); got != adminRememberedSessionTTL {
		t.Fatalf("remembered login lifetime = %s, want %s", got, adminRememberedSessionTTL)
	}

	basicRefreshReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	basicRefreshReq.SetBasicAuth("admin", "admin-password")
	basicRefreshRR := httptest.NewRecorder()
	handler.ServeHTTP(basicRefreshRR, basicRefreshReq)
	if basicRefreshRR.Code != http.StatusUnauthorized {
		t.Fatalf("Basic-auth refresh status = %d, want 401", basicRefreshRR.Code)
	}

	missingCSRFReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	missingCSRFReq.AddCookie(loginCookie)
	missingCSRFRR := httptest.NewRecorder()
	handler.ServeHTTP(missingCSRFRR, missingCSRFReq)
	if missingCSRFRR.Code != http.StatusForbidden {
		t.Fatalf("refresh without CSRF status = %d, want 403", missingCSRFRR.Code)
	}

	now = now.Add(adminSessionRefreshInterval - time.Second)
	earlyRefreshReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	earlyRefreshReq.AddCookie(loginCookie)
	earlyRefreshReq.Header.Set(adminCSRFHeader, loginResp.CSRFToken)
	earlyRefreshRR := httptest.NewRecorder()
	handler.ServeHTTP(earlyRefreshRR, earlyRefreshReq)
	if earlyRefreshRR.Code != http.StatusOK {
		t.Fatalf("early refresh status = %d, want 200; body=%s", earlyRefreshRR.Code, earlyRefreshRR.Body.String())
	}
	if cookies := earlyRefreshRR.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("early refresh cookies = %#v, want no Set-Cookie", cookies)
	}
	var earlyResp authSessionResponse
	if err := json.Unmarshal(earlyRefreshRR.Body.Bytes(), &earlyResp); err != nil {
		t.Fatalf("decode early refresh response: %v", err)
	}
	if earlyResp != loginResp {
		t.Fatalf("early refresh response = %#v, want %#v", earlyResp, loginResp)
	}

	srv.auth.sessionTTL = 45 * 24 * time.Hour
	now = now.Add(time.Second)
	refreshNow := now
	refreshReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	refreshReq.AddCookie(loginCookie)
	refreshReq.Header.Set(adminCSRFHeader, loginResp.CSRFToken)
	refreshRR := httptest.NewRecorder()
	handler.ServeHTTP(refreshRR, refreshReq)
	if refreshRR.Code != http.StatusOK {
		t.Fatalf("refresh status = %d, want 200; body=%s", refreshRR.Code, refreshRR.Body.String())
	}
	var refreshResp authSessionResponse
	if err := json.Unmarshal(refreshRR.Body.Bytes(), &refreshResp); err != nil {
		t.Fatalf("decode refresh response: %v", err)
	}
	refreshCookies := refreshRR.Result().Cookies()
	if len(refreshCookies) != 1 {
		t.Fatalf("refresh cookies = %#v, want one renewed session cookie", refreshCookies)
	}
	refreshCookie := refreshCookies[0]
	if refreshCookie.Value == loginCookie.Value {
		t.Fatal("refresh reused the previous session token")
	}
	if want := int(adminRememberedSessionTTL.Seconds()); refreshCookie.MaxAge != want {
		t.Fatalf("refreshed max age = %d, want %d", refreshCookie.MaxAge, want)
	}
	if want := refreshNow.Add(adminRememberedSessionTTL); !refreshCookie.Expires.Equal(want) {
		t.Fatalf("refreshed expires = %s, want %s", refreshCookie.Expires, want)
	}
	refreshClaims, ok := srv.auth.verifySession(refreshCookie.Value)
	if !ok {
		t.Fatal("refreshed cookie did not contain a valid session")
	}
	if refreshClaims.CSRFToken != loginClaims.CSRFToken ||
		refreshClaims.SessionID != loginClaims.SessionID ||
		refreshClaims.Remember != loginClaims.Remember {
		t.Fatalf("refreshed claims = %#v, want preserved session family, CSRF, and remember mode", refreshClaims)
	}
	if got := sessionLifetime(refreshClaims); got != adminRememberedSessionTTL {
		t.Fatalf("refreshed lifetime = %s, want %s", got, adminRememberedSessionTTL)
	}
	if want := refreshNow.Add(adminSessionRefreshInterval).Format(time.RFC3339); refreshResp.RefreshAfter != want {
		t.Fatalf("refreshed refresh_after = %q, want %q", refreshResp.RefreshAfter, want)
	}

	oldSessionReq := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
	oldSessionReq.AddCookie(loginCookie)
	oldSessionRR := httptest.NewRecorder()
	handler.ServeHTTP(oldSessionRR, oldSessionReq)
	if oldSessionRR.Code != http.StatusOK {
		t.Fatalf("old family token before logout status = %d, want 200", oldSessionRR.Code)
	}

	now = now.Add(time.Second)
	logoutReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	logoutReq.AddCookie(refreshCookie)
	logoutReq.Header.Set(adminCSRFHeader, refreshResp.CSRFToken)
	logoutRR := httptest.NewRecorder()
	handler.ServeHTTP(logoutRR, logoutReq)
	if logoutRR.Code != http.StatusNoContent {
		t.Fatalf("refreshed logout status = %d, want 204; body=%s", logoutRR.Code, logoutRR.Body.String())
	}

	for name, cookie := range map[string]*http.Cookie{
		"original":  loginCookie,
		"refreshed": refreshCookie,
	} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
		req.AddCookie(cookie)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s family token after logout status = %d, want 401", name, rr.Code)
		}
	}
}

func TestAdminAuthRememberedSessionUsesLongerConfiguredTTL(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	srv.auth.sessionTTL = 45 * 24 * time.Hour
	start := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return start }

	_, claims, err := srv.auth.newSession(true)
	if err != nil {
		t.Fatalf("new remembered session: %v", err)
	}
	if got := sessionLifetime(claims); got != srv.auth.sessionTTL {
		t.Fatalf("remembered session lifetime = %s, want configured %s", got, srv.auth.sessionTTL)
	}
}

func TestAdminAuthRefreshRejectsExpiredSession(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return now }
	token, claims, err := srv.auth.newSession(false)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	handler := srv.withAdminAuth(mux)

	now = time.Unix(claims.ExpiresAt, 0).UTC()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	req.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
	req.Header.Set(adminCSRFHeader, claims.CSRFToken)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expired refresh status = %d, want 401", rr.Code)
	}
	if cookies := rr.Result().Cookies(); len(cookies) != 1 || cookies[0].MaxAge != -1 {
		t.Fatalf("expired refresh cookies = %#v, want cleared session cookie", cookies)
	}
}

func TestAdminAuthRefreshBridgesLegacySessionFamily(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	start := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	legacyClaims := authSessionClaims{
		Username:  "admin",
		IssuedAt:  start.Unix(),
		ExpiresAt: start.Add(time.Hour).Unix(),
		CSRFToken: "legacy-csrf-token",
	}
	legacyToken, err := srv.auth.signClaims(legacyClaims)
	if err != nil {
		t.Fatalf("sign legacy claims: %v", err)
	}

	now := start.Add(adminSessionRefreshInterval)
	srv.auth.now = func() time.Time { return now }
	refreshedToken, refreshedClaims, ok, err := srv.auth.refreshSession(legacyToken)
	if err != nil {
		t.Fatalf("refresh legacy session: %v", err)
	}
	if !ok || refreshedToken == "" {
		t.Fatalf("refresh legacy session = ok %t, token present %t; want both true", ok, refreshedToken != "")
	}
	if want := srv.auth.sessionTokenHash(legacyToken); refreshedClaims.SessionID != want {
		t.Fatalf("legacy refresh session ID = %q, want token fingerprint %q", refreshedClaims.SessionID, want)
	}
	if refreshedClaims.Remember {
		t.Fatal("legacy refresh enabled remember mode")
	}
	if !srv.auth.revokeSession(refreshedToken, refreshedClaims) {
		t.Fatal("revoke refreshed legacy family = false, want true")
	}
	if !srv.auth.sessionRevoked(legacyToken, legacyClaims, now) {
		t.Fatal("legacy token remained valid after its refreshed family was revoked")
	}
	if !srv.auth.sessionRevoked(refreshedToken, refreshedClaims, now) {
		t.Fatal("refreshed legacy token remained valid after family revocation")
	}
}

func TestAdminAuthRefreshAndLogoutAreAtomicallyOrdered(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return now }

	for i := 0; i < 50; i++ {
		issuedAt := now.Add(time.Duration(i) * time.Hour)
		now = issuedAt
		token, claims, err := srv.auth.newSession(false)
		if err != nil {
			t.Fatalf("new session %d: %v", i, err)
		}
		now = issuedAt.Add(adminSessionRefreshInterval)

		var refreshedToken string
		var refreshedClaims authSessionClaims
		var refreshErr error
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			refreshedToken, refreshedClaims, _, refreshErr = srv.auth.refreshSession(token)
		}()
		go func() {
			defer wg.Done()
			<-start
			if !srv.auth.revokeSession(token, claims) {
				t.Errorf("revoke session %d = false, want true", i)
			}
		}()
		close(start)
		wg.Wait()
		if refreshErr != nil {
			t.Fatalf("refresh session %d: %v", i, refreshErr)
		}
		if refreshedToken != "" {
			key := srv.auth.sessionRevocationKey(refreshedClaims.SessionID)
			revokedUntil := srv.auth.revokedSessions[key]
			refreshedExpiresAt := time.Unix(refreshedClaims.ExpiresAt, 0)
			if revokedUntil.Before(refreshedExpiresAt) {
				t.Fatalf(
					"revoked session %d until %s, before refreshed expiry %s",
					i,
					revokedUntil,
					refreshedExpiresAt,
				)
			}
		}

		for name, candidate := range map[string]string{
			"original":  token,
			"refreshed": refreshedToken,
		} {
			if candidate == "" {
				continue
			}
			req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
			req.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: candidate})
			if _, ok := srv.auth.sessionFromRequest(req); ok {
				t.Fatalf("%s session %d remained valid after concurrent logout", name, i)
			}
		}
	}
}

func TestAdminAuthLoginLimitsRequestBody(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	handler := srv.withAdminAuth(mux)

	body := strings.NewReader(`{"username":"admin","password":"` + strings.Repeat("x", 11*1024) + `"}`)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("oversized login status = %d, want 400", rr.Code)
	}
}

func TestAdminAuthBasicAuthRejectsCrossSiteBrowserUnsafeRequests(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	mux.HandleFunc("POST /api/v1/settings", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
	})
	handler := srv.withAdminAuth(mux)

	if err := srv.WithTrustedProxies([]string{"10.0.0.0/24"}); err != nil {
		t.Fatalf("WithTrustedProxies: %v", err)
	}

	for _, tc := range []struct {
		name       string
		method     string
		target     string
		host       string
		remoteAddr string
		headers    map[string]string
		wantStatus int
	}{
		{
			name:       "cli unsafe request without browser headers",
			method:     http.MethodPost,
			target:     "/api/v1/settings",
			wantStatus: http.StatusOK,
		},
		{
			name:       "cross-site fetch metadata",
			method:     http.MethodPost,
			target:     "http://admin.example.test/api/v1/settings",
			host:       "admin.example.test",
			headers:    map[string]string{"Sec-Fetch-Site": "cross-site"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "cross-site safe method",
			method:     http.MethodGet,
			target:     "http://admin.example.test/api/v1/system/info",
			host:       "admin.example.test",
			headers:    map[string]string{"Sec-Fetch-Site": "cross-site"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "cross-origin header",
			method:     http.MethodPost,
			target:     "http://admin.example.test/api/v1/settings",
			host:       "admin.example.test",
			headers:    map[string]string{"Origin": "https://evil.example.test"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "same origin header",
			method:     http.MethodPost,
			target:     "http://admin.example.test/api/v1/settings",
			host:       "admin.example.test",
			headers:    map[string]string{"Origin": "http://admin.example.test"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "trusted forwarded host",
			method:     http.MethodPost,
			target:     "http://127.0.0.1:8080/api/v1/settings",
			host:       "127.0.0.1:8080",
			remoteAddr: "10.0.0.7:1234",
			headers: map[string]string{
				"Origin":            "https://admin.example.test",
				"X-Forwarded-Host":  "admin.example.test",
				"X-Forwarded-Proto": "https",
				"X-Forwarded-For":   "198.51.100.23",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "trusted appended forwarded proto",
			method:     http.MethodPost,
			target:     "http://127.0.0.1:8080/api/v1/settings",
			host:       "127.0.0.1:8080",
			remoteAddr: "10.0.0.7:1234",
			headers: map[string]string{
				"Origin":            "https://admin.example.test",
				"X-Forwarded-Host":  "admin.example.test",
				"X-Forwarded-Proto": "http, https",
				"X-Forwarded-For":   "198.51.100.24",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "untrusted forwarded host",
			method:     http.MethodPost,
			target:     "http://127.0.0.1:8080/api/v1/settings",
			host:       "127.0.0.1:8080",
			remoteAddr: "203.0.113.40:1234",
			headers: map[string]string{
				"Origin":            "https://admin.example.test",
				"X-Forwarded-Host":  "admin.example.test",
				"X-Forwarded-Proto": "https",
			},
			wantStatus: http.StatusForbidden,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body *bytes.Reader
			if unsafeMethod(tc.method) {
				body = bytes.NewReader([]byte(`{}`))
			} else {
				body = bytes.NewReader(nil)
			}
			req := httptest.NewRequest(tc.method, tc.target, body)
			if tc.host != "" {
				req.Host = tc.host
			}
			if tc.remoteAddr != "" {
				req.RemoteAddr = tc.remoteAddr
			}
			for name, value := range tc.headers {
				req.Header.Set(name, value)
			}
			req.SetBasicAuth("admin", "admin-password")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tc.wantStatus, rr.Body.String())
			}
		})
	}
}

func TestAdminAuthFailureLimitLogoutExpiryAndLegacyProtection(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "metrics"})
	})
	mux.HandleFunc("GET /admin/exhausted-tasks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "legacy"})
	})
	handler := srv.withAdminAuth(mux)

	for i := 0; i < 5; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
		req.RemoteAddr = "203.0.113.10:1000"
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("login attempt %d status = %d, want 401", i+1, rr.Code)
		}
	}
	limitedRR := httptest.NewRecorder()
	limitedReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
	limitedReq.RemoteAddr = "203.0.113.10:1000"
	handler.ServeHTTP(limitedRR, limitedReq)
	if limitedRR.Code != http.StatusTooManyRequests {
		t.Fatalf("limited login status = %d, want 429", limitedRR.Code)
	}

	for _, path := range []string{"/metrics", "/admin/exhausted-tasks"} {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s unauth status = %d, want 401", path, rr.Code)
		}

		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "203.0.113.11:1000"
		req.SetBasicAuth("admin", "admin-password")
		basicRR := httptest.NewRecorder()
		handler.ServeHTTP(basicRR, req)
		if basicRR.Code != http.StatusOK {
			t.Fatalf("%s basic status = %d, want 200", path, basicRR.Code)
		}
	}

	start := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return start }
	token, _, err := srv.auth.newSession(false)
	if err != nil {
		t.Fatalf("newSession: %v", err)
	}
	srv.auth.now = func() time.Time { return start.Add(2 * time.Hour) }
	expiredReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	expiredReq.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
	expiredRR := httptest.NewRecorder()
	handler.ServeHTTP(expiredRR, expiredReq)
	if expiredRR.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status = %d, want 401", expiredRR.Code)
	}
	if cookies := expiredRR.Result().Cookies(); len(cookies) != 1 || cookies[0].Name != adminSessionCookieName || cookies[0].MaxAge != -1 {
		t.Fatalf("expired session cookies = %#v, want cleared admin session", cookies)
	}
}

func TestAdminAuthBasicAuthFailuresAreLimited(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	handler := srv.withAdminAuth(mux)

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
		req.RemoteAddr = "203.0.113.20:1000"
		req.SetBasicAuth("admin", "wrong")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("basic attempt %d status = %d, want 401", i+1, rr.Code)
		}
	}

	limitedReq := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
	limitedReq.RemoteAddr = "203.0.113.20:1000"
	limitedReq.SetBasicAuth("admin", "wrong")
	limitedRR := httptest.NewRecorder()
	handler.ServeHTTP(limitedRR, limitedReq)
	if limitedRR.Code != http.StatusTooManyRequests {
		t.Fatalf("limited basic status = %d, want 429", limitedRR.Code)
	}
}

func TestAdminAuthBasicAuthAllowsParallelValidRequests(t *testing.T) {
	srv := newTestAuthServerWithCost(t, "admin-password", bcrypt.MinCost+4)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	handler := srv.withAdminAuth(mux)

	start := make(chan struct{})
	statuses := make(chan int, loginFailureLimit+1)
	var wg sync.WaitGroup
	for i := 0; i < loginFailureLimit+1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
			req.RemoteAddr = "203.0.113.21:1000"
			req.SetBasicAuth("admin", "admin-password")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			statuses <- rr.Code
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)

	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("parallel valid basic auth status = %d, want 200", code)
		}
	}
}

func TestAdminAuthBasicAuthPasswordLockDoesNotCoverProtectedHandler(t *testing.T) {
	srv := newTestAuthServerWithCost(t, "admin-password", bcrypt.MinCost+4)
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		if passwordLockHeldForTest(srv.auth.passwordLocks, loginFailureKey("203.0.113.22")) {
			t.Error("basic auth password lock was still held while serving protected handler")
			http.Error(w, "password lock held during protected handler", http.StatusGatewayTimeout)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	handler := srv.withAdminAuth(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
	req.RemoteAddr = "203.0.113.22:1000"
	req.SetBasicAuth("admin", "admin-password")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("basic auth status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
}

func passwordLockHeldForTest(locks *keyedLockSet, key string) bool {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	lock := locks.locks[key]
	if lock == nil {
		return false
	}
	if lock.mu.TryLock() {
		lock.mu.Unlock()
		return false
	}
	return true
}

func TestAdminAuthSerializesConcurrentPasswordChecksByClientIP(t *testing.T) {
	srv := newTestAuthServerWithCost(t, "admin-password", bcrypt.MinCost+4)
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	handler := srv.withAdminAuth(mux)

	start := make(chan struct{})
	statuses := make(chan int, loginFailureLimit+1)
	var wg sync.WaitGroup
	for i := 0; i < loginFailureLimit+1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
			req.RemoteAddr = "203.0.113.23:1000"
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			statuses <- rr.Code
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)

	counts := map[int]int{}
	for code := range statuses {
		counts[code]++
	}
	if counts[http.StatusUnauthorized] != loginFailureLimit || counts[http.StatusTooManyRequests] != 1 {
		t.Fatalf("parallel wrong login statuses = %#v, want %d x 401 and 1 x 429", counts, loginFailureLimit)
	}
}

func TestAdminAuthBasicAuthCachesSuccessfulCredentialsBriefly(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	handler := srv.withAdminAuth(mux)
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
	req.RemoteAddr = "203.0.113.22:1000"
	req.SetBasicAuth("admin", "admin-password")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("first basic auth status = %d, want 200", rr.Code)
	}

	cachedReq := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
	cachedReq.RemoteAddr = "203.0.113.22:1000"
	cachedReq.SetBasicAuth("admin", "admin-password")
	cachedRR := httptest.NewRecorder()
	handler.ServeHTTP(cachedRR, cachedReq)
	if cachedRR.Code != http.StatusOK {
		t.Fatalf("cached basic auth status = %d, want 200", cachedRR.Code)
	}
}

func TestAdminAuthBasicAuthCacheMissesAfterPasswordHashChanges(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/system/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "test"})
	})
	handler := srv.withAdminAuth(mux)
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
	req.RemoteAddr = "203.0.113.26:1000"
	req.SetBasicAuth("admin", "admin-password")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("first basic auth status = %d, want 200", rr.Code)
	}

	replacementHash, err := bcrypt.GenerateFromPassword([]byte("rotated-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword replacement: %v", err)
	}
	srv.auth.passwordHash = replacementHash

	expiredReq := httptest.NewRequest(http.MethodGet, "/api/v1/system/info", nil)
	expiredReq.RemoteAddr = "203.0.113.26:1000"
	expiredReq.SetBasicAuth("admin", "admin-password")
	expiredRR := httptest.NewRecorder()
	handler.ServeHTTP(expiredRR, expiredReq)
	if expiredRR.Code != http.StatusUnauthorized {
		t.Fatalf("rotated password old basic auth status = %d, want 401", expiredRR.Code)
	}
}

func TestAdminAuthBasicAuthCacheCleanupIsThrottled(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	key := srv.auth.basicAuthCacheKey("203.0.113.24", "admin", "admin-password")
	srv.auth.basicAuthCache = map[string]time.Time{
		key:       now.Add(2 * time.Minute),
		"expired": now.Add(-time.Minute),
	}
	srv.auth.basicAuthNextCleanup = now.Add(time.Minute)

	if !srv.auth.basicAuthCacheHit("203.0.113.24", "admin", "admin-password", now) {
		t.Fatal("basic auth cache hit = false, want true")
	}
	if _, ok := srv.auth.basicAuthCache["expired"]; !ok {
		t.Fatal("expired unrelated cache entry was cleaned before next cleanup")
	}

	if !srv.auth.basicAuthCacheHit("203.0.113.24", "admin", "admin-password", now.Add(time.Minute)) {
		t.Fatal("basic auth cache hit after cleanup = false, want true")
	}
	if _, ok := srv.auth.basicAuthCache["expired"]; ok {
		t.Fatal("expired cache entry was not cleaned after next cleanup")
	}
}

func TestAdminAuthBasicAuthCacheSkipsNewEntryWhenFull(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.basicAuthCache = map[string]time.Time{}
	for i := 0; i < basicAuthCacheMaxItems; i++ {
		srv.auth.basicAuthCache[strconv.Itoa(i)] = now.Add(time.Duration(i+1) * time.Minute)
	}
	srv.auth.basicAuthNextCleanup = now.Add(time.Minute)

	srv.auth.rememberBasicAuth("203.0.113.25", "admin", "admin-password", now)
	newKey := srv.auth.basicAuthCacheKey("203.0.113.25", "admin", "admin-password")
	if _, ok := srv.auth.basicAuthCache[newKey]; !ok {
		t.Fatal("full basic auth cache did not record a new key")
	}
	if _, ok := srv.auth.basicAuthCache["0"]; ok {
		t.Fatal("full basic auth cache did not evict the oldest key")
	}
	if len(srv.auth.basicAuthCache) != basicAuthCacheMaxItems {
		t.Fatalf("basic auth cache entries = %d, want %d", len(srv.auth.basicAuthCache), basicAuthCacheMaxItems)
	}
}

func TestAdminAuthFailuresAreLimitedByClientIPBeforePasswordCheck(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	mux := http.NewServeMux()
	srv.registerAuthRoutes(mux)
	handler := srv.withAdminAuth(mux)

	for i := 0; i < loginFailureLimit; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"user-`+strconv.Itoa(i)+`","password":"wrong"}`))
		req.RemoteAddr = "203.0.113.30:1000"
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("login attempt %d status = %d, want 401", i+1, rr.Code)
		}
	}

	limitedReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"another-user","password":"wrong"}`))
	limitedReq.RemoteAddr = "203.0.113.30:1000"
	limitedRR := httptest.NewRecorder()
	handler.ServeHTTP(limitedRR, limitedReq)
	if limitedRR.Code != http.StatusTooManyRequests {
		t.Fatalf("limited login status = %d, want 429", limitedRR.Code)
	}

	start := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return start }
	srv.auth.limiter = newLoginFailureLimiter()
	for i := 0; i < loginFailureMaxEntries; i++ {
		srv.auth.limiter.failures["blocked-"+strconv.Itoa(i)] = loginFailure{
			Count:     loginFailureLimit,
			BlockedAt: start,
			LastSeen:  start,
		}
	}

	fullCorrectReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"admin-password"}`))
	fullCorrectReq.RemoteAddr = "203.0.113.31:1000"
	fullCorrectRR := httptest.NewRecorder()
	handler.ServeHTTP(fullCorrectRR, fullCorrectReq)
	if fullCorrectRR.Code != http.StatusTooManyRequests {
		t.Fatalf("full limiter correct login status = %d, want 429; body=%s", fullCorrectRR.Code, fullCorrectRR.Body.String())
	}

	fullWrongReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
	fullWrongReq.RemoteAddr = "203.0.113.32:1000"
	fullWrongRR := httptest.NewRecorder()
	handler.ServeHTTP(fullWrongRR, fullWrongReq)
	if fullWrongRR.Code != http.StatusTooManyRequests {
		t.Fatalf("full limiter wrong login status = %d, want 429", fullWrongRR.Code)
	}
	if _, ok := srv.auth.limiter.failures["203.0.113.32"]; ok {
		t.Fatal("full limiter recorded a new failed login key")
	}
}

func TestLoginFailureLimiterResetsExpiredBlocks(t *testing.T) {
	limiter := newLoginFailureLimiter()
	start := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	key := "127.0.0.1"

	blockLoginFailureKey(t, limiter, key, start)
	if limiter.Begin(key, start) {
		t.Fatal("Begin() before block expiry = true, want false")
	}

	afterBlock := start.Add(2 * time.Minute)
	recordLoginFailure(t, limiter, key, afterBlock)
	if !limiter.Begin(key, afterBlock) {
		t.Fatal("Begin() after one post-expiry failure = false, want true")
	}
}

func TestLoginFailureLimiterExpiresAndRejectsNewKeysWhenBounded(t *testing.T) {
	limiter := newLoginFailureLimiter()
	start := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	recordLoginFailure(t, limiter, "old", start)
	recordLoginFailure(t, limiter, "new", start.Add(11*time.Minute))
	if _, ok := limiter.failures["old"]; ok {
		t.Fatal("expired failure key was not removed")
	}
	if _, ok := limiter.failures["new"]; !ok {
		t.Fatal("new failure key was removed")
	}

	limiter = newLoginFailureLimiter()
	for i := 0; i < loginFailureMaxEntries; i++ {
		if !limiter.Begin(strconv.Itoa(i), start.Add(time.Duration(i)*time.Millisecond)) {
			t.Fatalf("Begin(%d) = false, want true", i)
		}
	}
	if !limiter.Begin("new-client", start.Add(time.Second)) {
		t.Fatal("Begin() with a full limiter = false, want true")
	}
	limiter.RecordFailure("new-client", start.Add(time.Second))
	if _, ok := limiter.failures["new-client"]; !ok {
		t.Fatal("new key was not recorded after bounded eviction")
	}
	if len(limiter.failures) != loginFailureMaxEntries {
		t.Fatalf("failure entries = %d, want %d", len(limiter.failures), loginFailureMaxEntries)
	}
	if _, ok := limiter.failures["0"]; ok {
		t.Fatal("oldest evictable key was not evicted")
	}
}

func TestLoginFailureLimiterKeepsActiveBlockedEntriesWhenBounded(t *testing.T) {
	limiter := newLoginFailureLimiter()
	start := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	blockedKey := "198.51.100.10"
	blockLoginFailureKey(t, limiter, blockedKey, start)
	if limiter.Begin(blockedKey, start) {
		t.Fatal("blocked key Begin() = true, want false")
	}

	for i := 0; i < loginFailureMaxEntries+1; i++ {
		limiter.Begin("noise-"+strconv.Itoa(i), start.Add(time.Duration(i+1)*time.Millisecond))
	}
	if _, ok := limiter.failures[blockedKey]; !ok {
		t.Fatal("active blocked key was evicted before block expiry")
	}
	if len(limiter.failures) != loginFailureMaxEntries {
		t.Fatalf("failure entries = %d, want %d", len(limiter.failures), loginFailureMaxEntries)
	}
}

func TestLoginFailureLimiterRefusesNewKeysWhenAllEntriesAreActiveBlocks(t *testing.T) {
	limiter := newLoginFailureLimiter()
	start := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	for i := 0; i < loginFailureMaxEntries; i++ {
		blockLoginFailureKey(t, limiter, "blocked-"+strconv.Itoa(i), start.Add(time.Duration(i)*time.Millisecond))
	}

	if limiter.Begin("new-client", start.Add(time.Second)) {
		t.Fatal("Begin() for new key with all active blocks = true, want false")
	}
	if len(limiter.failures) != loginFailureMaxEntries {
		t.Fatalf("failure entries = %d, want %d", len(limiter.failures), loginFailureMaxEntries)
	}
	if _, ok := limiter.failures["blocked-0"]; !ok {
		t.Fatal("active blocked key was evicted")
	}
}

func TestRevokedSessionCleanupIsThrottled(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	token := "session-token"
	claims := authSessionClaims{SessionID: "session-id"}
	key := srv.auth.sessionRevocationKey(claims.SessionID)
	srv.auth.revokedSessions = map[string]time.Time{
		key:       now.Add(time.Hour),
		"expired": now.Add(-time.Minute),
	}
	srv.auth.revokedNextCleanup = now.Add(time.Minute)

	if !srv.auth.sessionRevoked(token, claims, now) {
		t.Fatal("sessionRevoked() = false, want true")
	}
	if _, ok := srv.auth.revokedSessions["expired"]; !ok {
		t.Fatal("expired unrelated revoked session was cleaned before next cleanup")
	}

	if !srv.auth.sessionRevoked(token, claims, now.Add(time.Minute)) {
		t.Fatal("sessionRevoked() after cleanup = false, want true")
	}
	if _, ok := srv.auth.revokedSessions["expired"]; ok {
		t.Fatal("expired revoked session was not cleaned after next cleanup")
	}
}

func TestRevokedSessionMapRefusesNewSessionWhenBounded(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	srv.auth.now = func() time.Time { return now }
	srv.auth.revokedSessions = map[string]time.Time{}
	for i := 0; i < revokedSessionMaxItems; i++ {
		srv.auth.revokedSessions[strconv.Itoa(i)] = now.Add(time.Duration(i+1) * time.Minute)
	}
	srv.auth.revokedNextCleanup = now.Add(time.Minute)

	claims := authSessionClaims{
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(2 * time.Hour).Unix(),
		SessionID: "new-session",
	}
	if srv.auth.revokeSession("new-token", claims) {
		t.Fatal("revokeSession() with a full active map = true, want false")
	}
	newKey := srv.auth.sessionRevocationKey(claims.SessionID)
	if _, ok := srv.auth.revokedSessions[newKey]; ok {
		t.Fatal("new revoked session was recorded when the map was full")
	}
	if _, ok := srv.auth.revokedSessions["0"]; !ok {
		t.Fatal("active revoked session was evicted")
	}
	if len(srv.auth.revokedSessions) != revokedSessionMaxItems {
		t.Fatalf("revoked sessions = %d, want %d", len(srv.auth.revokedSessions), revokedSessionMaxItems)
	}
}

func TestAdminTrustedProxiesResolveForwardedClientIP(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	if err := srv.WithTrustedProxies([]string{"10.0.0.0/24"}); err != nil {
		t.Fatalf("WithTrustedProxies: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	req.RemoteAddr = "10.0.0.7:1234"
	req.Header.Set("X-Forwarded-For", "198.51.100.23, 10.0.0.8")
	if got := srv.clientIP(req); got != "198.51.100.23" {
		t.Fatalf("clientIP trusted XFF = %q, want 198.51.100.23", got)
	}

	withPortReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	withPortReq.RemoteAddr = "10.0.0.7:1234"
	withPortReq.Header.Set("X-Forwarded-For", "198.51.100.24:50123, 10.0.0.8")
	if got := srv.clientIP(withPortReq); got != "198.51.100.24" {
		t.Fatalf("clientIP trusted XFF with port = %q, want 198.51.100.24", got)
	}

	untrustedReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	untrustedReq.RemoteAddr = "203.0.113.10:1234"
	untrustedReq.Header.Set("X-Forwarded-For", "198.51.100.23")
	if got := srv.clientIP(untrustedReq); got != "203.0.113.10" {
		t.Fatalf("clientIP untrusted XFF = %q, want 203.0.113.10", got)
	}

	realIPReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	realIPReq.RemoteAddr = "10.0.0.7:1234"
	realIPReq.Header.Set("X-Real-IP", "198.51.100.25")
	if got := srv.clientIP(realIPReq); got != "198.51.100.25" {
		t.Fatalf("clientIP trusted X-Real-IP = %q, want 198.51.100.25", got)
	}

	untrustedRealIPReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	untrustedRealIPReq.RemoteAddr = "203.0.113.10:1234"
	untrustedRealIPReq.Header.Set("X-Real-IP", "198.51.100.25")
	if got := srv.clientIP(untrustedRealIPReq); got != "203.0.113.10" {
		t.Fatalf("clientIP untrusted X-Real-IP = %q, want 203.0.113.10", got)
	}
}

func newTestAuthServer(t *testing.T, password string) *Server {
	t.Helper()
	return newTestAuthServerWithCost(t, password, bcrypt.MinCost)
}

func recordLoginFailure(t *testing.T, limiter *loginFailureLimiter, key string, now time.Time) {
	t.Helper()
	if !limiter.Begin(key, now) {
		t.Fatalf("Begin(%q) = false, want true", key)
	}
	limiter.RecordFailure(key, now)
}

func blockLoginFailureKey(t *testing.T, limiter *loginFailureLimiter, key string, now time.Time) {
	t.Helper()
	for i := 0; i < loginFailureLimit; i++ {
		recordLoginFailure(t, limiter, key, now)
	}
}

func newTestAuthServerWithCost(t *testing.T, password string, cost int) *Server {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		t.Fatalf("GenerateFromPassword: %v", err)
	}
	srv := &Server{logger: testLogger()}
	if err := srv.WithAuthConfig(config.AdminAuthConfig{
		Enabled:       true,
		Username:      "admin",
		PasswordHash:  string(hash),
		SessionSecret: "test-admin-session-secret",
		SessionTTL:    time.Hour,
	}); err != nil {
		t.Fatalf("WithAuthConfig: %v", err)
	}
	return srv
}
