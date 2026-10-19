package admin

import (
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/adminsession"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/securetoken"
	"golang.org/x/crypto/bcrypt"
)

const (
	adminSessionCookieName      = "synaps3_admin_session"
	adminCSRFHeader             = "X-SynapS3-CSRF"
	loginFailureLimit           = 5
	loginFailureBlock           = time.Minute
	loginFailureTTL             = 10 * time.Minute
	loginFailureCleanup         = time.Minute
	loginFailureMaxEntries      = 4096
	adminLoginMaxBodyBytes      = 10 * 1024
	basicAuthSuccessTTL         = time.Minute
	basicAuthCacheMaxItems      = 4096
	authCleanupInterval         = time.Minute
	adminSessionStoreTimeout    = 5 * time.Second
	adminSessionCleanupLimit    = 128
	adminSessionAudience        = "synaps3-admin-v1"
	adminSessionRefreshInterval = 5 * time.Minute
	adminRememberedSessionTTL   = 30 * 24 * time.Hour
)

type authService struct {
	username             string
	passwordHash         []byte
	sessionSecret        []byte
	sessionTTL           time.Duration
	dummyHash            []byte
	now                  func() time.Time
	limiter              *loginFailureLimiter
	passwordLocks        *keyedLockSet
	bcryptGate           chan struct{}
	basicCacheMu         sync.Mutex
	basicAuthCache       map[string]time.Time
	basicAuthNextCleanup time.Time
	sessions             adminsession.Store
	audience             string
	cleanupMu            sync.Mutex
	sessionNextCleanup   time.Time
}

type adminBasicAuthContextKey struct{}

type authSessionClaims struct {
	Audience  string `json:"audience"`
	Username  string `json:"username"`
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
	CSRFToken string `json:"csrf_token"`
	SessionID string `json:"session_id,omitempty"`
	Remember  bool   `json:"remember,omitempty"`
}

type authSessionResponse struct {
	Username     string `json:"username"`
	CSRFToken    string `json:"csrf_token"`
	ExpiresAt    string `json:"expires_at"`
	RefreshAfter string `json:"refresh_after"`
}

type authLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Remember bool   `json:"remember"`
}

type keyedLockSet struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

func newKeyedLockSet() *keyedLockSet {
	return &keyedLockSet{locks: map[string]*keyedLock{}}
}

func (s *keyedLockSet) withLock(key string, fn func()) {
	s.mu.Lock()
	lock := s.locks[key]
	if lock == nil {
		lock = &keyedLock{}
		s.locks[key] = lock
	}
	lock.refs++
	s.mu.Unlock()

	lock.mu.Lock()
	defer func() {
		lock.mu.Unlock()
		s.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.locks, key)
		}
		s.mu.Unlock()
	}()
	fn()
}

func (s *Server) WithAuthConfig(cfg config.AdminAuthConfig) error {
	if !cfg.Enabled {
		s.auth = nil
		return nil
	}
	auth, err := newAuthService(cfg)
	if err != nil {
		return err
	}
	if s.setupOnly {
		domain, err := securetoken.URL(32)
		if err != nil {
			return fmt.Errorf("creating setup authentication domain: %w", err)
		}
		auth.audience = "synaps3-setup-" + domain
		auth.sessions = adminsession.NewMemoryStore()
	} else {
		if s.repos == nil || s.repos.AdminSessions == nil {
			return errors.New("admin authentication requires a session repository")
		}
		auth.sessions = s.repos.AdminSessions
	}
	s.auth = auth
	return nil
}

func (s *Server) WithTrustedProxies(values []string) error {
	proxies := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		proxy, err := parseTrustedProxy(value)
		if err != nil {
			return err
		}
		proxies = append(proxies, proxy)
	}
	s.trustedProxies = proxies
	return nil
}

func newAuthService(cfg config.AdminAuthConfig) (*authService, error) {
	username := strings.TrimSpace(cfg.Username)
	if username == "" {
		return nil, errors.New("admin auth username is empty")
	}
	if strings.TrimSpace(cfg.PasswordHash) == "" {
		return nil, errors.New("admin auth password hash is empty")
	}
	if strings.TrimSpace(cfg.SessionSecret) == "" {
		return nil, errors.New("admin auth session secret is empty")
	}
	if cfg.SessionTTL <= 0 {
		return nil, errors.New("admin auth session ttl must be positive")
	}
	passwordHash := []byte(cfg.PasswordHash)
	cost, err := bcrypt.Cost(passwordHash)
	if err != nil {
		return nil, fmt.Errorf("reading admin password hash cost: %w", err)
	}
	dummyHash, err := bcrypt.GenerateFromPassword([]byte("synaps3-admin-dummy-password"), cost)
	if err != nil {
		return nil, fmt.Errorf("creating admin password dummy hash: %w", err)
	}
	return &authService{
		username:       username,
		passwordHash:   passwordHash,
		sessionSecret:  []byte(cfg.SessionSecret),
		sessionTTL:     cfg.SessionTTL,
		dummyHash:      dummyHash,
		now:            time.Now,
		limiter:        newLoginFailureLimiter(),
		passwordLocks:  newKeyedLockSet(),
		bcryptGate:     make(chan struct{}, max(1, runtime.GOMAXPROCS(0))),
		basicAuthCache: map[string]time.Time{},
		audience:       adminSessionAudience,
	}, nil
}

func (s *Server) registerAuthRoutes(mux *http.ServeMux) {
	if s.auth == nil {
		return
	}
	mux.HandleFunc("POST /api/v1/auth/login", s.handleAPIAuthLogin)
	mux.HandleFunc("GET /api/v1/auth/session", s.handleAPIAuthSession)
	mux.HandleFunc("POST /api/v1/auth/refresh", s.handleAPIAuthRefresh)
	mux.HandleFunc("POST /api/v1/auth/logout", s.handleAPIAuthLogout)
}

func (s *Server) withAdminAuth(next http.Handler) http.Handler {
	if s.auth == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawAuthPath := rawAdminAuthPath(r.URL.Path)
		authPath := canonicalAdminAuthPath(r.URL.Path)
		if !adminAuthProtectedPath(rawAuthPath) && !adminAuthProtectedPath(authPath) {
			next.ServeHTTP(w, r)
			return
		}
		if rawAuthPath == authPath && adminAuthPublicPath(authPath) {
			next.ServeHTTP(w, r)
			return
		}
		if rawAuthPath == authPath && authPath == "/api/v1/auth/logout" {
			claims, ok := s.auth.signedSessionFromRequest(r, false)
			if !ok {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
				return
			}
			if !adminTokenEqual(r.Header.Get(adminCSRFHeader), claims.CSRFToken) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin CSRF token required"})
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if rawAuthPath == authPath && adminAuthBrowserSessionMutationPath(authPath) {
			claims, ok := s.auth.signedSessionFromRequest(r, true)
			if !ok {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
				return
			}
			if !adminTokenEqual(r.Header.Get(adminCSRFHeader), claims.CSRFToken) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin CSRF token required"})
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if username, password, ok := r.BasicAuth(); ok {
			if !s.basicAuthBrowserRequestAllowed(r) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin browser request forbidden"})
				return
			}
			clientIP := s.clientIP(r)
			switch s.auth.checkBasicAuthPassword(clientIP, username, password) {
			case passwordAuthOK:
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), adminBasicAuthContextKey{}, true)))
				return
			case passwordAuthLimited:
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many failed login attempts"})
				return
			default:
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
				return
			}
		}
		claims, ok, err := s.auth.sessionFromRequest(r)
		if err != nil {
			writeAuthStoreUnavailable(w)
			return
		}
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
			return
		}
		if unsafeMethod(r.Method) && !adminTokenEqual(r.Header.Get(adminCSRFHeader), claims.CSRFToken) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin CSRF token required"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func rawAdminAuthPath(rawPath string) string {
	return "/" + strings.TrimLeft(rawPath, "/")
}

func canonicalAdminAuthPath(rawPath string) string {
	return path.Clean("/" + strings.TrimLeft(rawPath, "/"))
}

func adminAuthProtectedPath(path string) bool {
	return path == "/metrics" ||
		path == "/api/v1" || strings.HasPrefix(path, "/api/v1/") ||
		path == "/admin" || strings.HasPrefix(path, "/admin/")
}

func adminAuthPublicPath(path string) bool {
	switch path {
	case "/api/v1/auth/login", "/api/v1/auth/session":
		return true
	default:
		return false
	}
}

func adminAuthBrowserSessionMutationPath(path string) bool {
	switch path {
	case "/api/v1/auth/refresh", "/api/v1/auth/logout":
		return true
	default:
		return false
	}
}

func unsafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

func (s *Server) handleAPIAuthLogin(w http.ResponseWriter, r *http.Request) {
	var req authLoginRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, adminLoginMaxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid login request"})
		return
	}
	key := loginFailureKey(s.clientIP(r))
	switch s.auth.checkLoginPassword(key, req.Username, req.Password) {
	case passwordAuthOK:
	case passwordAuthLimited:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many failed login attempts"})
		return
	default:
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid admin credentials"})
		return
	}
	token, claims, err := s.auth.newSession(r.Context(), req.Remember)
	if err != nil {
		writeAuthStoreUnavailable(w)
		return
	}
	setAdminSessionCookie(
		w,
		token,
		sessionLifetime(claims),
		time.Unix(claims.ExpiresAt, 0).UTC(),
		claims.Remember,
		s.requestScheme(r) == "https",
	)
	writeJSON(w, http.StatusOK, authSessionResponseFromClaims(claims))
}

func (s *Server) handleAPIAuthSession(w http.ResponseWriter, r *http.Request) {
	claims, ok, err := s.auth.sessionFromRequest(r)
	if err != nil {
		writeAuthStoreUnavailable(w)
		return
	}
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
		return
	}
	writeJSON(w, http.StatusOK, authSessionResponseFromClaims(claims))
}

func (s *Server) handleAPIAuthRefresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(adminSessionCookieName)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
		return
	}
	token, claims, ok, err := s.auth.refreshSession(r.Context(), cookie.Value)
	if err != nil {
		writeAuthStoreUnavailable(w)
		return
	}
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
		return
	}
	if token != "" {
		setAdminSessionCookie(
			w,
			token,
			sessionLifetime(claims),
			time.Unix(claims.ExpiresAt, 0).UTC(),
			claims.Remember,
			s.requestScheme(r) == "https",
		)
	}
	writeJSON(w, http.StatusOK, authSessionResponseFromClaims(claims))
}

func (s *Server) handleAPIAuthLogout(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.auth.signedSessionFromRequest(r, false)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
		return
	}
	if err := s.auth.deleteSession(r.Context(), claims); err != nil {
		writeAuthStoreUnavailable(w)
		return
	}
	clearAdminSessionCookie(w, s.requestScheme(r) == "https")
	w.WriteHeader(http.StatusNoContent)
}

func writeAuthStoreUnavailable(w http.ResponseWriter) {
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "admin authentication temporarily unavailable"})
}

func (a *authService) checkPassword(username, password string) bool {
	a.bcryptGate <- struct{}{}
	defer func() { <-a.bcryptGate }()
	if strings.TrimSpace(username) != a.username {
		_ = bcrypt.CompareHashAndPassword(a.dummyHash, []byte(password))
		return false
	}
	return bcrypt.CompareHashAndPassword(a.passwordHash, []byte(password)) == nil
}

type passwordAuthStatus int

const (
	passwordAuthOK passwordAuthStatus = iota
	passwordAuthInvalid
	passwordAuthLimited
)

func (a *authService) checkBasicAuthPassword(clientIP, username, password string) passwordAuthStatus {
	key := loginFailureKey(clientIP)
	now := a.now()
	if a.basicAuthCacheHit(clientIP, username, password, now) {
		a.limiter.Clear(key)
		return passwordAuthOK
	}
	if !a.limiter.Begin(key, now) {
		return passwordAuthLimited
	}
	status := passwordAuthInvalid
	a.passwordLocks.withLock(key, func() {
		now = a.now()
		if a.basicAuthCacheHit(clientIP, username, password, now) {
			a.limiter.Clear(key)
			status = passwordAuthOK
			return
		}
		if !a.limiter.Begin(key, now) {
			status = passwordAuthLimited
			return
		}
		if a.checkPassword(username, password) {
			a.limiter.Clear(key)
			a.rememberBasicAuth(clientIP, username, password, now)
			status = passwordAuthOK
			return
		}
		a.limiter.RecordFailure(key, now)
	})
	return status
}

func (a *authService) checkLoginPassword(key, username, password string) passwordAuthStatus {
	now := a.now()
	if !a.limiter.Begin(key, now) {
		return passwordAuthLimited
	}
	status := passwordAuthInvalid
	a.passwordLocks.withLock(key, func() {
		now = a.now()
		if !a.limiter.Begin(key, now) {
			status = passwordAuthLimited
			return
		}
		if a.checkPassword(username, password) {
			a.limiter.Clear(key)
			status = passwordAuthOK
			return
		}
		a.limiter.RecordFailure(key, now)
	})
	return status
}

func (a *authService) basicAuthCacheHit(clientIP, username, password string, now time.Time) bool {
	key := a.basicAuthCacheKey(clientIP, username, password)
	a.basicCacheMu.Lock()
	defer a.basicCacheMu.Unlock()
	a.maybeCleanupBasicAuthCacheLocked(now)
	expiresAt, ok := a.basicAuthCache[key]
	if !ok || !now.Before(expiresAt) {
		delete(a.basicAuthCache, key)
		return false
	}
	return true
}

func (a *authService) rememberBasicAuth(clientIP, username, password string, now time.Time) {
	key := a.basicAuthCacheKey(clientIP, username, password)
	a.basicCacheMu.Lock()
	defer a.basicCacheMu.Unlock()
	a.maybeCleanupBasicAuthCacheLocked(now)
	if len(a.basicAuthCache) >= basicAuthCacheMaxItems {
		a.evictSoonestBasicAuthCacheLocked()
	}
	if len(a.basicAuthCache) >= basicAuthCacheMaxItems {
		return
	}
	a.basicAuthCache[key] = now.Add(basicAuthSuccessTTL)
}

func (a *authService) maybeCleanupBasicAuthCacheLocked(now time.Time) {
	if a.basicAuthNextCleanup.IsZero() || !now.Before(a.basicAuthNextCleanup) {
		a.cleanupBasicAuthCacheLocked(now)
		a.basicAuthNextCleanup = now.Add(authCleanupInterval)
	}
}

func (a *authService) cleanupBasicAuthCacheLocked(now time.Time) {
	for key, expiresAt := range a.basicAuthCache {
		if !now.Before(expiresAt) {
			delete(a.basicAuthCache, key)
		}
	}
}

func (a *authService) evictSoonestBasicAuthCacheLocked() {
	var oldestKey string
	var oldestExpiry time.Time
	hasOldest := false
	for key, expiresAt := range a.basicAuthCache {
		if !hasOldest || expiresAt.Before(oldestExpiry) {
			oldestKey = key
			oldestExpiry = expiresAt
			hasOldest = true
		}
	}
	if hasOldest {
		delete(a.basicAuthCache, oldestKey)
	}
}

func (a *authService) basicAuthCacheKey(clientIP, username, password string) string {
	mac := hmac.New(sha256.New, a.sessionSecret)
	_, _ = mac.Write([]byte(strings.TrimSpace(clientIP)))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strings.TrimSpace(username)))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(a.passwordHash)
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(password))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (a *authService) newSession(ctx context.Context, remember bool) (string, authSessionClaims, error) {
	now := a.now().UTC()
	csrf, err := securetoken.URL(32)
	if err != nil {
		return "", authSessionClaims{}, err
	}
	sessionID, err := securetoken.URL(32)
	if err != nil {
		return "", authSessionClaims{}, err
	}
	lifetime := a.sessionLifetime(remember)
	claims := authSessionClaims{
		Audience:  a.audience,
		Username:  a.username,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(lifetime).Unix(),
		CSRFToken: csrf,
		SessionID: sessionID,
		Remember:  remember,
	}
	token, err := a.signClaims(claims)
	if err != nil {
		return "", authSessionClaims{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, adminSessionStoreTimeout)
	defer cancel()
	if err := a.sessions.Create(ctx, &adminsession.Session{
		SessionIDHash: a.sessionTokenHash(sessionID), CreatedAt: now, ExpiresAt: time.Unix(claims.ExpiresAt, 0).UTC(),
	}); err != nil {
		return "", authSessionClaims{}, err
	}
	a.cleanupSessions(ctx, now)
	return token, claims, nil
}

func (a *authService) sessionFromRequest(r *http.Request) (authSessionClaims, bool, error) {
	claims, ok := a.signedSessionFromRequest(r, true)
	if !ok {
		return authSessionClaims{}, false, nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), adminSessionStoreTimeout)
	defer cancel()
	session, err := a.sessions.GetActive(ctx, a.sessionTokenHash(claims.SessionID), a.now())
	if err != nil {
		return authSessionClaims{}, false, err
	}
	verifiedAt := a.now()
	if session == nil || !session.ExpiresAt.After(verifiedAt) || claims.ExpiresAt <= verifiedAt.Unix() {
		return authSessionClaims{}, false, nil
	}
	return claims, true, nil
}

func (a *authService) signedSessionFromRequest(r *http.Request, requireUnexpired bool) (authSessionClaims, bool) {
	cookie, err := r.Cookie(adminSessionCookieName)
	if err != nil {
		return authSessionClaims{}, false
	}
	return a.verifySignedSession(cookie.Value, a.now(), requireUnexpired)
}

func (a *authService) sessionLifetime(remember bool) time.Duration {
	if remember {
		return max(a.sessionTTL, adminRememberedSessionTTL)
	}
	return a.sessionTTL
}

func sessionLifetime(claims authSessionClaims) time.Duration {
	return time.Duration(claims.ExpiresAt-claims.IssuedAt) * time.Second
}

func sessionRefreshAfter(claims authSessionClaims) time.Time {
	lifetime := sessionLifetime(claims)
	return time.Unix(claims.IssuedAt, 0).UTC().Add(min(adminSessionRefreshInterval, lifetime/2))
}

func authSessionResponseFromClaims(claims authSessionClaims) authSessionResponse {
	return authSessionResponse{
		Username:     claims.Username,
		CSRFToken:    claims.CSRFToken,
		ExpiresAt:    time.Unix(claims.ExpiresAt, 0).UTC().Format(time.RFC3339),
		RefreshAfter: sessionRefreshAfter(claims).Format(time.RFC3339Nano),
	}
}

func (a *authService) refreshSession(ctx context.Context, token string) (string, authSessionClaims, bool, error) {
	now := a.now().UTC()
	claims, ok := a.verifySessionAt(token, now)
	if !ok {
		return "", authSessionClaims{}, false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, adminSessionStoreTimeout)
	defer cancel()
	key := a.sessionTokenHash(claims.SessionID)
	if now.Before(sessionRefreshAfter(claims)) {
		session, err := a.sessions.GetActive(ctx, key, now)
		verifiedAt := a.now()
		return "", claims, session != nil && session.ExpiresAt.After(verifiedAt) && claims.ExpiresAt > verifiedAt.Unix(), err
	}
	presentedExpiresAt := claims.ExpiresAt
	lifetime := sessionLifetime(claims)
	claims.IssuedAt = now.Unix()
	claims.ExpiresAt = now.Add(lifetime).Unix()
	refreshedToken, err := a.signClaims(claims)
	if err != nil {
		return "", authSessionClaims{}, false, err
	}
	active, err := a.sessions.ExtendActive(ctx, key, now, time.Unix(claims.ExpiresAt, 0).UTC())
	if err != nil || !active {
		return "", authSessionClaims{}, false, err
	}
	if presentedExpiresAt <= a.now().Unix() {
		return "", authSessionClaims{}, false, nil
	}
	a.cleanupSessions(ctx, now)
	return refreshedToken, claims, true, nil
}

func (a *authService) signClaims(claims authSessionClaims) (string, error) {
	data, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(data)
	sig := a.sign(payload)
	return payload + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (a *authService) verifySession(token string) (authSessionClaims, bool) {
	return a.verifySessionAt(token, a.now())
}

func (a *authService) verifySessionAt(token string, now time.Time) (authSessionClaims, bool) {
	return a.verifySignedSession(token, now, true)
}

func (a *authService) verifySignedSession(token string, now time.Time, requireUnexpired bool) (authSessionClaims, bool) {
	payload, sigText, ok := strings.Cut(token, ".")
	if !ok || payload == "" || sigText == "" {
		return authSessionClaims{}, false
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigText)
	if err != nil {
		return authSessionClaims{}, false
	}
	if !hmac.Equal(sig, a.sign(payload)) {
		return authSessionClaims{}, false
	}
	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return authSessionClaims{}, false
	}
	var claims authSessionClaims
	if err := json.Unmarshal(data, &claims); err != nil {
		return authSessionClaims{}, false
	}
	sessionID, sessionIDErr := base64.RawURLEncoding.DecodeString(claims.SessionID)
	if claims.Audience != a.audience || sessionIDErr != nil || len(sessionID) != 32 ||
		base64.RawURLEncoding.EncodeToString(sessionID) != claims.SessionID || claims.Username != a.username ||
		claims.IssuedAt <= 0 ||
		claims.ExpiresAt <= claims.IssuedAt ||
		(requireUnexpired && claims.ExpiresAt <= now.Unix()) ||
		claims.CSRFToken == "" {
		return authSessionClaims{}, false
	}
	return claims, true
}

func (a *authService) sign(payload string) []byte {
	mac := hmac.New(sha256.New, a.sessionSecret)
	_, _ = mac.Write([]byte(payload))
	return mac.Sum(nil)
}

func (a *authService) deleteSession(ctx context.Context, claims authSessionClaims) error {
	ctx, cancel := context.WithTimeout(ctx, adminSessionStoreTimeout)
	defer cancel()
	if err := a.sessions.Delete(ctx, a.sessionTokenHash(claims.SessionID)); err != nil {
		return err
	}
	a.cleanupSessions(ctx, a.now())
	return nil
}

func (a *authService) cleanupSessions(ctx context.Context, now time.Time) {
	a.cleanupMu.Lock()
	if now.Before(a.sessionNextCleanup) {
		a.cleanupMu.Unlock()
		return
	}
	a.sessionNextCleanup = now.Add(authCleanupInterval)
	a.cleanupMu.Unlock()
	_, _ = a.sessions.DeleteExpired(ctx, now, adminSessionCleanupLimit)
}

func (a *authService) sessionTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func setAdminSessionCookie(
	w http.ResponseWriter,
	token string,
	ttl time.Duration,
	expiresAt time.Time,
	persistent,
	secure bool,
) {
	cookie := &http.Cookie{
		Name:     adminSessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   secure,
	}
	if persistent {
		cookie.MaxAge = int(ttl.Seconds())
		cookie.Expires = expiresAt
	}
	http.SetCookie(w, cookie)
}

func clearAdminSessionCookie(w http.ResponseWriter, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     adminSessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		Secure:   secure,
	})
}

func adminTokenEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (s *Server) basicAuthBrowserRequestAllowed(r *http.Request) bool {
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site") {
		return false
	}
	if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" {
		return s.requestOriginMatches(r, origin)
	}
	if referer := strings.TrimSpace(r.Header.Get("Referer")); referer != "" {
		return s.requestOriginMatches(r, referer)
	}
	return true
}

func (s *Server) requestOriginMatches(r *http.Request, raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Scheme, s.requestScheme(r)) && strings.EqualFold(u.Host, s.requestHost(r))
}

func (s *Server) requestScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if s.remoteAddrTrusted(r.RemoteAddr) {
		proto := strings.ToLower(lastHeaderValue(r.Header.Get("X-Forwarded-Proto")))
		if proto == "https" || proto == "http" {
			return proto
		}
	}
	return "http"
}

func (s *Server) requestHost(r *http.Request) string {
	if s.remoteAddrTrusted(r.RemoteAddr) {
		for _, part := range strings.Split(r.Header.Get("X-Forwarded-Host"), ",") {
			if host := strings.TrimSpace(part); host != "" {
				return host
			}
		}
	}
	return r.Host
}

func (s *Server) clientIP(r *http.Request) string {
	remoteHost := hostFromRemoteAddr(r.RemoteAddr)
	remoteAddr, ok := parseIP(remoteHost)
	if !ok || !s.addrTrusted(remoteAddr) {
		return remoteHost
	}
	if ip, ok := s.forwardedForClientIP(r.Header.Get("X-Forwarded-For")); ok {
		return ip.String()
	}
	if ip, ok := parseIP(r.Header.Get("X-Real-IP")); ok {
		return ip.String()
	}
	return remoteAddr.String()
}

func (s *Server) forwardedForClientIP(value string) (netip.Addr, bool) {
	parts := strings.Split(value, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		ip, ok := parseForwardedIP(parts[i])
		if !ok {
			continue
		}
		if s.addrTrusted(ip) {
			continue
		}
		return ip, true
	}
	return netip.Addr{}, false
}

func (s *Server) remoteAddrTrusted(remoteAddr string) bool {
	ip, ok := parseIP(hostFromRemoteAddr(remoteAddr))
	return ok && s.addrTrusted(ip)
}

func (s *Server) addrTrusted(ip netip.Addr) bool {
	for _, proxy := range s.trustedProxies {
		if proxy.Contains(ip) {
			return true
		}
	}
	return false
}

func lastHeaderValue(value string) string {
	parts := strings.Split(value, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		if part := strings.TrimSpace(parts[i]); part != "" {
			return part
		}
	}
	return ""
}

func parseTrustedProxy(value string) (netip.Prefix, error) {
	value = strings.TrimSpace(value)
	if prefix, err := netip.ParsePrefix(value); err == nil {
		return prefix, nil
	}
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return netip.Prefix{}, err
	}
	return netip.PrefixFrom(addr, addr.BitLen()), nil
}

func parseIP(value string) (netip.Addr, bool) {
	ip, err := netip.ParseAddr(strings.TrimSpace(value))
	return ip, err == nil
}

func parseForwardedIP(value string) (netip.Addr, bool) {
	value = strings.TrimSpace(value)
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	}
	return parseIP(value)
}

func hostFromRemoteAddr(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return strings.TrimSpace(remoteAddr)
	}
	return host
}

func loginFailureKey(clientIP string) string {
	return strings.TrimSpace(clientIP)
}

type loginFailureLimiter struct {
	mu          sync.Mutex
	failures    map[string]loginFailure
	evictable   *list.List
	nextCleanup time.Time
}

type loginFailure struct {
	Count     int
	BlockedAt time.Time
	LastSeen  time.Time
	element   *list.Element
}

func newLoginFailureLimiter() *loginFailureLimiter {
	return &loginFailureLimiter{failures: map[string]loginFailure{}, evictable: list.New()}
}

func (l *loginFailureLimiter) Begin(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maybeCleanup(now)
	failure, ok := l.failures[key]
	if ok && failure.activeBlocked(now) {
		failure.LastSeen = now
		l.failures[key] = failure
		return false
	}
	if !ok && !l.makeRoomForNewKeyLocked(now) {
		return false
	}
	failure.LastSeen = now
	l.storeFailureLocked(key, failure, now)
	return true
}

func (l *loginFailureLimiter) RecordFailure(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maybeCleanup(now)
	failure, ok := l.failures[key]
	if ok && failure.activeBlocked(now) {
		failure.LastSeen = now
		l.failures[key] = failure
		return
	}
	if !ok && !l.makeRoomForNewKeyLocked(now) {
		return
	}
	if failure.Count >= loginFailureLimit {
		failure.Count = 0
		failure.BlockedAt = time.Time{}
	}
	failure.Count++
	failure.LastSeen = now
	if failure.Count >= loginFailureLimit {
		failure.BlockedAt = now
	}
	l.storeFailureLocked(key, failure, now)
}

func (f loginFailure) activeBlocked(now time.Time) bool {
	return f.Count >= loginFailureLimit && now.Sub(f.BlockedAt) <= loginFailureBlock
}

func (l *loginFailureLimiter) makeRoomForNewKeyLocked(now time.Time) bool {
	if len(l.failures) < loginFailureMaxEntries {
		return true
	}
	for l.evictable.Len() > 0 {
		element := l.evictable.Front()
		key, _ := element.Value.(string)
		failure, ok := l.failures[key]
		if !ok || failure.element != element {
			l.evictable.Remove(element)
			continue
		}
		if failure.activeBlocked(now) {
			l.untrackFailureLocked(failure)
			failure.element = nil
			l.failures[key] = failure
			continue
		}
		l.deleteFailureLocked(key)
		return true
	}
	return len(l.failures) < loginFailureMaxEntries
}

func (l *loginFailureLimiter) Clear(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.deleteFailureLocked(key)
}

func (l *loginFailureLimiter) cleanup(now time.Time) {
	for key, failure := range l.failures {
		if failure.LastSeen.IsZero() || now.Sub(failure.LastSeen) > loginFailureTTL {
			l.deleteFailureLocked(key)
		}
	}
}

func (l *loginFailureLimiter) maybeCleanup(now time.Time) {
	if l.nextCleanup.IsZero() || !now.Before(l.nextCleanup) {
		l.cleanup(now)
		l.nextCleanup = now.Add(loginFailureCleanup)
	}
}

func (l *loginFailureLimiter) storeFailureLocked(key string, failure loginFailure, now time.Time) {
	if failure.activeBlocked(now) {
		l.untrackFailureLocked(failure)
		failure.element = nil
	} else if failure.element == nil {
		failure.element = l.evictable.PushBack(key)
	} else {
		l.evictable.MoveToBack(failure.element)
	}
	l.failures[key] = failure
}

func (l *loginFailureLimiter) deleteFailureLocked(key string) {
	failure, ok := l.failures[key]
	if !ok {
		return
	}
	l.untrackFailureLocked(failure)
	delete(l.failures, key)
}

func (l *loginFailureLimiter) untrackFailureLocked(failure loginFailure) {
	if failure.element != nil {
		l.evictable.Remove(failure.element)
	}
}
