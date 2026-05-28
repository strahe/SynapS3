package admin

import (
	"container/list"
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

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/securetoken"
	"golang.org/x/crypto/bcrypt"
)

const (
	adminSessionCookieName = "synaps3_admin_session"
	adminCSRFHeader        = "X-SynapS3-CSRF"
	loginFailureLimit      = 5
	loginFailureBlock      = time.Minute
	loginFailureTTL        = 10 * time.Minute
	loginFailureCleanup    = time.Minute
	loginFailureMaxEntries = 4096
	adminLoginMaxBodyBytes = 10 * 1024
	basicAuthSuccessTTL    = time.Minute
	basicAuthCacheMaxItems = 4096
	revokedSessionMaxItems = 4096
	authCleanupInterval    = time.Minute
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
	revokedMu            sync.Mutex
	revokedTokens        map[string]time.Time
	revokedNextCleanup   time.Time
}

type authSessionClaims struct {
	Username  string `json:"username"`
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
	CSRFToken string `json:"csrf_token"`
}

type authSessionResponse struct {
	Username  string `json:"username"`
	CSRFToken string `json:"csrf_token"`
	ExpiresAt string `json:"expires_at"`
}

type authLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
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
		revokedTokens:  map[string]time.Time{},
	}, nil
}

func (s *Server) registerAuthRoutes(mux *http.ServeMux) {
	if s.auth == nil {
		return
	}
	mux.HandleFunc("POST /api/v1/auth/login", s.handleAPIAuthLogin)
	mux.HandleFunc("GET /api/v1/auth/session", s.handleAPIAuthSession)
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
		if rawAuthPath == authPath && adminAuthLogoutPath(authPath) {
			claims, ok := s.auth.sessionFromRequest(r)
			if !ok {
				clearAdminSessionCookie(w, s.requestScheme(r) == "https")
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
				next.ServeHTTP(w, r)
				return
			case passwordAuthLimited:
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many failed login attempts"})
				return
			default:
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
				return
			}
		}
		claims, ok := s.auth.sessionFromRequest(r)
		if !ok {
			clearAdminSessionCookie(w, s.requestScheme(r) == "https")
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

func adminAuthLogoutPath(path string) bool {
	return path == "/api/v1/auth/logout"
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
	token, claims, err := s.auth.newSession()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create admin session"})
		return
	}
	setAdminSessionCookie(w, token, s.auth.sessionTTL, time.Unix(claims.ExpiresAt, 0).UTC(), s.requestScheme(r) == "https")
	writeJSON(w, http.StatusOK, authSessionResponse{
		Username:  claims.Username,
		CSRFToken: claims.CSRFToken,
		ExpiresAt: time.Unix(claims.ExpiresAt, 0).UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleAPIAuthSession(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.auth.sessionFromRequest(r)
	if !ok {
		clearAdminSessionCookie(w, s.requestScheme(r) == "https")
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
		return
	}
	writeJSON(w, http.StatusOK, authSessionResponse{
		Username:  claims.Username,
		CSRFToken: claims.CSRFToken,
		ExpiresAt: time.Unix(claims.ExpiresAt, 0).UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleAPIAuthLogout(w http.ResponseWriter, r *http.Request) {
	clearAdminSessionCookie(w, s.requestScheme(r) == "https")
	if !s.auth.revokeSessionFromRequest(r) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "admin session revocation capacity exceeded"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

func (a *authService) newSession() (string, authSessionClaims, error) {
	now := a.now().UTC()
	csrf, err := securetoken.URL(32)
	if err != nil {
		return "", authSessionClaims{}, err
	}
	claims := authSessionClaims{
		Username:  a.username,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(a.sessionTTL).Unix(),
		CSRFToken: csrf,
	}
	token, err := a.signClaims(claims)
	if err != nil {
		return "", authSessionClaims{}, err
	}
	return token, claims, nil
}

func (a *authService) sessionFromRequest(r *http.Request) (authSessionClaims, bool) {
	cookie, err := r.Cookie(adminSessionCookieName)
	if err != nil {
		return authSessionClaims{}, false
	}
	claims, ok := a.verifySession(cookie.Value)
	if !ok || a.sessionRevoked(cookie.Value, a.now()) {
		return authSessionClaims{}, false
	}
	return claims, true
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
	if claims.Username != a.username || claims.ExpiresAt <= a.now().Unix() || claims.CSRFToken == "" {
		return authSessionClaims{}, false
	}
	return claims, true
}

func (a *authService) sign(payload string) []byte {
	mac := hmac.New(sha256.New, a.sessionSecret)
	_, _ = mac.Write([]byte(payload))
	return mac.Sum(nil)
}

func (a *authService) revokeSessionFromRequest(r *http.Request) bool {
	cookie, err := r.Cookie(adminSessionCookieName)
	if err != nil {
		return true
	}
	claims, ok := a.verifySession(cookie.Value)
	if !ok {
		return true
	}
	return a.revokeSession(cookie.Value, time.Unix(claims.ExpiresAt, 0))
}

func (a *authService) revokeSession(token string, expiresAt time.Time) bool {
	now := a.now()
	if !expiresAt.After(now) {
		return true
	}
	a.revokedMu.Lock()
	defer a.revokedMu.Unlock()
	a.maybeCleanupRevokedSessionsLocked(now)
	key := a.sessionTokenHash(token)
	if _, ok := a.revokedTokens[key]; !ok && len(a.revokedTokens) >= revokedSessionMaxItems {
		a.cleanupRevokedSessionsLocked(now)
		if len(a.revokedTokens) >= revokedSessionMaxItems {
			return false
		}
	}
	a.revokedTokens[key] = expiresAt
	return true
}

func (a *authService) sessionRevoked(token string, now time.Time) bool {
	a.revokedMu.Lock()
	defer a.revokedMu.Unlock()
	a.maybeCleanupRevokedSessionsLocked(now)
	key := a.sessionTokenHash(token)
	expiresAt, ok := a.revokedTokens[key]
	if !ok {
		return false
	}
	if !expiresAt.After(now) {
		delete(a.revokedTokens, key)
		return false
	}
	return true
}

func (a *authService) maybeCleanupRevokedSessionsLocked(now time.Time) {
	if a.revokedNextCleanup.IsZero() || !now.Before(a.revokedNextCleanup) {
		a.cleanupRevokedSessionsLocked(now)
		a.revokedNextCleanup = now.Add(authCleanupInterval)
	}
}

func (a *authService) cleanupRevokedSessionsLocked(now time.Time) {
	for key, expiresAt := range a.revokedTokens {
		if !expiresAt.After(now) {
			delete(a.revokedTokens, key)
		}
	}
}

func (a *authService) sessionTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func setAdminSessionCookie(w http.ResponseWriter, token string, ttl time.Duration, expiresAt time.Time, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     adminSessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(ttl.Seconds()),
		Expires:  expiresAt,
		Secure:   secure,
	})
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
