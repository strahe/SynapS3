package admin

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/ui"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/auth"
)

// WorkerHealthChecker provides worker liveness info. Implemented by worker.Manager.
type WorkerHealthChecker interface {
	WorkerHealth() map[string]bool
}

// Server provides /healthz and /metrics endpoints on a separate port.
type Server struct {
	addr                  string
	db                    *bun.DB
	cache                 cache.Cache
	objectReader          *objectreader.Reader
	objectStorage         synapse.StorageClient
	cacheGate             *cacheaccess.Gate
	cacheAccessTracker    *cacheaccess.Tracker
	objectUploader        objectUploader
	objectVersionRestorer objectVersionRestorer
	cacheMaxBytes         int64
	repos                 *repository.Repositories
	taskService           *taskengine.Service
	bucketLifecycle       *bucketlifecycle.Service
	workerHealth          WorkerHealthChecker
	wallet                synapse.WalletQuerier
	filecoinReadiness     filecoinReadinessProbe
	observability         *observability.Service
	warmStorageMarket     WarmStorageMarket
	marketChainID         uint64
	usdfcAddress          string
	providerIdentity      providerIdentityLookup
	events                *EventHub
	settings              *SettingsService
	auth                  *authService
	trustedProxies        []netip.Prefix
	s3IAM                 auth.IAMService
	s3RootAccess          string
	filecoinDefaultCopies int
	// replacementSelector resolves an automatic replacement provider. Nil means
	// only an explicit Provider ID can be confirmed.
	replacementSelector providerReplacementSelector
	setupOnly           bool
	logger              *slog.Logger
	startedAt           time.Time

	// Track previously seen label sets to zero stale entries on refresh.
	prevTaskLabels   map[[2]string]struct{}
	prevObjectLabels map[string]struct{}
}

func (s *Server) WithTaskService(service *taskengine.Service) *Server {
	s.taskService = service
	s.bucketLifecycle.SetTaskService(service)
	return s
}

// New creates a new admin HTTP server.
func New(
	addr string,
	db *bun.DB,
	c cache.Cache,
	cacheGate *cacheaccess.Gate,
	cacheAccessTracker *cacheaccess.Tracker,
	cacheMaxBytes int64,
	repos *repository.Repositories,
	wh WorkerHealthChecker,
	wallet synapse.WalletQuerier,
	filecoinDefaultCopies int,
	logger *slog.Logger,
) *Server {
	if cacheGate == nil {
		panic("admin server requires a cache access gate")
	}
	if cacheAccessTracker == nil {
		panic("admin server requires a cache access tracker")
	}
	s := &Server{
		addr:                  addr,
		db:                    db,
		cache:                 c,
		cacheGate:             cacheGate,
		cacheAccessTracker:    cacheAccessTracker,
		objectReader:          objectreader.New(repos, c, nil, cacheGate, cacheAccessTracker, logger),
		cacheMaxBytes:         cacheMaxBytes,
		repos:                 repos,
		bucketLifecycle:       bucketlifecycle.New(repos, c, boundedBucketCopies(filecoinDefaultCopies), logger),
		workerHealth:          wh,
		wallet:                newCachedWalletQuerier(wallet, walletCacheTTL, time.Now),
		events:                newAdminEventHub(),
		filecoinDefaultCopies: boundedBucketCopies(filecoinDefaultCopies),
		logger:                logger,
		startedAt:             time.Now(),
	}
	s.watchWalletOperationEvents()
	return s
}

// WithObjectStorage enables provider-backed object reads for admin downloads.
func (s *Server) WithObjectStorage(storage synapse.StorageClient) *Server {
	s.objectStorage = storage
	s.resetObjectReader()
	return s
}

func (s *Server) resetObjectReader() {
	s.objectReader = objectreader.New(
		s.repos,
		s.cache,
		s.objectStorage,
		s.cacheGate,
		s.cacheAccessTracker,
		s.logger,
	)
}

func (s *Server) WithEventHub(events *EventHub) *Server {
	s.events = events
	s.watchWalletOperationEvents()
	return s
}

func (s *Server) watchWalletOperationEvents() {
	if s == nil || s.events == nil {
		return
	}
	s.events.onPublish(func(topic string) {
		if topic == "wallet_operation_updated" {
			s.invalidateWalletCache()
		}
	})
}

type walletCacheInvalidator interface {
	Invalidate()
}

func (s *Server) invalidateWalletCache() {
	cache, ok := s.wallet.(walletCacheInvalidator)
	if !ok {
		return
	}
	cache.Invalidate()
}

// WithProviderIdentityResolver enables provider identity enrichment for admin APIs.
func (s *Server) WithProviderIdentityResolver(resolver providerIdentityLookup) *Server {
	s.providerIdentity = resolver
	if setter, ok := resolver.(providerIdentityPublisherSetter); ok {
		setter.SetProviderIdentityPublisher(s.publishProviderIdentity)
	}
	return s
}

// NewSetup creates an admin HTTP server for first-run/setup mode.
func NewSetup(addr string, settings *SettingsService, logger *slog.Logger) *Server {
	return &Server{
		addr:      addr,
		settings:  settings,
		setupOnly: true,
		logger:    logger,
		startedAt: time.Now(),
	}
}

// WithSettings enables settings API routes on a full admin server.
func (s *Server) WithSettings(settings *SettingsService) *Server {
	s.settings = settings
	return s
}

// WithFilecoinReadiness enables Filecoin readiness API routes.
func (s *Server) WithFilecoinReadiness(probe filecoinReadinessProbe) *Server {
	s.filecoinReadiness = probe
	return s
}

func (s *Server) WithObservability(service *observability.Service) *Server {
	s.observability = service
	return s
}

// WithS3IAM enables S3 user management API routes.
func (s *Server) WithS3IAM(iam auth.IAMService, rootAccess string) *Server {
	s.s3IAM = iam
	s.s3RootAccess = rootAccess
	return s
}

// Run starts the admin HTTP server at its configured address.
func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.addr, err)
	}
	return s.Serve(ctx, listener)
}

// Serve serves the admin API on listener until ctx is cancelled. The listener
// is owned by Serve and closed during shutdown.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("admin listener is required")
	}
	if !s.setupOnly {
		go s.refreshMetricsLoop(ctx)
		if runner, ok := s.providerIdentity.(providerIdentityRunner); ok {
			go runner.Run(ctx)
		}
	}

	mux := http.NewServeMux()
	if s.setupOnly {
		mux.HandleFunc("GET /healthz", s.handleSetupHealthz)
	} else {
		mux.HandleFunc("GET /healthz", s.handleHealthz)
		mux.Handle("GET /metrics", promhttp.Handler())
		// Dashboard API
		mux.HandleFunc("GET /api/v1/overview", s.handleAPIOverview)
		mux.HandleFunc("GET /api/v1/events", s.handleAPIEvents)
		mux.HandleFunc("GET /api/v1/buckets", s.handleAPIListBuckets)
		mux.HandleFunc("POST /api/v1/buckets", s.handleAPICreateBucket)
		mux.HandleFunc("GET /api/v1/buckets/{name}", s.handleAPIGetBucket)
		mux.HandleFunc("PUT /api/v1/buckets/{name}/owner", s.handleAPIUpdateBucketOwner)
		mux.HandleFunc("PUT /api/v1/buckets/{name}/copy-policy", s.handleAPIUpdateBucketCopyPolicy)
		mux.HandleFunc("POST /api/v1/buckets/{name}/data-sets/{id}/replacement", s.handleAPIStartDataSetReplacement)
		mux.HandleFunc("GET /api/v1/buckets/{name}/data-sets/{id}/replacement/providers", s.handleAPIListDataSetReplacementProviders)
		mux.HandleFunc("POST /api/v1/storage-replacements/{id}/retry", s.handleAPIRetryStorageReplacement)
		mux.HandleFunc("GET /api/v1/storage-confirmations", s.handleAPIListStorageConfirmations)
		mux.HandleFunc("POST /api/v1/storage-confirmations/{id}/release", s.handleAPIReleaseStorageConfirmation)
		mux.HandleFunc("DELETE /api/v1/buckets/{name}", s.handleAPIDeleteBucket)
		mux.HandleFunc("GET /api/v1/buckets/{name}/objects", s.handleAPIBucketObjects)
		mux.HandleFunc("DELETE /api/v1/buckets/{name}/objects", s.handleAPIDeleteBucketObject)
		mux.HandleFunc("GET /api/v1/buckets/{name}/storage-health/affected-versions", s.handleAPIBucketStorageHealthAffectedVersions)
		mux.HandleFunc("GET /api/v1/buckets/{name}/objects/deleted", s.handleAPIBucketDeletedObjects)
		mux.HandleFunc("POST /api/v1/buckets/{name}/objects/deleted/permanent-delete", s.handleAPIPermanentDeleteDeletedBucketObject)
		mux.HandleFunc("GET /api/v1/buckets/{name}/objects/deletions", s.handleAPIBucketObjectDeletions)
		mux.HandleFunc("POST /api/v1/buckets/{name}/objects/permanent-delete", s.handleAPIPermanentDeleteBucketObject)
		mux.HandleFunc("POST /api/v1/buckets/{name}/objects/restore", s.handleAPIRestoreBucketObject)
		mux.HandleFunc("GET /api/v1/buckets/{name}/objects/status-detail", s.handleAPIBucketObjectStatusDetail)
		mux.HandleFunc("GET /api/v1/buckets/{name}/objects/provenance", s.handleAPIBucketObjectProvenance)
		mux.HandleFunc("GET /api/v1/buckets/{name}/objects/versions", s.handleAPIBucketObjectVersions)
		mux.HandleFunc("POST /api/v1/buckets/{name}/objects/versions/restore", s.handleAPIRestoreObjectVersion)
		mux.HandleFunc("GET /api/v1/buckets/{name}/objects/download", s.handleAPIDownloadObject)
		mux.HandleFunc("POST /api/v1/buckets/{name}/objects/upload", s.handleAPIUploadObject)
		mux.HandleFunc("GET /api/v1/tasks", s.handleAPITasks)
		mux.HandleFunc("GET /api/v1/tasks/stats", s.handleAPITaskStats)
		mux.HandleFunc("POST /api/v1/tasks/{id}/retry", s.handleAPITaskRetry)
		mux.HandleFunc("POST /api/v1/tasks/{id}/acknowledge", s.handleAPITaskAcknowledge)
		mux.HandleFunc("GET /api/v1/tasks/acknowledge/preview", s.handleAPITaskAcknowledgePreview)
		mux.HandleFunc("POST /api/v1/tasks/acknowledge", s.handleAPITaskAcknowledgeMatching)
		mux.HandleFunc("GET /api/v1/system/info", s.handleAPISystemInfo)
		mux.HandleFunc("GET /api/v1/workers", s.handleAPIWorkers)
		mux.HandleFunc("GET /api/v1/cache/stats", s.handleAPICacheStats)
		mux.HandleFunc("GET /api/v1/wallet", s.handleAPIWallet)
		mux.HandleFunc("POST /api/v1/wallet/fund", s.handleAPIWalletFund)
		mux.HandleFunc("POST /api/v1/wallet/withdraw", s.handleAPIWalletWithdraw)
		mux.HandleFunc("POST /api/v1/wallet/approve", s.handleAPIWalletApprove)
		mux.HandleFunc("GET /api/v1/wallet/operations", s.handleAPIWalletOperations)
		mux.HandleFunc("GET /api/v1/filecoin/readiness", s.handleAPIFilecoinReadiness)
		mux.HandleFunc("GET /api/v1/filecoin/warm-storage/price-list", s.handleAPIWarmStoragePrice)
		mux.HandleFunc("GET /api/v1/observability/providers", s.handleAPIObservabilityProviders)
		mux.HandleFunc("POST /api/v1/observability/provider-tiers/refresh", s.handleAPIRefreshProviderTiers)
		mux.HandleFunc("POST /api/v1/observability/providers/refresh", s.handleAPIRefreshObservabilityProviders)
		mux.HandleFunc("POST /api/v1/observability/providers/{provider_id}/upload-speed-test", s.handleAPIProviderUploadSpeedTest)
		mux.HandleFunc("POST /api/v1/observability/providers/{provider_id}/refresh", s.handleAPIRefreshProvider)
		mux.HandleFunc("GET /api/v1/observability/data-sets", s.handleAPIObservabilityDataSets)
		mux.HandleFunc("POST /api/v1/observability/data-sets/refresh", s.handleAPIRefreshObservabilityDataSets)
		if s.s3IAM != nil {
			mux.HandleFunc("GET /api/v1/s3-users", s.handleAPIListS3Users)
			mux.HandleFunc("POST /api/v1/s3-users", s.handleAPICreateS3User)
			mux.HandleFunc("PUT /api/v1/s3-users/{accessKey}", s.handleAPIUpdateS3User)
			mux.HandleFunc("POST /api/v1/s3-users/{accessKey}/secret", s.handleAPIRotateS3UserSecret)
			mux.HandleFunc("DELETE /api/v1/s3-users/{accessKey}", s.handleAPIDeleteS3User)
		}
	}
	s.registerAuthRoutes(mux)
	if s.settings != nil {
		mux.HandleFunc("GET /api/v1/settings", s.handleAPIGetSettings)
		mux.HandleFunc("PUT /api/v1/settings", s.handleAPIUpdateSettings)
		mux.HandleFunc("POST /api/v1/settings/validate", s.handleAPIValidateSettings)
		mux.HandleFunc("POST /api/v1/filecoin/readiness/preflight", s.handleAPIFilecoinReadinessPreflight)
	}

	// Serve embedded SPA frontend (fallback for non-API routes)
	distFS := ui.DistFS()
	if sub, err := fs.Sub(distFS, "dist"); err == nil {
		// Check if the embedded FS has content (production build)
		if entries, err := fs.ReadDir(sub, "."); err == nil && len(entries) > 0 {
			mux.Handle("/", spaFileServer(sub))
		}
	}

	srv := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           withSecurityHeaders(s.withAdminAuth(mux)),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(_ net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("admin server listening", "addr", listener.Addr().String())
		errCh <- srv.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return errors.Join(err, srv.Shutdown(shutCtx))
	}
}

func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'")
		cleanPath := canonicalAdminAuthPath(r.URL.Path)
		if cleanPath == "/api" || strings.HasPrefix(cleanPath, "/api/") ||
			cleanPath == "/admin" || strings.HasPrefix(cleanPath, "/admin/") ||
			cleanPath == "/metrics" || cleanPath == "/healthz" {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

type healthResponse struct {
	Status string   `json:"status"`
	Errors []string `json:"errors,omitempty"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	var errs []string

	if err := s.db.PingContext(r.Context()); err != nil {
		s.logger.Warn("health check: db ping failed", "error", err)
		errs = append(errs, "db: unreachable")
	}

	// Check cache dir exists via a lightweight stat on the root.
	cacheDir := s.cacheRootDir()
	if cacheDir != "" {
		if _, err := os.Stat(cacheDir); err != nil {
			s.logger.Warn("health check: cache dir missing", "error", err)
			errs = append(errs, "cache: directory not found")
		}
	}

	// Worker liveness check
	if s.workerHealth != nil {
		for name, ok := range s.workerHealth.WorkerHealth() {
			if !ok {
				errs = append(errs, fmt.Sprintf("worker/%s: not responding", name))
			}
		}
	}

	// Update cache used bytes metric opportunistically.
	CacheUsedBytes.Set(float64(s.cache.UsedBytes()))

	w.Header().Set("Content-Type", "application/json")

	resp := healthResponse{Status: "ok"}
	status := http.StatusOK
	if len(errs) > 0 {
		resp.Status = "unhealthy"
		resp.Errors = errs
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, resp)
}

func (s *Server) handleSetupHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "setup"})
}

// cacheRootDir extracts the root directory from a filesystem cache.
// Returns empty string if the cache doesn't expose its root.
func (s *Server) cacheRootDir() string {
	type rootDirer interface {
		RootDir() string
	}
	if rd, ok := s.cache.(rootDirer); ok {
		return rd.RootDir()
	}
	return ""
}

func (s *Server) handleAPITaskRetry(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	if err := s.taskService.Retry(r.Context(), id); err != nil {
		if errors.Is(err, taskengine.ErrRetryUnsupported) {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "This operation cannot be recovered from Tasks.",
				"code":  "task_retry_unsupported",
			})
		} else if errors.Is(err, repository.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "failed task not found"})
		} else {
			s.logger.Error("api: failed to retry task", "taskID", id, "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "pending"})
}

func (s *Server) handleAPITaskAcknowledge(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	if err := s.taskService.Acknowledge(r.Context(), id); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "failed task not found"})
		} else {
			s.logger.Error("api: failed to acknowledge task", "taskID", id, "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "acknowledged"})
}

func (s *Server) refreshMetricsLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Refresh immediately on startup.
	s.refreshMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshMetrics(ctx)
		}
	}
}

func (s *Server) refreshMetrics(ctx context.Context) {
	taskCounts, err := s.repos.Tasks.CountByStatus(ctx)
	if err != nil {
		s.logger.Warn("failed to refresh task queue metrics", "error", err)
	} else {
		currentTaskLabels := make(map[[2]string]struct{}, len(taskCounts))
		for _, tc := range taskCounts {
			if !isActiveTaskStatus(tc.Status) {
				continue
			}
			key := [2]string{tc.Type, tc.Status}
			currentTaskLabels[key] = struct{}{}
			TaskQueueDepth.WithLabelValues(tc.Type, tc.Status).Set(float64(tc.Count))
		}
		// Zero out labels that disappeared since the last refresh.
		for key := range s.prevTaskLabels {
			if _, ok := currentTaskLabels[key]; !ok {
				TaskQueueDepth.WithLabelValues(key[0], key[1]).Set(0)
			}
		}
		s.prevTaskLabels = currentTaskLabels
	}

	objCounts, err := s.repos.Objects.CountByState(ctx)
	if err != nil {
		s.logger.Warn("failed to refresh object state metrics", "error", err)
	} else {
		currentObjLabels := make(map[string]struct{}, len(objCounts))
		for _, oc := range objCounts {
			currentObjLabels[oc.State] = struct{}{}
			ObjectStateDistribution.WithLabelValues(oc.State).Set(float64(oc.Count))
		}
		for key := range s.prevObjectLabels {
			if _, ok := currentObjLabels[key]; !ok {
				ObjectStateDistribution.WithLabelValues(key).Set(0)
			}
		}
		s.prevObjectLabels = currentObjLabels
	}
}

func isActiveTaskStatus(status string) bool {
	switch status {
	case string(model.TaskStatusPending), string(model.TaskStatusRunning):
		return true
	default:
		return false
	}
}
