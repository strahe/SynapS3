package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/uptrace/bun"
)

// WorkerHealthChecker provides worker liveness info. Implemented by worker.Manager.
type WorkerHealthChecker interface {
	WorkerHealth() map[string]bool
}

// Server provides /healthz and /metrics endpoints on a separate port.
type Server struct {
	addr         string
	db           *bun.DB
	cache        cache.Cache
	repos        *repository.Repositories
	workerHealth WorkerHealthChecker
	logger       *slog.Logger

	// Track previously seen label sets to zero stale entries on refresh.
	prevTaskLabels   map[[2]string]struct{}
	prevObjectLabels map[string]struct{}
}

// New creates a new admin HTTP server.
func New(addr string, db *bun.DB, c cache.Cache, repos *repository.Repositories, wh WorkerHealthChecker, logger *slog.Logger) *Server {
	return &Server{
		addr:         addr,
		db:           db,
		cache:        c,
		repos:        repos,
		workerHealth: wh,
		logger:       logger,
	}
}

// Run starts the admin HTTP server. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	// Start periodic metrics refresh
	go s.refreshMetricsLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /admin/dead-letters", s.handleListDeadLetters)
	mux.HandleFunc("POST /admin/dead-letters/{id}/retry", s.handleRetryDeadLetter)

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(_ net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("admin server listening", "addr", s.addr)
		errCh <- srv.ListenAndServe()
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
		return err
	}
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
	if len(errs) > 0 {
		resp.Status = "unhealthy"
		resp.Errors = errs
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	_ = json.NewEncoder(w).Encode(resp)
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

func (s *Server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	const maxDeadLetterLimit = 1000
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxDeadLetterLimit {
		limit = maxDeadLetterLimit
	}

	tasks, err := s.repos.Tasks.ListDeadLetters(r.Context(), limit)
	if err != nil {
		s.logger.Error("failed to list dead-letter tasks", "error", err)
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tasks)
}

func (s *Server) handleRetryDeadLetter(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}

	if err := s.repos.Tasks.RetryDeadLetter(r.Context(), id); err != nil {
		s.logger.Error("failed to retry dead-letter task", "taskID", id, "error", err)
		if errors.Is(err, repository.ErrNotFound) {
			http.Error(w, `{"error":"not found or not in dead_letter state"}`, http.StatusNotFound)
		} else {
			http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"requeued"}`))
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
