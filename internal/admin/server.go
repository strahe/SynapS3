package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/uptrace/bun"
)

// Server provides /healthz and /metrics endpoints on a separate port.
type Server struct {
	addr   string
	db     *bun.DB
	cache  cache.Cache
	logger *slog.Logger
}

// New creates a new admin HTTP server.
func New(addr string, db *bun.DB, c cache.Cache, logger *slog.Logger) *Server {
	return &Server{
		addr:   addr,
		db:     db,
		cache:  c,
		logger: logger,
	}
}

// Run starts the admin HTTP server. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())

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
