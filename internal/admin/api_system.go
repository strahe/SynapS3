package admin

import (
	"net/http"
	"time"

	"github.com/strahe/synaps3/internal/buildinfo"
)

type systemInfoResponse struct {
	Version       string `json:"version"`
	Commit        string `json:"commit"`
	BuildDate     string `json:"build_date"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

func (s *Server) handleAPISystemInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, systemInfoResponse{
		Version:       buildinfo.Version,
		Commit:        buildinfo.Commit,
		BuildDate:     buildinfo.Date,
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
	})
}

type workerStatusResponse struct {
	Workers map[string]bool `json:"workers"`
}

func (s *Server) handleAPIWorkers(w http.ResponseWriter, r *http.Request) {
	workers := make(map[string]bool)
	if s.workerHealth != nil {
		workers = s.workerHealth.WorkerHealth()
	}
	writeJSON(w, http.StatusOK, workerStatusResponse{Workers: workers})
}

type cacheStatsResponse struct {
	UsedBytes int64 `json:"used_bytes"`
	MaxBytes  int64 `json:"max_bytes"`
}

func (s *Server) handleAPICacheStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, cacheStatsResponse{
		UsedBytes: s.cache.UsedBytes(),
		MaxBytes:  s.cacheMaxBytes,
	})
}
