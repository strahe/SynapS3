package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/strahe/synaps3/internal/buildinfo"
	"github.com/strahe/synaps3/internal/model"
)

type overviewResponse struct {
	Buckets bucketOverview  `json:"buckets"`
	Objects objectOverview  `json:"objects"`
	Tasks   taskOverview    `json:"tasks"`
	Cache   cacheOverview   `json:"cache"`
	Workers map[string]bool `json:"workers"`
	System  systemOverview  `json:"system"`
}

type bucketOverview struct {
	Total    int64            `json:"total"`
	ByStatus map[string]int64 `json:"by_status"`
}

type objectOverview struct {
	Total          int64            `json:"total"`
	TotalSizeBytes int64            `json:"total_size_bytes"`
	ByState        map[string]int64 `json:"by_state"`
}

type taskOverview struct {
	ByStatus map[string]int64 `json:"by_status"`
}

type cacheOverview struct {
	UsedBytes int64 `json:"used_bytes"`
	MaxBytes  int64 `json:"max_bytes"`
}

type systemOverview struct {
	Version       string `json:"version"`
	Commit        string `json:"commit"`
	BuildDate     string `json:"build_date"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

func (s *Server) handleAPIOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	resp := overviewResponse{
		Buckets: bucketOverview{ByStatus: make(map[string]int64)},
		Objects: objectOverview{ByState: make(map[string]int64)},
		Tasks:   taskOverview{ByStatus: make(map[string]int64)},
	}

	// Buckets
	bucketCounts, err := s.repos.Buckets.CountByStatus(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to count buckets", "error", err)
	} else {
		for _, bc := range bucketCounts {
			resp.Buckets.ByStatus[bc.Status] = bc.Count
			if model.BucketStatus(bc.Status).IsVisible() {
				resp.Buckets.Total += bc.Count
			}
		}
	}

	// Objects
	objCounts, err := s.repos.Objects.CountByState(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to count objects", "error", err)
	} else {
		for _, oc := range objCounts {
			resp.Objects.ByState[oc.State] = oc.Count
			resp.Objects.Total += oc.Count
		}
	}

	totalSize, err := s.repos.Objects.TotalSize(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to get total size", "error", err)
	} else {
		resp.Objects.TotalSizeBytes = totalSize
	}

	// Tasks
	taskCounts, err := s.repos.Tasks.CountByStatus(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to count tasks", "error", err)
	} else {
		for _, tc := range taskCounts {
			resp.Tasks.ByStatus[tc.Status] += tc.Count
		}
	}

	// Cache
	resp.Cache = cacheOverview{
		UsedBytes: s.cache.UsedBytes(),
		MaxBytes:  s.cacheMaxBytes,
	}

	// Workers
	if s.workerHealth != nil {
		resp.Workers = s.workerHealth.WorkerHealth()
	} else {
		resp.Workers = make(map[string]bool)
	}

	// System
	resp.System = systemOverview{
		Version:       buildinfo.Version,
		Commit:        buildinfo.Commit,
		BuildDate:     buildinfo.Date,
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		slog.Warn("failed to marshal JSON response", "error", err)
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
