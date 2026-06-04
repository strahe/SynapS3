package admin

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/strahe/synaps3/internal/buildinfo"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
)

type overviewResponse struct {
	Buckets               bucketOverview                `json:"buckets"`
	Objects               objectOverview                `json:"objects"`
	Tasks                 taskOverview                  `json:"tasks"`
	Cache                 cacheOverview                 `json:"cache"`
	Workers               map[string]bool               `json:"workers"`
	System                systemOverview                `json:"system"`
	FilecoinStorageHealth filecoinStorageHealthOverview `json:"filecoin_storage_health"`
}

type bucketOverview struct {
	Total    int64            `json:"total"`
	ByStatus map[string]int64 `json:"by_status"`
}

type objectOverview struct {
	Total          int64                   `json:"total"`
	TotalSizeBytes int64                   `json:"total_size_bytes"`
	ByState        map[string]int64        `json:"by_state"`
	Attention      objectAttentionOverview `json:"attention"`
}

type taskOverview struct {
	ByStatus       map[string]int64       `json:"by_status"`
	Attention      taskAttentionOverview  `json:"attention"`
	ActivePipeline []taskPipelineOverview `json:"active_pipeline"`
}

type objectAttentionOverview struct {
	NeedsAttention int64 `json:"needs_attention"`
	Unavailable    int64 `json:"unavailable"`
}

type taskAttentionOverview struct {
	Failed    int64 `json:"failed"`
	Exhausted int64 `json:"exhausted"`
}

type taskPipelineOverview struct {
	Pipeline string           `json:"pipeline"`
	ByStatus map[string]int64 `json:"by_status"`
	Total    int64            `json:"total"`
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

type filecoinStorageHealthOverview struct {
	Level         observability.SignalLevel                 `json:"level"`
	Providers     *filecoinStorageHealthObservationOverview `json:"providers"`
	DataSets      *filecoinStorageHealthObservationOverview `json:"data_sets"`
	PartialErrors map[string]string                         `json:"partial_errors"`
}

type filecoinStorageHealthObservationOverview struct {
	Summary       observability.Summary       `json:"summary"`
	SummarySignal observability.SummarySignal `json:"summary_signal"`
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
	objCounts, err := s.repos.Objects.AggregateByState(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to aggregate objects", "error", err)
	} else {
		for _, oc := range objCounts {
			resp.Objects.ByState[oc.State] = oc.Count
			resp.Objects.Total += oc.Count
			resp.Objects.TotalSizeBytes += oc.TotalSize
		}
	}
	objectAttention, err := s.repos.Objects.CountOverviewAttention(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to count object attention", "error", err)
	} else {
		resp.Objects.Attention = objectAttentionOverview{
			NeedsAttention: objectAttention.NeedsAttention,
			Unavailable:    objectAttention.Unavailable,
		}
	}

	// Tasks
	taskCounts, err := s.repos.Tasks.CountByStatus(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to count tasks", "error", err)
	} else {
		for _, tc := range taskCounts {
			resp.Tasks.ByStatus[tc.Status] += tc.Count
		}
		resp.Tasks.Attention = taskAttentionOverview{
			Failed:    resp.Tasks.ByStatus[string(model.TaskStatusFailed)],
			Exhausted: resp.Tasks.ByStatus[string(model.TaskStatusExhausted)],
		}
	}
	taskPipelineCounts, err := s.repos.Tasks.CountOverviewActivePipeline(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to count active task pipeline", "error", err)
	} else {
		resp.Tasks.ActivePipeline = taskPipelineOverviewRows(taskPipelineCounts)
	}
	resp.FilecoinStorageHealth = s.filecoinStorageHealthOverview(ctx)

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

func (s *Server) filecoinStorageHealthOverview(ctx context.Context) filecoinStorageHealthOverview {
	health := filecoinStorageHealthOverview{
		Level:         observability.SignalOK,
		PartialErrors: make(map[string]string),
	}

	if s.observability == nil {
		health.PartialErrors["observability"] = "observability not available"
		health.Level = observability.WorstSignalLevel(health.Level, observability.SignalWarning)
		return health
	}

	providers, err := s.observability.ListProviderObservations(ctx, observability.ListOptions{Limit: 1})
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("overview: failed to load provider observability summary", "error", err)
		}
		health.PartialErrors["observability_providers"] = "provider health query failed"
		health.Level = observability.WorstSignalLevel(health.Level, observability.SignalWarning)
	} else {
		health.Providers = &filecoinStorageHealthObservationOverview{
			Summary:       providers.Summary,
			SummarySignal: providers.SummarySignal,
		}
		health.Level = observability.WorstSignalLevel(health.Level, providers.SummarySignal.Level)
	}

	dataSets, err := s.observability.ListDataSetObservations(ctx, observability.ListOptions{Limit: 1})
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("overview: failed to load data set observability summary", "error", err)
		}
		health.PartialErrors["observability_data_sets"] = "data set health query failed"
		health.Level = observability.WorstSignalLevel(health.Level, observability.SignalWarning)
	} else {
		health.DataSets = &filecoinStorageHealthObservationOverview{
			Summary:       dataSets.Summary,
			SummarySignal: dataSets.SummarySignal,
		}
		health.Level = observability.WorstSignalLevel(health.Level, dataSets.SummarySignal.Level)
	}
	return health
}

func taskPipelineOverviewRows(counts []repository.TaskPipelineCount) []taskPipelineOverview {
	pipelines := []string{"prepare", "upload", "commit", "sync", "evict", "cleanup"}
	rows := make([]taskPipelineOverview, 0, len(pipelines))
	index := make(map[string]int, len(pipelines))
	for _, pipeline := range pipelines {
		index[pipeline] = len(rows)
		rows = append(rows, taskPipelineOverview{
			Pipeline: pipeline,
			ByStatus: map[string]int64{
				string(model.TaskStatusQueued):    0,
				string(model.TaskStatusScheduled): 0,
				string(model.TaskStatusWaiting):   0,
				string(model.TaskStatusRunning):   0,
			},
		})
	}
	for _, count := range counts {
		i, ok := index[count.Pipeline]
		if !ok {
			continue
		}
		rows[i].ByStatus[count.Status] += count.Count
		rows[i].Total += count.Count
	}
	return rows
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
