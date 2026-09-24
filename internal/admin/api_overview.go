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
	Failed int64 `json:"failed"`
}

type taskPipelineOverview struct {
	Operation string           `json:"operation"`
	ByStatus  map[string]int64 `json:"by_status"`
	Total     int64            `json:"total"`
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
			if model.TaskType(tc.Type).IsRecurringSystem() && tc.Status != string(model.TaskStatusFailed) {
				continue
			}
			resp.Tasks.ByStatus[tc.Status] += tc.Count
		}
	}
	unacknowledgedFailed, err := s.repos.Tasks.CountUnacknowledgedFailed(ctx)
	if err != nil {
		s.logger.Warn("overview: failed to count task attention", "error", err)
	} else {
		resp.Tasks.Attention = taskAttentionOverview{Failed: unacknowledgedFailed}
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

	dataSetRows, providerStates, dataSetStates, providerCheckedAt, dataSetCheckedAt, err := s.repos.Observability.OverviewStorageStates(ctx)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("overview: failed to load storage observability summary", "error", err)
		}
		health.PartialErrors["observability"] = "storage health query failed"
		health.Level = observability.WorstSignalLevel(health.Level, observability.SignalWarning)
		return health
	}
	providerByID := make(map[string]observability.ProviderState, len(providerStates))
	for _, state := range providerStates {
		providerByID[state.ProviderID.String()] = state
		providerCheckedAt = olderOverviewObservation(providerCheckedAt, state.LastCheckedAt)
	}
	dataSetByID := make(map[int64]observability.DataSetState, len(dataSetStates))
	for _, state := range dataSetStates {
		dataSetByID[state.LocalDataSetID] = state
		dataSetCheckedAt = olderOverviewObservation(dataSetCheckedAt, state.LastCheckedAt)
	}
	providers := observability.Summary{}
	dataSets := observability.Summary{Total: len(dataSetRows)}
	seenProviders := make(map[string]bool, len(dataSetRows))
	for _, row := range dataSetRows {
		if state, ok := dataSetByID[row.ID]; ok {
			addStorageHealthStatus(&dataSets, state.Status)
		} else {
			dataSets.Unknown++
		}
		id := row.ProviderID.String()
		if seenProviders[id] {
			continue
		}
		seenProviders[id] = true
		providers.Total++
		if state, ok := providerByID[id]; ok {
			addStorageHealthStatus(&providers, state.Status)
		} else {
			providers.Unknown++
		}
	}
	now := time.Now().UTC()
	interval := s.observability.RefreshInterval()
	providerSignal := observability.DefaultAttentionSummarySignal(providers, providerCheckedAt, interval, now)
	dataSetSignal := observability.DefaultAttentionSummarySignal(dataSets, dataSetCheckedAt, interval, now)
	health.Providers = &filecoinStorageHealthObservationOverview{Summary: providers, SummarySignal: providerSignal}
	health.DataSets = &filecoinStorageHealthObservationOverview{Summary: dataSets, SummarySignal: dataSetSignal}
	health.Level = observability.WorstSignalLevel(providerSignal.Level, dataSetSignal.Level)
	return health
}

func olderOverviewObservation(current *time.Time, observed time.Time) *time.Time {
	if observed.IsZero() || (current != nil && !observed.Before(*current)) {
		return current
	}
	return &observed
}

func addStorageHealthStatus(summary *observability.Summary, status observability.Status) {
	switch status {
	case observability.StatusAvailable:
		summary.Available++
	case observability.StatusDegraded:
		summary.Degraded++
	case observability.StatusUnavailable:
		summary.Unavailable++
	default:
		summary.Unknown++
	}
}

func taskPipelineOverviewRows(counts []repository.TaskPipelineCount) []taskPipelineOverview {
	rows := make([]taskPipelineOverview, 0)
	index := make(map[string]int)
	for _, count := range counts {
		i, ok := index[count.Pipeline]
		if !ok {
			i = len(rows)
			index[count.Pipeline] = i
			rows = append(rows, taskPipelineOverview{
				Operation: count.Pipeline,
				ByStatus: map[string]int64{
					string(model.TaskStatusPending): 0,
					string(model.TaskStatusRunning): 0,
				},
			})
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
