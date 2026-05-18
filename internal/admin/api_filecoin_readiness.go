package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
)

type filecoinReadinessProbe interface {
	CheckRuntime(context.Context) synapse.ReadinessResult
	CheckDraft(context.Context, synapse.ReadinessConfig) synapse.ReadinessResult
}

type filecoinReadinessPreflightRequest struct {
	Filecoin *settingsFilecoinUpdate `json:"filecoin,omitempty"`
}

func (s *Server) handleAPIFilecoinReadiness(w http.ResponseWriter, r *http.Request) {
	if s.filecoinReadiness == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "filecoin readiness not available"})
		return
	}
	result := s.filecoinReadiness.CheckRuntime(r.Context())
	result = s.withObservabilityReadiness(r.Context(), result)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleAPIFilecoinReadinessPreflight(w http.ResponseWriter, r *http.Request) {
	if s.settings == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "settings not available"})
		return
	}
	if s.filecoinReadiness == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "filecoin readiness not available"})
		return
	}
	if !s.settingsWritable() {
		writeJSON(w, http.StatusForbidden, settingsErrorResponse{Error: "settings writes require loopback admin binding"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "settings writes require application/json"})
		return
	}
	if r.Header.Get(settingsWriteHeader) != settingsWriteHeaderValue {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "missing settings write header"})
		return
	}

	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	var req filecoinReadinessPreflightRequest
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid readiness payload"})
		return
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid readiness payload"})
		return
	}

	cfg, fieldErrs := s.settings.FilecoinDraftConfig(req.Filecoin)
	if len(fieldErrs) > 0 {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid settings", Fields: fieldErrs})
		return
	}
	writeJSON(w, http.StatusOK, s.filecoinReadiness.CheckDraft(r.Context(), filecoinReadinessConfig(cfg)))
}

func (s *Server) withObservabilityReadiness(ctx context.Context, result synapse.ReadinessResult) synapse.ReadinessResult {
	if s.observability == nil {
		return result
	}
	providers, err := s.observability.ListProviders(ctx, observabilityReadinessListOptions())
	if err != nil {
		result.Checks = append(result.Checks, synapse.ReadinessCheck{
			ID:      "observability_providers",
			Status:  synapse.ReadinessStatusWarning,
			Message: "Provider health state could not be loaded.",
		})
		addReadinessPartialError(&result, "observability_providers", err)
		return finishReadinessResult(result)
	}
	result.Checks = append(result.Checks, providerObservabilityReadinessCheck(
		"observability_providers",
		"Provider health has no unavailable or unknown state.",
		"Provider health needs attention.",
		providers.Summary,
		providers.LastCheckedAt,
		s.observability.RefreshInterval(),
	))

	dataSets, err := s.observability.ListDataSets(ctx, observabilityReadinessListOptions())
	if err != nil {
		result.Checks = append(result.Checks, synapse.ReadinessCheck{
			ID:      "observability_data_sets",
			Status:  synapse.ReadinessStatusWarning,
			Message: "Local data set storage health state could not be loaded.",
		})
		addReadinessPartialError(&result, "observability_data_sets", err)
		return finishReadinessResult(result)
	}
	result.Checks = append(result.Checks, observabilityReadinessCheck(
		"observability_data_sets",
		"Local data set storage health is healthy.",
		"Local data set storage health needs attention.",
		dataSets.Summary,
		dataSets.LastCheckedAt,
		s.observability.RefreshInterval(),
		dataSets.LastCheckedAt == nil && dataSets.Summary.Total == 0 && s.localDataSetInventoryEmpty(ctx),
	))
	return finishReadinessResult(result)
}

func observabilityReadinessListOptions() observability.ListOptions {
	return observability.ListOptions{Limit: 1}
}

func providerObservabilityReadinessCheck(id, readyMessage, attentionMessage string, summary observability.Summary, lastCheckedAt *time.Time, interval time.Duration) synapse.ReadinessCheck {
	if lastCheckedAt == nil {
		return synapse.ReadinessCheck{
			ID:      id,
			Status:  synapse.ReadinessStatusWarning,
			Message: attentionMessage + " No health state has been recorded yet.",
		}
	}
	stale, _ := observabilityFreshness(lastCheckedAt, interval)
	if stale {
		return synapse.ReadinessCheck{
			ID:      id,
			Status:  synapse.ReadinessStatusWarning,
			Message: attentionMessage + " The latest health state is stale.",
		}
	}
	if summary.Unknown > 0 || summary.Unavailable > 0 {
		return synapse.ReadinessCheck{
			ID:     id,
			Status: synapse.ReadinessStatusWarning,
			Message: fmt.Sprintf(
				"%s unavailable=%d unknown=%d.",
				attentionMessage,
				summary.Unavailable,
				summary.Unknown,
			),
		}
	}
	return synapse.ReadinessCheck{ID: id, Status: synapse.ReadinessStatusReady, Message: readyMessage}
}

func observabilityReadinessCheck(id, readyMessage, attentionMessage string, summary observability.Summary, lastCheckedAt *time.Time, interval time.Duration, emptyInventory bool) synapse.ReadinessCheck {
	if lastCheckedAt == nil {
		if emptyInventory && summary.Total == 0 {
			return synapse.ReadinessCheck{ID: id, Status: synapse.ReadinessStatusReady, Message: readyMessage}
		}
		return synapse.ReadinessCheck{
			ID:      id,
			Status:  synapse.ReadinessStatusWarning,
			Message: attentionMessage + " No health state has been recorded yet.",
		}
	}
	stale, _ := observabilityFreshness(lastCheckedAt, interval)
	if stale {
		return synapse.ReadinessCheck{
			ID:      id,
			Status:  synapse.ReadinessStatusWarning,
			Message: attentionMessage + " The latest health state is stale.",
		}
	}
	if summary.Unknown > 0 || summary.Unavailable > 0 || summary.Degraded > 0 {
		return synapse.ReadinessCheck{
			ID:     id,
			Status: synapse.ReadinessStatusWarning,
			Message: fmt.Sprintf(
				"%s degraded=%d unavailable=%d unknown=%d.",
				attentionMessage,
				summary.Degraded,
				summary.Unavailable,
				summary.Unknown,
			),
		}
	}
	return synapse.ReadinessCheck{ID: id, Status: synapse.ReadinessStatusReady, Message: readyMessage}
}

func (s *Server) localDataSetInventoryEmpty(ctx context.Context) bool {
	if s == nil || s.repos == nil || s.repos.Uploads == nil {
		return false
	}
	summaries, err := s.repos.Uploads.ListDataSetSummaries(ctx, 0)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("api: failed to list local data set inventory for readiness", "error", err)
		}
		return false
	}
	return len(summaries) == 0
}

func finishReadinessResult(result synapse.ReadinessResult) synapse.ReadinessResult {
	result.Finish()
	return result
}

func addReadinessPartialError(result *synapse.ReadinessResult, field string, err error) {
	if result == nil || err == nil {
		return
	}
	if result.PartialErrors == nil {
		result.PartialErrors = make(map[string]string)
	}
	result.PartialErrors[field] = "health query failed"
}

func filecoinReadinessConfig(cfg *config.Config) synapse.ReadinessConfig {
	if cfg == nil {
		return synapse.ReadinessConfig{}
	}
	return synapse.ReadinessConfigFromFilecoinConfig(cfg.Filecoin)
}
