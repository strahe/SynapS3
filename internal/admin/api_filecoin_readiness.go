package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/strahe/synaps3/internal/config"
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
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "settings writes require application/json"})
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

func filecoinReadinessConfig(cfg *config.Config) synapse.ReadinessConfig {
	if cfg == nil {
		return synapse.ReadinessConfig{}
	}
	return synapse.ReadinessConfigFromFilecoinConfig(cfg.Filecoin)
}
