package admin

import (
	"net/http"
	"sync"
	"time"
)

type providerTierRefreshResult struct {
	Success     bool       `json:"success"`
	AttemptedAt time.Time  `json:"attempted_at"`
	CheckedAt   *time.Time `json:"checked_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

func (s *Server) handleAPIRefreshProviderTiers(w http.ResponseWriter, r *http.Request) {
	if s.observability == nil || !s.observability.ProviderTiersAvailable() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Provider lists are unavailable"})
		return
	}
	s.extendObservabilityRefreshWriteDeadline(w)
	var approved, endorsed providerTierRefreshResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		at, err := s.observability.RefreshApprovedProviders(r.Context())
		approved.AttemptedAt = at
		if err != nil {
			s.logger.Warn("api: failed to refresh approved providers", "error", err)
			approved.Error = "Could not refresh approved providers"
			return
		}
		approved.Success = true
		approved.CheckedAt = &at
	}()
	go func() {
		defer wg.Done()
		at, err := s.observability.RefreshEndorsedProviders(r.Context())
		endorsed.AttemptedAt = at
		if err != nil {
			s.logger.Warn("api: failed to refresh endorsed providers", "error", err)
			endorsed.Error = "Could not refresh endorsed providers"
			return
		}
		endorsed.Success = true
		endorsed.CheckedAt = &at
	}()
	wg.Wait()
	if approved.Success || endorsed.Success {
		if s.events != nil {
			s.events.Publish("provider_catalog_updated", map[string]any{})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"approved_result": approved, "endorsed_result": endorsed})
}
