package admin

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/observability"
	idtypes "github.com/strahe/synaps3/internal/types"
)

const (
	observabilityRefreshWriteGrace = 5 * time.Second
)

func (s *Server) handleAPIObservabilityProviders(w http.ResponseWriter, r *http.Request) {
	if s.observability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "observability not available"})
		return
	}
	opts, ok := s.parseObservabilityListOptions(w, r, false)
	if !ok {
		return
	}
	page, err := s.observability.ListProviderObservations(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to list provider observability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleAPIRefreshObservabilityProviders(w http.ResponseWriter, r *http.Request) {
	if s.observability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "observability not available"})
		return
	}
	if !s.requireObservabilityRefresh(w, r) {
		return
	}
	opts, ok := s.parseObservabilityListOptions(w, r, false)
	if !ok {
		return
	}
	s.extendObservabilityRefreshWriteDeadline(w)
	page, err := s.observability.RefreshProviderObservations(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to refresh provider observability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleAPIObservabilityDataSets(w http.ResponseWriter, r *http.Request) {
	if s.observability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "observability not available"})
		return
	}
	opts, ok := s.parseObservabilityListOptions(w, r, true)
	if !ok {
		return
	}
	page, err := s.observability.ListDataSetObservations(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to list data set observability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleAPIRefreshObservabilityDataSets(w http.ResponseWriter, r *http.Request) {
	if s.observability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "observability not available"})
		return
	}
	if !s.requireObservabilityRefresh(w, r) {
		return
	}
	opts, ok := s.parseObservabilityListOptions(w, r, true)
	if !ok {
		return
	}
	s.extendObservabilityRefreshWriteDeadline(w)
	page, err := s.observability.RefreshDataSetObservations(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to refresh data set observability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) parseObservabilityListOptions(w http.ResponseWriter, r *http.Request, allowBucket bool) (observability.ListOptions, bool) {
	query := r.URL.Query()
	limit, ok := parseOptionalNonNegativeInt(w, query.Get("limit"), "limit")
	if !ok {
		return observability.ListOptions{}, false
	}
	offset, ok := parseOptionalNonNegativeInt(w, query.Get("offset"), "offset")
	if !ok {
		return observability.ListOptions{}, false
	}
	status, ok := parseObservabilityStatus(w, query.Get("status"))
	if !ok {
		return observability.ListOptions{}, false
	}
	providerID, ok := parseOptionalOnChainID(w, query.Get("provider_id"), "provider_id")
	if !ok {
		return observability.ListOptions{}, false
	}
	opts := observability.ListOptions{
		Limit:      limit,
		Offset:     offset,
		Status:     status,
		ProviderID: providerID,
	}
	if allowBucket {
		bucketID, ok := s.parseObservabilityBucketFilter(r, w, query.Get("bucket"), query.Get("bucket_id"))
		if !ok {
			return observability.ListOptions{}, false
		}
		opts.BucketID = bucketID
	}
	return opts, true
}

func parseOptionalNonNegativeInt(w http.ResponseWriter, raw string, field string) (int, bool) {
	if strings.TrimSpace(raw) == "" {
		return 0, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": field + " must be a non-negative integer"})
		return 0, false
	}
	return value, true
}

func parseObservabilityStatus(w http.ResponseWriter, raw string) (observability.Status, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", true
	}
	status := observability.Status(raw)
	switch status {
	case observability.StatusAvailable, observability.StatusDegraded, observability.StatusUnavailable, observability.StatusUnknown:
		return status, true
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "status must be available, degraded, unavailable, or unknown"})
		return "", false
	}
}

func parseOptionalOnChainID(w http.ResponseWriter, raw string, field string) (*idtypes.OnChainID, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}
	id, err := idtypes.ParseOnChainID(field, raw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return nil, false
	}
	return &id, true
}

func (s *Server) parseObservabilityBucketFilter(r *http.Request, w http.ResponseWriter, rawName string, rawID string) (int64, bool) {
	rawName = strings.TrimSpace(rawName)
	rawID = strings.TrimSpace(rawID)
	if rawName != "" && rawID != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bucket and bucket_id filters are mutually exclusive"})
		return 0, false
	}
	if rawID != "" {
		id, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || id <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bucket_id must be a positive integer"})
			return 0, false
		}
		return id, true
	}
	if rawName == "" {
		return 0, true
	}
	bucket, err := s.repos.Buckets.GetByName(r.Context(), rawName)
	if err != nil {
		s.logger.Error("api: failed to resolve observability bucket filter", "error", err, "bucket", rawName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return 0, false
	}
	if bucket == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bucket filter does not match a bucket"})
		return 0, false
	}
	return bucket.ID, true
}

func (s *Server) extendObservabilityRefreshWriteDeadline(w http.ResponseWriter) {
	if s.observability == nil {
		return
	}
	deadline := time.Now().Add(s.observability.RefreshTimeout() + observabilityRefreshWriteGrace)
	if err := http.NewResponseController(w).SetWriteDeadline(deadline); err != nil && s.logger != nil {
		s.logger.Warn("api: failed to set observability refresh write deadline", "error", err)
	}
}

func (s *Server) requireObservabilityRefresh(w http.ResponseWriter, r *http.Request) bool {
	return true
}
