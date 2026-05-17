package admin

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/availability"
	idtypes "github.com/strahe/synaps3/internal/types"
)

const availabilityRefreshWriteGrace = 5 * time.Second

type availabilityProviderListResponse struct {
	Items         []availability.ProviderSnapshot `json:"items"`
	Summary       availability.Summary            `json:"summary"`
	LastCheckedAt *time.Time                      `json:"last_checked_at,omitempty"`
	Stale         bool                            `json:"stale"`
	Warnings      []string                        `json:"warnings"`
	Total         int                             `json:"total"`
	Limit         int                             `json:"limit"`
	Offset        int                             `json:"offset"`
}

type availabilityDataSetListResponse struct {
	Items         []availability.DataSetSnapshot `json:"items"`
	Summary       availability.Summary           `json:"summary"`
	LastCheckedAt *time.Time                     `json:"last_checked_at,omitempty"`
	Stale         bool                           `json:"stale"`
	Warnings      []string                       `json:"warnings"`
	Total         int                            `json:"total"`
	Limit         int                            `json:"limit"`
	Offset        int                            `json:"offset"`
}

func (s *Server) handleAPIAvailabilityProviders(w http.ResponseWriter, r *http.Request) {
	if s.availability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "availability not available"})
		return
	}
	opts, ok := s.parseAvailabilityListOptions(w, r, false)
	if !ok {
		return
	}
	page, err := s.availability.ListProviders(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to list provider availability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s.providerAvailabilityResponse(page))
}

func (s *Server) handleAPIRefreshAvailabilityProviders(w http.ResponseWriter, r *http.Request) {
	if s.availability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "availability not available"})
		return
	}
	if !s.requireAvailabilityWrite(w, r) {
		return
	}
	opts, ok := s.parseAvailabilityListOptions(w, r, false)
	if !ok {
		return
	}
	s.extendAvailabilityRefreshWriteDeadline(w)
	page, err := s.availability.RefreshProviders(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to refresh provider availability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s.providerAvailabilityResponse(page))
}

func (s *Server) handleAPIAvailabilityDataSets(w http.ResponseWriter, r *http.Request) {
	if s.availability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "availability not available"})
		return
	}
	opts, ok := s.parseAvailabilityListOptions(w, r, true)
	if !ok {
		return
	}
	page, err := s.availability.ListDataSets(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to list data set availability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s.dataSetAvailabilityResponse(page))
}

func (s *Server) handleAPIRefreshAvailabilityDataSets(w http.ResponseWriter, r *http.Request) {
	if s.availability == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "availability not available"})
		return
	}
	if !s.requireAvailabilityWrite(w, r) {
		return
	}
	opts, ok := s.parseAvailabilityListOptions(w, r, true)
	if !ok {
		return
	}
	s.extendAvailabilityRefreshWriteDeadline(w)
	page, err := s.availability.RefreshDataSets(r.Context(), opts)
	if err != nil {
		s.logger.Error("api: failed to refresh data set availability", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s.dataSetAvailabilityResponse(page))
}

func (s *Server) parseAvailabilityListOptions(w http.ResponseWriter, r *http.Request, allowBucket bool) (availability.ListOptions, bool) {
	query := r.URL.Query()
	limit, ok := parseOptionalNonNegativeInt(w, query.Get("limit"), "limit")
	if !ok {
		return availability.ListOptions{}, false
	}
	offset, ok := parseOptionalNonNegativeInt(w, query.Get("offset"), "offset")
	if !ok {
		return availability.ListOptions{}, false
	}
	status, ok := parseAvailabilityStatus(w, query.Get("status"))
	if !ok {
		return availability.ListOptions{}, false
	}
	providerID, ok := parseOptionalOnChainID(w, query.Get("provider_id"), "provider_id")
	if !ok {
		return availability.ListOptions{}, false
	}
	opts := availability.ListOptions{
		Limit:      limit,
		Offset:     offset,
		Status:     status,
		ProviderID: providerID,
	}
	if allowBucket {
		bucketID, ok := s.parseAvailabilityBucketFilter(r, w, query.Get("bucket"), query.Get("bucket_id"))
		if !ok {
			return availability.ListOptions{}, false
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

func parseAvailabilityStatus(w http.ResponseWriter, raw string) (availability.Status, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", true
	}
	status := availability.Status(raw)
	switch status {
	case availability.StatusAvailable, availability.StatusDegraded, availability.StatusUnavailable, availability.StatusUnknown:
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

func (s *Server) parseAvailabilityBucketFilter(r *http.Request, w http.ResponseWriter, rawName string, rawID string) (int64, bool) {
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
		s.logger.Error("api: failed to resolve availability bucket filter", "error", err, "bucket", rawName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return 0, false
	}
	if bucket == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bucket filter does not match a bucket"})
		return 0, false
	}
	return bucket.ID, true
}

func (s *Server) extendAvailabilityRefreshWriteDeadline(w http.ResponseWriter) {
	if s.availability == nil {
		return
	}
	deadline := time.Now().Add(s.availability.RefreshTimeout() + availabilityRefreshWriteGrace)
	if err := http.NewResponseController(w).SetWriteDeadline(deadline); err != nil && s.logger != nil {
		s.logger.Warn("api: failed to set availability refresh write deadline", "error", err)
	}
}

func (s *Server) providerAvailabilityResponse(page availability.ProviderSnapshotPage) availabilityProviderListResponse {
	stale, warnings := availabilityFreshness(page.LastCheckedAt, s.availability.RefreshInterval())
	return availabilityProviderListResponse{
		Items:         page.Items,
		Summary:       page.Summary,
		LastCheckedAt: page.LastCheckedAt,
		Stale:         stale,
		Warnings:      warnings,
		Total:         page.Total,
		Limit:         page.Limit,
		Offset:        page.Offset,
	}
}

func (s *Server) dataSetAvailabilityResponse(page availability.DataSetSnapshotPage) availabilityDataSetListResponse {
	stale, warnings := availabilityFreshness(page.LastCheckedAt, s.availability.RefreshInterval())
	return availabilityDataSetListResponse{
		Items:         page.Items,
		Summary:       page.Summary,
		LastCheckedAt: page.LastCheckedAt,
		Stale:         stale,
		Warnings:      warnings,
		Total:         page.Total,
		Limit:         page.Limit,
		Offset:        page.Offset,
	}
}

func availabilityFreshness(lastCheckedAt *time.Time, interval time.Duration) (bool, []string) {
	if lastCheckedAt == nil {
		return false, []string{"no_snapshots"}
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if time.Since(*lastCheckedAt) > interval*2 {
		return true, []string{string(availability.ReasonStaleSnapshot)}
	}
	return false, []string{}
}

func (s *Server) requireAvailabilityWrite(w http.ResponseWriter, r *http.Request) bool {
	if !s.settingsWritable() {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "availability refresh requires loopback admin binding"})
		return false
	}
	if r.Header.Get(settingsWriteHeader) != settingsWriteHeaderValue {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing settings write header"})
		return false
	}
	return true
}
