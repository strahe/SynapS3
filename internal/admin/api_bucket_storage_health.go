package admin

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	idtypes "github.com/strahe/synaps3/internal/types"
)

const (
	bucketStorageHealthAffectedVersionsCap     = 200
	bucketStorageHealthQueryFailureLastError   = "storage health query failed"
	bucketStorageHealthAffectedVersionsDefault = 50
	bucketStorageHealthAffectedVersionsMax     = 1000
)

var (
	errBucketStorageRiskMarkerIncomplete    = errors.New("key_marker, version_marker, created_at_marker, and stale_before must be provided together")
	errBucketStorageRiskCreatedAtMarkerTime = errors.New("created_at_marker must be RFC3339Nano")
	errBucketStorageRiskStaleBeforeTime     = errors.New("stale_before must be RFC3339Nano")
)

type bucketStorageHealthSummaryResponse struct {
	Status                     string                     `json:"status"`
	ReasonCodes                []observability.ReasonCode `json:"reason_codes"`
	Stale                      bool                       `json:"stale"`
	LastCheckedAt              string                     `json:"last_checked_at,omitempty"`
	LastError                  *string                    `json:"last_error,omitempty"`
	AbnormalDataSets           int                        `json:"abnormal_data_sets"`
	AffectedVersionsCapped     int                        `json:"affected_versions_capped"`
	AffectedVersionsCap        int                        `json:"affected_versions_cap"`
	AffectedVersionsExceedsCap bool                       `json:"affected_versions_exceeds_cap"`
}

type bucketStorageHealthAffectedVersionsResponse struct {
	Versions            []bucketStorageHealthAffectedVersionResponse `json:"versions"`
	HasMore             bool                                         `json:"has_more"`
	NextKeyMarker       string                                       `json:"next_key_marker,omitempty"`
	NextVersionMarker   string                                       `json:"next_version_marker,omitempty"`
	NextCreatedAtMarker string                                       `json:"next_created_at_marker,omitempty"`
	StaleBefore         string                                       `json:"stale_before"`
}

type bucketStorageHealthAffectedVersionResponse struct {
	Key                      string                                   `json:"key"`
	VersionID                string                                   `json:"version_id"`
	Size                     int64                                    `json:"size"`
	State                    string                                   `json:"state"`
	IsCurrent                bool                                     `json:"is_current"`
	InCache                  bool                                     `json:"in_cache"`
	ContentType              string                                   `json:"content_type"`
	ETag                     string                                   `json:"etag"`
	CreatedAt                string                                   `json:"created_at"`
	UpdatedAt                string                                   `json:"updated_at"`
	ReadableAlternativeCount int                                      `json:"readable_alternative_count"`
	HasReadableAlternative   bool                                     `json:"has_readable_alternative"`
	RiskDataSets             []bucketStorageHealthRiskDataSetResponse `json:"risk_data_sets"`
}

type bucketStorageHealthRiskDataSetResponse struct {
	LocalDataSetID   int64                     `json:"local_data_set_id"`
	BucketID         int64                     `json:"bucket_id"`
	CopyIndex        int                       `json:"copy_index"`
	ProviderID       string                    `json:"provider_id"`
	ProviderIdentity *providerIdentityResponse `json:"provider_identity,omitempty"`
	DataSetID        *string                   `json:"data_set_id,omitempty"`
	ClientDataSetID  *string                   `json:"client_data_set_id,omitempty"`
	LocalStatus      string                    `json:"local_status"`
	StorageHealth    dataSetStorageHealthInfo  `json:"storage_health"`
}

func (s *Server) handleAPIBucketStorageHealthAffectedVersions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	prefix := r.URL.Query().Get("prefix")
	key := r.URL.Query().Get("key")
	if prefix != "" && key != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "prefix and key are mutually exclusive"})
		return
	}
	var localDataSetID int64
	if raw := r.URL.Query().Get("local_data_set_id"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "local_data_set_id must be a positive integer"})
			return
		}
		localDataSetID = parsed
	}
	limit := parseAdminPositiveLimit(r.URL.Query().Get("limit"), bucketStorageHealthAffectedVersionsDefault, bucketStorageHealthAffectedVersionsMax)
	keyMarker := r.URL.Query().Get("key_marker")
	versionMarker := r.URL.Query().Get("version_marker")
	createdAtMarkerRaw := r.URL.Query().Get("created_at_marker")
	staleBeforeRaw := r.URL.Query().Get("stale_before")
	createdAtMarker, staleBefore, markerErr := parseBucketStorageRiskMarker(keyMarker, versionMarker, createdAtMarkerRaw, staleBeforeRaw)
	if markerErr != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": markerErr.Error()})
		return
	}
	if staleBefore.IsZero() {
		staleBefore = s.bucketStorageHealthStaleBefore()
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket storage risk", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}
	if s.repos.Uploads == nil {
		writeJSON(w, http.StatusOK, bucketStorageHealthAffectedVersionsResponse{Versions: []bucketStorageHealthAffectedVersionResponse{}})
		return
	}

	page, err := s.repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:        bucket.ID,
		LocalDataSetID:  localDataSetID,
		Prefix:          prefix,
		Key:             key,
		KeyMarker:       keyMarker,
		VersionIDMarker: versionMarker,
		CreatedAtMarker: createdAtMarker,
		StaleBefore:     staleBefore,
		Limit:           limit,
	})
	if err != nil {
		s.logger.Error("api: failed to list bucket storage risk versions", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s.bucketStorageHealthAffectedVersionsResponse(page, staleBefore))
}

func (s *Server) bucketStorageHealthSummaries(ctx context.Context, bucketID int64) (map[int64]bucketStorageHealthSummaryResponse, bool) {
	if s.repos.Uploads == nil {
		return nil, false
	}
	staleBefore := s.bucketStorageHealthStaleBefore()
	summaries, err := s.repos.Uploads.ListBucketStorageHealthSummaries(ctx, bucketID, staleBefore, bucketStorageHealthAffectedVersionsCap)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("api: failed to load bucket storage health facts", "error", err, "bucketID", bucketID)
		}
		return nil, true
	}
	out := make(map[int64]bucketStorageHealthSummaryResponse, len(summaries))
	for _, summary := range summaries {
		out[summary.BucketID] = bucketStorageHealthSummaryFromRepository(summary)
	}
	return out, false
}

func bucketStorageHealthSummaryForBucket(summaries map[int64]bucketStorageHealthSummaryResponse, bucketID int64, queryFailed bool) bucketStorageHealthSummaryResponse {
	if queryFailed {
		return bucketStorageHealthQueryFailureSummary()
	}
	if summary, ok := summaries[bucketID]; ok {
		return summary
	}
	return emptyBucketStorageHealthSummary()
}

func emptyBucketStorageHealthSummary() bucketStorageHealthSummaryResponse {
	return bucketStorageHealthSummaryResponse{
		Status:              string(observability.StatusAvailable),
		ReasonCodes:         []observability.ReasonCode{},
		AffectedVersionsCap: bucketStorageHealthAffectedVersionsCap,
	}
}

func bucketStorageHealthQueryFailureSummary() bucketStorageHealthSummaryResponse {
	errText := bucketStorageHealthQueryFailureLastError
	return bucketStorageHealthSummaryResponse{
		Status:              string(observability.StatusUnknown),
		ReasonCodes:         []observability.ReasonCode{},
		LastError:           &errText,
		AffectedVersionsCap: bucketStorageHealthAffectedVersionsCap,
	}
}

func bucketStorageHealthSummaryFromRepository(summary repository.BucketStorageHealthSummary) bucketStorageHealthSummaryResponse {
	resp := bucketStorageHealthSummaryResponse{
		Status:                     string(bucketStorageHealthStatusFromRepository(summary)),
		ReasonCodes:                bucketStorageHealthReasonsFromRepository(summary),
		Stale:                      summary.ObservationStale,
		AbnormalDataSets:           summary.AbnormalDataSets,
		AffectedVersionsCapped:     summary.AffectedVersionsCapped,
		AffectedVersionsCap:        summary.AffectedVersionsCap,
		AffectedVersionsExceedsCap: summary.AffectedVersionsExceedsCap,
	}
	if summary.LastCheckedAt != nil {
		resp.LastCheckedAt = summary.LastCheckedAt.UTC().Format(time.RFC3339)
	}
	return resp
}

func parseBucketStorageRiskMarker(keyMarker, versionMarker, createdAtMarkerRaw, staleBeforeRaw string) (time.Time, time.Time, error) {
	// stale_before pins one Storage Risk pagination session to a stable risk snapshot.
	hasKey := keyMarker != ""
	hasVersion := versionMarker != ""
	hasCreatedAt := createdAtMarkerRaw != ""
	hasStaleBefore := staleBeforeRaw != ""
	if !hasKey && !hasVersion && !hasCreatedAt {
		if !hasStaleBefore {
			return time.Time{}, time.Time{}, nil
		}
		staleBefore, err := time.Parse(time.RFC3339Nano, staleBeforeRaw)
		if err != nil {
			return time.Time{}, time.Time{}, errBucketStorageRiskStaleBeforeTime
		}
		return time.Time{}, staleBefore.UTC(), nil
	}
	if !hasKey || !hasVersion || !hasCreatedAt || !hasStaleBefore {
		return time.Time{}, time.Time{}, errBucketStorageRiskMarkerIncomplete
	}
	createdAt, err := time.Parse(time.RFC3339Nano, createdAtMarkerRaw)
	if err != nil {
		return time.Time{}, time.Time{}, errBucketStorageRiskCreatedAtMarkerTime
	}
	staleBefore, err := time.Parse(time.RFC3339Nano, staleBeforeRaw)
	if err != nil {
		return time.Time{}, time.Time{}, errBucketStorageRiskStaleBeforeTime
	}
	return createdAt.UTC(), staleBefore.UTC(), nil
}

func (s *Server) bucketStorageHealthStaleBefore() time.Time {
	// Allow one missed refresh interval before marking an observation stale.
	return time.Now().UTC().Add(-(s.copyHealthRefreshInterval() * 2))
}

func bucketStorageHealthStatusFromRepository(summary repository.BucketStorageHealthSummary) observability.Status {
	if summary.AffectedVersionsCapped == 0 {
		return observability.StatusAvailable
	}
	if summary.LocalStatusNotReady || summary.ObservationUnavailable {
		return observability.StatusUnavailable
	}
	if summary.ObservationMissing || summary.ObservationStale || summary.ObservationUnknown {
		return observability.StatusUnknown
	}
	return observability.StatusDegraded
}

func bucketStorageHealthReasonsFromRepository(summary repository.BucketStorageHealthSummary) []observability.ReasonCode {
	if summary.AffectedVersionsCapped == 0 {
		return []observability.ReasonCode{}
	}
	reasons := make([]observability.ReasonCode, 0, len(summary.ReasonCodes)+1)
	for _, reason := range summary.ReasonCodes {
		reasons = observability.AppendReasonCode(reasons, reason)
	}
	if summary.ObservationMissing || summary.ObservationStale || summary.ObservationUnknown {
		reasons = observability.AppendReasonCode(reasons, observability.ReasonCopyObservationMissing)
	}
	return reasons
}

func (s *Server) bucketStorageHealthAffectedVersionsResponse(page repository.BucketStorageHealthAffectedVersionPage, staleBefore time.Time) bucketStorageHealthAffectedVersionsResponse {
	providerIDs := make([]idtypes.OnChainID, 0)
	seenProviderIDs := make(map[string]struct{})
	for _, version := range page.Versions {
		for _, dataSet := range version.RiskDataSets {
			key := dataSet.ProviderID.String()
			if _, ok := seenProviderIDs[key]; ok {
				continue
			}
			seenProviderIDs[key] = struct{}{}
			providerIDs = append(providerIDs, dataSet.ProviderID)
		}
	}
	identities := s.providerIdentities(providerIDs)
	resp := bucketStorageHealthAffectedVersionsResponse{
		Versions:          make([]bucketStorageHealthAffectedVersionResponse, 0, len(page.Versions)),
		HasMore:           page.HasMore,
		NextKeyMarker:     page.NextKeyMarker,
		NextVersionMarker: page.NextVersionIDMarker,
		StaleBefore:       staleBefore.UTC().Format(time.RFC3339Nano),
	}
	if !page.NextCreatedAtMarker.IsZero() {
		resp.NextCreatedAtMarker = page.NextCreatedAtMarker.UTC().Format(time.RFC3339Nano)
	}
	for _, version := range page.Versions {
		riskDataSets := make([]bucketStorageHealthRiskDataSetResponse, 0, len(version.RiskDataSets))
		for _, dataSet := range version.RiskDataSets {
			riskDataSets = append(riskDataSets, bucketStorageHealthRiskDataSetResponse{
				LocalDataSetID:   dataSet.LocalDataSetID,
				BucketID:         dataSet.BucketID,
				CopyIndex:        dataSet.CopyIndex,
				ProviderID:       dataSet.ProviderID.String(),
				ProviderIdentity: providerIdentityFromSnapshot(identities, dataSet.ProviderID),
				DataSetID:        onChainIDStringPtr(dataSet.DataSetID),
				ClientDataSetID:  onChainIDStringPtr(dataSet.ClientDataSetID),
				LocalStatus:      string(dataSet.LocalStatus),
				StorageHealth:    bucketStorageHealthRiskDataSetStorageHealth(dataSet),
			})
		}
		resp.Versions = append(resp.Versions, bucketStorageHealthAffectedVersionResponse{
			Key:                      version.Version.Key,
			VersionID:                version.Version.VersionID,
			Size:                     version.Version.Size,
			State:                    string(version.Version.State),
			IsCurrent:                version.Version.IsCurrent,
			InCache:                  version.Version.InCache,
			ContentType:              version.Version.ContentType,
			ETag:                     version.Version.ETag,
			CreatedAt:                version.Version.CreatedAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt:                version.Version.UpdatedAt.UTC().Format(time.RFC3339Nano),
			ReadableAlternativeCount: version.ReadableAlternativeCount,
			HasReadableAlternative:   version.ReadableAlternativeCount > 0,
			RiskDataSets:             riskDataSets,
		})
	}
	return resp
}

func parseAdminPositiveLimit(raw string, defaultLimit int, maxLimit int) int {
	n, err := strconv.Atoi(raw)
	if raw == "" || err != nil || n <= 0 {
		return defaultLimit
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

func bucketStorageHealthRiskDataSetStorageHealth(dataSet repository.BucketStorageHealthRiskDataSet) dataSetStorageHealthInfo {
	status := observability.StatusUnknown
	if dataSet.ObservationStatus != nil {
		status = *dataSet.ObservationStatus
	}
	reasons := make([]observability.ReasonCode, 0, len(dataSet.ReasonCodes)+2)
	for _, reason := range dataSet.ReasonCodes {
		reasons = observability.AppendReasonCode(reasons, reason)
	}
	if dataSet.ObservationMissing || dataSet.ObservationStale || status == observability.StatusUnknown {
		reasons = observability.AppendReasonCode(reasons, observability.ReasonCopyObservationMissing)
	}
	if dataSet.LocalStatus != model.StorageDataSetStatusReady && dataSet.LocalStatus != model.StorageDataSetStatusDraining {
		reasons = observability.AppendReasonCode(reasons, observability.ReasonLocalStatusNotReady)
	}
	var lastCheckedAt string
	if dataSet.LastCheckedAt != nil {
		lastCheckedAt = dataSet.LastCheckedAt.UTC().Format(time.RFC3339Nano)
	}
	return dataSetStorageHealthInfo{
		Status:        string(status),
		ReasonCodes:   reasons,
		LastCheckedAt: lastCheckedAt,
		LastError:     dataSet.LastError,
		Stale:         dataSet.ObservationStale,
	}
}
