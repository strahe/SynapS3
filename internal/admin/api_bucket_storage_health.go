package admin

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/observability"
)

const (
	bucketStorageHealthAffectedVersionsCap   = 200
	bucketStorageHealthQueryFailureLastError = "storage health query failed"
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

func (s *Server) bucketStorageHealthSummaries(ctx context.Context, bucketID int64) (map[int64]bucketStorageHealthSummaryResponse, bool) {
	if s.repos.Uploads == nil {
		return nil, false
	}
	interval := s.copyHealthRefreshInterval()
	staleBefore := time.Now().UTC().Add(-(interval * 2))
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
