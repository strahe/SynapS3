package admin

import (
	"context"
	"sort"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/types"
)

const copyHealthQueryFailureLastError = "copy health query failed"

type copyHealthInfo struct {
	Status        string                     `json:"status"`
	ReasonCodes   []observability.ReasonCode `json:"reason_codes"`
	Stale         bool                       `json:"stale"`
	LastCheckedAt string                     `json:"last_checked_at,omitempty"`
	LastError     *string                    `json:"last_error,omitempty"`
}

type copyHealthSummaryResponse struct {
	Status           string                     `json:"status"`
	ReasonCodes      []observability.ReasonCode `json:"reason_codes"`
	Stale            bool                       `json:"stale"`
	LastCheckedAt    string                     `json:"last_checked_at,omitempty"`
	LastError        *string                    `json:"last_error,omitempty"`
	TotalObjects     int                        `json:"total_objects"`
	UnhealthyObjects int                        `json:"unhealthy_objects"`
	RequestedCopies  int                        `json:"requested_copies"`
	ReadableCopies   int                        `json:"readable_copies"`
	PendingCopies    int                        `json:"pending_copies"`
	FailedCopies     int                        `json:"failed_copies"`
	UnknownCopies    int                        `json:"unknown_copies"`
}

type copyHealthFact struct {
	BucketID        int64
	VersionID       string
	UploadID        *int64
	UploadStatus    *model.StorageUploadStatus
	RequestedCopies int
	CopyIndex       *int
	CopyStatus      *model.StorageUploadCopyStatus
	ProviderID      *types.OnChainID
	LocalDataSetID  *int64
	ChainDataSetID  *types.OnChainID
	PieceID         *types.OnChainID
	RetrievalURL    *string
	LastError       *string
}

func (s *Server) copyHealthDataSetObservations(ctx context.Context, localIDs []int64) (map[int64]observability.DataSetObservation, bool) {
	if len(localIDs) == 0 {
		return nil, false
	}
	if s.observability != nil {
		states, err := s.observability.DataSetObservationsByLocalIDs(ctx, localIDs)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("api: failed to enrich copy health", "error", err)
			}
			return nil, true
		}
		return states, false
	}
	if s.repos == nil || s.repos.Observability == nil {
		return nil, false
	}
	fallback := observability.NewService(observability.ServiceOptions{
		Store:           s.repos.Observability,
		RefreshInterval: s.copyHealthRefreshInterval(),
	})
	states, err := fallback.DataSetObservationsByLocalIDs(ctx, localIDs)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("api: failed to enrich copy health", "error", err)
		}
		return nil, true
	}
	return states, false
}

func copyHealthLocalDataSetIDs(facts []copyHealthFact) []int64 {
	seen := make(map[int64]struct{})
	ids := make([]int64, 0)
	for _, fact := range facts {
		if fact.LocalDataSetID == nil || *fact.LocalDataSetID == 0 {
			continue
		}
		if _, ok := seen[*fact.LocalDataSetID]; ok {
			continue
		}
		seen[*fact.LocalDataSetID] = struct{}{}
		ids = append(ids, *fact.LocalDataSetID)
	}
	return ids
}

func copyHealthSummaryForBucket(summaries map[int64]copyHealthSummaryResponse, bucketID int64, queryFailed bool) copyHealthSummaryResponse {
	if queryFailed {
		return copyHealthQueryFailureSummary()
	}
	if summary, ok := summaries[bucketID]; ok {
		return summary
	}
	return emptyCopyHealthSummary()
}

type copyHealthObjectKey struct {
	bucketID  int64
	versionID string
}

func copyHealthSummariesByBucket(facts []copyHealthFact, observations map[int64]observability.DataSetObservation, observationFailed bool, interval time.Duration) map[int64]copyHealthSummaryResponse {
	now := time.Now().UTC()
	objects := make(map[copyHealthObjectKey]*copyHealthObjectAccumulator)
	order := make([]copyHealthObjectKey, 0)
	for _, fact := range facts {
		key := copyHealthObjectKey{bucketID: fact.BucketID, versionID: fact.VersionID}
		object := objects[key]
		if object == nil {
			object = &copyHealthObjectAccumulator{
				bucketID:        fact.BucketID,
				requestedCopies: fact.RequestedCopies,
				status:          observability.StatusAvailable,
				reasonSet:       make(map[observability.ReasonCode]struct{}),
			}
			objects[key] = object
			order = append(order, key)
		}
		if fact.UploadID != nil {
			object.hasUpload = true
		}
		if fact.UploadStatus != nil {
			object.uploadStatus = fact.UploadStatus
		}
		if fact.CopyStatus == nil {
			continue
		}
		signal := copyHealthSignalFromFact(fact, observations, observationFailed, interval, now)
		object.addCandidate(fact, signal)
	}

	summaries := make(map[int64]copyHealthSummaryResponse)
	for _, key := range order {
		object := objects[key]
		object.summarizePolicyCandidates()
		summary := summaries[object.bucketID]
		if summary.Status == "" {
			summary = emptyCopyHealthSummary()
		}
		summary.TotalObjects++
		if object.status != observability.StatusAvailable || len(object.reasons) > 0 {
			summary.UnhealthyObjects++
		}
		summary.Status = string(worstObservabilityStatus(observability.Status(summary.Status), object.status))
		summary.RequestedCopies += object.requestedCopies
		summary.ReadableCopies += object.readableCopies
		summary.PendingCopies += object.pendingCopies
		summary.FailedCopies += object.failedCopies
		summary.UnknownCopies += object.unknownCopies
		summary.Stale = summary.Stale || object.stale
		summary.LastCheckedAt = oldestLastCheckedAtString(summary.LastCheckedAt, object.lastCheckedAt)
		if summary.LastError == nil {
			summary.LastError = object.lastError
		}
		for _, reason := range object.reasons {
			summary.ReasonCodes = observability.AppendReasonCode(summary.ReasonCodes, reason)
		}
		summaries[object.bucketID] = summary
	}
	return summaries
}

type copyHealthCandidateKind int

const (
	copyHealthCandidateReadable copyHealthCandidateKind = iota
	copyHealthCandidateUnverified
	copyHealthCandidatePending
	copyHealthCandidateFailed
	copyHealthCandidateUnknown
)

type copyHealthCandidate struct {
	copyIndex       int
	kind            copyHealthCandidateKind
	signal          observability.Signal
	underReplicated bool
}

type copyHealthObjectAccumulator struct {
	bucketID        int64
	requestedCopies int
	hasUpload       bool
	uploadStatus    *model.StorageUploadStatus
	candidates      []copyHealthCandidate
	status          observability.Status
	reasons         []observability.ReasonCode
	reasonSet       map[observability.ReasonCode]struct{}
	stale           bool
	lastCheckedAt   *time.Time
	lastError       *string
	readableCopies  int
	pendingCopies   int
	failedCopies    int
	unknownCopies   int
}

func (a *copyHealthObjectAccumulator) addCandidate(fact copyHealthFact, signal observability.Signal) {
	copyIndex := 0
	if fact.CopyIndex != nil {
		copyIndex = *fact.CopyIndex
	}
	kind := copyHealthCandidateKindForFact(fact, signal)
	a.candidates = append(a.candidates, copyHealthCandidate{
		copyIndex:       copyIndex,
		kind:            kind,
		signal:          signal,
		underReplicated: copyHealthCandidateUnderReplicated(kind),
	})
}

func (a *copyHealthObjectAccumulator) summarizePolicyCandidates() {
	a.status = observability.StatusAvailable
	a.reasons = nil
	a.reasonSet = make(map[observability.ReasonCode]struct{})
	a.stale = false
	a.lastCheckedAt = nil
	a.lastError = nil
	a.readableCopies = 0
	a.pendingCopies = 0
	a.failedCopies = 0
	a.unknownCopies = 0
	underReplicated := false

	if a.requestedCopies <= 0 {
		if !a.hasUpload {
			a.markNoUpload()
		}
		return
	}

	sort.SliceStable(a.candidates, func(i, j int) bool {
		leftPriority := copyHealthCandidatePriority(a.candidates[i].kind)
		rightPriority := copyHealthCandidatePriority(a.candidates[j].kind)
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		return a.candidates[i].copyIndex < a.candidates[j].copyIndex
	})

	selected := min(a.requestedCopies, len(a.candidates))
	for i := 0; i < selected; i++ {
		candidate := a.candidates[i]
		a.addSelectedCandidate(candidate)
		underReplicated = underReplicated || candidate.underReplicated
	}
	gapCopies := a.requestedCopies - selected
	if gapCopies > 0 {
		underReplicated = true
		a.classifyGapCopies(gapCopies)
	}
	a.status = worstObservabilityStatus(a.status, copyHealthStatusFromCounts(a.failedCopies, a.unknownCopies, a.pendingCopies))
	if underReplicated {
		a.addReason(observability.ReasonCopyUnderReplicated)
		if a.status == observability.StatusAvailable {
			a.status = observability.StatusDegraded
		}
	}
}

func (a *copyHealthObjectAccumulator) addSelectedCandidate(candidate copyHealthCandidate) {
	signal := candidate.signal
	a.status = worstObservabilityStatus(a.status, signal.Status)
	a.stale = a.stale || signal.Freshness.Stale
	if signal.Freshness.LastCheckedAt != nil && (a.lastCheckedAt == nil || signal.Freshness.LastCheckedAt.Before(*a.lastCheckedAt)) {
		checkedAt := *signal.Freshness.LastCheckedAt
		a.lastCheckedAt = &checkedAt
	}
	if a.lastError == nil {
		a.lastError = signal.LastError
	}
	for _, reason := range signal.ReasonCodes {
		a.addReason(reason)
	}
	switch candidate.kind {
	case copyHealthCandidateReadable:
		a.readableCopies++
	case copyHealthCandidatePending:
		a.pendingCopies++
	case copyHealthCandidateFailed:
		a.failedCopies++
	default:
		a.unknownCopies++
	}
}

func (a *copyHealthObjectAccumulator) markNoUpload() {
	a.status = observability.StatusUnknown
	a.addReason(observability.ReasonCopyObservationMissing)
}

func (a *copyHealthObjectAccumulator) classifyGapCopies(count int) {
	if count <= 0 {
		return
	}
	switch {
	case a.uploadStatus != nil && (*a.uploadStatus == model.StorageUploadStatusFailed || *a.uploadStatus == model.StorageUploadStatusRejected):
		a.failedCopies += count
		a.addReason(observability.ReasonCopyFailed)
	case a.uploadStatus != nil && (*a.uploadStatus == model.StorageUploadStatusRunning || *a.uploadStatus == model.StorageUploadStatusIngressReady || *a.uploadStatus == model.StorageUploadStatusReadable):
		a.pendingCopies += count
		a.addReason(observability.ReasonCopyPending)
	default:
		a.unknownCopies += count
		a.addReason(observability.ReasonCopyObservationMissing)
	}
}

func (a *copyHealthObjectAccumulator) addReason(reason observability.ReasonCode) {
	if _, ok := a.reasonSet[reason]; ok {
		return
	}
	a.reasonSet[reason] = struct{}{}
	a.reasons = append(a.reasons, reason)
}

func (s *Server) copyHealthRefreshInterval() time.Duration {
	if s.observability == nil {
		return 5 * time.Minute
	}
	return s.observability.RefreshInterval()
}

func copyHealthCandidateKindForFact(fact copyHealthFact, signal observability.Signal) copyHealthCandidateKind {
	switch derefCopyStatus(fact.CopyStatus) {
	case model.StorageUploadCopyStatusPending, model.StorageUploadCopyStatusPieceReady, model.StorageUploadCopyStatusCommitting:
		return copyHealthCandidatePending
	case model.StorageUploadCopyStatusFailed:
		return copyHealthCandidateFailed
	case model.StorageUploadCopyStatusCommitted:
		if signal.Status == observability.StatusAvailable {
			return copyHealthCandidateReadable
		}
		if hasAnyReason(signal.ReasonCodes,
			observability.ReasonCopyMissingProvider,
			observability.ReasonCopyMissingDataSet,
			observability.ReasonCopyMissingPiece,
			observability.ReasonCopyMissingRetrievalURL,
		) {
			return copyHealthCandidateUnknown
		}
		return copyHealthCandidateUnverified
	default:
		return copyHealthCandidateUnknown
	}
}

func copyHealthCandidatePriority(kind copyHealthCandidateKind) int {
	switch kind {
	case copyHealthCandidateReadable:
		return 0
	case copyHealthCandidateUnverified:
		return 1
	case copyHealthCandidatePending:
		return 2
	case copyHealthCandidateFailed:
		return 3
	default:
		return 4
	}
}

func copyHealthCandidateUnderReplicated(kind copyHealthCandidateKind) bool {
	return kind != copyHealthCandidateReadable && kind != copyHealthCandidateUnverified
}

func copyHealthStatusFromCounts(failedCopies int, unknownCopies int, pendingCopies int) observability.Status {
	switch {
	case failedCopies > 0:
		return observability.StatusUnavailable
	case unknownCopies > 0:
		return observability.StatusUnknown
	case pendingCopies > 0:
		return observability.StatusDegraded
	default:
		return observability.StatusAvailable
	}
}

func copyHealthSignalFromFact(fact copyHealthFact, observations map[int64]observability.DataSetObservation, observationFailed bool, interval time.Duration, now time.Time) observability.Signal {
	var observation *observability.DataSetObservation
	if !observationFailed && fact.LocalDataSetID != nil {
		if value, ok := observations[*fact.LocalDataSetID]; ok {
			observation = &value
		}
	}
	signal := observability.CopyHealthFromFacts(observability.CopyFacts{
		Status:         derefCopyStatus(fact.CopyStatus),
		ProviderID:     fact.ProviderID,
		LocalDataSetID: fact.LocalDataSetID,
		ChainDataSetID: fact.ChainDataSetID,
		PieceID:        fact.PieceID,
		RetrievalURL:   fact.RetrievalURL,
		LastError:      fact.LastError,
	}, observation, interval, now)
	if observationFailed && derefCopyStatus(fact.CopyStatus) == model.StorageUploadCopyStatusCommitted && hasAnyReason(signal.ReasonCodes, observability.ReasonCopyObservationMissing) {
		return copyHealthQueryFailureSignal(interval, now)
	}
	return signal
}

func provenanceCopyHealthFacts(bucketID int64, versionID string, upload model.StorageUpload, copies []model.StorageUploadCopy) []copyHealthFact {
	if len(copies) == 0 {
		uploadID := upload.ID
		uploadStatus := upload.Status
		return []copyHealthFact{{
			BucketID:        bucketID,
			VersionID:       versionID,
			UploadID:        &uploadID,
			UploadStatus:    &uploadStatus,
			RequestedCopies: upload.RequestedCopies,
		}}
	}
	facts := make([]copyHealthFact, 0, len(copies))
	for _, copyRow := range copies {
		uploadID := upload.ID
		uploadStatus := upload.Status
		copyIndex := copyRow.CopyIndex
		copyStatus := copyRow.Status
		facts = append(facts, copyHealthFact{
			BucketID:        bucketID,
			VersionID:       versionID,
			UploadID:        &uploadID,
			UploadStatus:    &uploadStatus,
			RequestedCopies: upload.RequestedCopies,
			CopyIndex:       &copyIndex,
			CopyStatus:      &copyStatus,
			ProviderID:      copyRow.ProviderID,
			LocalDataSetID:  copyRow.StorageDataSetID,
			ChainDataSetID:  copyRow.DataSetID,
			PieceID:         copyRow.PieceID,
			RetrievalURL:    copyRow.RetrievalURL,
			LastError:       copyRow.LastError,
		})
	}
	return facts
}

func copyHealthQueryFailureSignal(interval time.Duration, now time.Time) observability.Signal {
	errText := copyHealthQueryFailureLastError
	return observability.BuildSignal(observability.StatusUnknown, []observability.ReasonCode{}, &errText, nil, interval, now)
}

func copyHealthInfoFromSignal(signal observability.Signal) copyHealthInfo {
	var lastCheckedAt string
	if signal.Freshness.LastCheckedAt != nil {
		lastCheckedAt = signal.Freshness.LastCheckedAt.Format(time.RFC3339)
	}
	reasons := make([]observability.ReasonCode, 0, len(signal.ReasonCodes))
	reasons = append(reasons, signal.ReasonCodes...)
	return copyHealthInfo{
		Status:        string(signal.Status),
		ReasonCodes:   reasons,
		Stale:         signal.Freshness.Stale,
		LastCheckedAt: lastCheckedAt,
		LastError:     signal.LastError,
	}
}

func emptyCopyHealthSummary() copyHealthSummaryResponse {
	return copyHealthSummaryResponse{
		Status:      string(observability.StatusAvailable),
		ReasonCodes: []observability.ReasonCode{},
	}
}

func copyHealthQueryFailureSummary() copyHealthSummaryResponse {
	errText := copyHealthQueryFailureLastError
	return copyHealthSummaryResponse{
		Status:      string(observability.StatusUnknown),
		ReasonCodes: []observability.ReasonCode{},
		LastError:   &errText,
	}
}

func noUploadCopyHealthSummary() copyHealthSummaryResponse {
	return copyHealthSummaryResponse{
		Status:           string(observability.StatusUnknown),
		ReasonCodes:      []observability.ReasonCode{observability.ReasonCopyObservationMissing},
		TotalObjects:     1,
		UnhealthyObjects: 1,
	}
}

func oldestLastCheckedAtString(current string, candidate *time.Time) string {
	if candidate == nil {
		return current
	}
	if current == "" {
		return candidate.UTC().Format(time.RFC3339)
	}
	currentTime, err := time.Parse(time.RFC3339, current)
	if err != nil || candidate.Before(currentTime) {
		return candidate.UTC().Format(time.RFC3339)
	}
	return current
}

func derefCopyStatus(status *model.StorageUploadCopyStatus) model.StorageUploadCopyStatus {
	if status == nil {
		return ""
	}
	return *status
}

func worstObservabilityStatus(current observability.Status, candidate observability.Status) observability.Status {
	if observabilityStatusRank(candidate) > observabilityStatusRank(current) {
		return candidate
	}
	return current
}

func observabilityStatusRank(status observability.Status) int {
	switch status {
	case observability.StatusUnavailable:
		return 3
	case observability.StatusUnknown:
		return 2
	case observability.StatusDegraded:
		return 1
	default:
		return 0
	}
}

func hasAnyReason(reasons []observability.ReasonCode, want ...observability.ReasonCode) bool {
	for _, reason := range reasons {
		for _, target := range want {
			if reason == target {
				return true
			}
		}
	}
	return false
}
