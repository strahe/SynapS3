package providerselect

import (
	"slices"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/types"
)

type ExclusionReason string

const (
	ExclusionBucketInUse       ExclusionReason = "bucket_in_use"
	ExclusionPreviouslyUsed    ExclusionReason = "previously_used"
	ExclusionContextOpenFailed ExclusionReason = "context_open_failed"
	ExclusionSelected          ExclusionReason = "selected_this_round"
)

func (r ExclusionReason) Valid() bool {
	//exhaustive:enforce
	switch r {
	case ExclusionBucketInUse, ExclusionPreviouslyUsed, ExclusionContextOpenFailed, ExclusionSelected:
		return true
	default:
		return false
	}
}

type SelectionFailureKind string

const (
	SelectionRequiredTierEmpty  SelectionFailureKind = "required_tier_empty"
	SelectionRequiredTierInUse  SelectionFailureKind = "required_tier_in_use"
	SelectionTemporary          SelectionFailureKind = "temporarily_unavailable"
	SelectionProviderContext    SelectionFailureKind = "provider_context_unavailable"
	SelectionUploadSpeedPending SelectionFailureKind = "upload_speed_tests_pending"
)

func (k SelectionFailureKind) Valid() bool {
	//exhaustive:enforce
	switch k {
	case SelectionRequiredTierEmpty, SelectionRequiredTierInUse, SelectionTemporary, SelectionProviderContext, SelectionUploadSpeedPending:
		return true
	default:
		return false
	}
}

type ProviderRejection struct {
	ProviderID           types.OnChainID `json:"provider_id"`
	Reason               string          `json:"reason"`
	ObservationCheckedAt *time.Time      `json:"observation_checked_at,omitempty"`
}

type SelectionDiagnostics struct {
	Tier            Tier                `json:"tier"`
	TierCheckedAt   *time.Time          `json:"tier_checked_at,omitempty"`
	HasTrusted      bool                `json:"has_trusted"`
	RequestedCount  int                 `json:"requested_count"`
	EligibleCount   int                 `json:"eligible_count"`
	RejectedTotal   int                 `json:"rejected_total"`
	RejectedOmitted int                 `json:"rejected_omitted"`
	Rejections      []ProviderRejection `json:"rejections"`
	refreshRequired bool
}

// NeedsRefresh is computed from all selection obstacles before diagnostics are capped.
func (d SelectionDiagnostics) NeedsRefresh() bool { return d.refreshRequired }

type SelectionError struct {
	Kind        SelectionFailureKind
	Cause       error
	Diagnostics SelectionDiagnostics
}

func (e *SelectionError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	//exhaustive:enforce
	switch e.Kind {
	case SelectionRequiredTierEmpty, SelectionRequiredTierInUse:
		return ErrNoTrustedProvider.Error()
	case SelectionTemporary, SelectionProviderContext:
		return ErrCandidatesTemporarilyUnavailable.Error()
	case SelectionUploadSpeedPending:
		return ErrUploadSpeedTestsPending.Error()
	default:
		return ErrInventoryUnavailable.Error()
	}
}

func (e *SelectionError) Unwrap() []error {
	causes := make([]error, 0, 2)
	if e.Cause != nil {
		causes = append(causes, e.Cause)
	}
	//exhaustive:enforce
	switch e.Kind {
	case SelectionRequiredTierEmpty, SelectionRequiredTierInUse:
		causes = append(causes, ErrNoTrustedProvider)
	case SelectionTemporary:
		causes = append(causes, ErrCandidatesTemporarilyUnavailable)
	case SelectionProviderContext:
		// Provider errors retain their original retry and terminal classification.
	case SelectionUploadSpeedPending:
		causes = append(causes, ErrUploadSpeedTestsPending)
	default:
		causes = append(causes, ErrInventoryUnavailable)
	}
	return causes
}

func selectionCandidates(in Inventory, excluded map[string]ExclusionReason) ([]Candidate, []ProviderRejection) {
	eligible := make([]Candidate, 0, len(in.Candidates))
	rejections := make([]ProviderRejection, 0, len(in.Candidates))
	seen := make(map[string]bool, len(in.Candidates))
	for _, c := range in.Candidates {
		key := c.ID.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		reason := exclusionReason(excluded[key])
		if excluded[key] == ExclusionSelected {
			continue
		}
		if reason == "" {
			reason = candidateRejectionReason(c)
		}
		if reason == "" && in.UploadSpeedRequired && (!c.UploadSpeedSettled || c.BytesPerSecond <= 0) {
			reason = "upload_speed_untested"
			if c.UploadSpeedSettled {
				reason = "upload_speed_failed"
			}
		}
		if reason == "" {
			eligible = append(eligible, c)
		} else {
			rejections = append(rejections, ProviderRejection{ProviderID: c.ID, Reason: reason, ObservationCheckedAt: c.ObservationCheckedAt})
		}
	}
	for _, id := range in.Admission.TrustedIDs {
		key := id.String()
		if seen[key] || excluded[key] == ExclusionSelected {
			continue
		}
		seen[key] = true
		reason := exclusionReason(excluded[key])
		if reason == "" {
			reason = "observation_missing"
		}
		rejections = append(rejections, ProviderRejection{ProviderID: id, Reason: reason})
	}
	return eligible, rejections
}

func exclusionReason(reason ExclusionReason) string {
	//exhaustive:enforce
	switch reason {
	case "", ExclusionSelected:
		return ""
	case ExclusionBucketInUse, ExclusionPreviouslyUsed, ExclusionContextOpenFailed:
		return string(reason)
	default:
		return "provider_unavailable"
	}
}

func candidateRejectionReason(c Candidate) string {
	reason := c.IneligibleReason()
	if reason != "provider_unavailable" {
		return reason
	}
	switch {
	case c.ID.IsZero():
		return "provider_unavailable"
	case !c.Active:
		return "provider_inactive"
	case !c.HasPDP:
		return "provider_missing_pdp"
	case !c.Healthy:
		return "provider_unhealthy"
	case c.ServiceURL == "":
		return "service_url_missing"
	default:
		return reason
	}
}

func allTrustedInUse(admission Admission, excluded map[string]ExclusionReason) bool {
	return len(admission.TrustedIDs) > 0 && !slices.ContainsFunc(admission.TrustedIDs, func(id types.OnChainID) bool {
		reason := excluded[id.String()]
		return reason != ExclusionBucketInUse && reason != ExclusionPreviouslyUsed
	})
}

func selectionDiagnostics(in Inventory, count int, hasTrusted bool, eligibleCount int, rejections []ProviderRejection) SelectionDiagnostics {
	const maxRejections = 100
	ordered := slices.Clone(rejections)
	slices.SortFunc(ordered, func(a, b ProviderRejection) int {
		aID, bID := a.ProviderID.String(), b.ProviderID.String()
		if n := compare(len(aID), len(bID)); n != 0 {
			return n
		}
		return strings.Compare(aID, bID)
	})
	omitted := max(len(ordered)-maxRejections, 0)
	if len(ordered) > maxRejections {
		ordered = ordered[:maxRejections]
	}
	if ordered == nil {
		ordered = []ProviderRejection{}
	}
	return SelectionDiagnostics{
		Tier: in.Admission.Tier, TierCheckedAt: in.Admission.CheckedAt, HasTrusted: hasTrusted,
		RequestedCount: count, EligibleCount: eligibleCount, RejectedTotal: len(rejections), RejectedOmitted: omitted, Rejections: ordered,
	}
}

// Diagnose records the complete decision inputs without provider URLs or errors.
func Diagnose(in Inventory, count int, excluded map[string]ExclusionReason, hasTrusted bool) SelectionDiagnostics {
	_, diagnostics, _ := inspectSelection(in, count, excluded, hasTrusted)
	return diagnostics
}

func inspectSelection(in Inventory, count int, excluded map[string]ExclusionReason, hasTrusted bool) ([]Candidate, SelectionDiagnostics, SelectionFailureKind) {
	eligible, rejections := selectionCandidates(in, excluded)
	diagnostics := selectionDiagnostics(in, count, hasTrusted, len(eligible), rejections)
	if count <= 0 {
		return eligible, diagnostics, ""
	}
	if in.UploadSpeedRequired && slices.ContainsFunc(in.Candidates, func(c Candidate) bool {
		return c.TierListed && !c.UploadSpeedSettled && excluded[c.ID.String()] == "" && c.IneligibleReason() == ""
	}) {
		return eligible, diagnostics, SelectionUploadSpeedPending
	}
	if !in.Admission.Satisfied(hasTrusted) {
		if len(in.Admission.TrustedIDs) == 0 {
			return eligible, diagnostics, SelectionRequiredTierEmpty
		}
		if allTrustedInUse(in.Admission, excluded) {
			return eligible, diagnostics, SelectionRequiredTierInUse
		}
		if !slices.ContainsFunc(eligible, func(c Candidate) bool { return in.Admission.Trusted(c.ID) }) {
			diagnostics.refreshRequired = refreshNeeded(rejections, in.Admission.Trusted)
			return eligible, diagnostics, SelectionTemporary
		}
	}
	if len(eligible) < count && hasTemporaryRejection(rejections) {
		diagnostics.refreshRequired = refreshNeeded(rejections, func(types.OnChainID) bool { return true })
		return eligible, diagnostics, SelectionTemporary
	}
	return eligible, diagnostics, ""
}

func hasTemporaryRejection(rejections []ProviderRejection) bool {
	return slices.ContainsFunc(rejections, func(r ProviderRejection) bool {
		return r.Reason != string(ExclusionBucketInUse) && r.Reason != string(ExclusionPreviouslyUsed)
	})
}

func refreshNeeded(rejections []ProviderRejection, relevant func(types.OnChainID) bool) bool {
	return slices.ContainsFunc(rejections, func(r ProviderRejection) bool {
		return relevant(r.ProviderID) && (r.Reason == "observation_missing" || r.Reason == "observation_stale" || r.Reason == "profile_missing" || r.Reason == "profile_url_changed")
	})
}
