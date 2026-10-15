package admin

import (
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerselect"
	idtypes "github.com/strahe/synaps3/internal/types"
)

// Reasons a provider cannot take over a replica. They mirror what Authorize
// rejects, so the dashboard never offers a choice the API refuses.
const (
	providerIneligibleCurrentSource = "current_source"
	providerIneligibleServesBucket  = "already_serves_bucket"
)

// replacementProviderCandidate is one provider offered for a replacement.
// Ineligible providers are still listed: an operator looking for a provider
// they expected to see needs to know why it cannot be chosen, rather than
// finding it silently missing.
type replacementProviderCandidate struct {
	ProviderID       idtypes.OnChainID
	Observation      *observability.ProviderObservation
	Profile          *observability.ProviderProfile
	ApprovedFresh    bool
	Eligible         bool
	IneligibleReason string
	Selection        providerselect.Candidate
	// PreviouslyUsed excludes a provider from automatic replacement; manual
	// selection permits reuse when all of its data sets in the bucket are retired.
	PreviouslyUsed bool
}

// replacementProviderCandidates lists manual choices and bucket usage for
// automatic selection. Unretired generations reserve their provider for the bucket.
func replacementProviderCandidates(
	providers []idtypes.OnChainID,
	bindings []model.StorageDataSet,
	source *model.StorageDataSet,
) []replacementProviderCandidate {
	serving := make(map[string]bool, len(bindings))
	used := make(map[string]bool, len(bindings))
	for i := range bindings {
		key := bindings[i].ProviderID.String()
		used[key] = true
		if bindings[i].Status != model.StorageDataSetStatusRetired {
			serving[key] = true
		}
	}
	sourceKey := ""
	if source != nil {
		sourceKey = source.ProviderID.String()
	}

	candidates := make([]replacementProviderCandidate, 0, len(providers))
	for _, providerID := range providers {
		key := providerID.String()
		candidate := replacementProviderCandidate{
			ProviderID:     providerID,
			Eligible:       true,
			PreviouslyUsed: used[key],
		}
		switch {
		case sourceKey != "" && key == sourceKey:
			candidate.Eligible = false
			candidate.IneligibleReason = providerIneligibleCurrentSource
		case serving[key]:
			candidate.Eligible = false
			candidate.IneligibleReason = providerIneligibleServesBucket
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}
