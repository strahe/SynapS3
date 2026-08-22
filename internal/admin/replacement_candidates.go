package admin

import (
	"github.com/strahe/synaps3/internal/model"
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
	Eligible         bool
	IneligibleReason string
	// PreviouslyUsed marks a provider this bucket has used before and fully
	// retired. It can be chosen again, which automatic selection avoids and an
	// operator may still want.
	PreviouslyUsed bool
}

// replacementProviderCandidates applies the target rules once, so automatic
// selection and the dashboard's provider list can never disagree about what is
// choosable.
//
// The rules follow StorageReplacementRepository.Authorize: the generation being
// replaced cannot replace itself, and a provider still holding an unretired
// generation of this bucket would collide with it. A provider whose earlier
// generations are all retired is choosable again.
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

// firstAutomaticChoice picks the provider an automatic replacement should use.
// It goes further than eligibility and skips every provider the bucket has ever
// used, so an automatic choice never lands back where the operator moved away
// from. Choosing one of those again stays available as a manual decision.
func firstAutomaticChoice(candidates []replacementProviderCandidate) (idtypes.OnChainID, bool) {
	for _, candidate := range candidates {
		if candidate.Eligible && !candidate.PreviouslyUsed {
			return candidate.ProviderID, true
		}
	}
	return idtypes.OnChainID{}, false
}
