package admin

import (
	"testing"

	"github.com/strahe/synaps3/internal/model"
	idtypes "github.com/strahe/synaps3/internal/types"
)

func candidateProviders(ids ...string) []idtypes.OnChainID {
	out := make([]idtypes.OnChainID, 0, len(ids))
	for _, id := range ids {
		out = append(out, onChainIDValue(id))
	}
	return out
}

func candidateBinding(providerID string, status model.StorageDataSetStatus) model.StorageDataSet {
	return model.StorageDataSet{ProviderID: onChainIDValue(providerID), Status: status}
}

func candidateByProvider(t *testing.T, candidates []replacementProviderCandidate, providerID string) replacementProviderCandidate {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.ProviderID.String() == providerID {
			return candidate
		}
	}
	t.Fatalf("provider %s missing from candidates %v", providerID, candidates)
	return replacementProviderCandidate{}
}

// The rules here are the same ones Authorize enforces. If they drift, the
// dashboard offers a provider the confirmation then rejects.
func TestReplacementProviderCandidates_MirrorsWhatAuthorizeAccepts(t *testing.T) {
	source := candidateBinding("101", model.StorageDataSetStatusReady)
	candidates := replacementProviderCandidates(
		candidateProviders("101", "202", "303", "404"),
		[]model.StorageDataSet{
			source,
			candidateBinding("202", model.StorageDataSetStatusDraining),
			candidateBinding("303", model.StorageDataSetStatusRetired),
		},
		&source,
	)

	if got := candidateByProvider(t, candidates, "101"); got.Eligible || got.IneligibleReason != providerIneligibleCurrentSource {
		t.Fatalf("source provider = %#v, want ineligible as the current source", got)
	}
	if got := candidateByProvider(t, candidates, "202"); got.Eligible || got.IneligibleReason != providerIneligibleServesBucket {
		t.Fatalf("serving provider = %#v, want ineligible while it still holds a generation", got)
	}
	// A provider whose earlier service was retired is free to take the replica
	// again; only automatic selection avoids it.
	if got := candidateByProvider(t, candidates, "303"); !got.Eligible || !got.PreviouslyUsed {
		t.Fatalf("retired provider = %#v, want it choosable and marked as used before", got)
	}
	if got := candidateByProvider(t, candidates, "404"); !got.Eligible || got.PreviouslyUsed {
		t.Fatalf("unused provider = %#v, want it plainly choosable", got)
	}
}

// Ineligible providers stay in the list. An operator hunting for a provider
// they expected needs the reason, not a silent absence.
func TestReplacementProviderCandidates_KeepsIneligibleProvidersVisible(t *testing.T) {
	source := candidateBinding("101", model.StorageDataSetStatusReady)
	candidates := replacementProviderCandidates(
		candidateProviders("101", "202"),
		[]model.StorageDataSet{source},
		&source,
	)
	if len(candidates) != 2 {
		t.Fatalf("candidates = %d, want every approved provider listed", len(candidates))
	}
}
