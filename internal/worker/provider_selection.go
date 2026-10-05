package worker

import (
	"context"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/types"
)

func validateBindingSelection(ctx context.Context, repos *repository.Repositories, bucketID int64, plan []uploadBindingPlan) error {
	var admission *providerselect.Admission
	var targets []types.OnChainID
	for _, entry := range plan {
		if entry.admission != nil {
			admission = entry.admission
			targets = append(targets, entry.provider)
		}
	}
	if admission == nil {
		return nil
	}
	return repos.ValidateProviderSelection(ctx, bucketID, 0, *admission, targets)
}
