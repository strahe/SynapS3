package binding

import (
	"context"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/types"
)

func ValidateSelection(ctx context.Context, repos *repository.Repositories, bucketID int64, plan []Plan) error {
	var admission *providerselect.Admission
	var targets []types.OnChainID
	for _, entry := range plan {
		if entry.Admission != nil {
			admission = entry.Admission
			targets = append(targets, entry.Provider)
		}
	}
	if admission == nil {
		return nil
	}
	return repos.ValidateProviderSelection(ctx, bucketID, 0, *admission, targets)
}
