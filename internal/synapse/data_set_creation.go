package synapse

import "context"

func normalizeCreateDataSetError(ctx context.Context, err error) error {
	return NormalizeProviderOperationError(ctx, err)
}
