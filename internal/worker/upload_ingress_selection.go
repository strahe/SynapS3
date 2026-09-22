package worker

import (
	"context"

	"github.com/strahe/synaps3/internal/providerbenchmark"
)

func (h *TaskHandlers) fastestIngressIndex(ctx context.Context, plan []uploadBindingPlan) int {
	if len(plan) < 2 || h.deps.Observability == nil || h.deps.Repositories.ProviderUploadSpeed == nil {
		return 0
	}
	ids := make([]string, len(plan))
	for i := range plan {
		ids[i] = plan[i].provider.String()
	}
	results, err := h.deps.Repositories.ProviderUploadSpeed.ListByProviderIDs(ctx, ids)
	if err != nil {
		return 0
	}
	bestIndex, bestSpeed := 0, int64(0)
	for i := range plan {
		row, ok := results[ids[i]]
		if !ok || row.State != providerbenchmark.StateSucceeded || row.BytesPerSecond == nil || *row.BytesPerSecond <= 0 {
			return 0
		}
		url, eligible, err := providerbenchmark.CurrentServiceURL(ctx, h.deps.Observability, plan[i].provider)
		if err != nil || !eligible || providerbenchmark.URLHash(url) != row.ServiceURLHash {
			return 0
		}
		if *row.BytesPerSecond > bestSpeed {
			bestIndex, bestSpeed = i, *row.BytesPerSecond
		}
	}
	return bestIndex
}
