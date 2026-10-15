package provider

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func (h *ObservabilityHandler) scheduleMissingProviderSpeedTests(ctx context.Context) error {
	if h.deps.Observability == nil || h.deps.UploadSpeedProbe == nil || h.deps.Scheduler == nil || h.deps.Repositories.ProviderUploadSpeed == nil {
		return nil
	}
	if err := h.deps.Observability.RefreshMissingProviderTiers(ctx); err != nil {
		return err
	}
	in, err := h.deps.Observability.ProviderCandidates(ctx)
	if err != nil || len(in.Candidates) == 0 {
		return err
	}
	ids := make([]string, len(in.Candidates))
	providerIDs := make([]types.OnChainID, len(in.Candidates))
	for i, c := range in.Candidates {
		ids[i], providerIDs[i] = c.ID.String(), c.ID
	}
	existing, err := h.deps.Repositories.ProviderUploadSpeed.ListByProviderIDs(ctx, ids)
	if err != nil {
		return err
	}
	loads, err := h.deps.Repositories.Contents.ProviderDataSetLoads(ctx, providerIDs)
	if err != nil {
		return err
	}
	memberships := []map[string]bool{make(map[string]bool), make(map[string]bool)}
	for rank, tier := range []string{"endorsed", "approved"} {
		snapshot, err := h.deps.Repositories.Observability.GetProviderTierSnapshot(ctx, tier)
		if err != nil {
			return err
		}
		if snapshot == nil {
			continue
		}
		var members []string
		if err := json.Unmarshal(snapshot.ProviderIDs, &members); err != nil {
			return err
		}
		for _, id := range members {
			memberships[rank][id] = true
		}
	}
	rank := func(c providerselect.Candidate) int {
		id := c.ID.String()
		return providerbenchmark.Priority(memberships[0][id], memberships[1][id], loads[id] > 0)
	}
	slices.SortFunc(in.Candidates, func(a, b providerselect.Candidate) int {
		if difference := rank(a) - rank(b); difference != 0 {
			return difference
		}
		return strings.Compare(a.ID.String(), b.ID.String())
	})
	now := time.Now().UTC()
	var testedBefore time.Time
	if h.deps.UploadSpeedTestRetestInterval > 0 {
		testedBefore = now.Add(-h.deps.UploadSpeedTestRetestInterval)
	}
	otherQueued := 0
	otherAllowed := false
	otherChecked := false
	for _, candidate := range in.Candidates {
		if candidate.IneligibleReason() != "" {
			continue
		}
		providerID := candidate.ID.String()
		var row *providerbenchmark.Result
		if value, exists := existing[providerID]; exists {
			row = &value
		}
		hash := providerbenchmark.URLHash(candidate.ServiceURL)
		if !providerbenchmark.Due(row, hash, h.deps.UploadSpeedTestRetestInterval, now) {
			continue
		}
		if rank(candidate) == 3 {
			if h.deps.UploadSpeedTestAutoScope != "all" || otherQueued >= 4 {
				continue
			}
			if !otherChecked {
				otherAllowed = true
				for _, status := range []model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning} {
					page, err := h.deps.Repositories.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeProviderUploadSpeedTest, Status: status, Limit: 1})
					if err != nil {
						return err
					}
					otherAllowed = otherAllowed && len(page.Tasks) == 0
				}
				otherChecked = true
			}
			if !otherAllowed {
				continue
			}
		}
		input := providerbenchmark.Input{ProviderID: providerID, ServiceURLHash: hash}
		err = h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
			taskRow, _, err := h.deps.Scheduler.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
				Type:  model.TaskTypeProviderUploadSpeedTest,
				Input: input,
			})
			if err != nil {
				return err
			}
			return repos.ProviderUploadSpeed.BeginIfDue(ctx, providerID, input.ServiceURLHash, taskRow.ID, testedBefore, providerbenchmark.NonMeasuredFailureCodes())
		})
		if err != nil && !errors.Is(err, repository.ErrConflict) {
			return err
		}
		if err == nil && rank(candidate) == 3 {
			otherQueued++
		}
	}
	return nil
}
