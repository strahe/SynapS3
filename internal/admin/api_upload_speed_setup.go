package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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

type uploadSpeedCounts struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Testing   int `json:"testing"`
	Untested  int `json:"untested"`
}

type uploadSpeedSummary struct {
	AutoScope           string            `json:"auto_scope"`
	Trusted             uploadSpeedCounts `json:"trusted"`
	Other               uploadSpeedCounts `json:"other"`
	SelectableProviders int               `json:"selectable_providers"`
	RequiredCopies      int               `json:"required_copies"`
	Ready               bool              `json:"ready"`
	CanProvisionDefault bool              `json:"can_provision_default"`
	CanTestRemaining    bool              `json:"can_test_remaining"`
}

func summarizeUploadSpeed(in providerselect.Inventory, rows map[string]providerbenchmark.Result, scope string, copies int) uploadSpeedSummary {
	summary := uploadSpeedSummary{AutoScope: scope, RequiredCopies: copies}
	for _, candidate := range in.Candidates {
		counts := &summary.Other
		if candidate.TierListed {
			counts = &summary.Trusted
		}
		counts.Total++
		row := rows[candidate.ID.String()]
		bps, settled := row.Outcome(candidate.ProfileURL)
		switch {
		case row.State == providerbenchmark.StateTesting:
			counts.Testing++
		case bps > 0:
			counts.Succeeded++
		case settled:
			counts.Failed++
		default:
			counts.Untested++
		}
		if !settled && row.State != providerbenchmark.StateTesting && candidate.IneligibleReason() == "" {
			summary.CanTestRemaining = true
		}
	}
	diagnostics := providerselect.Diagnose(in, copies, nil, false)
	summary.SelectableProviders = diagnostics.EligibleCount
	_, err := providerselect.Select(in, providerselect.StrategySpeed, copies, nil, false)
	summary.Ready = !errors.Is(err, providerselect.ErrUploadSpeedTestsPending)
	summary.CanProvisionDefault = err == nil && diagnostics.EligibleCount >= copies
	return summary
}

func (s *Server) uploadSpeedSetupInventory(ctx context.Context) (providerselect.Inventory, map[string]providerbenchmark.Result, error) {
	in, err := s.observability.ProviderCandidates(ctx)
	if err != nil {
		return in, nil, err
	}
	if err := s.repos.EnrichProviderCandidates(ctx, &in); err != nil {
		return in, nil, err
	}
	ids := make([]string, len(in.Candidates))
	for i, candidate := range in.Candidates {
		ids[i] = candidate.ID.String()
	}
	rows, err := s.repos.ProviderUploadSpeed.ListByProviderIDs(ctx, ids)
	return in, rows, err
}

func (s *Server) handleAPIUploadSpeedSummary(w http.ResponseWriter, r *http.Request) {
	if s.observability == nil || s.repos == nil || s.repos.ProviderUploadSpeed == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "upload speed testing unavailable"})
		return
	}
	ctx := r.Context()
	in, rows, err := s.uploadSpeedSetupInventory(ctx)
	if err == nil {
		in.Admission = providerselect.Admission{Tier: s.anchorProviderTier}
		if s.anchorProviderTier != providerselect.TierNone {
			snapshot, readErr := s.repos.Observability.GetProviderTierSnapshot(ctx, string(s.anchorProviderTier))
			err = readErr
			if err == nil && snapshot != nil {
				var ids []string
				err = json.Unmarshal(snapshot.ProviderIDs, &ids)
				for _, raw := range ids {
					if err != nil {
						break
					}
					var id types.OnChainID
					id, err = types.ParseOnChainID("provider_id", raw)
					in.Admission.TrustedIDs = append(in.Admission.TrustedIDs, id)
				}
				in.Admission.CheckedAt = &snapshot.CheckedAt
			}
		}
	}
	if err != nil {
		s.logger.Error("api: failed to summarize provider upload speeds", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	scope := s.uploadSpeedTestAutoScope
	if scope == "" {
		scope = "trusted"
	}
	summary := summarizeUploadSpeed(in, rows, scope, s.filecoinDefaultCopies)
	if s.anchorProviderTier != providerselect.TierNone && (in.Admission.CheckedAt == nil || in.Admission.CheckedAt.IsZero() || time.Since(*in.Admission.CheckedAt) > 2*s.observability.RefreshInterval()) {
		summary.CanProvisionDefault = false
	}
	summary.CanTestRemaining = summary.CanTestRemaining && s.taskService != nil
	writeJSON(w, http.StatusOK, summary)
}

func (s *Server) handleAPIStartAllUploadSpeedTests(w http.ResponseWriter, r *http.Request) {
	if s.observability == nil || s.taskService == nil || s.repos == nil || s.repos.ProviderUploadSpeed == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "upload speed testing unavailable"})
		return
	}
	ctx := r.Context()
	in, rows, err := s.uploadSpeedSetupInventory(ctx)
	ids := make([]types.OnChainID, len(in.Candidates))
	for i, candidate := range in.Candidates {
		ids[i] = candidate.ID
	}
	if err != nil {
		s.logger.Error("api: failed to load providers for speed tests", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	profiles, err := s.repos.Observability.ProviderProfiles(ctx, ids)
	if err != nil {
		s.logger.Error("api: failed to load provider profiles for speed tests", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	priority := func(candidate providerselect.Candidate) int {
		profile := profiles[candidate.ID.String()]
		return providerbenchmark.Priority(profile.Endorsed, profile.Approved, candidate.Load > 0)
	}
	slices.SortFunc(in.Candidates, func(a, b providerselect.Candidate) int {
		if difference := priority(a) - priority(b); difference != 0 {
			return difference
		}
		return strings.Compare(a.ID.String(), b.ID.String())
	})
	queued := 0
	for _, candidate := range in.Candidates {
		row := rows[candidate.ID.String()]
		_, settled := row.Outcome(candidate.ProfileURL)
		if settled || row.State == providerbenchmark.StateTesting || candidate.IneligibleReason() != "" {
			continue
		}
		input := providerbenchmark.Input{ProviderID: candidate.ID.String(), ServiceURLHash: providerbenchmark.URLHash(candidate.ProfileURL)}
		_, _, err := s.taskService.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeProviderUploadSpeedTest, Input: input},
			func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
				return repos.ProviderUploadSpeed.BeginIfDue(ctx, input.ProviderID, input.ServiceURLHash, task.ID, time.Time{}, nil)
			})
		if errors.Is(err, repository.ErrConflict) {
			continue
		}
		if err != nil {
			s.logger.Error("api: failed to queue provider speed tests", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		queued++
	}
	writeJSON(w, http.StatusAccepted, map[string]int{"queued": queued})
}
