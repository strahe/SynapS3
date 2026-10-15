package binding

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/providerselect"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func SelectionNeedsWait(err error) bool {
	var selectionErr *providerselect.SelectionError
	return errors.As(err, &selectionErr) && selectionErr.Cause == nil &&
		(selectionErr.Kind == providerselect.SelectionTemporary || selectionErr.Kind == providerselect.SelectionRequiredTierInUse || selectionErr.Kind == providerselect.SelectionUploadSpeedPending)
}

func SelectionWait(err error) (reason, message string) {
	if errors.Is(err, providerselect.ErrUploadSpeedTestsPending) {
		return "provider_speed_tests", "Waiting for provider speed tests to finish"
	}
	return "providers", "Waiting for storage providers"
}

func SelectionFailed(err error) bool {
	var selectionErr *providerselect.SelectionError
	return errors.As(err, &selectionErr) && selectionErr.Cause != nil
}

// SelectionSettlement saves the decision with the task's fenced transition.
func SelectionSettlement(taskID int64, err error) taskengine.Settlement {
	return selectionSettlement(taskID, err, false)
}

// SelectionWaitSettlement records changed waiting decisions, not every poll.
func SelectionWaitSettlement(taskID int64, err error) taskengine.Settlement {
	return selectionSettlement(taskID, err, true)
}

type selectionEventDetails struct {
	ProviderSelection providerselect.SelectionDiagnostics `json:"provider_selection"`
}

func selectionSettlement(taskID int64, err error, deduplicate bool) taskengine.Settlement {
	var selectionErr *providerselect.SelectionError
	if !errors.As(err, &selectionErr) {
		return nil
	}
	report := selectionErr.Diagnostics
	return func(ctx context.Context, repos *repository.Repositories) error {
		if deduplicate {
			events, err := repos.Tasks.ListEvents(ctx, taskID, 0, 1)
			if err != nil {
				return err
			}
			if len(events) > 0 && events[0].Type == "provider_selection" {
				var previous selectionEventDetails
				if json.Unmarshal(events[0].Details, &previous) == nil && sameSelectionDecision(previous.ProviderSelection, report) {
					return nil
				}
			}
		}
		details, err := json.Marshal(selectionEventDetails{ProviderSelection: report})
		if err != nil {
			return err
		}
		return repos.Tasks.AppendEvent(ctx, taskID, "provider_selection", details)
	}
}

func sameSelectionDecision(a, b providerselect.SelectionDiagnostics) bool {
	if a.Tier != b.Tier || a.HasTrusted != b.HasTrusted || a.RequestedCount != b.RequestedCount ||
		a.EligibleCount != b.EligibleCount || a.RejectedTotal != b.RejectedTotal || a.RejectedOmitted != b.RejectedOmitted ||
		len(a.Rejections) != len(b.Rejections) {
		return false
	}
	for i, rejection := range a.Rejections {
		if !rejection.ProviderID.Equal(b.Rejections[i].ProviderID) || rejection.Reason != b.Rejections[i].Reason {
			return false
		}
	}
	return true
}
