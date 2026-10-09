package repository_test

import (
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
)

func TestSupersededReplacementReschedulesArchivedCoordinator(t *testing.T) {
	repos, source, _ := localReplacementSource(t, testDB(t), "replacement-history")
	first, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := localCoordinatorTask(t, repos, first)
	if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: first.TargetDataSetID, DataSetID: onChainID(t, "2002")}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Replacements.MarkFailed(t.Context(), first.ID, nil, "confirmation stopped"); err != nil {
		t.Fatal(err)
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	// The original local ensure precedes the coordinator and is already fenced.
	if claimed.ID != coordinator.ID {
		if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusCancelled, ResumeMode: model.TaskResumeModeRecover}); err != nil {
			t.Fatal(err)
		}
		claimed, err = repos.Tasks.ClaimNext(t.Context(), time.Minute)
	}
	if err != nil || claimed == nil || claimed.ID != coordinator.ID {
		t.Fatalf("coordinator claim = %#v, %v", claimed, err)
	}
	reason := "attempts_exhausted"
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.AcknowledgeFailed(t.Context(), coordinator.ID); err != nil {
		t.Fatal(err)
	}
	preflight, err := repos.Replacements.Preflight(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "303"), ClientRequestID: "second", Preflight: preflight,
	})
	if err != nil {
		t.Fatal(err)
	}
	unscheduled, err := repos.Replacements.ListUnscheduledSuperseded(t.Context(), second.ID)
	if err != nil || len(unscheduled) != 1 || unscheduled[0].ID != first.ID || unscheduled[0].TaskID != nil || unscheduled[0].TaskGeneration != first.TaskGeneration+1 || unscheduled[0].TargetDataSetID != first.TargetDataSetID {
		t.Fatalf("archived coordinator left target unscheduled: %#v, %v", unscheduled, err)
	}
	old, err := repos.Tasks.GetByID(t.Context(), coordinator.ID)
	if err != nil || old.AcknowledgedAt == nil || old.Status != model.TaskStatusFailed {
		t.Fatalf("old history changed: %#v, %v", old, err)
	}
}
