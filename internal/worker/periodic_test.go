package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func periodicHarness(t *testing.T) taskHarness {
	t.Helper()
	return periodicHarnessWithDB(t, testutil.NewTestFileDB(t))
}

func periodicHarnessWithDB(t *testing.T, db *bun.DB) taskHarness {
	t.Helper()
	definition := testDefinition(nil, true)
	definition.Type = model.TaskTypeObservabilityRefresh
	h := newTaskHarnessWithDB(t, db, scriptedHandler{definition: definition, execute: func(context.Context, Execution) Result {
		return Fail(errors.New("refresh failed"), "refresh_failed", nil)
	}, recover: func(context.Context, Execution) Result { return CompleteCycle(time.Hour, "Refreshed", nil) }}, nil)
	if err := h.service.bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestPeriodicDispatchAndManualRetryShareOneHead(t *testing.T) {
	assertPeriodicRetryCompetition(t, periodicHarness(t))
}

func assertPeriodicRetryCompetition(t *testing.T, h taskHarness) {
	t.Helper()
	source := claimTestTask(t, h)
	h.engine.executeClaim(t.Context(), source)
	if err := h.service.Acknowledge(t.Context(), source.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.repos.TaskSchedules.ScheduleNext(t.Context(), "test-cycle", source.ID, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := h.service.Retry(t.Context(), source.ID)
		if err != nil && !errors.Is(err, repository.ErrConflict) {
			t.Error(err)
		}
	})
	for range 8 {
		wg.Go(func() {
			if err := h.service.dispatchSchedules(t.Context()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	schedule, err := h.repos.TaskSchedules.GetForUpdate(t.Context(), "test-cycle")
	if err != nil {
		t.Fatal(err)
	}
	page, err := h.repos.Tasks.List(t.Context(), repository.TaskListFilter{Type: source.Type, Status: model.TaskStatusPending, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tasks) != 1 || schedule.LatestTaskID == nil || *schedule.LatestTaskID != page.Tasks[0].ID {
		t.Fatalf("schedule %#v pending %#v", schedule, page)
	}
	child, _ := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
	if child != nil && schedule.Generation != 1 {
		t.Fatal("manual retry incremented normal cycle generation")
	}
	if child == nil && schedule.Generation != 2 {
		t.Fatal("normal cycle did not increment generation")
	}
}

func TestNewCycleRetainsFailureUntilItEndsAndDoesNotCatchUp(t *testing.T) {
	h := periodicHarness(t)
	source := claimTestTask(t, h)
	h.engine.executeClaim(t.Context(), source)
	if err := h.service.Acknowledge(t.Context(), source.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.repos.TaskSchedules.ScheduleNext(t.Context(), "test-cycle", source.ID, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := h.service.dispatchSchedules(t.Context()); err != nil {
		t.Fatal(err)
	}
	old, _ := h.repos.Tasks.GetByID(t.Context(), source.ID)
	if old.SupersededAt != nil {
		t.Fatal("starting a cycle hid the old failure")
	}
	if err := h.service.dispatchSchedules(t.Context()); err != nil {
		t.Fatal(err)
	}
	page, _ := h.repos.Tasks.List(t.Context(), repository.TaskListFilter{Type: source.Type, Status: model.TaskStatusPending, Limit: 100})
	if len(page.Tasks) != 1 {
		t.Fatal("scheduled catch-up cycles overlapped")
	}
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	old, _ = h.repos.Tasks.GetByID(t.Context(), source.ID)
	if old.Status != model.TaskStatusFailed || old.SupersededAt == nil {
		t.Fatal("finished successor did not move the old failure into history")
	}
	schedule, _ := h.repos.TaskSchedules.GetForUpdate(t.Context(), "test-cycle")
	if !schedule.NextRunAt.After(time.Now().Add(59 * time.Minute)) {
		t.Fatal("schedule caught up instead of using completion plus interval")
	}
}
