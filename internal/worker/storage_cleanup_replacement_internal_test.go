package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
)

type retirementWaitReplacementRepo struct {
	repository.StorageReplacementRepository
	reason storagereplacement.WaitReason
}

func (r *retirementWaitReplacementRepo) MarkWaiting(_ context.Context, _ int64, reason storagereplacement.WaitReason) error {
	r.reason = reason
	return nil
}

type retirementWaitTaskRepo struct {
	repository.TaskRepository
	reason  model.TaskWaitReason
	message string
}

func (r *retirementWaitTaskRepo) WaitRunning(
	_ context.Context,
	_ *model.Task,
	reason model.TaskWaitReason,
	message string,
	_ time.Duration,
) error {
	r.reason = reason
	r.message = message
	return nil
}

func TestReplacementRetirementPrioritizesActiveConfirmations(t *testing.T) {
	replacements := new(retirementWaitReplacementRepo)
	tasks := new(retirementWaitTaskRepo)
	worker := &StorageCleanupWorker{repos: &repository.Repositories{
		Replacements: replacements,
		Tasks:        tasks,
	}}
	worker.waitForReplacementRetirement(
		t.Context(),
		&model.Task{},
		42,
		repository.RetirementGate{SlotOwned: true, ActiveAttempts: 1, WaitingItems: 1},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	if replacements.reason != storagereplacement.WaitReasonProvider {
		t.Fatalf("replacement wait reason = %s, want %s", replacements.reason, storagereplacement.WaitReasonProvider)
	}
	if tasks.reason != model.TaskWaitReasonDependency || tasks.message != "Waiting for storage confirmations" {
		t.Fatalf("task wait = (%s, %q), want dependency confirmation message", tasks.reason, tasks.message)
	}
}
