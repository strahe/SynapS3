package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	sdktypes "github.com/strahe/synapse-go/types"
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

type abandonedTargetUploadRepo struct {
	repository.StorageUploadRepository
	target         *model.StorageDataSet
	activeAttempts int
	activeErr      error
}

func (r *abandonedTargetUploadRepo) GetDataSetBindingByID(context.Context, int64) (*model.StorageDataSet, error) {
	return r.target, nil
}

func (r *abandonedTargetUploadRepo) CountActiveCommitAttemptsForDataSet(context.Context, int64) (int, error) {
	return r.activeAttempts, r.activeErr
}

type abandonedTargetReplacementRepo struct {
	repository.StorageReplacementRepository
}

func (*abandonedTargetReplacementRepo) CountAbandonedTargetSoleCopies(context.Context, int64) (int, error) {
	return 0, nil
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

func TestAbandonedTargetRetirementDistinguishesConfirmationCountErrors(t *testing.T) {
	countErr := errors.New("count active attempts")
	tests := []struct {
		name           string
		activeAttempts int
		activeErr      error
		wantMessage    string
	}{
		{
			name:        "database error",
			activeErr:   countErr,
			wantMessage: "Waiting to end the unused storage service",
		},
		{
			name:           "active confirmations",
			activeAttempts: 1,
			wantMessage:    "Waiting for storage confirmations",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataSetID := onChainID(t, "2002")
			target := &model.StorageDataSet{
				ID:        77,
				DataSetID: &dataSetID,
				Status:    model.StorageDataSetStatusReady,
			}
			uploads := &abandonedTargetUploadRepo{
				target: target, activeAttempts: tt.activeAttempts, activeErr: tt.activeErr,
			}
			tasks := new(retirementWaitTaskRepo)
			terminationCalls := 0
			terminator := &testutil.MockServiceTerminator{
				TerminateServiceFunc: func(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
					terminationCalls++
					return nil, errors.New("unexpected termination")
				},
			}
			worker := &StorageCleanupWorker{
				repos: &repository.Repositories{
					Uploads: uploads, Replacements: new(abandonedTargetReplacementRepo), Tasks: tasks,
				},
				terminator: terminator,
				epochs:     new(testutil.MockChainEpochReader),
			}

			worker.retireAbandonedTarget(
				t.Context(),
				&model.Task{},
				&storagereplacement.Replacement{TargetDataSetID: target.ID},
				slog.New(slog.NewTextHandler(io.Discard, nil)),
			)

			if terminationCalls != 0 {
				t.Fatalf("termination calls = %d, want 0", terminationCalls)
			}
			if tasks.reason != model.TaskWaitReasonDependency || tasks.message != tt.wantMessage {
				t.Fatalf("task wait = (%s, %q), want dependency with %q", tasks.reason, tasks.message, tt.wantMessage)
			}
		})
	}
}
