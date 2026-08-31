package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
)

type storageCommitWaitTaskRepo struct {
	repository.TaskRepository
	delay time.Duration
}

func (r *storageCommitWaitTaskRepo) WaitRunning(
	_ context.Context,
	_ *model.Task,
	_ model.TaskWaitReason,
	_ string,
	delay time.Duration,
) error {
	r.delay = delay
	return nil
}

func TestCommitObservationDelayUsesMinuteFloor(t *testing.T) {
	for _, tc := range []struct {
		name string
		poll time.Duration
		want time.Duration
	}{
		{name: "default poll", poll: storageCommitPollDelay, want: time.Minute},
		{name: "long configured poll", poll: 2 * time.Minute, want: 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := commitObservationDelay(tc.poll); got != tc.want {
				t.Fatalf("commitObservationDelay(%s) = %s, want %s", tc.poll, got, tc.want)
			}
		})
	}
}

func TestUploaderCommitWaitDelays(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name         string
		pollInterval time.Duration
		result       storagecommit.AdvanceResult
		want         time.Duration
	}{
		{
			name:   "pending uses normal poll",
			result: storagecommit.AdvanceResult{State: storagecommit.AdvancePending},
			want:   storageCommitPollDelay,
		},
		{
			name:   "observing attention uses minute floor",
			result: storagecommit.AdvanceResult{State: storagecommit.AdvanceNeedsAttention, Continue: true},
			want:   time.Minute,
		},
		{
			name:         "observing attention respects longer poll",
			pollInterval: 2 * time.Minute,
			result:       storagecommit.AdvanceResult{State: storagecommit.AdvanceNeedsAttention, Continue: true},
			want:         2 * time.Minute,
		},
		{
			name:   "operator attention remains parked",
			result: storagecommit.AdvanceResult{State: storagecommit.AdvanceNeedsAttention},
			want:   storageCommitAttentionDelay,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := new(storageCommitWaitTaskRepo)
			uploader := &Uploader{
				repos:        &repository.Repositories{Tasks: tasks},
				pollInterval: tc.pollInterval,
			}
			if !uploader.waitForCommitAdvance(t.Context(), &model.Task{}, logger, tc.result) {
				t.Fatal("waitForCommitAdvance rejected commit wait state")
			}
			if tasks.delay != tc.want {
				t.Fatalf("wait delay = %s, want %s", tasks.delay, tc.want)
			}
		})
	}
}
