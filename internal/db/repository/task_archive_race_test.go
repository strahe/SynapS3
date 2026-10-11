package repository_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestPostgresConcurrentTaskAcknowledgementArchivesOnce(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	source := enqueueAndClaimTask(t, repos, "archive-lock-race", time.Minute)
	if err := repos.Tasks.Settle(ctx, source.ID, source.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: new("test_failure"),
	}); err != nil {
		t.Fatal(err)
	}

	locked, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	first := make(chan error, 1)
	go func() {
		first <- repos.WithTx(ctx, func(tx *repository.Repositories) error {
			row, err := tx.Tasks.GetForUpdate(ctx, source.ID)
			if err != nil {
				return err
			}
			if row == nil || row.Status != model.TaskStatusFailed {
				return fmt.Errorf("missing failed hot source: %#v", row)
			}
			close(locked)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			return tx.Tasks.AcknowledgeFailed(ctx, source.ID)
		})
	}()
	select {
	case <-locked:
	case err := <-first:
		t.Fatalf("locking archive source: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	const waiterComment = "task_archive_waiting_writer"
	waiterCtx := bun.WithComment(ctx, waiterComment)
	second := make(chan error, 1)
	var relocated *model.Task
	go func() {
		second <- repos.WithTx(waiterCtx, func(tx *repository.Repositories) error {
			if err := tx.Tasks.AcknowledgeFailed(waiterCtx, source.ID); err != nil {
				return err
			}
			var err error
			relocated, err = tx.Tasks.GetForUpdate(waiterCtx, source.ID)
			return err
		})
	}()
	if err := waitForTaskArchiveLock(ctx, db, waiterComment); err != nil {
		t.Fatal(err)
	}
	close(release)
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("archive race transaction: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if relocated == nil || relocated.ID != source.ID || relocated.Status != model.TaskStatusFailed || relocated.AcknowledgedAt == nil {
		t.Fatalf("waiting writer did not see the acknowledgement: %#v", relocated)
	}
	events, err := repos.Tasks.ListEvents(ctx, source.ID, 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	ackEvents := 0
	for _, event := range events {
		if event.Type == "acknowledged" {
			ackEvents++
		}
	}
	if ackEvents != 1 {
		t.Fatalf("acknowledgement events = %d, want 1: %#v", ackEvents, events)
	}
}

func waitForTaskArchiveLock(ctx context.Context, db *bun.DB, comment string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := db.NewRaw(`SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE ?
		)`, "%"+comment+"%").Scan(ctx, &blocked); err != nil {
			return fmt.Errorf("checking waiting task writer: %w", err)
		}
		if blocked {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("task writer never waited for the hot row lock: %w", ctx.Err())
		}
	}
}
