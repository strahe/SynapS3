//go:build postgres

package repository_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestPostgresTaskLockRelocatesAfterConcurrentArchive(t *testing.T) {
	assertPostgresTaskArchiveLockRace(t, false)
}

func TestPostgresConcurrentTaskAcknowledgementArchivesOnce(t *testing.T) {
	assertPostgresTaskArchiveLockRace(t, true)
}

func assertPostgresTaskArchiveLockRace(t *testing.T, acknowledge bool) {
	t.Helper()
	db := migratedPostgresDB(t)
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
			if acknowledge {
				if err := tx.Tasks.AcknowledgeFailed(waiterCtx, source.ID); err != nil {
					return err
				}
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
		t.Fatalf("waiting writer did not relocate to acknowledged history: %#v", relocated)
	}
	var counts struct {
		Work    int
		History int
	}
	if err := db.NewRaw(`SELECT
		(SELECT count(*) FROM tasks WHERE id = ?) AS work,
		(SELECT count(*) FROM task_history WHERE task_id = ?) AS history`, source.ID, source.ID).Scan(ctx, &counts); err != nil || counts.Work != 0 || counts.History != 1 {
		t.Fatalf("physical round counts = %#v, %v", counts, err)
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

func TestPostgresTaskScopesLoadJSONForFinalPageOnly(t *testing.T) {
	db := migratedPostgresDB(t)
	repos := repository.NewRepositories(db)
	for i := range 3 {
		row := enqueueAndClaimTask(t, repos, fmt.Sprintf("history-projection-%d", i), time.Minute)
		if err := repos.Tasks.Settle(t.Context(), row.ID, row.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		if _, _, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
			Type: "repository_test", IdempotencyKey: fmt.Sprintf("work-projection-%d", i),
			InputVersion: 1, InputHash: "projection", Input: []byte(`{"data":"only final page"}`),
		})); err != nil {
			t.Fatal(err)
		}
	}
	capture := new(postgresQueryCapture)
	db.AddQueryHook(capture)
	for _, scope := range []repository.TaskScope{repository.TaskScopeWork, repository.TaskScopeHistory} {
		t.Run(string(scope), func(t *testing.T) {
			capture.reset()
			page, err := repos.Tasks.List(t.Context(), repository.TaskListFilter{Scope: scope, Limit: 2})
			if err != nil || len(page.Tasks) != 2 || page.NextBeforeID == 0 {
				t.Fatalf("scope page = %#v, %v", page, err)
			}
			if len(capture.queries) != 2 || strings.Contains(capture.queries[0], "_json") {
				t.Fatalf("candidate query loaded JSON: %#v", capture.queries)
			}
			loadedIDs := fmt.Sprintf("task.idIN(%d,%d)", page.Tasks[0].ID, page.Tasks[1].ID)
			query := strings.ReplaceAll(capture.queries[1], " ", "")
			if !strings.Contains(query, loadedIDs) || !strings.Contains(query, "input_json") {
				t.Fatalf("JSON query did not target the final page only: %s", capture.queries[1])
			}
			for _, row := range page.Tasks {
				if len(row.Input) == 0 || len(row.Policy) == 0 || len(row.Events) == 0 {
					t.Fatalf("final page lost task evidence: %#v", row)
				}
			}
		})
	}
}
