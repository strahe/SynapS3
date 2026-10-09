//go:build postgres

package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestPostgresTaskDuplicateAcrossTablesIsReported(t *testing.T) {
	assertTaskDuplicateAcrossTablesIsReported(t, migratedPostgresDB(t))
}

func TestPostgresTaskHistoryPaginationAcrossLongArchivedChain(t *testing.T) {
	assertTaskHistoryPaginationAcrossLongArchivedChain(t, migratedPostgresDB(t))
}

func TestPostgresTaskArchiveAndCreationKeepOneIdentity(t *testing.T) {
	db := migratedPostgresDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	source := enqueueAndClaimTask(t, repos, "archive-create-race", time.Minute)
	if err := repos.Tasks.Settle(ctx, source.ID, source.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover}); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	errs := make(chan error, 17)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			<-gate
			row, created, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: source.Type, IdempotencyKey: source.IdempotencyKey, InputVersion: source.InputVersion, Input: source.Input, InputHash: source.InputHash}))
			if err == nil && (created || row.ID != source.ID) {
				err = errors.New("creation lost archived identity")
			}
			errs <- err
		})
	}
	wg.Go(func() { <-gate; errs <- repos.Tasks.AcknowledgeFailed(ctx, source.ID) })
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	work, err := repos.Tasks.List(ctx, repository.TaskListFilter{})
	if err != nil || len(work.Tasks) != 0 {
		t.Fatalf("work = %#v, %v", work, err)
	}
	history, err := repos.Tasks.List(ctx, repository.TaskListFilter{Scope: repository.TaskScopeHistory})
	if err != nil || len(history.Tasks) != 1 || history.Tasks[0].ID != source.ID {
		t.Fatalf("history = %#v, %v", history, err)
	}
}

func TestPostgresTaskIdentityContentionHonorsDeadline(t *testing.T) {
	db := migratedPostgresDB(t)
	repos := repository.NewRepositories(db)
	held, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
			_, _, err := tx.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeUploadPlan, IdempotencyKey: "held-identity", InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: "test"}))
			if err != nil {
				close(held)
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: model.TaskTypeUploadPlan, IdempotencyKey: "held-identity", InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: "test"}))
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contention error = %v", err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.NewRaw("SELECT (SELECT COUNT(*) FROM tasks) + (SELECT COUNT(*) FROM task_history)").Scan(t.Context(), &count); err != nil || count != 1 {
		t.Fatalf("round count = %d, %v", count, err)
	}
}
