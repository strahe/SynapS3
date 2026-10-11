package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestPostgresTaskArchiveAndCreationKeepOneIdentity(t *testing.T) {
	db := testDB(t)
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
			row, created, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: source.Type, SubjectType: source.SubjectType, SubjectKey: source.SubjectKey, Generation: source.Generation, InputVersion: source.InputVersion, Input: source.Input, InputHash: source.InputHash}))
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
	db := testDB(t)
	repos := repository.NewRepositories(db)
	held, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
			_, _, err := tx.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeUploadPlan, SubjectType: "test", SubjectKey: "held-identity", Generation: new(int64(0)), InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: "test"}))
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
	_, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: model.TaskTypeUploadPlan, SubjectType: "test", SubjectKey: "held-identity", Generation: new(int64(0)), InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: "test"}))
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contention error = %v", err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.NewRaw("SELECT COUNT(*) FROM tasks").Scan(t.Context(), &count); err != nil || count != 1 {
		t.Fatalf("round count = %d, %v", count, err)
	}
}

type taskIdentitySupersessionRace struct {
	repos      *repository.Repositories
	input      *model.Task
	source     *model.Task
	superseded bool
	err        error
}

func (h *taskIdentitySupersessionRace) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if h.source != nil || h.err != nil || ctx.Value(postgresRaceOperation{}) != "enqueue" ||
		!strings.Contains(event.Query, `INSERT INTO "tasks"`) || !strings.Contains(event.Query, "'"+h.input.SubjectKey+"'") {
		return ctx
	}
	// The competing transaction commits after the identity lookup, so the
	// original insert must take the ON CONFLICT path.
	winnerCtx := postgresRaceContext(ctx, "winner")
	winner := *h.input
	h.source, _, h.err = h.repos.Tasks.Enqueue(winnerCtx, &winner)
	if h.err != nil {
		return ctx
	}
	claimed, err := h.repos.Tasks.ClaimNext(winnerCtx, time.Minute, repository.TaskClaimFilter{})
	if err != nil {
		h.err = err
	} else if claimed == nil || claimed.ID != h.source.ID {
		h.err = errors.New("competing round was not claimed")
	} else {
		h.err = h.repos.Tasks.Settle(winnerCtx, claimed.ID, claimed.ClaimGeneration,
			repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover})
	}
	return ctx
}

func (h *taskIdentitySupersessionRace) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if h.source == nil || h.superseded || h.err != nil || event.Err != nil || ctx.Value(postgresRaceOperation{}) != "enqueue" ||
		!strings.Contains(event.Query, `INSERT INTO "tasks"`) || !strings.Contains(event.Query, "'"+h.input.SubjectKey+"'") {
		return
	}
	// Release the conflicting identity before the original transaction rereads it.
	h.err = h.repos.Tasks.SupersedeTerminal(postgresRaceContext(ctx, "winner"), h.source.ID)
	h.superseded = h.err == nil
}

func TestPostgresTaskEnqueueRetriesAfterConcurrentSupersession(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	input := repositoryTestTask(&model.Task{
		Type: model.TaskTypeUploadPlan, SubjectType: "test", SubjectKey: "superseded-identity", Generation: new(int64(0)),
		InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: "test",
	})
	hook := &taskIdentitySupersessionRace{repos: repos, input: input}
	testutil.AddQueryHook(db, hook)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = postgresRaceContext(ctx, "enqueue")
	attempts := 0
	var current *model.Task
	err := repos.WithTx(ctx, func(tx *repository.Repositories) error {
		attempts++
		// The marker and its event must roll back with the contended attempt.
		marker := *input
		marker.SubjectKey, marker.Generation = "transaction-marker", nil
		if _, _, err := tx.Tasks.Enqueue(ctx, &marker); err != nil {
			return err
		}
		var created bool
		var err error
		current, created, err = tx.Tasks.Enqueue(ctx, input)
		if err == nil && !created {
			return errors.New("enqueue did not create the replacement round")
		}
		return err
	})
	if err != nil || hook.err != nil || !hook.superseded || attempts != 2 || current == nil {
		t.Fatalf("enqueue=%#v err=%v race=%v superseded=%v attempts=%d", current, err, hook.err, hook.superseded, attempts)
	}
	stored, err := repos.Tasks.GetByIdentity(ctx, input.Type, input.SubjectType, input.SubjectKey, *input.Generation)
	if err != nil || stored == nil || stored.ID != current.ID {
		t.Fatalf("current identity=%#v err=%v", stored, err)
	}
	history, err := repos.Tasks.List(ctx, repository.TaskListFilter{Scope: repository.TaskScopeHistory})
	if err != nil || len(history.Tasks) != 1 || history.Tasks[0].ID != hook.source.ID || history.Tasks[0].SupersededAt == nil {
		t.Fatalf("history=%#v err=%v", history, err)
	}
	var counts struct{ Tasks, Events int }
	if err := db.NewRaw(`SELECT (SELECT COUNT(*) FROM tasks) AS tasks, (SELECT COUNT(*) FROM task_events) AS events`).Scan(ctx, &counts); err != nil || counts.Tasks != 3 || counts.Events != 4 {
		t.Fatalf("committed rows=%#v err=%v", counts, err)
	}
}

func TestPostgresConcurrentEnqueueCreatesOneRound(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	type result struct {
		id      int64
		created bool
		err     error
	}
	gate := make(chan struct{})
	results := make(chan result, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			<-gate
			row, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeUploadPlan, SubjectType: "test", SubjectKey: "racing-identity", Generation: new(int64(0)), InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: "test"}))
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{id: row.ID, created: created}
		})
	}
	close(gate)
	wg.Wait()
	close(results)
	ids, creations := map[int64]bool{}, 0
	for r := range results {
		if r.err != nil {
			t.Fatal(r.err)
		}
		ids[r.id] = true
		if r.created {
			creations++
		}
	}
	if len(ids) != 1 || creations != 1 {
		t.Fatalf("rounds = %v, creations = %d; want one of each", ids, creations)
	}
}

// Writing a task pointer takes a key-share lock on the task through its foreign
// key; locking the task for an update must not block that write.
func TestPostgresTaskLockDoesNotBlockReferencingWrites(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	row := enqueueAndClaimTask(t, repos, "referenced", time.Minute)
	held, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
			if _, err := tx.Tasks.GetForUpdate(t.Context(), row.ID); err != nil {
				close(held)
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `INSERT INTO task_schedules (key, next_run_at, latest_task_id, generation) VALUES ('referencing', CURRENT_TIMESTAMP, ?, 0)`, row.ID)
	close(release)
	if err != nil {
		t.Fatalf("referencing write while the task is locked: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
