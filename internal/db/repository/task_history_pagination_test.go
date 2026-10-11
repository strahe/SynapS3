package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/testutil"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

type taskHistoryQueries struct{ queries []string }

func (h *taskHistoryQueries) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *taskHistoryQueries) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	h.queries = append(h.queries, event.Query)
}

func TestTaskIdentityLookupUsesIdentityIndex(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()
	var history []*model.Task
	for id := int64(1); id <= 512; id++ {
		history = append(history, repositoryTestTask(&model.Task{
			ID: id, Type: model.TaskTypeUploadPlan, SubjectType: "test", SubjectKey: "identity", Generation: new(id),
			InputVersion: 1, InputHash: "identity", Input: json.RawMessage(`{}`),
			Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: now, FinishedAt: &now, CreatedAt: now, UpdatedAt: now,
		}))
	}
	if _, err := db.NewInsert().Model(&history).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	var live []*model.Task
	for id := int64(513); id <= 1024; id++ {
		live = append(live, repositoryTestTask(&model.Task{
			ID: id, Type: model.TaskTypeUploadPlan, SubjectType: "test", SubjectKey: "identity", Generation: new(id),
			InputVersion: 1, InputHash: "identity", Input: json.RawMessage(`{}`),
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: now, CreatedAt: now, UpdatedAt: now,
		}))
	}
	if _, err := db.NewInsert().Model(&live).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "ANALYZE"); err != nil {
		t.Fatal(err)
	}
	capture := new(taskHistoryQueries)
	testutil.AddQueryHook(db, capture)
	repos := repository.NewRepositories(db)
	row, err := repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeUploadPlan, "test", "identity", 256)
	if err != nil || row == nil || row.ID != 256 || len(capture.queries) != 1 {
		t.Fatalf("identity lookup = %#v, queries=%d, err=%v", row, len(capture.queries), err)
	}
	assertPostgresPlanUsesIndex(t, db, "uq_tasks_identity", capture.queries[0])
}

func TestTaskHistoryPaginationAcrossLongArchivedChain(t *testing.T) {
	assertTaskHistoryPaginationAcrossLongArchivedChain(t, testDB(t))
}

func TestCurrentFailedSubjectHistoryPagination(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()
	st, sk := "periodic", "cache"
	var history []*model.Task
	for id := int64(1); id <= 512; id++ {
		row := repositoryTestTask(&model.Task{
			ID: id, Type: model.TaskTypeUploadPlan, SubjectType: st, SubjectKey: "other",
			InputVersion: 1, InputHash: "subject", Input: json.RawMessage(`{}`),
			Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: now, FinishedAt: &now, CreatedAt: now, UpdatedAt: now,
		})
		if id%2 == 0 {
			row.SubjectKey = sk
		}
		if id <= 6 {
			row.Status, row.AcknowledgedAt = model.TaskStatusFailed, &now
		}
		if id <= 2 {
			row.SupersededAt = &now
		}
		history = append(history, row)
	}
	if _, err := db.NewInsert().Model(&history).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	repos := repository.NewRepositories(db)
	for _, subject := range []struct {
		st, sk string
		ids    []int64
	}{{st, sk, []int64{6, 4}}, {st, "other", []int64{5, 3}}} {
		cursor := int64(0)
		for i, id := range subject.ids {
			page, err := repos.Tasks.ListCurrentFailedForSubject(t.Context(), subject.st, subject.sk, cursor, 1, model.TaskTypeUploadPlan)
			if err != nil || len(page.Tasks) != 1 || page.Tasks[0].ID != id {
				t.Fatalf("subject=%s/%s cursor=%d page=%+v, err=%v", subject.st, subject.sk, cursor, page, err)
			}
			wantCursor := id
			if i == len(subject.ids)-1 {
				wantCursor = 0
			}
			if page.NextBeforeID != wantCursor {
				t.Fatalf("cursor=%d, want %d", page.NextBeforeID, wantCursor)
			}
			cursor = page.NextBeforeID
		}
	}
}

func assertTaskHistoryPaginationAcrossLongArchivedChain(t *testing.T, db *bun.DB) {
	t.Helper()
	const length = 128
	now := time.Now().UTC()
	round := func(id int64) *model.Task {
		row := repositoryTestTask(&model.Task{
			ID: id, Type: model.TaskTypeUploadPlan, SubjectType: "test", SubjectKey: "paged-history", Generation: new(int64(0)),
			InputVersion: 1, InputHash: "history", Input: json.RawMessage(`{"source":"retained"}`),
			Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
			AvailableAt: now, FinishedAt: &now, CreatedAt: now, UpdatedAt: now,
		})
		if id > 1 {
			previous := id - 1
			row.RetryOfTaskID = &previous
		}
		return row
	}
	var archived []*model.Task
	for id := int64(1); id < length; id++ {
		row := round(id)
		row.SupersededAt = &now
		archived = append(archived, row)
	}
	if _, err := db.NewInsert().Model(&archived).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewInsert().Model(round(length)).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	unrelated := round(256)
	unrelated.SubjectKey, unrelated.RetryOfTaskID = "unrelated-history", nil
	if _, err := db.NewInsert().Model(unrelated).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	repos := repository.NewRepositories(db)
	capture := new(taskHistoryQueries)
	testutil.AddQueryHook(db, capture)
	checkPage := func(anchor, cursor, first int64, size int, next int64) {
		t.Helper()
		capture.queries = nil
		page, err := repos.Tasks.ListHistory(t.Context(), anchor, cursor, 20)
		if err != nil || len(page.Tasks) != size || page.NextBeforeID != next {
			t.Fatalf("history anchor=%d cursor=%d page=%#v err=%v", anchor, cursor, page, err)
		}
		for i, row := range page.Tasks {
			var input struct {
				Source string `json:"source"`
			}
			if row.ID != first-int64(i) || json.Unmarshal(row.Input, &input) != nil || input.Source != "retained" {
				t.Fatalf("history order or evidence at %d: %#v", i, row)
			}
		}
		if len(capture.queries) != 1 {
			t.Fatalf("history traversal made per-round queries: %#v", capture.queries)
		}
	}
	checkPage(1, 0, 128, 20, 109)
	checkPage(64, 0, 128, 20, 109)
	checkPage(128, 109, 108, 20, 89)
	checkPage(64, 21, 20, 20, 0)
	checkPage(128, 1, 0, 0, 0)
	if _, err := repos.Tasks.ListHistory(t.Context(), 1, 999, 20); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("missing cursor=%v", err)
	}
	if _, err := repos.Tasks.ListHistory(t.Context(), 1, unrelated.ID, 20); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("cursor from another chain=%v", err)
	}
	if _, err := repos.Tasks.ListHistory(t.Context(), 999, 0, 20); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("missing anchor=%v", err)
	}
	if err := repos.Tasks.AcknowledgeFailed(t.Context(), length); err != nil {
		t.Fatal(err)
	}
	checkPage(64, 109, 108, 20, 89)
}
