package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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

func TestTaskIdentityLookupUsesBothIdentityIndexes(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()
	var history []*model.TaskHistory
	for id := int64(1); id <= 512; id++ {
		row := repositoryTestTask(&model.Task{
			ID: id, Type: model.TaskTypeUploadPlan, IdempotencyKey: fmt.Sprintf("identity-%d", id),
			InputVersion: 1, InputHash: "identity", Input: json.RawMessage(`{}`), Events: json.RawMessage(`[]`),
			Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: now, FinishedAt: &now, CreatedAt: now, UpdatedAt: now,
		})
		history = append(history, model.TaskHistoryFromTask(row))
	}
	if _, err := db.NewInsert().Model(&history).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "ANALYZE"); err != nil {
		t.Fatal(err)
	}
	capture := new(taskHistoryQueries)
	db.AddQueryHook(capture)
	repos := repository.NewRepositories(db)
	row, err := repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeUploadPlan, "identity-256")
	if err != nil || row == nil || row.ID != 256 || len(capture.queries) != 1 {
		t.Fatalf("identity lookup = %#v, queries=%d, err=%v", row, len(capture.queries), err)
	}
	query := capture.queries[0]
	plan, err := db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plan.Close() }()
	var details []string
	for plan.Next() {
		var id, parent, unused int
		var detail string
		if err := plan.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH tasks USING INDEX uq_tasks_type_key") || !strings.Contains(joined, "SEARCH task_history USING INDEX uq_task_history_type_key") {
		t.Fatalf("identity lookup scans unrelated rounds:\n%s", joined)
	}
}

func TestTaskHistoryPaginationAcrossLongArchivedChain(t *testing.T) {
	assertTaskHistoryPaginationAcrossLongArchivedChain(t, testDB(t))
}

func assertTaskHistoryPaginationAcrossLongArchivedChain(t *testing.T, db *bun.DB) {
	t.Helper()
	const length = 128
	now := time.Now().UTC()
	round := func(id int64) *model.Task {
		row := repositoryTestTask(&model.Task{
			ID: id, Type: model.TaskTypeUploadPlan, IdempotencyKey: "paged-history",
			InputVersion: 1, InputHash: "history", Input: json.RawMessage(`{"source":"retained"}`),
			Events: json.RawMessage(`[]`), Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
			AvailableAt: now, FinishedAt: &now, CreatedAt: now, UpdatedAt: now,
		})
		if id > 1 {
			previous := id - 1
			row.RetryOfTaskID = &previous
		}
		return row
	}
	var archived []*model.TaskHistory
	for id := int64(1); id < length; id++ {
		row := round(id)
		row.SupersededAt = &now
		archived = append(archived, model.TaskHistoryFromTask(row))
	}
	if _, err := db.NewInsert().Model(&archived).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewInsert().Model(round(length)).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	unrelated := round(256)
	unrelated.IdempotencyKey, unrelated.RetryOfTaskID = "unrelated-history", nil
	if _, err := db.NewInsert().Model(unrelated).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	repos := repository.NewRepositories(db)
	capture := new(taskHistoryQueries)
	db.AddQueryHook(capture)
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
		if len(capture.queries) > 3 || strings.Contains(capture.queries[0], "_json") {
			t.Fatalf("history traversal fetched payloads or made per-round queries: %#v", capture.queries)
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
	if _, err := db.NewDelete().Model((*model.TaskHistory)(nil)).Where("task_id = ?", 64).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Tasks.ListHistory(t.Context(), length, 0, 20); !errors.Is(err, repository.ErrTaskDataCorrupted) {
		t.Fatalf("broken predecessor was silently truncated: %v", err)
	}
}
