//go:build postgres

package repository_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestPostgresCurrentFailedHistoryPlanWithSupersededSkew(t *testing.T) {
	db := migratedPostgresDB(t)
	ctx := t.Context()
	now := time.Now().UTC()
	st, sk := "periodic", "cache"
	template := repositoryTestTask(&model.Task{
		ID: 700001, Type: model.TaskTypeUploadPlan, IdempotencyKey: "history-template",
		InputVersion: 1, InputHash: "history", Input: json.RawMessage(`{}`), Events: json.RawMessage(`[]`),
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute,
		SubjectType: &st, SubjectKey: &sk, AvailableAt: now, FinishedAt: &now, CreatedAt: now, UpdatedAt: now,
	})
	if _, err := db.NewInsert().Model(model.TaskHistoryFromTask(template)).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	var columns []string
	if err := db.NewRaw(`SELECT attname FROM pg_attribute WHERE attrelid = 'task_history'::regclass
		AND attnum > 0 AND NOT attisdropped ORDER BY attnum`).Scan(ctx, &columns); err != nil {
		t.Fatal(err)
	}
	expressions := make([]string, len(columns))
	for i, column := range columns {
		expressions[i] = "base." + column
		switch column {
		case "task_id":
			expressions[i] = "n"
		case "idempotency_key":
			expressions[i] = "'history-' || n::text"
		case "status":
			expressions[i] = "CASE WHEN n <= 100006 THEN 'failed' WHEN n <= 100009 THEN 'cancelled' ELSE 'completed' END"
		case "acknowledged_at":
			expressions[i] = "CASE WHEN n > 100000 AND n <= 100006 THEN base.finished_at ELSE NULL END"
		case "superseded_at":
			expressions[i] = "CASE WHEN n <= 100000 THEN base.finished_at + n * interval '1 microsecond' ELSE NULL END"
		case "subject_type", "subject_key":
			expressions[i] = "CASE WHEN n % 2 = 0 THEN base." + column + " ELSE NULL END"
		}
	}
	// Most failed rows have distinct supersession times; only six remain
	// current. This defeats the planner's independence assumption for status
	// and superseded_at unless the ordered subject index is usable.
	query := "INSERT INTO task_history (" + strings.Join(columns, ",") + ") SELECT " + strings.Join(expressions, ",") +
		" FROM task_history base CROSS JOIN generate_series(1,600000) series(n) WHERE base.task_id = 700001"
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM task_history WHERE task_id = 700001`); err != nil {
		t.Fatal(err)
	}
	for i, id := range []int64{600001, 600002} {
		row := repositoryTestTask(&model.Task{
			ID: id, Type: model.TaskTypeUploadPlan, IdempotencyKey: fmt.Sprintf("work-%d", id),
			InputVersion: 1, InputHash: "work", Input: json.RawMessage(`{}`), Events: json.RawMessage(`[]`),
			Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: now, FinishedAt: &now, CreatedAt: now, UpdatedAt: now,
		})
		if i == 0 {
			row.SubjectType, row.SubjectKey = &st, &sk
		}
		if _, err := db.NewInsert().Model(row).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "ANALYZE tasks, task_history"); err != nil {
		t.Fatal(err)
	}
	capture := new(taskHistoryQueries)
	db.AddQueryHook(capture)
	repos := repository.NewRepositories(db)
	for _, scenario := range []struct {
		name, subjectType, subjectKey string
		before                        int64
		types                         []model.TaskType
		want                          []int64
	}{
		{"subject", st, sk, 0, nil, []int64{600001, 100006}},
		{"null_subject", "", "", 0, nil, []int64{600002, 100005}},
		{"subject_cursor_type", st, sk, 100006, []model.TaskType{model.TaskTypeUploadPlan}, []int64{100004, 100002}},
		{"null_cursor_type", "", "", 100005, []model.TaskType{model.TaskTypeUploadPlan}, []int64{100003, 100001}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			capture.queries = nil
			page, err := repos.Tasks.ListCurrentFailedForSubject(ctx, scenario.subjectType, scenario.subjectKey, scenario.before, 2, scenario.types...)
			if err != nil || len(capture.queries) != 1 {
				t.Fatalf("page queries=%d, err=%v", len(capture.queries), err)
			}
			var ids []int64
			for _, row := range page.Tasks {
				ids = append(ids, row.ID)
			}
			wantCursor := int64(0)
			if scenario.before == 0 {
				wantCursor = scenario.want[len(scenario.want)-1]
			}
			if !slices.Equal(ids, scenario.want) || page.NextBeforeID != wantCursor {
				t.Fatalf("page IDs=%v, cursor=%v; want %v, %v", ids, page.NextBeforeID, scenario.want, wantCursor)
			}
			assertPostgresPlanUsesIndex(t, db, "idx_task_history_current_failed_subject", capture.queries[0])
		})
	}
	capture.queries = nil
	page, err := repos.Tasks.List(ctx, repository.TaskListFilter{
		Scope: repository.TaskScopeHistory, Status: model.TaskStatusFailed, Limit: 2,
	})
	if err != nil || len(page.Tasks) != 2 || page.Tasks[0].ID != 100006 || page.Tasks[1].ID != 100005 || page.NextBeforeID != 100005 {
		t.Fatalf("status page=%+v, err=%v", page, err)
	}
	// A selective history status uses the status index. The planner can still
	// choose the primary key for common statuses, depending on distribution.
	capture.queries = nil
	page, err = repos.Tasks.List(ctx, repository.TaskListFilter{
		Scope: repository.TaskScopeHistory, Status: model.TaskStatusCancelled, Limit: 2,
	})
	if err != nil || len(page.Tasks) != 2 || page.Tasks[0].ID != 100009 || page.Tasks[1].ID != 100008 || page.NextBeforeID != 100008 {
		t.Fatalf("cancelled page=%+v, err=%v", page, err)
	}
	assertPostgresPlanUsesIndex(t, db, "idx_task_history_status_id", capture.queries[0])
}
