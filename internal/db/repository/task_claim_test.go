package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/testutil"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestPostgresConcurrentTaskClaimsAreUnique(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter repository.TaskClaimFilter
	}{
		{name: "unfiltered"},
		{name: "filtered", filter: repository.TaskClaimFilter{ExcludedTypes: []string{"full"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertConcurrentTaskClaimsAreUnique(t, tc.filter)
		})
	}
}

func assertConcurrentTaskClaimsAreUnique(t *testing.T, filter repository.TaskClaimFilter) {
	t.Helper()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	const taskCount = 24
	input := json.RawMessage(`{}`)
	for index := range taskCount {
		if _, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
			Type: model.TaskType("postgres_claim_test"), SubjectType: "test", SubjectKey: fmt.Sprintf("claim-%d", index), Generation: new(int64(0)),
			InputVersion: 1, Input: input,
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		})); err != nil || !created {
			t.Fatalf("enqueue task %d: created=%t err=%v", index, created, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	claimed := make(chan *model.Task, taskCount)
	errorsFound := make(chan error, taskCount)
	var workers sync.WaitGroup
	for range taskCount {
		workers.Go(func() {
			row, err := repos.Tasks.ClaimNext(ctx, time.Minute, filter)
			if err != nil {
				errorsFound <- err
				return
			}
			claimed <- row
		})
	}
	workers.Wait()
	close(claimed)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("claim task: %v", err)
	}
	seen := make(map[int64]struct{}, taskCount)
	for row := range claimed {
		if row == nil {
			t.Error("concurrent claim returned no task")
			continue
		}
		if _, exists := seen[row.ID]; exists {
			t.Errorf("task %d was claimed more than once", row.ID)
		}
		seen[row.ID] = struct{}{}
	}
	if len(seen) != taskCount {
		t.Fatalf("unique claims = %d, want %d", len(seen), taskCount)
	}
}

type claimQueryRecorder struct {
	queries []string
}

func (h *claimQueryRecorder) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *claimQueryRecorder) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if strings.HasPrefix(event.Query, "UPDATE tasks\nSET status = 'running'") ||
		strings.HasPrefix(event.Query, "WITH RECURSIVE task_types") {
		h.queries = append(h.queries, event.Query)
	}
}

var taskClaimDiagnostics = flag.Bool("task-claim-diagnostics", false, "Record PostgreSQL task claim plans with an excluded backlog")

func captureTaskClaimSQL(t *testing.T, db *bun.DB, recorder *claimQueryRecorder, filter repository.TaskClaimFilter) (*model.Task, []string) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	before := len(recorder.queries)
	row, err := repository.NewRepositories(tx).Tasks.ClaimNext(t.Context(), time.Minute, filter)
	queries := append([]string(nil), recorder.queries[before:]...)
	rollbackErr := tx.Rollback()
	if err != nil || rollbackErr != nil {
		t.Fatalf("capture claim: %v, rollback: %v", err, rollbackErr)
	}
	return row, queries
}

func claimDiagnosticMode(query string, status model.TaskStatus) bool {
	return strings.Contains(query, "WHERE status = '"+string(status)+"'") ||
		strings.Contains(query, "AND status = '"+string(status)+"'")
}

func explainDiagnosticClaims(t *testing.T, db *bun.DB, label string, queries []string) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for index, query := range queries {
		var plan []string
		if err := tx.NewRaw("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) "+query).Scan(t.Context(), &plan); err != nil {
			t.Fatalf("explain %s/%d: %v", label, index, err)
		}
		t.Logf("%s/%d:\n%s", label, index, strings.Join(plan, "\n"))
	}
}

func taskClaimIndexMaintenanceDiagnostics(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("type indexes=%t", indexed), func(t *testing.T) {
			db := testDB(t)
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if !indexed {
				for _, name := range []string{"idx_tasks_pending_type_available", "idx_tasks_recovery_type_lease"} {
					if _, err := tx.ExecContext(t.Context(), "DROP INDEX "+name); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Bulk writes isolate index maintenance; they do not measure worker throughput.
			for _, operation := range []struct {
				name  string
				query string
			}{
				{name: "insert", query: `INSERT INTO tasks
					(type,subject_type,subject_key,generation,input_version,status,resume_mode,available_at,
					created_at,updated_at,input_json,runtime_json)
					SELECT 'write-test','test',id::text,0,1,'pending','execute',CURRENT_TIMESTAMP,
					CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}','{}'
					FROM generate_series(1,500) AS id`},
				{name: "claim", query: `UPDATE tasks SET status='running', claim_generation=1,
					lease_until=CURRENT_TIMESTAMP+interval '1 minute'
					WHERE type='write-test'`},
				{name: "renew", query: `UPDATE tasks SET lease_until=lease_until+interval '1 minute'
					WHERE type='write-test'`},
				{name: "settle", query: `UPDATE tasks SET status='completed',
					lease_until=NULL, finished_at=CURRENT_TIMESTAMP WHERE type='write-test'`},
			} {
				var plan []string
				if err := tx.NewRaw("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) "+operation.query).Scan(t.Context(), &plan); err != nil {
					t.Fatal(err)
				}
				t.Logf("%s, type indexes=%t:\n%s", operation.name, indexed, strings.Join(plan, "\n"))
			}
		})
	}
}

func assertFilteredClaimPlan(t *testing.T, plan postgresExecutionPlan, index, orderingColumn string) {
	t.Helper()
	boundedHead := false
	for _, condition := range planIndexConditions(plan, index) {
		if strings.Contains(condition, "allowed_types.type") && strings.Contains(condition, orderingColumn) {
			boundedHead = true
		}
	}
	var scannedTasks float64
	var countScans func(postgresExecutionPlan)
	countScans = func(node postgresExecutionPlan) {
		if node.RelationName == "tasks" && strings.Contains(node.NodeType, "Scan") {
			scannedTasks += (node.ActualRows + node.RowsRemovedByFilter) * node.ActualLoops
		}
		for _, child := range node.Children {
			countScans(child)
		}
	}
	countScans(plan)
	// Four types and three eligible heads need far fewer reads than the 5,000 excluded tasks.
	if scannedTasks > 128 {
		t.Fatalf("candidate discovery processed %.0f task rows, want at most 128: %#v", scannedTasks, plan)
	}
	if !boundedHead {
		t.Fatalf("candidate lookup lacks type/time bounds on %s: %#v", index, plan)
	}
}

func TestPostgresTaskClaimFilteredBacklogPlans(t *testing.T) {
	type claimPlanCase struct {
		name     string
		excluded int
		allowed  int
		locked   int
	}
	cases := []claimPlanCase{
		{name: "excluded_5000", excluded: 5000, allowed: 3},
	}
	if *taskClaimDiagnostics {
		cases = append(cases,
			claimPlanCase{name: "excluded_0", allowed: 3},
			claimPlanCase{name: "excluded_50000", excluded: 50000, allowed: 3},
			claimPlanCase{name: "allowed_backlog", excluded: 5000, allowed: 50000},
			claimPlanCase{name: "no_candidate", excluded: 50000},
			claimPlanCase{name: "locked_heads", excluded: 5000, allowed: 3, locked: 2},
		)
	}
	for _, tc := range cases {
		for _, status := range []model.TaskStatus{model.TaskStatusRunning, model.TaskStatusPending} {
			t.Run(fmt.Sprintf("%s/%s", tc.name, status), func(t *testing.T) {
				db := testDB(t)
				ctx := t.Context()
				old := time.Now().UTC().Add(-time.Hour)
				if _, err := db.ExecContext(ctx, `INSERT INTO tasks
					(id,type,subject_type,subject_key,generation,input_version,status,resume_mode,available_at,
					claim_generation,lease_until,created_at,updated_at,input_json,runtime_json)
					SELECT id,CASE WHEN id <= ? THEN 'full' ELSE 'available-'||(id%3) END,
					       'test','backlog-'||id,0,1,?,'execute',?,
					       CASE WHEN ?='running' THEN 1 ELSE 0 END,
					       CASE WHEN ?='running' THEN ?::timestamptz ELSE NULL END,
					       ?,?,'{}','{}'
					FROM generate_series(1, ?) AS id`,
					tc.excluded, status, old, status, status, old,
					old, old, tc.excluded+tc.allowed); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, "ANALYZE tasks"); err != nil {
					t.Fatal(err)
				}
				if tc.locked > 0 {
					blocker, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = blocker.Rollback() }()
					if _, err := blocker.ExecContext(ctx, `SELECT id FROM tasks
						WHERE id > ? ORDER BY id LIMIT ? FOR NO KEY UPDATE`, tc.excluded, tc.locked); err != nil {
						t.Fatal(err)
					}
				}
				recorder := new(claimQueryRecorder)
				testutil.AddQueryHook(db, recorder)
				row, queries := captureTaskClaimSQL(t, db, recorder,
					repository.TaskClaimFilter{ExcludedTypes: []string{"full"}})
				if tc.allowed == 0 {
					if row != nil {
						t.Fatalf("claim with no eligible tasks = %#v", row)
					}
				} else if want := int64(tc.excluded + tc.locked + 1); row == nil || row.ID != want {
					t.Fatalf("filtered claim = %#v, want task %d", row, want)
				}
				filteredQueries := make([]string, 0, len(queries))
				for _, query := range queries {
					if claimDiagnosticMode(query, status) {
						filteredQueries = append(filteredQueries, query)
					}
				}
				if len(filteredQueries) == 0 {
					t.Fatal("claim SQL was not captured")
				}
				if tc.name == "excluded_5000" {
					index, orderingColumn := "idx_tasks_pending_type_available", "available_at"
					if status == model.TaskStatusRunning {
						index, orderingColumn = "idx_tasks_recovery_type_lease", "lease_until"
					}
					candidates := 0
					for _, query := range filteredQueries {
						if strings.HasPrefix(query, "WITH RECURSIVE task_types") {
							assertFilteredClaimPlan(t, explainPostgresPlan(t, db, query), index, orderingColumn)
							candidates++
						}
					}
					if candidates == 0 {
						t.Fatal("candidate discovery SQL was not captured")
					}
				}
				if !*taskClaimDiagnostics {
					return
				}
				_, originalQueries := captureTaskClaimSQL(t, db, recorder, repository.TaskClaimFilter{})
				var legacyQueries []string
				for _, query := range originalQueries {
					if claimDiagnosticMode(query, status) {
						// The unfiltered fast path retains the former claim statement.
						legacyQueries = append(legacyQueries, strings.Replace(query,
							"\n    ORDER BY ", " AND type NOT IN ('full')\n    ORDER BY ", 1))
					}
				}
				if len(legacyQueries) == 0 {
					t.Fatal("legacy claim SQL was not captured")
				}
				t.Logf("excluded=%d allowed=%d locked=%d; legacy statements=%d filtered statements=%d",
					tc.excluded, tc.allowed, tc.locked, len(legacyQueries), len(filteredQueries))
				explainDiagnosticClaims(t, db, "legacy", legacyQueries)
				explainDiagnosticClaims(t, db, "filtered", filteredQueries)
			})
		}
	}
	if *taskClaimDiagnostics {
		t.Run("index maintenance", taskClaimIndexMaintenanceDiagnostics)
	}
}

func TestPostgresTaskClaimSkipsLockedHeadWithoutLegacyAdvisoryLock(t *testing.T) {
	for _, status := range []model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning} {
		for _, filtered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/filtered=%t", status, filtered), func(t *testing.T) {
				filter := repository.TaskClaimFilter{}
				if filtered {
					filter.ExcludedTypes = []string{"full"}
				}
				assertTaskClaimSkipsLockedHead(t, status, filter)
			})
		}
	}
}

func assertTaskClaimSkipsLockedHead(t *testing.T, status model.TaskStatus, filter repository.TaskClaimFilter) {
	t.Helper()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	availableAt := time.Now().Add(-time.Minute)
	orderingColumn := "available_at"
	if status == model.TaskStatusRunning {
		orderingColumn = "lease_until"
	}
	for index := range 2 {
		row, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
			Type: model.TaskType("postgres_skip_locked_test"), SubjectType: "test", SubjectKey: fmt.Sprintf("claim-%d", index), Generation: new(int64(0)),
			InputVersion: 1, Input: json.RawMessage(`{}`),
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: availableAt.Add(time.Duration(index) * time.Second),
		}))
		if err != nil || !created {
			t.Fatalf("enqueue task %d: created=%t err=%v", index, created, err)
		}
		if status == model.TaskStatusRunning {
			if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET status='running', claim_generation=1,
				lease_until=? WHERE id=?`, row.AvailableAt, row.ID); err != nil {
				t.Fatal(err)
			}
		}
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), "SELECT pg_advisory_xact_lock(384)"); err != nil {
		t.Fatalf("hold legacy advisory lock: %v", err)
	}
	var lockedID int64
	if err := tx.NewRaw(fmt.Sprintf(`SELECT id FROM tasks
		WHERE status = ? AND %s <= ?
		ORDER BY %s, id
		LIMIT 1
		FOR UPDATE`, orderingColumn, orderingColumn), status, time.Now()).Scan(t.Context(), &lockedID); err != nil {
		t.Fatalf("lock queue head: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	type claimResult struct {
		task *model.Task
		err  error
	}
	result := make(chan claimResult, 1)
	go func() {
		claimed, claimErr := repos.Tasks.ClaimNext(ctx, time.Minute, filter)
		result <- claimResult{task: claimed, err: claimErr}
	}()
	select {
	case claimed := <-result:
		if claimed.err != nil || claimed.task == nil {
			t.Fatalf("claim unlocked task = %#v err=%v", claimed.task, claimed.err)
		}
		if claimed.task.ID == lockedID {
			t.Fatalf("claimed locked queue head %d", lockedID)
		}
	case <-ctx.Done():
		t.Fatal("claim blocked behind the queue head or legacy advisory lock")
	}
}

func TestFilteredTaskClaimRechecksCandidateAndPreservesLease(t *testing.T) {
	for _, tc := range []struct {
		name      string
		recovery  bool
		mutation  string
		wantFirst bool
		lateDue   bool
		canceled  bool
	}{
		{name: "generation changed", mutation: "claim_generation=claim_generation+1"},
		{name: "ordering changed", mutation: "available_at=available_at+interval '30 minutes'"},
		{
			name: "recovery became pending", recovery: true, wantFirst: true,
			mutation: "status='pending', lease_until=NULL",
		},
		{name: "due cutoff stays fixed", lateDue: true, mutation: "claim_generation=claim_generation+1"},
		{name: "context canceled after discovery", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			old := time.Now().Add(-time.Hour)
			rows := make([]*model.Task, 2)
			for index := range rows {
				availableAt := old.Add(time.Duration(index) * time.Minute)
				if tc.lateDue && index == 1 {
					availableAt = time.Now().Add(time.Hour)
				}
				row, _, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
					Type: "available", SubjectType: "test", SubjectKey: fmt.Sprintf("race-%d", index), Generation: new(int64(0)),
					InputVersion: 1, Input: []byte(`{}`), AvailableAt: availableAt,
				}))
				if err != nil {
					t.Fatal(err)
				}
				rows[index] = row
			}
			if tc.recovery {
				if _, err := db.ExecContext(t.Context(), `UPDATE tasks SET status='running', claim_generation=1,
						lease_until=? WHERE id=?`, old, rows[0].ID); err != nil {
					t.Fatal(err)
				}
			}
			hold := newPostgresRaceHold("claim", func(query string) bool {
				return strings.HasPrefix(query, "with recursive task_types")
			})
			testutil.AddQueryHook(db, hold)
			defer hold.Release()
			type claimResult struct {
				task *model.Task
				err  error
			}
			result := make(chan claimResult, 1)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			claimCtx, cancelClaim := context.WithCancel(ctx)
			defer cancelClaim()
			go func() {
				row, err := repos.Tasks.ClaimNext(postgresRaceContext(claimCtx, "claim"), time.Minute,
					repository.TaskClaimFilter{ExcludedTypes: []string{"full"}})
				result <- claimResult{task: row, err: err}
			}()
			select {
			case <-hold.reached:
			case <-ctx.Done():
				t.Fatal("claim did not discover a candidate")
			}
			if tc.canceled {
				cancelClaim()
			} else {
				if _, err := db.ExecContext(ctx, "UPDATE tasks SET "+tc.mutation+" WHERE id=?", rows[0].ID); err != nil {
					t.Fatal(err)
				}
			}
			if tc.lateDue {
				if _, err := db.ExecContext(ctx, "UPDATE tasks SET available_at=? WHERE id=?", time.Now(), rows[1].ID); err != nil {
					t.Fatal(err)
				}
			}
			releasedAt := time.Now().UTC().Truncate(time.Microsecond)
			hold.Release()
			select {
			case got := <-result:
				if tc.canceled {
					if got.task != nil || !errors.Is(got.err, context.Canceled) {
						t.Fatalf("canceled claim = %#v, %v", got.task, got.err)
					}
					for _, original := range rows {
						var stored model.Task
						if err := db.NewSelect().Model(&stored).Where("id = ?", original.ID).Scan(ctx); err != nil {
							t.Fatal(err)
						}
						if stored.Status != model.TaskStatusPending || stored.ClaimGeneration != original.ClaimGeneration ||
							stored.LeaseUntil != nil {
							t.Fatalf("canceled claim changed task: %#v", stored)
						}
					}
					return
				}
				if tc.lateDue {
					if got.err != nil || got.task != nil {
						t.Fatalf("claim included newly due work: %#v, %v", got.task, got.err)
					}
					next, err := repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{ExcludedTypes: []string{"full"}})
					if err != nil || next == nil || next.ID != rows[0].ID {
						t.Fatalf("next claim retained retry exclusions: %#v, %v", next, err)
					}
					return
				}
				want := rows[1]
				if tc.wantFirst {
					want = rows[0]
				}
				if got.err != nil || got.task == nil || got.task.ID != want.ID {
					t.Fatalf("claim = %#v, %v; want %d", got.task, got.err, want.ID)
				}
				if got.task.LeaseUntil == nil || got.task.LeaseUntil.Before(releasedAt.Add(time.Minute)) ||
					got.task.LeaseUntil.After(time.Now().Add(time.Minute)) {
					t.Fatalf("claim after retry did not retain its full lease: %#v", got.task)
				}
			case <-ctx.Done():
				t.Fatal("claim did not retry its changed candidate")
			}
		})
	}
}
