//go:build postgres

package repository_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testpg"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func TestPostgresConcurrentTaskClaimsAreUnique(t *testing.T) {
	db := newPostgresTaskDB(t)
	repos := repository.NewRepositories(db)
	const taskCount = 24
	input := json.RawMessage(`{}`)
	sum := sha256.Sum256(input)
	for index := range taskCount {
		if _, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
			Type: model.TaskType("postgres_claim_test"), IdempotencyKey: fmt.Sprintf("claim-%d", index),
			InputVersion: 1, Input: input, InputHash: hex.EncodeToString(sum[:]),
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
			row, err := repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{})
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

func TestPostgresTaskClaimsClearWaitDetailsAndPreserveWorkStart(t *testing.T) {
	assertTaskClaimsClearWaitDetailsAndPreserveWorkStart(t, newPostgresTaskDB(t))
}

func TestPostgresTaskClaimFiltersPreserveOrderAndRecoveryPriority(t *testing.T) {
	assertTaskClaimFiltersPreserveOrderAndRecoveryPriority(t, newPostgresTaskDB(t))
}

type claimQueryRecorder struct {
	queries []string
}

func (h *claimQueryRecorder) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *claimQueryRecorder) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if strings.HasPrefix(event.Query, "UPDATE tasks\nSET status = 'running'") {
		h.queries = append(h.queries, event.Query)
	}
}

func TestPostgresTaskClaimFilteredBacklogQueryPlan(t *testing.T) {
	db := newPostgresTaskDB(t)
	ctx := t.Context()
	const backlogPerMode = 5000
	old := time.Now().UTC().Add(-time.Hour)
	if _, err := db.ExecContext(ctx, `INSERT INTO tasks
		(id,type,idempotency_key,input_version,input_hash,status,resume_mode,available_at,
		claim_generation,claimed_at,lease_until,created_at,updated_at,input_json,policy_json,runtime_json,events_json)
		SELECT id,'full','backlog-'||id,1,'hash',CASE WHEN id <= ? THEN 'running' ELSE 'pending' END,
		'execute',?,CASE WHEN id <= ? THEN 1 ELSE 0 END,
		CASE WHEN id <= ? THEN ?::timestamptz ELSE NULL END,CASE WHEN id <= ? THEN ?::timestamptz ELSE NULL END,
		?,?,'{}','{"version":2,"max_attempts":6}','{}','[]' FROM generate_series(1, ?) AS id`,
		backlogPerMode, old, backlogPerMode, backlogPerMode, old, backlogPerMode, old,
		old, old, 2*backlogPerMode); err != nil {
		t.Fatal(err)
	}
	for index, status := range []model.TaskStatus{model.TaskStatusRunning, model.TaskStatusPending} {
		row := repositoryTestTask(&model.Task{
			ID: int64(2*backlogPerMode + index + 1), Type: "available", IdempotencyKey: fmt.Sprintf("eligible-%d", index),
			InputVersion: 1, Input: []byte(`{}`), InputHash: "hash", Status: status, Events: []byte(`[]`),
			ResumeMode: model.TaskResumeModeExecute, AvailableAt: old.Add(time.Minute),
		})
		if status == model.TaskStatusRunning {
			row.ClaimGeneration, row.ClaimedAt, row.LeaseUntil = 1, &old, new(old.Add(time.Minute))
		}
		if _, err := db.NewInsert().Model(row).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "ANALYZE tasks"); err != nil {
		t.Fatal(err)
	}
	recorder := new(claimQueryRecorder)
	db.AddQueryHook(recorder)
	repos := repository.NewRepositories(db)
	for index, mode := range []string{"recovery", "pending"} {
		before := len(recorder.queries)
		row, err := repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{ExcludedTypes: []string{"full"}})
		if err != nil || row == nil || row.ID != int64(2*backlogPerMode+index+1) {
			t.Fatalf("%s claim = %#v, err=%v", mode, row, err)
		}
		if len(recorder.queries) <= before {
			t.Fatal("claim query was not recorded")
		}
		query := recorder.queries[len(recorder.queries)-1]
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Replay the real claim under rollback so EXPLAIN measures candidate selection and mutation.
		if index == 0 {
			_, err = tx.ExecContext(ctx, "UPDATE tasks SET lease_until=? WHERE id=?", old.Add(time.Minute), row.ID)
		} else {
			_, err = tx.ExecContext(ctx, "UPDATE tasks SET status='pending',claimed_at=NULL,lease_until=NULL WHERE id=?", row.ID)
		}
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		var plan []string
		err = tx.NewRaw("EXPLAIN (ANALYZE, BUFFERS) "+query).Scan(ctx, &plan)
		rollbackErr := tx.Rollback()
		if err != nil || rollbackErr != nil {
			t.Fatalf("explain %s: %v, rollback: %v", mode, err, rollbackErr)
		}
		t.Logf("%s claim with %d older excluded rows:\n%s", mode, backlogPerMode, strings.Join(plan, "\n"))
	}
}

func TestPostgresTaskOperationAdmissionAndRollback(t *testing.T) {
	assertTaskOperationAdmissionAndRollback(t, newPostgresTaskDB(t))
}

func TestPostgresTaskEventsAreBoundedAndTransactional(t *testing.T) {
	assertTaskEventsAreBoundedAndTransactional(t, newPostgresTaskDB(t))
}

func TestPostgresTaskClaimSkipsLockedHeadWithoutLegacyAdvisoryLock(t *testing.T) {
	db := newPostgresTaskDB(t)
	repos := repository.NewRepositories(db)
	availableAt := time.Now().Add(-time.Minute)
	for index := range 2 {
		if _, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
			Type: model.TaskType("postgres_skip_locked_test"), IdempotencyKey: fmt.Sprintf("claim-%d", index),
			InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: fmt.Sprintf("hash-%d", index),
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: availableAt.Add(time.Duration(index) * time.Second),
		})); err != nil || !created {
			t.Fatalf("enqueue task %d: created=%t err=%v", index, created, err)
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
	if err := tx.NewRaw(`SELECT id FROM tasks
		WHERE status = 'pending' AND available_at <= ?
		ORDER BY available_at, id
		LIMIT 1
		FOR UPDATE`, time.Now()).Scan(t.Context(), &lockedID); err != nil {
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
		claimed, claimErr := repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{})
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

func newPostgresTaskDB(t *testing.T) *bun.DB {
	t.Helper()
	config, err := pgx.ParseConfig(testpg.SchemaDSN(t))
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	db := bun.NewDB(stdlib.OpenDB(*config), pgdialect.New())
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL test database: %v", err)
		}
	})
	if err := migrations.ValidateTarget(t.Context(), db); err != nil {
		t.Fatalf("validate empty PostgreSQL schema: %v", err)
	}
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(t.Context()); err != nil {
		t.Fatalf("initialize PostgreSQL migrations: %v", err)
	}
	if _, err := migrator.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate PostgreSQL schema: %v", err)
	}
	return db
}
