package repository_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
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
		if _, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
			Type: model.TaskType("postgres_claim_test"), IdempotencyKey: fmt.Sprintf("claim-%d", index),
			InputVersion: 1, Input: input, InputHash: hex.EncodeToString(sum[:]),
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		}); err != nil || !created {
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
			row, err := repos.Tasks.ClaimNext(ctx, time.Minute)
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

func TestPostgresTaskClaimSkipsLockedHeadWithoutLegacyAdvisoryLock(t *testing.T) {
	db := newPostgresTaskDB(t)
	repos := repository.NewRepositories(db)
	availableAt := time.Now().Add(-time.Minute)
	for index := range 2 {
		if _, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
			Type: model.TaskType("postgres_skip_locked_test"), IdempotencyKey: fmt.Sprintf("claim-%d", index),
			InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: fmt.Sprintf("hash-%d", index),
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
			AvailableAt: availableAt.Add(time.Duration(index) * time.Second),
		}); err != nil || !created {
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
		claimed, claimErr := repos.Tasks.ClaimNext(ctx, time.Minute)
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
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	adminSQL, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	adminDB := bun.NewDB(adminSQL, pgdialect.New())
	schema := fmt.Sprintf("task_claim_%x", sha256.Sum256([]byte(fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()))))[:40]
	quotedSchema := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	if _, err := adminDB.Exec("CREATE SCHEMA " + quotedSchema); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create PostgreSQL schema: %v", err)
	}
	config.RuntimeParams["search_path"] = schema
	db := bun.NewDB(stdlib.OpenDB(*config), pgdialect.New())
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = adminDB.Exec("DROP SCHEMA " + quotedSchema + " CASCADE")
		_ = adminDB.Close()
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
