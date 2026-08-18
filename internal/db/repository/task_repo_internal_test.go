package repository

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	synaps3db "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"
)

type recurringTaskLockHook struct {
	queries atomic.Int32
	second  chan struct{}
	once    sync.Once
}

func (h *recurringTaskLockHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if strings.Contains(event.Query, "SET status = status") && h.queries.Add(1) == 2 {
		h.once.Do(func() { close(h.second) })
	}
	return ctx
}

func (*recurringTaskLockHook) AfterQuery(context.Context, *bun.QueryEvent) {}

func TestClaimReadySQLUsesReadyScheduledIndex(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "claim-ready-plan.db")+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := synaps3db.RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	now := time.Now()
	rows, err := sqldb.QueryContext(ctx, "EXPLAIN QUERY PLAN "+claimReadySQL,
		model.TaskStatusRunning, now, now.Add(time.Minute), now,
		model.TaskTypeUpload,
		now,
	)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN ClaimReady: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var details []string
	for rows.Next() {
		var id int
		var parent int
		var notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate plan rows: %v", err)
	}

	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "idx_tasks_type_ready_scheduled") {
		t.Fatalf("ClaimReady plan =\n%s\nwant idx_tasks_type_ready_scheduled", plan)
	}
	if strings.Contains(plan, "USE TEMP B-TREE") {
		t.Fatalf("ClaimReady plan =\n%s\nwant no temp sort", plan)
	}
}

func TestRunningUploadCopyTaskPayloadExpressionsAreDialectSpecific(t *testing.T) {
	tests := []struct {
		name          string
		dialect       dialect.Name
		wantUploadID  string
		wantCopyIndex string
	}{
		{
			name:          "sqlite",
			dialect:       dialect.SQLite,
			wantUploadID:  "CAST(json_extract(payload, '$.upload_id') AS INTEGER)",
			wantCopyIndex: "CAST(json_extract(payload, '$.copy_index') AS INTEGER)",
		},
		{
			name:          "postgres",
			dialect:       dialect.PG,
			wantUploadID:  "CAST(payload ->> 'upload_id' AS BIGINT)",
			wantCopyIndex: "CAST(payload ->> 'copy_index' AS INTEGER)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uploadID, copyIndex := runningUploadCopyTaskPayloadExpressions(tt.dialect)
			if uploadID != tt.wantUploadID || copyIndex != tt.wantCopyIndex {
				t.Fatalf("payload expressions = %q, %q; want %q, %q", uploadID, copyIndex, tt.wantUploadID, tt.wantCopyIndex)
			}
		})
	}
}

func TestEnsureRecurringCannotLoseWakeupWhileCoordinatorCompletes(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "recurring-lock.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqldb.SetMaxOpenConns(4)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := synaps3db.RunMigrations(ctx, db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repos := NewRepositories(db)
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: 1,
		IdempotencyKey: "upload:repair-data-set:lost-wakeup", Status: model.TaskStatusQueued,
		MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimReady: task=%#v err=%v", claimed, err)
	}

	hook := &recurringTaskLockHook{second: make(chan struct{})}
	db.AddQueryHook(hook)
	locked := make(chan struct{})
	release := make(chan struct{})
	completeResult := make(chan error, 1)
	go func() {
		completeResult <- repos.WithTx(ctx, func(txRepos *Repositories) error {
			if err := txRepos.Tasks.LockRunningClaim(ctx, claimed); err != nil {
				return err
			}
			close(locked)
			<-release
			return txRepos.Tasks.Complete(ctx, claimed)
		})
	}()
	waitTestSignal(t, locked, "coordinator task lock")

	wakeup := *task
	wakeup.ID = 0
	wakeup.Payload = map[string]interface{}{"storage_upload_copy_id": int64(202)}
	wakeupResult := make(chan struct {
		created bool
		err     error
	}, 1)
	go func() {
		created, err := repos.Tasks.EnsureRecurring(ctx, &wakeup)
		wakeupResult <- struct {
			created bool
			err     error
		}{created: created, err: err}
	}()
	waitTestSignal(t, hook.second, "concurrent recurring wakeup")
	close(release)
	if err := <-completeResult; err != nil {
		t.Fatalf("complete coordinator: %v", err)
	}
	result := <-wakeupResult
	if result.err != nil || !result.created {
		t.Fatalf("EnsureRecurring after coordinator completion: created=%t err=%v", result.created, result.err)
	}
	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || got == nil || got.Status != model.TaskStatusQueued {
		t.Fatalf("recurring task after wakeup = %#v err=%v, want queued", got, err)
	}
}

func waitTestSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
