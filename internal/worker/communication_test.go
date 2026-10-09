package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

type testMessage string

func (m testMessage) MessageType() string { return string(m) }

type otherTestMessage struct{ kind string }

func (m *otherTestMessage) MessageType() string { return m.kind }

func communicationHarness(t *testing.T, db *bun.DB, types []model.TaskType, configure func(*Registry, *Service)) taskHarness {
	t.Helper()
	repos := repository.NewRepositories(db)
	registry := NewRegistry()
	service, err := NewService(registry, repos)
	if err != nil {
		t.Fatal(err)
	}
	for _, taskType := range types {
		definition := testDefinition(nil, true)
		definition.Type = taskType
		if err := registry.Register(NewFuncHandler(definition, nil, nil)); err != nil {
			t.Fatal(err)
		}
	}
	configure(registry, service)
	engine, err := NewEngine(EngineConfig{
		Concurrency: 1, PollInterval: time.Millisecond, LeaseDuration: time.Minute,
		ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
	}, repos, registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	engine.retryDelay = func(int) time.Duration { return 0 }
	return taskHarness{db: db, repos: repos, registry: registry, service: service, engine: engine}
}

func TestRegistryRequiresClosedCommunicationDeclarations(t *testing.T) {
	noop := func(context.Context, *repository.Repositories, Message) error { return nil }
	for _, test := range []struct {
		name string
		set  func(*Registry) error
		want string
	}{
		{"empty graph", func(*Registry) error { return nil }, ""},
		{"missing handover owner", func(r *Registry) error {
			_, err := r.Messenger("source", []string{"copy"}, nil)
			return err
		}, "no matching receiver"},
		{"notice without subscriber", func(r *Registry) error {
			_, err := r.Messenger("source", nil, []string{"ready"})
			return err
		}, "no matching receiver"},
		{"receiver without producer", func(r *Registry) error {
			return r.Subscribe("consumer", "ready", noop)
		}, "no matching producer"},
		{"wrong dispatch mode", func(r *Registry) error {
			if _, err := r.Messenger("source", []string{"ready"}, nil); err != nil {
				return err
			}
			return r.Subscribe("consumer", "ready", noop)
		}, "no matching receiver"},
		{"duplicate owner", func(r *Registry) error {
			if err := r.OwnHandover("first", "copy", noop); err != nil {
				return err
			}
			return r.OwnHandover("second", "copy", noop)
		}, "already has an owner"},
		{"duplicate handler id", func(r *Registry) error {
			if err := r.Subscribe("consumer", "one", noop); err != nil {
				return err
			}
			return r.Subscribe("consumer", "two", noop)
		}, "already registered"},
		{"duplicate producer", func(r *Registry) error {
			if _, err := r.Messenger("source", nil, nil); err != nil {
				return err
			}
			_, err := r.Messenger("source", nil, nil)
			return err
		}, "already declared"},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry()
			err := test.set(registry)
			if err == nil {
				err = registry.freeze()
			}
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSchedulerRequiresRegisteredTypesAtFreeze(t *testing.T) {
	registry := NewRegistry()
	service, err := NewService(registry, repository.NewRepositories(testutil.NewTestFileDB(t)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Scheduler("owner", testTaskType); err != nil {
		t.Fatal(err)
	}
	if err := registry.freeze(); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("freeze error = %v, want unknown type", err)
	}
	if err := registry.Register(NewFuncHandler(testDefinition(nil, true), nil, nil)); err != nil {
		t.Fatal(err)
	}
	if err := registry.freeze(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Scheduler("late", testTaskType); !errors.Is(err, ErrRegistryFrozen) {
		t.Fatalf("late scheduler error = %v", err)
	}
}

func TestMessengerRequiresTransactionPermissionsAndStableSubscriberOrder(t *testing.T) {
	var messenger *Messenger
	var received []string
	harness := communicationHarness(t, testutil.NewTestFileDB(t), nil, func(r *Registry, _ *Service) {
		declarations := []string{"ready"}
		var err error
		messenger, err = r.Messenger("source", nil, declarations)
		if err != nil {
			t.Fatal(err)
		}
		declarations[0] = "changed"
		for _, id := range []string{"30.third", "10.first", "20.second"} {
			if err := r.Subscribe(id, "ready", func(_ context.Context, _ *repository.Repositories, message Message) error {
				if _, ok := message.(testMessage); !ok {
					return repository.ErrInvalidInput
				}
				received = append(received, id)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	})
	if err := messenger.Notify(t.Context(), harness.repos, testMessage("ready")); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("root dispatch error = %v", err)
	}
	if err := harness.repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
		for _, message := range []Message{nil, (*otherTestMessage)(nil), testMessage("undeclared"), &otherTestMessage{"ready"}} {
			if err := messenger.Notify(t.Context(), tx, message); !errors.Is(err, repository.ErrInvalidInput) {
				return fmt.Errorf("invalid message %T error = %v", message, err)
			}
		}
		if err := messenger.Handover(t.Context(), tx, testMessage("ready")); !errors.Is(err, repository.ErrInvalidInput) {
			return fmt.Errorf("wrong mode error = %v", err)
		}
		return messenger.Notify(t.Context(), tx, testMessage("ready"))
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"10.first", "20.second", "30.third"}) {
		t.Fatalf("subscriber order = %v", received)
	}
	if err := harness.registry.Subscribe("late", "ready", func(context.Context, *repository.Repositories, Message) error { return nil }); !errors.Is(err, ErrRegistryFrozen) {
		t.Fatalf("late subscriber error = %v", err)
	}
}

func TestMessengerRejectsUnfrozenAndRecursiveDispatch(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestFileDB(t))
	registry := NewRegistry()
	messenger, err := registry.Messenger("source", []string{"recursive"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	if err := registry.OwnHandover("consumer", "recursive", func(ctx context.Context, tx *repository.Repositories, message Message) error {
		calls++
		return messenger.Handover(ctx, tx, message)
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
		return messenger.Handover(t.Context(), tx, testMessage("recursive"))
	}); err == nil || calls != 0 {
		t.Fatalf("unfrozen dispatch error = %v, calls = %d", err, calls)
	}
	if err := registry.freeze(); err != nil {
		t.Fatal(err)
	}
	if err := repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
		return messenger.Handover(t.Context(), tx, testMessage("recursive"))
	}); err == nil || !strings.Contains(err.Error(), "maximum dispatch depth") || calls != 8 {
		t.Fatalf("recursive dispatch error = %v, calls = %d", err, calls)
	}
}

func TestMessagesShareEngineSettlementTransaction(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			assertMessageSettlementRollback(t, testutil.NewTestFileDB(t), failure)
		})
	}
	testPostgresMessageSettlement(t)
}

func assertMessageSettlementRollback(t *testing.T, db *bun.DB, failure string) {
	t.Helper()
	var source, target *Scheduler
	var messenger *Messenger
	blocked := true
	var finalSubscriberCalls int
	sentinel := errors.New("receiver refused")
	harness := communicationHarness(t, db, []model.TaskType{testTaskType, "message_target"}, func(r *Registry, service *Service) {
		var err error
		source, err = service.Scheduler("source", testTaskType)
		if err != nil {
			t.Fatal(err)
		}
		target, err = service.Scheduler("target", "message_target")
		if err != nil {
			t.Fatal(err)
		}
		messenger, err = r.Messenger("source", nil, []string{"ready"})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Subscribe("10.target", "ready", func(ctx context.Context, tx *repository.Repositories, _ Message) error {
			_, _, err := target.EnqueueInTransaction(ctx, tx, EnqueueRequest{Type: "message_target", IdempotencyKey: "target", Input: testInput{"target"}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := r.Subscribe("20.failure", "ready", func(context.Context, *repository.Repositories, Message) error {
			if !blocked {
				return nil
			}
			if failure == "panic" {
				panic("receiver refused")
			}
			return sentinel
		}); err != nil {
			t.Fatal(err)
		}
		if err := r.Subscribe("30.final", "ready", func(context.Context, *repository.Repositories, Message) error {
			finalSubscriberCalls++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	enqueueTestTask(t, harness, "source", "source")
	claimed := claimTestTask(t, harness)
	harness.engine.settlementRetryDelays = []time.Duration{0}
	result := Complete("completed", func(ctx context.Context, tx *repository.Repositories) error {
		if _, _, err := source.EnqueueInTransaction(ctx, tx, EnqueueRequest{Type: testTaskType, IdempotencyKey: "source-effect", Input: testInput{"source-effect"}}); err != nil {
			return err
		}
		return messenger.Notify(ctx, tx, testMessage("ready"))
	})
	err := harness.engine.commitResult(t.Context(), claimed, result)
	if err == nil || failure == "error" && !errors.Is(err, sentinel) {
		t.Fatalf("failed settlement error = %v", err)
	}
	if finalSubscriberCalls != 0 {
		t.Fatalf("subscriber ran after dispatch failure %d times", finalSubscriberCalls)
	}
	for _, identity := range []struct {
		taskType model.TaskType
		key      string
	}{{testTaskType, "source-effect"}, {"message_target", "target"}} {
		if row, err := harness.repos.Tasks.GetByIdentity(t.Context(), identity.taskType, identity.key); err != nil || row != nil {
			t.Fatalf("rolled-back %s task = %#v, error = %v", identity.key, row, err)
		}
	}
	stored, err := harness.service.Get(t.Context(), claimed.ID)
	if err != nil || stored.Status != model.TaskStatusRunning {
		t.Fatalf("source status after rollback = %#v, error = %v", stored, err)
	}
	blocked = false
	if err := harness.engine.commitResult(t.Context(), claimed, result); err != nil {
		t.Fatal(err)
	}
	if finalSubscriberCalls != 1 {
		t.Fatalf("final subscriber calls after replay = %d", finalSubscriberCalls)
	}
	stored, err = harness.service.Get(t.Context(), claimed.ID)
	if err != nil || stored.Status != model.TaskStatusCompleted {
		t.Fatalf("source status after replay = %#v, error = %v", stored, err)
	}
	if row, err := harness.repos.Tasks.GetByIdentity(t.Context(), "message_target", "target"); err != nil || row == nil {
		t.Fatalf("target after replay = %#v, error = %v", row, err)
	}
}

func TestSchedulerWakePreservesTypeAndWaitReasonBoundaries(t *testing.T) {
	assertSchedulerWakeBoundaries(t, testutil.NewTestFileDB(t))
	testPostgresSchedulerWake(t)
}

func assertSchedulerWakeBoundaries(t *testing.T, db *bun.DB) {
	t.Helper()
	var scheduler *Scheduler
	types := []model.TaskType{model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull, model.TaskTypeWalletOperation}
	harness := communicationHarness(t, db, types, func(_ *Registry, service *Service) {
		var err error
		scheduler, err = service.Scheduler("transfer", types[:3]...)
		if err != nil {
			t.Fatal(err)
		}
	})
	rows := []struct {
		taskType model.TaskType
		wait     string
		wake     bool
		id       int64
	}{
		{model.TaskTypeStoragePull, storagepull.WaitQueueFull, false, 0},
		{model.TaskTypeStoragePull, "source", true, 0},
		{model.TaskTypeStorageStore, storagepull.WaitQueueFull, true, 0},
		{model.TaskTypeStorageTransferPlan, "", true, 0},
		{model.TaskTypeWalletOperation, "", false, 0},
	}
	var ids []int64
	for index := range rows {
		row := &rows[index]
		taskRow, _, err := harness.service.Enqueue(t.Context(), EnqueueRequest{Type: row.taskType, IdempotencyKey: fmt.Sprintf("wake-%d", index), Input: testInput{"wake"}})
		if err != nil {
			t.Fatal(err)
		}
		claimed := claimTestTask(t, harness)
		if claimed.ID != taskRow.ID {
			t.Fatalf("claimed %d, want %d", claimed.ID, taskRow.ID)
		}
		transition := repository.TaskTransition{Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now().Add(time.Hour)}
		if row.wait != "" {
			transition.WaitReason = &row.wait
		}
		if err := harness.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, transition); err != nil {
			t.Fatal(err)
		}
		row.id = taskRow.ID
		ids = append(ids, row.id)
	}
	filter := WakePendingFilter{Types: types[:2]}
	if _, err := scheduler.WakeInTransaction(t.Context(), harness.repos, ids, filter); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("root wake error = %v", err)
	}
	if _, _, err := scheduler.EnqueueInTransaction(t.Context(), harness.repos, EnqueueRequest{Type: types[0]}); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("root enqueue error = %v", err)
	}
	if err := harness.repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
		if _, _, err := scheduler.EnqueueInTransaction(t.Context(), tx, EnqueueRequest{Type: model.TaskTypeWalletOperation}); !errors.Is(err, repository.ErrInvalidInput) {
			return fmt.Errorf("unauthorized enqueue error = %v", err)
		}
		for _, rejected := range []WakePendingFilter{{}, {Types: types[3:]}} {
			if _, err := scheduler.WakeInTransaction(t.Context(), tx, ids, rejected); !errors.Is(err, repository.ErrInvalidInput) {
				return fmt.Errorf("unauthorized wake error = %v", err)
			}
		}
		if count, err := scheduler.WakeInTransaction(t.Context(), tx, ids, filter); err != nil || count != 2 {
			return fmt.Errorf("plan/store wake count=%d, error=%v", count, err)
		}
		count, err := scheduler.WakeInTransaction(t.Context(), tx, ids, WakePendingFilter{Types: []model.TaskType{model.TaskTypeStoragePull}, SkipWaitReasons: []string{storagepull.WaitQueueFull}})
		if err != nil || count != 1 {
			return fmt.Errorf("pull wake count=%d, error=%v", count, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		stored, err := harness.service.Get(t.Context(), row.id)
		if err != nil || stored == nil || stored.AvailableAt.After(time.Now()) == row.wake {
			t.Fatalf("task %d type=%s wait=%q wake=%v: %#v error=%v", row.id, row.taskType, row.wait, row.wake, stored, err)
		}
	}
}
