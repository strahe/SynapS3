package task_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type cleanupGateWaitHandler struct {
	taskengine.Handler
	t           *testing.T
	reader      io.Closer
	repos       *repository.Repositories
	contentID   int64
	invocations atomic.Int64
}

func (h *cleanupGateWaitHandler) Execute(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	return h.run(ctx, execution, h.Handler.Execute)
}

func (h *cleanupGateWaitHandler) Recover(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	return h.run(ctx, execution, h.Handler.Recover)
}

func (h *cleanupGateWaitHandler) run(ctx context.Context, execution taskengine.Execution, invoke func(context.Context, taskengine.Execution) taskengine.Result) taskengine.Result {
	// The engine cancels this context outside the fake clock bubble.
	_ = ctx.Done()
	count := h.invocations.Add(1)
	var result taskengine.Result
	if count <= 11 {
		synctest.Test(h.t, func(t *testing.T) {
			result = invoke(ctx, execution)
		})
	} else {
		result = invoke(ctx, execution)
	}
	if execution.RetryCount() != 0 {
		h.t.Errorf("invocation %d retry count = %d, want 0", count, execution.RetryCount())
	}
	if count > 1 && execution.Mode() != model.TaskResumeModeRecover {
		h.t.Errorf("invocation %d mode = %s, want recover", count, execution.Mode())
	}
	if count <= 11 {
		content, err := h.repos.Contents.GetByID(ctx, h.contentID)
		if err != nil || content == nil || content.CleanupTaskID == nil || *content.CleanupTaskID != execution.ID() {
			h.t.Errorf("cleanup owner while waiting = %#v, err=%v", content, err)
		}
	}
	if count == 11 {
		if err := h.reader.Close(); err != nil {
			h.t.Errorf("close cache reader: %v", err)
		}
	}
	return result
}

func TestStorageCleanupWaitsForReadersWithoutConsumingRetryBudget(t *testing.T) {
	var deleteCalls atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: &testutil.MockCache{DeleteFunc: func(context.Context, string, string) error {
			deleteCalls.Add(1)
			return nil
		}},
	})
	content, row := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusRemoved)
	opened, err := runtime.gate.Open(t.Context(), model.ContentCacheKey(content.ID), func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 1}, nil
	})
	if err != nil {
		t.Fatalf("open cache reader: %v", err)
	}
	t.Cleanup(func() {
		if err := opened.Body.Close(); err != nil {
			t.Errorf("close cache reader: %v", err)
		}
	})
	handler, _ := handlerExecutionRegistry(t, runtime).Handler(model.TaskTypeStorageCleanup)
	wrapped := &cleanupGateWaitHandler{Handler: handler, t: t, reader: opened.Body, repos: runtime.repos, contentID: content.ID}
	registry := taskengine.NewRegistry()
	if err := registry.Register(wrapped); err != nil {
		t.Fatalf("register cleanup: %v", err)
	}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
	}, runtime.repos, registry, slog.Default())
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)
	stored := waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
		return row.Status == model.TaskStatusCompleted || row.Status == model.TaskStatusFailed
	})
	if stored.Status != model.TaskStatusCompleted || stored.RetryCount != 0 {
		t.Fatalf("cleanup = status %s, retries %d, want completed without retries", stored.Status, stored.RetryCount)
	}
	if wrapped.invocations.Load() != 12 || deleteCalls.Load() != 1 {
		t.Fatalf("invocations = %d, deletions = %d, want 12 and 1", wrapped.invocations.Load(), deleteCalls.Load())
	}
	if remaining, err := runtime.repos.Contents.GetByID(t.Context(), content.ID); err != nil || remaining != nil {
		t.Fatalf("content after cleanup = %#v, err=%v", remaining, err)
	}
}
