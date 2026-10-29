package task_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type cacheEvictionWaitHandler struct {
	taskengine.Handler
	t           *testing.T
	reader      io.Closer
	repos       *repository.Repositories
	contentID   int64
	invocations atomic.Int64
}

func (h *cacheEvictionWaitHandler) Execute(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	return h.run(ctx, execution, h.Handler.Execute)
}

func (h *cacheEvictionWaitHandler) Recover(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	return h.run(ctx, execution, h.Handler.Recover)
}

func (h *cacheEvictionWaitHandler) run(ctx context.Context, execution taskengine.Execution, invoke func(context.Context, taskengine.Execution) taskengine.Result) taskengine.Result {
	var result taskengine.Result
	// The engine cancels this context outside the fake clock bubble.
	_ = ctx.Done()
	count := h.invocations.Add(1)
	if count <= 7 {
		synctest.Test(h.t, func(t *testing.T) {
			result = invoke(ctx, execution)
		})
	} else {
		result = invoke(ctx, execution)
	}
	if execution.RetryCount() != 0 {
		h.t.Errorf("invocation %d retry count = %d, want 0", count, execution.RetryCount())
	}
	if count <= 8 && execution.Mode() != model.TaskResumeModeRecover {
		h.t.Errorf("invocation %d mode = %s, want recover", count, execution.Mode())
	}
	if count <= 7 {
		entry, err := h.repos.CacheEvictions.GetCacheEntry(ctx, h.contentID)
		if err != nil || entry == nil || entry.CacheActiveTaskID == nil || *entry.CacheActiveTaskID != execution.ID() {
			h.t.Errorf("cache owner while waiting = %#v, err=%v", entry, err)
		}
	}
	if count == 7 {
		if err := h.reader.Close(); err != nil {
			h.t.Errorf("close cache reader: %v", err)
		}
	}
	return result
}

func TestAfterUploadEvictionWaitsWithoutConsumingRetryBudget(t *testing.T) {
	var deleteCalls atomic.Int64
	cacheStore := &testutil.MockCache{
		GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 11}, nil
		},
		DeleteFunc: func(context.Context, string, string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: cacheStore, policy: cache.EvictionPolicyAfterUpload})
	version := seedStoredCacheObject(t, runtime, 11, time.Now().UTC().Add(-time.Hour))
	contentID := *version.ContentID
	opened, err := runtime.gate.Open(t.Context(), version.CacheKey(), func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 11}, nil
	})
	if err != nil {
		t.Fatalf("open cache reader: %v", err)
	}
	t.Cleanup(func() {
		if err := opened.Body.Close(); err != nil {
			t.Errorf("close cache reader: %v", err)
		}
	})
	generation, err := runtime.repos.CacheEvictions.NextEvictionGeneration(t.Context(), contentID)
	if err != nil {
		t.Fatalf("next eviction generation: %v", err)
	}
	row, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type:  model.TaskTypeCacheEvict,
		Input: cacheeviction.EvictInput{ContentID: contentID, Generation: generation},
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.CacheEvictions.BindEvictionTask(ctx, contentID, generation, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue eviction: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
		t.Fatalf("set recovery mode: %v", err)
	}
	handler, _ := handlerExecutionRegistry(t, runtime).Handler(model.TaskTypeCacheEvict)
	wrapped := &cacheEvictionWaitHandler{Handler: handler, t: t, reader: opened.Body, repos: runtime.repos, contentID: contentID}
	registry := taskengine.NewRegistry()
	if err := registry.Register(wrapped); err != nil {
		t.Fatalf("register eviction: %v", err)
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
		t.Fatalf("eviction = status %s, retries %d, want completed without retries", stored.Status, stored.RetryCount)
	}
	if wrapped.invocations.Load() != 9 || deleteCalls.Load() != 1 {
		t.Fatalf("invocations = %d, deletions = %d, want 9 and 1", wrapped.invocations.Load(), deleteCalls.Load())
	}
	entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), contentID)
	if err != nil || entry == nil || entry.CacheActiveTaskID != nil || entry.InCache {
		t.Fatalf("cache after eviction = %#v, err=%v", entry, err)
	}
}
