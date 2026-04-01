package worker_test

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/testutil"
)

// testWorkerEnv holds all components needed to test workers.
type testWorkerEnv struct {
	repos   *repository.Repositories
	cache   cache.Cache
	sm      *state.Machine
	storage *testutil.MockStorageClient
	proof   *testutil.MockProofSetClient
}

// newTestWorkerEnv constructs a test environment with in-memory SQLite
// and a real filesystem cache.
func newTestWorkerEnv(t *testing.T) *testWorkerEnv {
	t.Helper()
	repos := testutil.NewTestRepos(t)
	fsCache := newWorkerTestCache(t, 1<<30)
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}
	pc := &testutil.MockProofSetClient{}

	return &testWorkerEnv{
		repos:   repos,
		cache:   fsCache,
		sm:      sm,
		storage: sc,
		proof:   pc,
	}
}

// newTestWorkerEnvWithMockCache constructs a test environment with mock cache.
func newTestWorkerEnvWithMockCache(t *testing.T, mc *testutil.MockCache) *testWorkerEnv {
	t.Helper()
	repos := testutil.NewTestRepos(t)
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}
	pc := &testutil.MockProofSetClient{}

	return &testWorkerEnv{
		repos:   repos,
		cache:   mc,
		sm:      sm,
		storage: sc,
		proof:   pc,
	}
}

// stubWorker is a minimal Worker implementation for manager-level tests.
type stubWorker struct {
	name      string
	isHealthy bool
}

func (s *stubWorker) Name() string                { return s.name }
func (s *stubWorker) Run(_ context.Context) error { return nil }
func (s *stubWorker) Healthy() bool               { return s.isHealthy }

func newWorkerTestCache(t *testing.T, maxBytes int64) cache.Cache {
	t.Helper()
	dir := t.TempDir()
	c, err := cache.NewFilesystem(dir, maxBytes)
	if err != nil {
		t.Fatalf("creating test cache: %v", err)
	}
	return c
}
