package backend_test

import (
	"log/slog"
	"testing"

	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
)

// testBackend holds all components needed to test the SynapseBackend.
type testBackend struct {
	backend *backend.SynapseBackend
	repos   *repository.Repositories
	cache   cache.Cache
	storage *testutil.MockStorageClient
	proof   *testutil.MockProofSetClient
}

// newTestBackend constructs a SynapseBackend backed by in-memory SQLite
// and a real filesystem cache rooted at t.TempDir().
func newTestBackend(t *testing.T) *testBackend {
	t.Helper()
	repos := testutil.NewTestRepos(t)
	fsCache := newTestCache(t, 1<<30) // 1 GB
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}
	pc := &testutil.MockProofSetClient{}
	logger := slog.Default()

	b := backend.New(repos, fsCache, sm, sc, pc, logger)
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
		storage: sc,
		proof:   pc,
	}
}

// newTestBackendWithMockCache constructs a SynapseBackend using a mock cache
// for fault injection tests.
func newTestBackendWithMockCache(t *testing.T, mc *testutil.MockCache) *testBackend {
	t.Helper()
	repos := testutil.NewTestRepos(t)
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}
	pc := &testutil.MockProofSetClient{}
	logger := slog.Default()

	b := backend.New(repos, mc, sm, sc, pc, logger)
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   mc,
		storage: sc,
		proof:   pc,
	}
}

// newTestBackendWithSDK constructs a SynapseBackend with custom SDK clients.
func newTestBackendWithSDK(t *testing.T, sc synapse.StorageClient, pc synapse.ProofSetClient) *testBackend {
	t.Helper()
	repos := testutil.NewTestRepos(t)
	fsCache := newTestCache(t, 1<<30)
	sm := state.NewObjectStateMachine()
	logger := slog.Default()

	b := backend.New(repos, fsCache, sm, sc, pc, logger)
	// Note: sc/pc are passed to backend.New but may not be MockStorageClient/MockProofSetClient,
	// so we don't store them in the typed mock fields.
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
	}
}

// newTestCache creates a real filesystem cache with the given max size.
func newTestCache(t *testing.T, maxBytes int64) cache.Cache {
	t.Helper()
	dir := t.TempDir()
	c, err := cache.NewFilesystem(dir, maxBytes)
	if err != nil {
		t.Fatalf("creating test cache: %v", err)
	}
	return c
}
