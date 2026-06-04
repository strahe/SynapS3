package backend_test

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/chain"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/auth"
)

// testBackend holds all components needed to test the SynapseBackend.
type testBackend struct {
	backend *backend.SynapseBackend
	repos   *repository.Repositories
	cache   cache.Cache
	storage *testutil.MockStorageClient
	db      *bun.DB
}

// newTestBackend constructs a SynapseBackend backed by in-memory SQLite
// and a real filesystem cache rooted at t.TempDir().
func newTestBackend(t *testing.T) *testBackend {
	return newTestBackendWithOptions(t)
}

func newTestBackendWithOptions(t *testing.T, opts ...backend.Option) *testBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	fsCache := newTestCache(t, 1<<30) // 1 GB
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}
	logger := slog.Default()

	b := backend.New(repos, fsCache, sm, sc, logger, opts...)
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
		storage: sc,
		db:      db,
	}
}

// newTestBackendWithMockCache constructs a SynapseBackend using a mock cache
// for fault injection tests.
func newTestBackendWithMockCache(t *testing.T, mc *testutil.MockCache) *testBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}
	logger := slog.Default()

	b := backend.New(repos, mc, sm, sc, logger)
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   mc,
		storage: sc,
		db:      db,
	}
}

// newTestBackendWithSDK constructs a SynapseBackend with a custom SDK client.
func newTestBackendWithSDK(t *testing.T, sc synapse.StorageClient) *testBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	fsCache := newTestCache(t, 1<<30)
	sm := state.NewObjectStateMachine()
	logger := slog.Default()

	b := backend.New(repos, fsCache, sm, sc, logger)
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
		db:      db,
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

func validTestObjectBody(seed string) string {
	if len(seed) >= chain.MinUploadSize {
		return seed
	}
	return seed + strings.Repeat(".", chain.MinUploadSize-len(seed))
}

func testSHA256Hex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func seedS3Account(t *testing.T, tb *testBackend, accessKey string) {
	t.Helper()
	if err := tb.repos.S3Accounts.Create(t.Context(), &model.S3Account{
		AccessKey: accessKey,
		SecretKey: "secret-" + accessKey,
		Role:      auth.RoleUserPlus,
	}); err != nil {
		t.Fatalf("S3Accounts.Create(%s): %v", accessKey, err)
	}
}
