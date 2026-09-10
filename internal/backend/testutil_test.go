package backend_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/chain"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
)

// testBackend holds all components needed to test the SynapseBackend.
type testBackend struct {
	backend *backend.SynapseBackend
	repos   *repository.Repositories
	cache   cache.Cache
	gate    *cacheaccess.Gate
	storage *testutil.MockStorageClient
	db      *bun.DB
	tasks   *taskengine.Service
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
	sc := &testutil.MockStorageClient{}
	logger := slog.Default()
	cacheGate, accessTracker := newBackendCacheAccess(repos)
	taskService := newBackendTaskService(t, repos)
	opts = append(opts, backend.WithTaskService(taskService))

	b := backend.New(
		repos,
		fsCache,
		sc,
		cacheGate,
		accessTracker,
		logger,
		opts...,
	)
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
		gate:    cacheGate,
		storage: sc,
		db:      db,
		tasks:   taskService,
	}
}

// newTestBackendWithMockCache constructs a SynapseBackend using a mock cache
// for fault injection tests.
func newTestBackendWithMockCache(t *testing.T, mc *testutil.MockCache) *testBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	sc := &testutil.MockStorageClient{}
	logger := slog.Default()
	cacheGate, accessTracker := newBackendCacheAccess(repos)

	taskService := newBackendTaskService(t, repos)
	b := backend.New(repos, mc, sc, cacheGate, accessTracker, logger, backend.WithTaskService(taskService))
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   mc,
		gate:    cacheGate,
		storage: sc,
		db:      db,
		tasks:   taskService,
	}
}

func newTestBackendWithCache(t *testing.T, c cache.Cache) *testBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	sc := &testutil.MockStorageClient{}
	logger := slog.Default()
	cacheGate, accessTracker := newBackendCacheAccess(repos)

	taskService := newBackendTaskService(t, repos)
	b := backend.New(repos, c, sc, cacheGate, accessTracker, logger, backend.WithTaskService(taskService))
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   c,
		gate:    cacheGate,
		storage: sc,
		db:      db,
		tasks:   taskService,
	}
}

// newTestBackendWithSDK constructs a SynapseBackend with a custom SDK client.
func newTestBackendWithSDK(t *testing.T, sc synapse.StorageClient) *testBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	fsCache := newTestCache(t, 1<<30)
	logger := slog.Default()
	cacheGate, accessTracker := newBackendCacheAccess(repos)

	taskService := newBackendTaskService(t, repos)
	b := backend.New(repos, fsCache, sc, cacheGate, accessTracker, logger, backend.WithTaskService(taskService))
	return &testBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
		gate:    cacheGate,
		db:      db,
		tasks:   taskService,
	}
}

type backendTestTaskHandler struct {
	definition taskengine.Definition
}

func (h backendTestTaskHandler) Definition() taskengine.Definition { return h.definition }
func (backendTestTaskHandler) Execute(context.Context, taskengine.Execution) taskengine.Result {
	return taskengine.Complete("", nil)
}

func (backendTestTaskHandler) Recover(context.Context, taskengine.Execution) taskengine.Result {
	return taskengine.Complete("", nil)
}

func newBackendTaskService(t *testing.T, repos *repository.Repositories) *taskengine.Service {
	t.Helper()
	registry := taskengine.NewRegistry()
	retryLimit := 5
	for _, taskType := range []model.TaskType{
		model.TaskTypeBucketProvision,
		model.TaskTypeUploadPlan,
		model.TaskTypeCacheEvict,
		model.TaskTypeStorageCleanup,
	} {
		err := registry.Register(backendTestTaskHandler{definition: taskengine.Definition{
			Type: taskType, InputVersion: 1,
			Codec:      taskengine.StrictJSONCodec[map[string]any](nil),
			RetryLimit: &retryLimit, AllowRetry: true,
		}})
		if err != nil {
			t.Fatalf("registering test task type %s: %v", taskType, err)
		}
	}
	service, err := taskengine.NewService(registry, repos, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("creating task service: %v", err)
	}
	return service
}

func newBackendCacheAccess(
	repos *repository.Repositories,
) (*cacheaccess.Gate, *cacheaccess.Tracker) {
	return cacheaccess.NewGate(),
		cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, repos.Objects)
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

const testInvalidArgumentDescription = "Invalid request argument."

func requireInvalidArgumentError(t *testing.T, err error, wantArgumentName string) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want InvalidArgument")
	}
	var invalidArg s3err.InvalidArgumentError
	if !errors.As(err, &invalidArg) {
		t.Fatalf("error = %T %v, want InvalidArgumentError", err, err)
	}
	baseErr := invalidArg.BaseError()
	if baseErr.Code != "InvalidArgument" {
		t.Fatalf("error code = %q, want InvalidArgument", baseErr.Code)
	}
	if invalidArg.StatusCode() != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", invalidArg.StatusCode(), http.StatusBadRequest)
	}
	if invalidArg.ArgumentName != wantArgumentName {
		t.Fatalf("argument name = %q, want %q", invalidArg.ArgumentName, wantArgumentName)
	}
	if invalidArg.Description != testInvalidArgumentDescription {
		t.Fatalf("description = %q, want %q", invalidArg.Description, testInvalidArgumentDescription)
	}
}

func requireAPIErrorCode(t *testing.T, err error, want s3err.APIError) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", want.Code)
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("error = %T %v, want APIError", err, err)
	}
	if apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}
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
