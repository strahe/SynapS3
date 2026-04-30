package worker_test

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

// testWorkerEnv holds all components needed to test workers.
type testWorkerEnv struct {
	repos   *repository.Repositories
	cache   cache.Cache
	sm      *state.Machine
	storage *testutil.MockStorageClient
	db      *bun.DB
}

// newTestWorkerEnv constructs a test environment with in-memory SQLite
// and a real filesystem cache.
func newTestWorkerEnv(t *testing.T) *testWorkerEnv {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	fsCache := newWorkerTestCache(t, 1<<30)
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}

	return &testWorkerEnv{
		repos:   repos,
		cache:   fsCache,
		sm:      sm,
		storage: sc,
		db:      db,
	}
}

// newTestWorkerEnvWithMockCache constructs a test environment with mock cache.
func newTestWorkerEnvWithMockCache(t *testing.T, mc *testutil.MockCache) *testWorkerEnv {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}

	return &testWorkerEnv{
		repos:   repos,
		cache:   mc,
		sm:      sm,
		storage: sc,
		db:      db,
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

func acceptWorkerVersionUpload(t *testing.T, env *testWorkerEnv, versionID string, pieceCID string, retrievalURL string) *model.StorageUpload {
	t.Helper()
	ctx := context.Background()
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for upload accept: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	providerID := "101"
	dataSetID := "dataset-" + versionID
	pieceID := "1"
	if err := env.repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        &pieceCID,
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{{
			ProviderID:   &providerID,
			DataSetID:    &dataSetID,
			PieceID:      &pieceID,
			Role:         "primary",
			RetrievalURL: &retrievalURL,
		}},
	}); err != nil {
		t.Fatalf("record upload result: %v", err)
	}
	if _, err := env.repos.Uploads.AcceptCompleteUploadForContent(ctx, repository.AcceptCompleteUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("accept upload result: %v", err)
	}
	return upload
}
