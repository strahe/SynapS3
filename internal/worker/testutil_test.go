package worker_test

import (
	"context"
	"strconv"
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

func taskPayloadInt64ForTest(payload map[string]interface{}, key string) int64 {
	raw, ok := payload[key]
	if !ok {
		return 0
	}
	switch v := raw.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

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
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	providerID := onChainID(t, strconv.FormatInt(100+upload.ID, 10))
	dataSetID := onChainID(t, strconv.FormatInt(1000+upload.ID, 10))
	pieceID := onChainIDPtr(t, strconv.FormatInt(2000+upload.ID, 10))
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          version.BucketID,
		ProviderID:        providerID,
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure dataset binding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: upload.ID, DataSetID: dataSetID}); err != nil {
		t.Fatalf("mark dataset ready: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       providerID,
	}}); err != nil {
		t.Fatalf("create upload copy: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      pieceID,
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("mark copy committed: %v", err)
	}
	if _, err := env.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("bind readable upload: %v", err)
	}
	if finalized, _, err := env.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID}); err != nil {
		t.Fatalf("finalize upload: %v", err)
	} else if !finalized {
		t.Fatal("finalize upload = false, want true")
	}
	return upload
}
