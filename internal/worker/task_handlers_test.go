package worker_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synaps3/internal/walletoperation"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/uptrace/bun"
)

type handlerTestRuntime struct {
	db       *bun.DB
	repos    *repository.Repositories
	cache    cache.Cache
	gate     *cacheaccess.Gate
	tracker  *cacheaccess.Tracker
	storage  *testutil.MockStorageClient
	handlers *worker.TaskHandlers
	registry *taskengine.Registry
	service  *taskengine.Service
	engine   *taskengine.Engine
}

type handlerRuntimeOptions struct {
	cache                  cache.Cache
	events                 worker.EventPublisher
	storage                *testutil.MockStorageClient
	wallet                 synapse.WalletOperator
	receipts               worker.WalletReceiptChecker
	walletBroadcastTimeout time.Duration
	walletReceiptTimeout   time.Duration
	terminator             synapse.ServiceTerminator
	epochs                 synapse.ChainEpochReader
	policy                 cache.EvictionPolicy
	maxBytes               int64
	highPercent            int
	lowPercent             int
	concurrency            int
	maxRetries             *int
	register               func(*worker.TaskHandlers, *taskengine.Registry) error
}

func newHandlerTestRuntime(t *testing.T, options handlerRuntimeOptions) handlerTestRuntime {
	t.Helper()
	db := testutil.NewTestFileDB(t)
	repos := repository.NewRepositories(db)
	cacheStore := options.cache
	if cacheStore == nil {
		cacheStore = &testutil.MockCache{}
	}
	storageClient := options.storage
	if storageClient == nil {
		storageClient = &testutil.MockStorageClient{}
	}
	gate := cacheaccess.NewGate()
	tracker := cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, repos.Objects)
	maxRetries := 5
	if options.maxRetries != nil {
		maxRetries = *options.maxRetries
	}
	handlers, err := worker.NewTaskHandlers(worker.TaskHandlerDependencies{
		Repositories: repos, Events: options.events, Cache: cacheStore, CacheGate: gate, CacheTracker: tracker,
		Storage: storageClient, Wallet: options.wallet, Receipts: options.receipts,
		WalletBroadcastTimeout: options.walletBroadcastTimeout,
		WalletReceiptTimeout:   options.walletReceiptTimeout,
		Terminator:             options.terminator, Epochs: options.epochs,
		EvictionPolicy: options.policy, MaxCacheBytes: options.maxBytes,
		LRUHighPercent: options.highPercent, LRULowPercent: options.lowPercent,
		DefaultCopies: 2, MaxRetries: maxRetries, Logger: slog.Default(),
	})
	if err != nil {
		t.Fatalf("new task handlers: %v", err)
	}
	registry := taskengine.NewRegistry()
	register := options.register
	if register == nil {
		register = func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterCore(registry)
		}
	}
	if err := register(handlers, registry); err != nil {
		t.Fatalf("register handlers: %v", err)
	}
	service, err := taskengine.NewService(registry, repos, time.Hour)
	if err != nil {
		t.Fatalf("new task service: %v", err)
	}
	handlers.SetTaskService(service)
	concurrency := max(options.concurrency, 1)
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: concurrency, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, repos, registry, slog.Default())
	if err != nil {
		t.Fatalf("new task engine: %v", err)
	}
	return handlerTestRuntime{
		db: db, repos: repos, cache: cacheStore, gate: gate, tracker: tracker,
		storage: storageClient, handlers: handlers, registry: registry, service: service, engine: engine,
	}
}

type testWalletOperator struct {
	fund func(context.Context, *big.Int) (string, error)
}

func (o testWalletOperator) FundUSDFC(ctx context.Context, amount *big.Int) (string, error) {
	if o.fund == nil {
		return "", errors.New("unexpected fund request")
	}
	return o.fund(ctx, amount)
}

func (testWalletOperator) WithdrawUSDFC(context.Context, *big.Int) (string, error) {
	return "", errors.New("unexpected withdraw request")
}

func (testWalletOperator) ApproveFWSS(context.Context) (string, error) {
	return "", errors.New("unexpected approval request")
}

type testReceiptChecker struct {
	check func(context.Context, common.Hash) (*ethtypes.Receipt, error)
}

type testServiceTerminator struct {
	calls  atomic.Int64
	result *synapse.TerminationResult
	err    error
}

func (t *testServiceTerminator) TerminateService(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
	t.calls.Add(1)
	return t.result, t.err
}

type testEpochReader struct {
	epoch int64
	err   error
}

func (r testEpochReader) CurrentEpoch(context.Context) (int64, error) {
	return r.epoch, r.err
}

func (c testReceiptChecker) TransactionReceipt(ctx context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
	if c.check == nil {
		return nil, errors.New("unexpected receipt request")
	}
	return c.check(ctx, hash)
}

func runHandlerEngine(t *testing.T, runtime handlerTestRuntime) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return runEngine(t, runtime.engine)
}

func runEngine(t *testing.T, engine *taskengine.Engine) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		_ = engine.Run(ctx)
		close(done)
	}()
	return cancel, done
}

func stopHandlerEngine(t *testing.T, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("task engine did not stop")
	}
}

func waitForTask(t *testing.T, repos *repository.Repositories, id int64, predicate func(*model.Task) bool) *model.Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		row, err := repos.Tasks.GetByID(t.Context(), id)
		if err != nil {
			t.Fatalf("get task %d: %v", id, err)
		}
		if row != nil && predicate(row) {
			return row
		}
		time.Sleep(5 * time.Millisecond)
	}
	row, _ := repos.Tasks.GetByID(t.Context(), id)
	t.Fatalf("task %d did not reach expected state: status=%s resume=%s reason=%s error=%s",
		id, taskField(row, func(r *model.Task) *string { s := string(r.Status); return &s }),
		taskField(row, func(r *model.Task) *string { s := string(r.ResumeMode); return &s }),
		taskField(row, func(r *model.Task) *string { return r.FailureReason }),
		taskField(row, func(r *model.Task) *string { return r.LastError }))
	return nil
}

// taskField renders one optional task field for a failure message.
func taskField(row *model.Task, pick func(*model.Task) *string) string {
	if row == nil {
		return "<no task>"
	}
	if value := pick(row); value != nil {
		return *value
	}
	return "<nil>"
}

func testOnChainID(t *testing.T, value int64) idtypes.OnChainID {
	t.Helper()
	id, err := idtypes.ParseOnChainID("test id", fmt.Sprintf("%d", value))
	if err != nil {
		t.Fatalf("parse on-chain id: %v", err)
	}
	return id
}

func testPieceCID(t *testing.T, seed string) cid.Cid {
	t.Helper()
	hash, err := multihash.Sum([]byte(seed), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("create piece CID: %v", err)
	}
	return cid.NewCidV1(cid.Raw, hash)
}

var storedObjectSequence atomic.Int64

func seedStoredCacheObject(t *testing.T, runtime handlerTestRuntime, size int64, accessedAt time.Time) *model.ObjectVersion {
	t.Helper()
	sequence := storedObjectSequence.Add(1)
	ctx := t.Context()
	bucket := &model.Bucket{Name: fmt.Sprintf("cache-task-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// Content identity is created before the version that points at it.
	upload, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: size,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("checksum-%d", sequence)), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("ensure storage content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "object.bin", Size: size,
		ETag: fmt.Sprintf("etag-%d", sequence), ContentType: "application/octet-stream",
		ContentID: &upload.ID,
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create object version: %v", err)
	}
	providerID := testOnChainID(t, 1000+sequence)
	dataSetID := testOnChainID(t, 2000+sequence)
	clientDataSetID := testOnChainID(t, 3000+sequence)
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: upload.ID,
	})
	if err != nil {
		t.Fatalf("create data set binding: %v", err)
	}
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: binding.ID, ContentID: upload.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
	}); err != nil {
		t.Fatalf("mark data set ready: %v", err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: providerID,
	}}); err != nil {
		t.Fatalf("create storage copy: %v", err)
	}
	copies, err := runtime.repos.Contents.ListCopies(ctx, upload.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("list storage copies = %#v, err=%v", copies, err)
	}
	pieceCID := testPieceCID(t, fmt.Sprintf("piece-%d", sequence))
	pieceID := testOnChainID(t, 4000+sequence)
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: upload.ID, CopyIndex: 0,
		PieceCID: pieceCID.String(), PieceID: &pieceID, RetrievalURL: "https://provider.example/piece/" + pieceCID.String(),
	})
	if _, err := runtime.repos.Contents.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		ContentID: upload.ID, BucketID: bucket.ID,
	}); err != nil {
		t.Fatalf("bind readable upload: %v", err)
	}
	finalized, _, err := runtime.repos.Contents.FinalizeUploadIfTargetCopiesMet(ctx, repository.NewFinalizeUploadInput(upload.ID))
	if err != nil || !finalized {
		t.Fatalf("finalize storage upload = %v, err=%v", finalized, err)
	}
	if err := runtime.repos.Objects.RecordContentCacheAccess(ctx, upload.ID, accessedAt); err != nil {
		t.Fatalf("record cache access: %v", err)
	}
	stored, err := runtime.repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || stored == nil || stored.State != model.ObjectStateStored {
		t.Fatalf("stored object = %#v, err=%v", stored, err)
	}
	return stored
}

func TestLRUDeletionWaitsForOpenReaderAndCancelsAfterNewAccess(t *testing.T) {
	var deleteCalls atomic.Int64
	cacheStore := &testutil.MockCache{
		UsedBytesFunc: func() int64 { return 11 },
		DeleteFunc: func(context.Context, string, string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, policy: cache.EvictionPolicyLRU, maxBytes: 10,
		highPercent: 90, lowPercent: 50, concurrency: 1,
	})
	plannedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	version := seedStoredCacheObject(t, runtime, 11, plannedAt)
	// The gate protects one cache file, and that file belongs to the content.
	cacheKey := version.CacheKey()
	opened, err := runtime.gate.Open(cacheKey, func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 11}, nil
	})
	if err != nil {
		t.Fatalf("open protected cache entry: %v", err)
	}
	planner, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey,
		Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity",
	})
	if err != nil {
		t.Fatalf("enqueue cache capacity task: %v", err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	var eviction *model.Task
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		page, listErr := runtime.repos.Tasks.List(t.Context(), repository.TaskListFilter{Type: model.TaskTypeCacheEvict, Limit: 10})
		if listErr != nil {
			t.Fatalf("list cache tasks: %v", listErr)
		}
		if len(page.Tasks) == 1 && page.Tasks[0].Status == model.TaskStatusRunning {
			eviction = &page.Tasks[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if eviction == nil {
		t.Fatalf("cache eviction did not block behind open reader; planner=%d", planner.ID)
	}
	if err := runtime.tracker.RecordAccess(t.Context(), *version.ContentID, version.CacheAccessedAt); err != nil {
		t.Fatalf("record newer cache access: %v", err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("close protected cache entry: %v", err)
	}
	waitForTask(t, runtime.repos, eviction.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCancelled })
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache delete calls = %d, want 0", deleteCalls.Load())
	}
	stored, err := runtime.repos.Objects.GetVersionByID(t.Context(), version.VersionID)
	if err != nil || stored == nil || !stored.InCache {
		t.Fatalf("recently used version = %#v, err=%v", stored, err)
	}
	entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), *version.ContentID)
	if err != nil || entry == nil || entry.CacheActiveTaskID != nil {
		t.Fatalf("cancelled cache eviction owner = %#v, err=%v", entry, err)
	}
}

type recordedWorkerEvent struct {
	topic   string
	payload map[string]any
}

type recordingWorkerEvents struct {
	events chan recordedWorkerEvent
}

func (p *recordingWorkerEvents) Publish(topic string, payload map[string]any) {
	select {
	case p.events <- recordedWorkerEvent{topic: topic, payload: payload}:
	default:
	}
}

// TestTerminalStoreFailureSettlesCopyAndContent checks that a store failure
// settles everything the transfer owns: the copy is failed and unbound, ingress
// progress stays on the copy that produced it, and the version's derived
// position follows the copies to failed without a stored column.
func TestTerminalStoreFailureSettlesCopyAndContent(t *testing.T) {
	events := &recordingWorkerEvents{events: make(chan recordedWorkerEvent, 8)}
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader("stored bytes")), &cache.ObjectInfo{Size: 12}, nil
	}}
	target := &testutil.MockStorageTarget{
		StoreFunc: func(_ context.Context, _ io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
			if options == nil || options.OnProgress == nil {
				return nil, errors.New("store progress callback is missing")
			}
			options.OnProgress(6)
			deadline := time.After(time.Second)
			for {
				select {
				case event := <-events.events:
					progress, ok := event.payload["progress"].(map[string]any)
					if event.topic == "upload_progress_updated" && ok && progress["uploaded_bytes"] == int64(6) {
						return &storage.StoreResult{}, nil
					}
				case <-deadline:
					return nil, errors.New("store progress was not published")
				}
			}
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, events: events, storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	ctx := t.Context()
	sequence := storedObjectSequence.Add(1)
	bucket := &model.Bucket{Name: fmt.Sprintf("terminal-copy-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create terminal copy bucket: %v", err)
	}
	content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 12,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("terminal-%d", sequence)), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("ensure terminal content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "terminal.bin", ContentID: &content.ID, Size: 12,
		ETag: "etag", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create terminal copy version: %v", err)
	}
	providerID := testOnChainID(t, 13000+sequence)
	dataSetID := testOnChainID(t, 14000+sequence)
	clientDataSetID := testOnChainID(t, 15000+sequence)
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: content.ID,
	})
	if err != nil {
		t.Fatalf("create terminal copy binding: %v", err)
	}
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: binding.ID, ContentID: content.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
	}); err != nil {
		t.Fatalf("mark terminal copy binding ready: %v", err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: providerID,
	}}); err != nil {
		t.Fatalf("create terminal copy: %v", err)
	}
	copies, err := runtime.repos.Contents.ListCopies(ctx, content.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("terminal copies = %#v, err=%v", copies, err)
	}
	target.ProviderIDValue = providerID.SDK()
	targetDataSetID := dataSetID.SDK()
	target.DataSetIDValue = &targetDataSetID
	target.ClientDataSetIDValue = clientDataSetID.SDK()
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, &copies[0], model.TaskTypeStorageStore)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failedTask := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if runtime.service.Retryable(failedTask) {
		t.Fatal("terminal copy task is unexpectedly retryable")
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(ctx, copies[0].ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusFailed || copyRow.ActiveTaskID != nil {
		t.Fatalf("terminal copy = %#v, err=%v", copyRow, err)
	}
	// Ingress progress belongs to the transfer, so it survives on the copy.
	if copyRow.IngressStoreAttempt != 1 || copyRow.IngressBytesTransferred != 6 || copyRow.ProgressUpdatedAt == nil {
		t.Fatalf("terminal copy progress = attempt:%d bytes:%d at:%v", copyRow.IngressStoreAttempt, copyRow.IngressBytesTransferred, copyRow.ProgressUpdatedAt)
	}
	storedVersion, err := runtime.repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || storedVersion.State != model.ObjectStateFailed {
		t.Fatalf("terminal version = %#v, err=%v", storedVersion, err)
	}
}

type testCleanupContext struct {
	pieceStatus func(context.Context, cid.Cid) (*storage.PieceStatus, error)
	deletePiece func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error)
}

func (c testCleanupContext) DeletePieceByID(ctx context.Context, pieceID sdktypes.BigInt) (*sdktypes.WriteResult, error) {
	return c.deletePiece(ctx, pieceID)
}

func (c testCleanupContext) PieceStatus(ctx context.Context, pieceCID cid.Cid) (*storage.PieceStatus, error) {
	return c.pieceStatus(ctx, pieceCID)
}

// TestStorageCleanupContinuesPastUnsupportedCopy checks that remote cleanup
// skips a copy the provider cannot delete and still schedules the rest.
func TestStorageCleanupContinuesPastUnsupportedCopy(t *testing.T) {
	var deleteCalls atomic.Int64
	cleanupContext := testCleanupContext{
		pieceStatus: func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
			return &storage.PieceStatus{Exists: true}, nil
		},
		deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
			deleteCalls.Add(1)
			return &sdktypes.WriteResult{Hash: common.HexToHash("0x1")}, nil
		},
	}
	storageClient := &testutil.MockStorageClient{
		OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return cleanupContext, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: storageClient})
	ctx := t.Context()
	sequence := storedObjectSequence.Add(1)
	bucket := &model.Bucket{Name: fmt.Sprintf("cleanup-task-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create cleanup bucket: %v", err)
	}
	content := &model.StorageContent{
		BucketID: bucket.ID, ContentSize: 1,
		Checksum:        testutil.StorageChecksum(fmt.Sprintf("cleanup-%d", sequence)),
		RequestedCopies: 2, CleanupGeneration: 1,
	}
	if _, err := runtime.db.NewInsert().Model(content).Exec(ctx); err != nil {
		t.Fatalf("create cleanup content: %v", err)
	}
	for copyIndex := range 2 {
		providerID := testOnChainID(t, 9000+sequence*10+int64(copyIndex))
		dataSetID := testOnChainID(t, 10000+sequence*10+int64(copyIndex))
		clientDataSetID := testOnChainID(t, 11000+sequence*10+int64(copyIndex))
		binding, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: copyIndex, CreatedByContentID: content.ID,
		})
		if err != nil {
			t.Fatalf("create cleanup binding %d: %v", copyIndex, err)
		}
		if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, ContentID: content.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
		}); err != nil {
			t.Fatalf("mark cleanup binding %d ready: %v", copyIndex, err)
		}
		status := model.StorageCleanupCopyStatusPending
		if copyIndex == 0 {
			status = model.StorageCleanupCopyStatusUnsupported
		}
		row := &model.StorageCleanupCopy{
			ContentID: content.ID, BucketID: bucket.ID, CopyIndex: copyIndex, ProviderID: providerID,
			StorageDataSetID: binding.ID, DataSetID: &dataSetID, ClientDataSetID: &clientDataSetID,
			PieceID:  testOnChainID(t, 12000+sequence*10+int64(copyIndex)),
			PieceCID: testPieceCID(t, fmt.Sprintf("cleanup-piece-%d-%d", sequence, copyIndex)).String(), Status: status,
		}
		if _, err := runtime.db.NewInsert().Model(row).Exec(ctx); err != nil {
			t.Fatalf("create cleanup copy %d: %v", copyIndex, err)
		}
	}
	taskRow, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageCleanup, IdempotencyKey: storagecleanup.TaskKey(content.ID, 1),
		Input: storagecleanup.Input{ContentID: content.ID, Generation: 1}, SubjectType: "storage_content", SubjectKey: fmt.Sprint(content.ID),
	})
	if err != nil {
		t.Fatalf("enqueue cleanup task: %v", err)
	}
	if err := runtime.repos.StorageCleanup.BindTask(ctx, content.ID, 1, taskRow.ID); err != nil {
		t.Fatalf("bind cleanup task: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if deleteCalls.Load() != 1 {
		t.Fatalf("cleanup delete calls = %d, want 1", deleteCalls.Load())
	}
	copies, err := runtime.repos.StorageCleanup.AuthorizeTask(ctx, content.ID, 1, taskRow.ID)
	if err != nil || len(copies) != 2 || copies[1].Status != model.StorageCleanupCopyStatusDeleteScheduled {
		t.Fatalf("cleanup copies = %#v, err=%v", copies, err)
	}
}

func TestCacheCapacityTaskStaysPendingWhenLRUDisabled(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyNone})
	planner, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey,
		Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity",
	})
	if err != nil || !created {
		t.Fatalf("enqueue cache capacity task = %#v created=%v err=%v", planner, created, err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	stored := waitForTask(t, runtime.repos, planner.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.StatusMessage != nil &&
			*task.StatusMessage == "Automatic cache cleanup is disabled"
	})
	if stored.FinishedAt != nil || stored.RetentionUntil != nil || stored.RetryCount != 0 {
		t.Fatalf("disabled recurring task became terminal: %#v", stored)
	}
	same, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey,
		Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity",
	})
	if err != nil || created || same.ID != planner.ID {
		t.Fatalf("reseed disabled cache capacity task = %#v created=%v err=%v", same, created, err)
	}
}

func TestCacheCapacityTaskEvictsLRUItemsOnlyToLowWatermark(t *testing.T) {
	var used atomic.Int64
	used.Store(33)
	var deletedMu sync.Mutex
	var deleted []string
	cacheStore := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		DeleteFunc: func(_ context.Context, _, key string) error {
			deletedMu.Lock()
			deleted = append(deleted, key)
			deletedMu.Unlock()
			used.Add(-11)
			return nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, policy: cache.EvictionPolicyLRU, maxBytes: 30,
		highPercent: 90, lowPercent: 60, concurrency: 1,
	})
	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Microsecond)
	versions := []*model.ObjectVersion{
		seedStoredCacheObject(t, runtime, 11, base),
		seedStoredCacheObject(t, runtime, 11, base.Add(time.Hour)),
		seedStoredCacheObject(t, runtime, 11, base.Add(2*time.Hour)),
	}
	planner, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey,
		Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity",
	})
	if err != nil || !created {
		t.Fatalf("enqueue cache capacity task = %#v created=%v err=%v", planner, created, err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && used.Load() > 11 {
		time.Sleep(5 * time.Millisecond)
	}
	if used.Load() != 11 {
		t.Fatalf("cache usage = %d, want 11", used.Load())
	}
	deletedMu.Lock()
	gotDeleted := append([]string(nil), deleted...)
	deletedMu.Unlock()
	wantDeleted := []string{versions[0].CacheKey(), versions[1].CacheKey()}
	if len(gotDeleted) != len(wantDeleted) || gotDeleted[0] != wantDeleted[0] || gotDeleted[1] != wantDeleted[1] {
		t.Fatalf("deleted cache keys = %#v, want %#v", gotDeleted, wantDeleted)
	}
	for index, version := range versions {
		stored, err := runtime.repos.Objects.GetVersionByID(t.Context(), version.VersionID)
		if err != nil || stored == nil {
			t.Fatalf("load version %d: %#v err=%v", index, stored, err)
		}
		if index < 2 && stored.InCache {
			t.Fatalf("version %d remained in cache", index)
		}
		if index == 2 && !stored.InCache {
			t.Fatal("most recently used version was evicted")
		}
	}
	page, err := runtime.repos.Tasks.List(t.Context(), repository.TaskListFilter{
		Type: model.TaskTypeCacheEvict, Status: model.TaskStatusCompleted, Limit: 10,
	})
	if err != nil || len(page.Tasks) != 2 {
		t.Fatalf("completed cache tasks = %#v, err=%v", page.Tasks, err)
	}
}

func TestCacheEvictionRecoverObservesBeforeReturningToExecute(t *testing.T) {
	var present atomic.Bool
	present.Store(true)
	var deleteCalls atomic.Int64
	cacheStore := &testutil.MockCache{
		GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
			if !present.Load() {
				return nil, nil, os.ErrNotExist
			}
			return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 11}, nil
		},
		DeleteFunc: func(context.Context, string, string) error {
			deleteCalls.Add(1)
			present.Store(false)
			return nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: cacheStore, policy: cache.EvictionPolicyNone})
	version := seedStoredCacheObject(t, runtime, 11, time.Now().UTC().Add(-time.Hour))
	generation, err := runtime.repos.CacheEvictions.NextEvictionGeneration(t.Context(), *version.ContentID)
	if err != nil {
		t.Fatalf("next eviction generation: %v", err)
	}
	taskRow, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(*version.ContentID, generation),
		Input:       cacheeviction.EvictInput{ContentID: *version.ContentID, Generation: generation},
		SubjectType: "storage_content", SubjectKey: fmt.Sprint(*version.ContentID),
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.CacheEvictions.BindEvictionTask(ctx, *version.ContentID, generation, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue cache eviction: %v", err)
	}
	if _, err := runtime.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", taskRow.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("force recovery mode: %v", err)
	}

	limitedRepos := *runtime.repos
	limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	firstPass, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, &limitedRepos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatalf("new limited task engine: %v", err)
	}
	cancelFirst, doneFirst := runEngine(t, firstPass)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	stopHandlerEngine(t, cancelFirst, doneFirst)
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("cache delete calls during recovery = %d, want 0", got)
	}

	cancelExecute, doneExecute := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancelExecute, doneExecute)
	stored := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("cache delete calls after execute = %d, want 1", got)
	}
	if len(stored.Checkpoint) == 0 {
		t.Fatal("cache eviction completed without an attempted-effect checkpoint")
	}
	entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), *version.ContentID)
	if err != nil || entry == nil {
		t.Fatalf("load cache entry after eviction = %#v, err=%v", entry, err)
	}
	if entry.CacheActiveTaskID != nil || entry.InCache {
		t.Fatalf("cache entry after eviction = %#v, want owner released and in_cache=false", entry)
	}
}

func TestCacheEvictionRecoverConvergesAfterDeletionWasAlreadyRecorded(t *testing.T) {
	var deleteCalls atomic.Int64
	cacheStore := &testutil.MockCache{
		GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		DeleteFunc: func(context.Context, string, string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: cacheStore, policy: cache.EvictionPolicyNone})
	version := seedStoredCacheObject(t, runtime, 11, time.Now().UTC().Add(-time.Hour))
	generation, err := runtime.repos.CacheEvictions.NextEvictionGeneration(t.Context(), *version.ContentID)
	if err != nil {
		t.Fatalf("next eviction generation: %v", err)
	}
	taskRow, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(*version.ContentID, generation),
		Input:       cacheeviction.EvictInput{ContentID: *version.ContentID, Generation: generation},
		SubjectType: "storage_content", SubjectKey: fmt.Sprint(*version.ContentID),
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.CacheEvictions.BindEvictionTask(ctx, *version.ContentID, generation, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue cache eviction: %v", err)
	}
	// Simulate a crash after unlink + RecordDeletion committed but before the
	// engine persisted the task's completed transition.
	if err := runtime.repos.CacheEvictions.RecordDeletion(t.Context(), *version.ContentID, generation, taskRow.ID); err != nil {
		t.Fatalf("record cache deletion before recovery: %v", err)
	}
	if _, err := runtime.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", taskRow.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("force recovery mode: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("cache delete calls during recovery = %d, want 0", got)
	}
	entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), *version.ContentID)
	if err != nil || entry == nil {
		t.Fatalf("load cache entry after recovery = %#v, err=%v", entry, err)
	}
	if entry.CacheActiveTaskID != nil || entry.InCache {
		t.Fatalf("cache entry after recovery = %#v, want owner released and in_cache=false", entry)
	}
}

func TestCacheEvictionRecoverCancelsAfterNewGenerationCompletes(t *testing.T) {
	cacheStore := &testutil.MockCache{
		GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: cacheStore, policy: cache.EvictionPolicyNone})
	version := seedStoredCacheObject(t, runtime, 11, time.Now().UTC().Add(-time.Hour))
	contentID := *version.ContentID

	firstGeneration, err := runtime.repos.CacheEvictions.NextEvictionGeneration(t.Context(), contentID)
	if err != nil {
		t.Fatalf("next first eviction generation: %v", err)
	}
	firstTask, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(contentID, firstGeneration),
		Input:       cacheeviction.EvictInput{ContentID: contentID, Generation: firstGeneration},
		SubjectType: "storage_content", SubjectKey: fmt.Sprint(contentID),
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.CacheEvictions.BindEvictionTask(ctx, contentID, firstGeneration, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue first cache eviction: %v", err)
	}
	if err := runtime.repos.CacheEvictions.RecordDeletion(t.Context(), contentID, firstGeneration, firstTask.ID); err != nil {
		t.Fatalf("record first cache deletion: %v", err)
	}
	if err := runtime.repos.Objects.RecordContentCacheCommit(t.Context(), contentID, time.Now()); err != nil {
		t.Fatalf("restore cache presence: %v", err)
	}

	reservation, err := runtime.repos.CacheEvictions.PrepareEviction(t.Context(), contentID)
	if err != nil {
		t.Fatalf("prepare replacement eviction: %v", err)
	}
	if reservation.ActiveTaskID != nil || reservation.Generation <= firstGeneration {
		t.Fatalf("replacement eviction reservation = %#v", reservation)
	}
	secondTask, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(contentID, reservation.Generation),
		Input:       cacheeviction.EvictInput{ContentID: contentID, Generation: reservation.Generation},
		SubjectType: "storage_content", SubjectKey: fmt.Sprint(contentID),
		AvailableAt: time.Now().Add(time.Hour),
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.CacheEvictions.BindEvictionTask(ctx, contentID, reservation.Generation, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue replacement cache eviction: %v", err)
	}
	if err := runtime.repos.CacheEvictions.RecordDeletion(t.Context(), contentID, reservation.Generation, secondTask.ID); err != nil {
		t.Fatalf("record replacement cache deletion: %v", err)
	}
	if _, err := runtime.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", firstTask.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("force first task recovery mode: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	stored := waitForTask(t, runtime.repos, firstTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCancelled
	})
	if stored.RetryCount != 0 || stored.LastError != nil {
		t.Fatalf("superseded task diagnostics = retry_count=%d last_error=%v", stored.RetryCount, stored.LastError)
	}
	entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), contentID)
	if err != nil || entry == nil {
		t.Fatalf("load cache entry after superseded recovery = %#v, err=%v", entry, err)
	}
	if entry.CacheOperationGeneration != reservation.Generation || entry.CacheActiveTaskID != nil || entry.InCache {
		t.Fatalf("newer cache generation changed by stale recovery: %#v", entry)
	}
}

func TestCacheEvictionPersistentDeleteFailureReleasesOwner(t *testing.T) {
	deleteErr := errors.New("cache filesystem is read-only")
	cacheStore := &testutil.MockCache{
		DeleteFunc: func(context.Context, string, string) error {
			return deleteErr
		},
	}
	noRetries := 0
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, policy: cache.EvictionPolicyNone, maxRetries: &noRetries,
	})
	version := seedStoredCacheObject(t, runtime, 11, time.Now().UTC().Add(-time.Hour))
	contentID := *version.ContentID
	generation, err := runtime.repos.CacheEvictions.NextEvictionGeneration(t.Context(), contentID)
	if err != nil {
		t.Fatalf("next eviction generation: %v", err)
	}
	taskRow, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(contentID, generation),
		Input:       cacheeviction.EvictInput{ContentID: contentID, Generation: generation},
		SubjectType: "storage_content", SubjectKey: fmt.Sprint(contentID),
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.CacheEvictions.BindEvictionTask(ctx, contentID, generation, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue cache eviction: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	stored := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if stored.FailureReason == nil || *stored.FailureReason != "cache_delete_failed" {
		t.Fatalf("failure reason = %v, want cache_delete_failed", stored.FailureReason)
	}
	if stored.LastError == nil || !strings.Contains(*stored.LastError, deleteErr.Error()) {
		t.Fatalf("last error = %v, want %q", stored.LastError, deleteErr)
	}
	entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), contentID)
	if err != nil || entry == nil {
		t.Fatalf("load cache entry after failed eviction = %#v, err=%v", entry, err)
	}
	if entry.CacheActiveTaskID != nil || !entry.InCache {
		t.Fatalf("cache entry after failed eviction = %#v, want released owner and retained cache", entry)
	}
}

type limitedClaimRepository struct {
	repository.TaskRepository
	maximum int64
	claims  atomic.Int64
}

func (r *limitedClaimRepository) ClaimNext(ctx context.Context, lease time.Duration) (*model.Task, error) {
	if r.claims.Load() >= r.maximum {
		return nil, nil
	}
	claimed, err := r.TaskRepository.ClaimNext(ctx, lease)
	if claimed != nil && err == nil {
		r.claims.Add(1)
	}
	return claimed, err
}

type seededCopyPipeline struct {
	upload       *model.StorageContent
	source       *model.StorageCopy
	target       *model.StorageCopy
	targetSet    *model.StorageDataSet
	targetClient sdktypes.BigInt
	pieceCID     cid.Cid
}

func seedCopyPipeline(t *testing.T, runtime handlerTestRuntime, targetStatus model.StorageCopyStatus) seededCopyPipeline {
	t.Helper()
	ctx := t.Context()
	sequence := storedObjectSequence.Add(1)
	bucket := &model.Bucket{Name: fmt.Sprintf("copy-task-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create copy bucket: %v", err)
	}
	// Content identity comes first: a data version cannot exist without it.
	upload, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 11,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("copy-checksum-%d", sequence)), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("start copy upload: %v", err)
	}
	bindings := make([]*model.StorageDataSet, 0, 2)
	clients := make([]idtypes.OnChainID, 0, 2)
	for copyIndex := range 2 {
		providerID := testOnChainID(t, 5000+sequence*10+int64(copyIndex))
		dataSetID := testOnChainID(t, 6000+sequence*10+int64(copyIndex))
		clientID := testOnChainID(t, 7000+sequence*10+int64(copyIndex))
		binding, bindErr := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: copyIndex, CreatedByContentID: upload.ID,
		})
		if bindErr != nil {
			t.Fatalf("create copy binding %d: %v", copyIndex, bindErr)
		}
		if readyErr := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, ContentID: upload.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
		}); readyErr != nil {
			t.Fatalf("mark copy binding %d ready: %v", copyIndex, readyErr)
		}
		binding.DataSetID = &dataSetID
		binding.ClientDataSetID = &clientID
		bindings = append(bindings, binding)
		clients = append(clients, clientID)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: bindings[0].ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: bindings[0].ProviderID},
		{StorageDataSetID: bindings[1].ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: bindings[1].ProviderID},
	}); err != nil {
		t.Fatalf("create copy rows: %v", err)
	}
	copies, err := runtime.repos.Contents.ListCopies(ctx, upload.ID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("list copy rows = %#v, err=%v", copies, err)
	}
	pieceCID := testPieceCID(t, fmt.Sprintf("copy-piece-%d", sequence))
	pieceID := testOnChainID(t, 8000+sequence)
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: upload.ID, CopyIndex: 0, PieceCID: pieceCID.String(),
		PieceID: &pieceID, RetrievalURL: "https://source.example/piece/" + pieceCID.String(),
	})
	if targetStatus == model.StorageCopyStatusPieceReady {
		if err := runtime.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageCopyID: copies[1].ID, ContentID: upload.ID, CopyIndex: 1,
			PieceCID: pieceCID.String(), RetrievalURL: "https://target.example/piece/" + pieceCID.String(), CommitExtraDataHex: "aabb",
		}); err != nil {
			t.Fatalf("mark target piece ready: %v", err)
		}
	}
	copies, err = runtime.repos.Contents.ListCopies(ctx, upload.ID)
	if err != nil {
		t.Fatalf("reload copy rows: %v", err)
	}
	return seededCopyPipeline{
		upload: upload, source: &copies[0], target: &copies[1], targetSet: bindings[1],
		targetClient: clients[1].SDK(), pieceCID: pieceCID,
	}
}

func bindCopyTask(t *testing.T, runtime handlerTestRuntime, copyRow *model.StorageCopy, taskType model.TaskType) *model.Task {
	t.Helper()
	generation, err := runtime.repos.Contents.NextCopyWorkGeneration(t.Context(), copyRow.ID)
	if err != nil {
		t.Fatalf("next copy generation: %v", err)
	}
	input := storagepipeline.CopyGenerationInput{CopyID: copyRow.ID, Generation: generation}
	key := storagepipeline.PullKey(copyRow.ID, generation)
	switch taskType {
	case model.TaskTypeStorageStore:
		key = storagepipeline.StoreKey(copyRow.ID, generation)
	case model.TaskTypeStorageCommit:
		key = storagepipeline.CommitKey(copyRow.ID, generation)
	}
	taskRow, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: taskType, IdempotencyKey: key, Input: input,
		SubjectType: "storage_copy", SubjectKey: fmt.Sprintf("%d", copyRow.ID),
	})
	if err != nil || !created {
		t.Fatalf("enqueue copy task = %#v created=%v err=%v", taskRow, created, err)
	}
	if err := runtime.repos.Contents.BindCopyTask(t.Context(), copyRow.ID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind copy task: %v", err)
	}
	return taskRow
}

func TestCompetingCoordinatorsBindOnlyOneCopyTask(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	generation, err := runtime.repos.Contents.NextCopyWorkGeneration(t.Context(), pipeline.target.ID)
	if err != nil {
		t.Fatalf("next copy generation: %v", err)
	}
	input := storagepipeline.CopyGenerationInput{CopyID: pipeline.target.ID, Generation: generation}
	bind := func(taskType model.TaskType, key string) (*model.Task, error) {
		returnTask, _, enqueueErr := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
			Type: taskType, IdempotencyKey: key, Input: input,
			SubjectType: "storage_copy", SubjectKey: fmt.Sprint(pipeline.target.ID),
		}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
			return repos.Contents.BindCopyTask(ctx, pipeline.target.ID, generation, taskRow.ID)
		})
		return returnTask, enqueueErr
	}

	winner, err := bind(model.TaskTypeStorageTransferPlan, storagepipeline.TransferPlanKey(pipeline.target.ID, generation))
	if err != nil {
		t.Fatalf("bind winning copy task: %v", err)
	}
	if _, err := bind(model.TaskTypeStoragePull, storagepipeline.PullKey(pipeline.target.ID, generation)); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("bind competing copy task error = %v, want conflict", err)
	}

	stored, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || stored == nil || stored.ActiveTaskID == nil || *stored.ActiveTaskID != winner.ID || stored.WorkGeneration != generation {
		t.Fatalf("copy task fence = %#v, winner=%#v, err=%v", stored, winner, err)
	}
	count, err := runtime.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("subject_type = ? AND subject_key = ?", "storage_copy", fmt.Sprint(pipeline.target.ID)).
		Count(t.Context())
	if err != nil {
		t.Fatalf("count copy tasks: %v", err)
	}
	if count != 1 {
		t.Fatalf("persisted copy tasks = %d, want one winning task", count)
	}
}

func TestBucketProvisionCreatesDataSetsBeforeMarkingReady(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	targets := make([]synapse.StorageTarget, 0, 2)
	for i := range 2 {
		providerID := testOnChainID(t, 12000+sequence*10+int64(i)).SDK()
		dataSetID := testOnChainID(t, 13000+sequence*10+int64(i)).SDK()
		clientDataSetID := testOnChainID(t, 14000+sequence*10+int64(i)).SDK()
		targets = append(targets, &testutil.MockStorageTarget{
			ProviderIDValue: providerID, DataSetIDValue: &dataSetID, ClientDataSetIDValue: clientDataSetID,
		})
	}
	storageClient := &testutil.MockStorageClient{
		SelectUploadTargetsFunc: func(_ context.Context, opts storage.SelectUploadContextsOptions) ([]synapse.StorageTarget, error) {
			if opts.Copies != 2 || opts.DataSetMetadata["bucket"] == "" {
				t.Fatalf("selection options = %#v", opts)
			}
			return targets, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	bucket := &model.Bucket{Name: fmt.Sprintf("provision-%d", sequence), Status: model.BucketStatusProvisioning, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	taskRow, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeBucketProvision, IdempotencyKey: bucketlifecycle.ProvisionKey(bucket.ID, bucket.DefaultCopies),
		Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID}, SubjectType: "bucket", SubjectKey: fmt.Sprint(bucket.ID),
	})
	if err != nil || !created {
		t.Fatalf("enqueue bucket provision task = %#v created=%v err=%v", taskRow, created, err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})

	stored, err := runtime.repos.Buckets.GetByID(t.Context(), bucket.ID)
	if err != nil || stored == nil || stored.Status != model.BucketStatusReady {
		t.Fatalf("ready bucket = %#v, err=%v", stored, err)
	}
	bindings, err := runtime.repos.Contents.ListDataSetBindings(t.Context(), bucket.ID)
	if err != nil {
		t.Fatalf("list data set bindings: %v", err)
	}
	if len(bindings) != 2 {
		t.Fatalf("data set bindings = %d, want 2", len(bindings))
	}
	for i := range bindings {
		if !bindings[i].IsCurrent || bindings[i].CopyIndex != i || bindings[i].Status != model.StorageDataSetStatusReady || bindings[i].DataSetID == nil {
			t.Fatalf("binding[%d] = %#v", i, bindings[i])
		}
	}
}

func TestDataSetEnsureSchedulesBoundCopyWorkWhenReady(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	sequence := storedObjectSequence.Add(1)
	bucket := &model.Bucket{Name: fmt.Sprintf("ensure-ready-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create ensure bucket: %v", err)
	}
	// Content identity comes first: a data version cannot exist without it.
	upload, err := runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 11,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("ensure-checksum-%d", sequence)), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("start ensure upload: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "ensure-ready.bin", ContentID: &upload.ID, Size: 11,
		ETag: "etag", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatalf("create ensure version: %v", err)
	}
	providerID := testOnChainID(t, 9000+sequence)
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: upload.ID,
	})
	if err != nil {
		t.Fatalf("create pending data set: %v", err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(t.Context(), upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: providerID,
	}}); err != nil {
		t.Fatalf("create bound copy: %v", err)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyForDataSet(t.Context(), upload.ID, binding.ID)
	if err != nil || copyRow == nil || copyRow.ActiveTaskID != nil {
		t.Fatalf("bound copy = %#v err=%v", copyRow, err)
	}
	ensureTask, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(binding.ID),
		Input: storagepipeline.DataSetInput{DataSetID: binding.ID}, SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(binding.ID),
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue data set ensure: %v", err)
	}
	dataSetID := testOnChainID(t, 10000+sequence)
	clientDataSetID := testOnChainID(t, 11000+sequence)
	if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: binding.ID, ContentID: upload.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
	}); err != nil {
		t.Fatalf("mark data set ready: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	copyRow, err = runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil || copyRow == nil || copyRow.ActiveTaskID == nil {
		t.Fatalf("scheduled copy = %#v err=%v", copyRow, err)
	}
	transferTask, err := runtime.repos.Tasks.GetByID(t.Context(), *copyRow.ActiveTaskID)
	if err != nil || transferTask == nil || transferTask.Type != model.TaskTypeStorageTransferPlan || transferTask.Status != model.TaskStatusPending {
		t.Fatalf("scheduled transfer task = %#v err=%v", transferTask, err)
	}
}

func TestDataSetDiscoveryFailureNeverCreatesDataSet(t *testing.T) {
	var createCalls atomic.Int64
	provider := &testutil.MockStorageTarget{
		CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			createCalls.Add(1)
			return nil, errors.New("unexpected data set creation")
		},
	}
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(_ context.Context, providerID sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			provider.ProviderIDValue = providerID.Copy()
			return provider, nil
		},
		FindMatchingDataSetFunc: func(context.Context, sdktypes.BigInt, map[string]string, bool) (*storage.DataSetRef, error) {
			return nil, &synapse.ProviderUnavailableError{Cause: errors.New("data set discovery unavailable")}
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	sequence := storedObjectSequence.Add(1)
	bucket := &model.Bucket{Name: fmt.Sprintf("ensure-task-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create ensure bucket: %v", err)
	}
	// Content identity comes first: a data version cannot exist without it.
	upload, err := runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 11,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("ensure-task-checksum-%d", sequence)), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("start ensure upload: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "ensure.bin", ContentID: &upload.ID, Size: 11,
		ETag: "etag", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatalf("create ensure version: %v", err)
	}
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: testOnChainID(t, 9000+sequence), CopyIndex: 0, CreatedByContentID: upload.ID,
	})
	if err != nil {
		t.Fatalf("create pending data set: %v", err)
	}
	input := storagepipeline.DataSetInput{DataSetID: binding.ID}
	taskRow, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(binding.ID),
		Input: input, SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(binding.ID),
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue data set ensure: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if createCalls.Load() != 0 {
		t.Fatalf("data set creation calls = %d, want zero while discovery is unavailable", createCalls.Load())
	}
}

func TestTaskGCLeavesStorageEvidenceIntact(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageTransferPlan)
	claimed, err := runtime.repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("claim copy task = %#v, err=%v", claimed, err)
	}
	if err := runtime.repos.Contents.CompleteCopyTask(t.Context(), pipeline.target.ID, pipeline.target.WorkGeneration+1, taskRow.ID); err != nil {
		t.Fatalf("clear copy task fence: %v", err)
	}
	retentionUntil := time.Now().Add(-time.Minute)
	if err := runtime.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover, RetentionUntil: &retentionUntil,
	}); err != nil {
		t.Fatalf("complete retained task: %v", err)
	}
	deleted, err := runtime.repos.Tasks.DeleteRetained(t.Context(), time.Now(), 10)
	if err != nil || deleted != 1 {
		t.Fatalf("delete retained tasks = %d, err=%v", deleted, err)
	}
	if stored, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID); err != nil || stored != nil {
		t.Fatalf("collected task = %#v, err=%v", stored, err)
	}
	if upload, err := runtime.repos.Contents.GetByID(t.Context(), pipeline.upload.ID); err != nil || upload == nil {
		t.Fatalf("storage upload evidence = %#v, err=%v", upload, err)
	}
	if copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID); err != nil || copyRow == nil {
		t.Fatalf("storage copy evidence = %#v, err=%v", copyRow, err)
	}
}

func TestStoreTasksReachAndRespectProviderMutationLimit(t *testing.T) {
	var running, maximum atomic.Int64
	entered := make(chan struct{}, 6)
	release := make(chan struct{})
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader("stored bytes")), &cache.ObjectInfo{Size: 12}, nil
	}}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, policy: cache.EvictionPolicyNone, concurrency: 8,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	clients := make(map[string]sdktypes.BigInt)
	tasks := make([]*model.Task, 0, 6)
	for range 6 {
		pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
		clients[pipeline.targetSet.DataSetID.String()] = pipeline.targetClient.Copy()
		tasks = append(tasks, bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore))
	}
	storageClient.OpenDataSetTargetFunc = func(_ context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		if opts.ProviderID == nil {
			return nil, errors.New("missing provider identity")
		}
		clientID, ok := clients[dataSetID.String()]
		if !ok {
			return nil, errors.New("unknown data set identity")
		}
		targetDataSetID := dataSetID.Copy()
		return &testutil.MockStorageTarget{
			ProviderIDValue: opts.ProviderID.Copy(), DataSetIDValue: &targetDataSetID,
			ClientDataSetIDValue: clientID,
			StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
				current := running.Add(1)
				for {
					observed := maximum.Load()
					if current <= observed || maximum.CompareAndSwap(observed, current) {
						break
					}
				}
				entered <- struct{}{}
				<-release
				running.Add(-1)
				return nil, errors.New("injected ambiguous store result")
			},
		}, nil
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	for range 4 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("store tasks did not reach provider mutation concurrency")
		}
	}
	select {
	case <-entered:
		close(release)
		t.Fatal("store tasks exceeded provider mutation concurrency")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for _, taskRow := range tasks {
		waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
			return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
		})
	}
	if maximum.Load() != 4 {
		t.Fatalf("store provider mutation maximum = %d, want 4", maximum.Load())
	}
}

func TestPullRecoverObservesThenRepeatsIdenticalRequestInExecute(t *testing.T) {
	var pullCalls, statusCalls atomic.Int64
	statusObserved := make(chan struct{})
	type observedPull struct {
		piece string
		extra string
		url   string
	}
	var observedMu sync.Mutex
	var observed []observedPull
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa, 0xbb}, nil },
		PullFunc: func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
			call := pullCalls.Add(1)
			observedMu.Lock()
			observed = append(observed, observedPull{
				piece: request.Pieces[0].String(),
				extra: hex.EncodeToString(request.ExtraData),
				url:   request.From(request.Pieces[0]),
			})
			observedMu.Unlock()
			if call == 1 {
				return nil, errors.New("ambiguous provider disconnect")
			}
			return &storage.PullResult{}, nil
		},
		PieceStatusFunc: func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
			if statusCalls.Add(1) == 1 {
				close(statusObserved)
			}
			return &storage.PieceStatus{Exists: false}, nil
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "pull-recovery.bin",
		ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize, ETag: "pull-recovery",
		ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatalf("create pull recovery version: %v", err)
	}
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	dataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &dataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStoragePull)
	limitedRepos := *runtime.repos
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 3}
	limitedRepos.Tasks = limited
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, &limitedRepos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatalf("new limited task engine: %v", err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)

	recovering := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	var checkpoint struct {
		AttemptID string `json:"attempt_id"`
	}
	if err := json.Unmarshal(recovering.Checkpoint, &checkpoint); err != nil {
		t.Fatalf("decode pull checkpoint: %v", err)
	}
	if checkpoint.AttemptID == "" {
		t.Fatal("pull checkpoint attempt_id is empty")
	}
	if strings.Contains(string(recovering.Checkpoint), `"request_id"`) {
		t.Fatalf("pull checkpoint retains removed request_id: %s", recovering.Checkpoint)
	}
	attempt := new(storagepull.Attempt)
	if err := runtime.db.NewSelect().Model(attempt).Where("attempt_id = ?", checkpoint.AttemptID).Scan(t.Context()); err != nil {
		t.Fatalf("load pull attempt %q: %v", checkpoint.AttemptID, err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("make pull recovery ready: %v", err)
	}
	select {
	case <-statusObserved:
	case <-time.After(3 * time.Second):
		t.Fatal("pull recovery did not inspect target piece")
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("make repeated pull ready: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if pullCalls.Load() != 2 {
		t.Fatalf("pull calls = %d, want 2", pullCalls.Load())
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observed) != 2 || observed[0] != observed[1] {
		t.Fatalf("pull requests = %#v, want two identical requests", observed)
	}
}

func TestPullProviderFailureAtomicallyAbandonsAttemptAndCopy(t *testing.T) {
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa, 0xbb}, nil },
		PullFunc: func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
			return nil, fmt.Errorf("provider pull: %w", pdp.ErrPullFailed)
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	dataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &dataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStoragePull)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "pull_failed" {
		t.Fatalf("pull task failure = %#v, want pull_failed", failed)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusFailed || copyRow.ActiveTaskID != nil {
		t.Fatalf("failed pull copy = %#v, err=%v", copyRow, err)
	}
	var attempts []storagepull.Attempt
	if err := runtime.db.NewSelect().Model(&attempts).Where("content_id = ?", pipeline.target.ContentID).Scan(t.Context()); err != nil {
		t.Fatalf("load pull attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != storagepull.AttemptStatusAbandoned ||
		attempts[0].ResolvedAt == nil || attempts[0].LastError == nil {
		t.Fatalf("pull attempts = %#v, want one resolved abandoned attempt", attempts)
	}
	if err := runtime.repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
		StorageCopyID: pipeline.target.ID, ContentID: pipeline.target.ContentID,
		CopyIndex: pipeline.target.CopyIndex, PullAttemptID: attempts[0].AttemptID,
		LastError: "replayed pull failure",
	}); err != nil {
		t.Fatalf("replay abandoned pull settlement: %v", err)
	}
	if err := runtime.repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
		StorageCopyID: pipeline.target.ID, ContentID: pipeline.target.ContentID,
		CopyIndex: pipeline.target.CopyIndex, PullAttemptID: "different-attempt",
		LastError: "mismatched pull failure",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("mismatched abandoned pull settlement = %v, want ErrConflict", err)
	}
}

func newPullErrorTask(
	t *testing.T,
	pullErr error,
	maxRetries *int,
	pullCalls *atomic.Int64,
) (handlerTestRuntime, seededCopyPipeline, *model.Task) {
	t.Helper()
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			return []byte{0xaa, 0xbb}, nil
		},
		PullFunc: func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
			pullCalls.Add(1)
			return nil, pullErr
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone, maxRetries: maxRetries,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	dataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &dataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	return runtime, pipeline, bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStoragePull)
}

func assertFailedPullSettlement(
	t *testing.T,
	runtime handlerTestRuntime,
	pipeline seededCopyPipeline,
	taskRow *model.Task,
	wantReason string,
) {
	t.Helper()
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != wantReason {
		t.Fatalf("pull task failure = %#v, want %s", failed, wantReason)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageCopyStatusFailed || copyRow.ActiveTaskID != nil {
		t.Fatalf("failed pull copy = %#v, err=%v", copyRow, err)
	}
	var attempts []storagepull.Attempt
	if err := runtime.db.NewSelect().Model(&attempts).Where("content_id = ?", pipeline.target.ContentID).Scan(t.Context()); err != nil {
		t.Fatalf("load pull attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != storagepull.AttemptStatusAbandoned || attempts[0].ResolvedAt == nil {
		t.Fatalf("pull attempts = %#v, want one resolved abandoned attempt", attempts)
	}
}

func TestPullTerminalClientErrorsSettleAttemptAndCopy(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "bad request", err: &pdp.HTTPError{StatusCode: http.StatusBadRequest}},
		{name: "invalid arguments", err: storage.ErrInvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pullCalls atomic.Int64
			runtime, pipeline, taskRow := newPullErrorTask(t, tt.err, nil, &pullCalls)
			assertFailedPullSettlement(t, runtime, pipeline, taskRow, "pull_failed")
			if pullCalls.Load() != 1 {
				t.Fatalf("pull calls = %d, want 1", pullCalls.Load())
			}
		})
	}
}

func TestPullRetryableProviderErrorKeepsCopyOpen(t *testing.T) {
	limit := 0
	var pullCalls atomic.Int64
	runtime, pipeline, taskRow := newPullErrorTask(
		t,
		&pdp.HTTPError{StatusCode: http.StatusServiceUnavailable},
		&limit,
		&pullCalls,
	)
	limitedRepos := *runtime.repos
	limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, &limitedRepos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatalf("new limited task engine: %v", err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)
	pending := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return pullCalls.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if pending.RetryCount != 0 {
		t.Fatalf("retry count = %d, want 0 for retryable provider failure", pending.RetryCount)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow == nil || copyRow.Status == model.StorageCopyStatusFailed || copyRow.ActiveTaskID == nil {
		t.Fatalf("retryable pull copy = %#v, err=%v", copyRow, err)
	}
}

func TestPullUnknownErrorFailsWhenRetryBudgetIsExhausted(t *testing.T) {
	limit := 0
	var pullCalls atomic.Int64
	runtime, pipeline, taskRow := newPullErrorTask(t, errors.New("new sdk pull failure"), &limit, &pullCalls)
	assertFailedPullSettlement(t, runtime, pipeline, taskRow, "pull_request_failed")
	if pullCalls.Load() != 1 {
		t.Fatalf("pull calls = %d, want 1", pullCalls.Load())
	}
}

func TestPullRecoverWithoutCheckpointReturnsToExecute(t *testing.T) {
	var presignCalls, pullCalls, statusCalls atomic.Int64
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			presignCalls.Add(1)
			return []byte{0xaa, 0xbb}, nil
		},
		PullFunc: func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
			pullCalls.Add(1)
			return nil, errors.New("unexpected pull")
		},
		PieceStatusFunc: func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
			statusCalls.Add(1)
			return nil, errors.New("unexpected status lookup")
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	dataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &dataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStoragePull)
	claimed, err := runtime.repos.Tasks.ClaimNext(t.Context(), 100*time.Millisecond)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("seed expired pull claim = %#v, err=%v", claimed, err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire pull claim: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if presignCalls.Load() != 0 || pullCalls.Load() != 0 || statusCalls.Load() != 0 {
		t.Fatalf("pull recovery calls = presign:%d pull:%d status:%d, want 0/0/0", presignCalls.Load(), pullCalls.Load(), statusCalls.Load())
	}
	// A recovery that never sent a request leaves no ledger row behind.
	attempts, err := runtime.db.NewSelect().
		Model((*storagepull.Attempt)(nil)).
		Where("content_id = ?", pipeline.target.ContentID).
		Count(t.Context())
	if err != nil || attempts != 0 {
		t.Fatalf("pull attempts = %d, err=%v, want none", attempts, err)
	}
}

func TestCommitRecoverWithoutAttemptCannotSubmit(t *testing.T) {
	var submitCalls atomic.Int64
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa, 0xbb}, nil },
		SubmitCommitFunc: func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
			submitCalls.Add(1)
			return nil, errors.New("unexpected submit")
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPieceReady)
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	dataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &dataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageCommit)
	claimed, err := runtime.repos.Tasks.ClaimNext(t.Context(), 100*time.Millisecond)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("seed expired commit claim = %#v, err=%v", claimed, err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire commit claim: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if submitCalls.Load() != 0 {
		t.Fatalf("commit submissions during recovery = %d, want 0", submitCalls.Load())
	}
}

func TestCommitRecoverableAttentionKeepsObserving(t *testing.T) {
	storageClient := &testutil.MockStorageClient{
		OpenDataSetTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
			return nil, storage.ErrDataSetUnavailable
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPieceReady)
	startedAt := time.Now().Add(-16 * time.Minute)
	identity := storagecommit.CopyIdentity{
		StorageCopyID:    pipeline.target.ID,
		ContentID:        pipeline.target.ContentID,
		CopyIndex:        pipeline.target.CopyIndex,
		StorageDataSetID: pipeline.target.StorageDataSetID,
	}
	if _, err := runtime.repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "worker-recoverable-attention", Now: startedAt,
	}); err != nil {
		t.Fatalf("reserve commit attempt: %v", err)
	}
	if _, err := runtime.repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "worker-recoverable-attention", ExtraDataHex: "aabb", Now: startedAt,
	}); err != nil {
		t.Fatalf("mark commit attempted: %v", err)
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageCommit)
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID ||
		copyRow.CommitAttentionCode == nil || *copyRow.CommitAttentionCode != string(storagecommit.AttentionDataSetUnavailable) {
		t.Fatalf("commit copy = %#v, err=%v, want fenced recoverable attention", copyRow, err)
	}
}

func TestWalletRecoveryObservesTransactionWithoutRebroadcast(t *testing.T) {
	var broadcasts, observations atomic.Int64
	transactionHash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		wallet: testWalletOperator{fund: func(_ context.Context, amount *big.Int) (string, error) {
			broadcasts.Add(1)
			if amount.String() != "100" {
				t.Fatalf("wallet amount = %s, want 100", amount)
			}
			return transactionHash, nil
		}},
		receipts: testReceiptChecker{check: func(_ context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
			observations.Add(1)
			if hash != common.HexToHash(transactionHash) {
				t.Fatalf("receipt hash = %s, want %s", hash, transactionHash)
			}
			return &ethtypes.Receipt{Status: ethtypes.ReceiptStatusSuccessful}, nil
		}},
	})
	var operation *model.WalletOperation
	var taskRow *model.Task
	if err := runtime.repos.WithTx(t.Context(), func(repos *repository.Repositories) error {
		var err error
		operation, _, err = repos.WalletOperations.CreateOrGet(t.Context(), repository.CreateWalletOperationInput{
			Type: model.WalletOperationTypeFund, ClientRequestID: "wallet-recovery", Amount: "100",
		})
		if err != nil {
			return err
		}
		taskRow, _, err = runtime.service.EnqueueInTransaction(t.Context(), repos, taskengine.EnqueueRequest{
			Type: model.TaskTypeWalletOperation, IdempotencyKey: walletoperation.TaskKey(operation.ID),
			Input: walletoperation.Input{OperationID: operation.ID}, SubjectType: "wallet_operation", SubjectKey: fmt.Sprint(operation.ID),
		})
		if err != nil {
			return err
		}
		return repos.WalletOperations.BindTask(t.Context(), operation.ID, taskRow.ID)
	}); err != nil {
		t.Fatalf("seed wallet task: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("make wallet recovery ready: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusCompleted
	})
	if broadcasts.Load() != 1 || observations.Load() != 1 {
		t.Fatalf("wallet calls = broadcasts:%d observations:%d, want 1/1", broadcasts.Load(), observations.Load())
	}
	stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || stored == nil || stored.Status != model.WalletOperationStatusConfirmed || stored.TaskID != nil {
		t.Fatalf("wallet operation = %#v, err=%v", stored, err)
	}
}

func TestWalletBroadcastHasIndependentDeadline(t *testing.T) {
	var broadcasts atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		walletBroadcastTimeout: 20 * time.Millisecond,
		wallet: testWalletOperator{fund: func(ctx context.Context, _ *big.Int) (string, error) {
			broadcasts.Add(1)
			<-ctx.Done()
			return "", context.Cause(ctx)
		}},
	})
	operation, taskRow := seedWalletTask(t, runtime, "wallet-broadcast-timeout")
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if broadcasts.Load() != 1 {
		t.Fatalf("wallet broadcasts = %d, want 1", broadcasts.Load())
	}
	stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || stored.Status != model.WalletOperationStatusUnknown {
		t.Fatalf("wallet operation = %#v, err=%v, want unknown", stored, err)
	}
}

func TestWalletReceiptLookupHasIndependentDeadline(t *testing.T) {
	var observations atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		walletReceiptTimeout: 20 * time.Millisecond,
		receipts: testReceiptChecker{check: func(ctx context.Context, _ common.Hash) (*ethtypes.Receipt, error) {
			observations.Add(1)
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}},
	})
	operation, taskRow := seedWalletTask(t, runtime, "wallet-receipt-timeout")
	transactionHash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	if err := runtime.repos.WalletOperations.MarkBroadcastAttempted(t.Context(), operation.ID, taskRow.ID); err != nil {
		t.Fatalf("mark wallet broadcast attempted: %v", err)
	}
	if err := runtime.repos.WalletOperations.MarkSubmitted(t.Context(), operation.ID, taskRow.ID, transactionHash); err != nil {
		t.Fatalf("mark wallet submitted: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if observations.Load() != 1 {
		t.Fatalf("wallet receipt observations = %d, want 1", observations.Load())
	}
}

func seedWalletTask(t *testing.T, runtime handlerTestRuntime, requestID string) (*model.WalletOperation, *model.Task) {
	t.Helper()
	var operation *model.WalletOperation
	var taskRow *model.Task
	if err := runtime.repos.WithTx(t.Context(), func(repos *repository.Repositories) error {
		var err error
		operation, _, err = repos.WalletOperations.CreateOrGet(t.Context(), repository.CreateWalletOperationInput{
			Type: model.WalletOperationTypeFund, ClientRequestID: requestID, Amount: "100",
		})
		if err != nil {
			return err
		}
		taskRow, _, err = runtime.service.EnqueueInTransaction(t.Context(), repos, taskengine.EnqueueRequest{
			Type: model.TaskTypeWalletOperation, IdempotencyKey: walletoperation.TaskKey(operation.ID),
			Input: walletoperation.Input{OperationID: operation.ID}, SubjectType: "wallet_operation", SubjectKey: fmt.Sprint(operation.ID),
		})
		if err != nil {
			return err
		}
		return repos.WalletOperations.BindTask(t.Context(), operation.ID, taskRow.ID)
	}); err != nil {
		t.Fatalf("seed wallet task: %v", err)
	}
	return operation, taskRow
}

func TestReplacementCoordinatorRetiresAfterCancelledItemsAreProcessed(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterReplacement(registry)
		},
	})
	ctx := t.Context()
	bucket := &model.Bucket{
		Name: "replacement-cancelled-item", Status: model.BucketStatusActive,
		DefaultCopies: 1, MinimumDurableCopies: 1,
	}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 11,
		Checksum: testutil.StorageChecksum("replacement-cancelled-item"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("ensure content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "cancelled.bin", Size: content.ContentSize,
		ETag: "cancelled", ContentType: "application/octet-stream", ContentID: &content.ID,
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create object version: %v", err)
	}
	sourceProvider := testOnChainID(t, 7401)
	source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: sourceProvider, CopyIndex: 0, CreatedByContentID: content.ID,
	})
	if err != nil {
		t.Fatalf("create source binding: %v", err)
	}
	sourceClientID := testOnChainID(t, 7402)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, ContentID: content.ID, DataSetID: testOnChainID(t, 7403), ClientDataSetID: &sourceClientID,
	}); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	replacement, created, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: testOnChainID(t, 7404), ClientRequestID: "cancelled-item",
	})
	if err != nil || !created {
		t.Fatalf("authorize replacement = %#v created=%v err=%v", replacement, created, err)
	}
	target, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("load target = %#v err=%v", target, err)
	}
	targetClientID := testOnChainID(t, 7405)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: target.ID, ContentID: content.ID, DataSetID: testOnChainID(t, 7406), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark target ready: %v", err)
	}
	if err := runtime.repos.Replacements.Activate(ctx, replacement.ID); err != nil {
		t.Fatalf("activate replacement: %v", err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: target.ID, CopyIndex: target.CopyIndex,
		TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: target.ProviderID,
	}}); err != nil {
		t.Fatalf("create target copy: %v", err)
	}
	item := &storagereplacement.Item{
		ReplacementID: replacement.ID, ContentID: content.ID, TargetDataSetID: target.ID,
		Status: storagereplacement.ItemStatusCancelled,
	}
	if _, err := runtime.db.NewInsert().Model(item).Exec(ctx); err != nil {
		t.Fatalf("insert cancelled replacement item: %v", err)
	}
	if _, err := runtime.db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("seeding_complete = ?", true).
		Set("items_total = ?", 1).
		Where("id = ?", replacement.ID).
		Exec(ctx); err != nil {
		t.Fatalf("complete replacement seeding: %v", err)
	}
	coordinateTask, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
		Type:           model.TaskTypeProviderReplacementCoordinate,
		IdempotencyKey: storagereplacement.CoordinateTaskKey(replacement.ID, replacement.TaskGeneration),
		Input: storagereplacement.CoordinateInput{
			ReplacementID: replacement.ID, Generation: replacement.TaskGeneration,
		},
		SubjectType: "storage_replacement", SubjectKey: fmt.Sprint(replacement.ID),
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue replacement coordinator: %v", err)
	}

	limitedRepos := *runtime.repos
	limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, &limitedRepos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatalf("new limited task engine: %v", err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, coordinateTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || stored == nil {
		t.Fatalf("load replacement after coordination = %#v err=%v", stored, err)
	}
	if stored.Status != storagereplacement.StatusRetiring || stored.TaskID != nil {
		t.Fatalf("replacement after cancelled item = %#v, want retiring with coordinator released", stored)
	}
	source, err = runtime.repos.Contents.GetDataSetBindingByID(ctx, source.ID)
	if err != nil || source == nil || source.RetirementTaskID == nil {
		t.Fatalf("source retirement reservation = %#v err=%v", source, err)
	}
}

func TestRetirementRecoveryPersistsReturnedEpochWithoutRepeatingTermination(t *testing.T) {
	terminator := &testServiceTerminator{result: &synapse.TerminationResult{TxHash: "0xretire", EndEpoch: 84}}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		terminator: terminator,
		epochs:     testEpochReader{epoch: 80},
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterReplacement(registry)
		},
	})
	ctx := t.Context()
	bucket := &model.Bucket{Name: "retirement-recovery", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create retirement bucket: %v", err)
	}
	source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: testOnChainID(t, 7101), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("create retirement source: %v", err)
	}
	sourceClientID := testOnChainID(t, 7201)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: testOnChainID(t, 7301), ClientDataSetID: &sourceClientID,
	}); err != nil {
		t.Fatalf("mark retirement source ready: %v", err)
	}
	first, created, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: testOnChainID(t, 7102), ClientRequestID: "retirement-first",
	})
	if err != nil || !created {
		t.Fatalf("authorize first replacement = %#v created=%v err=%v", first, created, err)
	}
	target, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, first.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("load retirement target = %#v err=%v", target, err)
	}
	targetClientID := testOnChainID(t, 7202)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: target.ID, DataSetID: testOnChainID(t, 7302), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark retirement target ready: %v", err)
	}
	if _, created, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: testOnChainID(t, 7103), ClientRequestID: "retirement-successor",
	}); err != nil || !created {
		t.Fatalf("authorize successor replacement created=%v err=%v", created, err)
	}
	first, err = runtime.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || first == nil || first.Status != storagereplacement.StatusSuperseded {
		t.Fatalf("superseded replacement = %#v err=%v", first, err)
	}
	generation, err := runtime.repos.Contents.NextDataSetRetirementGeneration(ctx, target.ID)
	if err != nil {
		t.Fatalf("next retirement generation: %v", err)
	}
	retirementTask, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
		Type:           model.TaskTypeStorageDataSetRetire,
		IdempotencyKey: storagereplacement.RetireTaskKey(target.ID, generation),
		Input: storagereplacement.RetireInput{
			ReplacementID: first.ID, DataSetID: target.ID, Generation: generation,
		},
		SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(target.ID),
	})
	if err != nil {
		t.Fatalf("enqueue retirement task: %v", err)
	}
	if err := runtime.repos.Contents.BindDataSetRetirementTask(ctx, target.ID, generation, retirementTask.ID); err != nil {
		t.Fatalf("bind retirement task: %v", err)
	}
	if _, err := runtime.db.ExecContext(ctx, `CREATE TRIGGER fail_retirement_evidence
		BEFORE INSERT ON storage_data_set_terminations
		WHEN NEW.role = 'abandoned_target'
		BEGIN SELECT RAISE(FAIL, 'injected retirement evidence failure'); END`); err != nil {
		t.Fatalf("create retirement evidence trigger: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	waitForTask(t, runtime.repos, retirementTask.ID, func(task *model.Task) bool {
		return strings.Contains(string(task.Checkpoint), `"termination_epoch":84`)
	})
	stopHandlerEngine(t, cancel, done)

	storedTask, err := runtime.repos.Tasks.GetByID(ctx, retirementTask.ID)
	if err != nil || storedTask == nil || storedTask.Status != model.TaskStatusRunning || storedTask.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("retirement task after failed settlement = %#v err=%v", storedTask, err)
	}
	first, err = runtime.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || first == nil || first.AbandonedTerminationEpoch != nil {
		t.Fatalf("retirement evidence unexpectedly settled = %#v err=%v", first, err)
	}
	if _, err := runtime.db.ExecContext(ctx, `DROP TRIGGER fail_retirement_evidence`); err != nil {
		t.Fatalf("drop retirement evidence trigger: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), retirementTask.ID).Exec(ctx); err != nil {
		t.Fatalf("expire retirement task lease: %v", err)
	}

	cancel, done = runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, retirementTask.ID, func(task *model.Task) bool {
		first, err = runtime.repos.Replacements.GetByID(ctx, first.ID)
		return err == nil && first != nil && first.AbandonedTerminationEpoch != nil &&
			*first.AbandonedTerminationEpoch == 84 && task.Status == model.TaskStatusPending
	})
	if terminator.calls.Load() != 1 {
		t.Fatalf("retirement termination calls = %d, want 1", terminator.calls.Load())
	}
}

// A replica index has to name a slot the bucket opened. Allocation used to look
// for a free index by counting up to the global maximum, so a bucket whose low
// slot was held by an unusable generation would reach past its own slots; the
// data set that came back could not be stored, because the slot behind that
// index does not exist. Allocation now reads the bucket's open slots.
func TestUploadPlanAllocatesOnlyWithinTheBucketsOpenSlots(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 21000+sequence).SDK()
	dataSetID := testOnChainID(t, 22000+sequence).SDK()
	clientDataSetID := testOnChainID(t, 23000+sequence).SDK()
	selection := make(chan storage.SelectUploadContextsOptions, 4)
	storageClient := &testutil.MockStorageClient{
		SelectUploadTargetsFunc: func(_ context.Context, opts storage.SelectUploadContextsOptions) ([]synapse.StorageTarget, error) {
			select {
			case selection <- opts:
			default:
			}
			return []synapse.StorageTarget{&testutil.MockStorageTarget{
				ProviderIDValue: providerID, DataSetIDValue: &dataSetID, ClientDataSetIDValue: clientDataSetID,
			}}, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	ctx := t.Context()

	// One open slot, but the content asks for two copies. Allocation has to be
	// bounded by the slots the bucket actually opened.
	bucket := &model.Bucket{
		Name: fmt.Sprintf("slot-bound-%d", sequence), Status: model.BucketStatusActive,
		DefaultCopies: 1, MinimumDurableCopies: 1,
	}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 11,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("slot-bound-checksum-%d", sequence)), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("ensure content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "object.bin", Size: 11,
		ETag: fmt.Sprintf("slot-bound-etag-%d", sequence), ContentType: "application/octet-stream",
		ContentID: &content.ID,
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create object version: %v", err)
	}

	taskRow, created, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeUploadPlan, IdempotencyKey: storagepipeline.UploadPlanKey(content.ID),
		Input:       storagepipeline.UploadPlanInput{ContentID: content.ID},
		SubjectType: "storage_content", SubjectKey: fmt.Sprint(content.ID),
	})
	if err != nil || !created {
		t.Fatalf("enqueue upload plan = %#v created=%v err=%v", taskRow, created, err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	var opts storage.SelectUploadContextsOptions
	select {
	case opts = <-selection:
	case <-time.After(5 * time.Second):
		stopHandlerEngine(t, cancel, done)
		t.Fatal("upload plan never asked for storage targets")
	}
	stopHandlerEngine(t, cancel, done)

	// Only one slot is open, so one target is what may be asked for. Asking for
	// two would hand back a target for an index the bucket never opened.
	if opts.Copies != 1 {
		t.Fatalf("requested copies = %d, want 1 bounded by the bucket's free open slots", opts.Copies)
	}
	bindings, err := runtime.repos.Contents.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("ListDataSetBindings: %v", err)
	}
	for i := range bindings {
		if bindings[i].CopyIndex >= bucket.DefaultCopies {
			t.Fatalf("data set bound to copy index %d, want an index inside the bucket's %d slots",
				bindings[i].CopyIndex, bucket.DefaultCopies)
		}
	}
}

// A generation that failed to be created must not keep bucket provisioning from
// finishing. The failed row gives up the slot, its provider is excluded from the
// next selection, and the bucket reaches ready on a different provider.
func TestBucketProvisionRecoversFromAFailedDataSetGeneration(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	failedProvider := testOnChainID(t, 15000+sequence)
	healthyProvider := testOnChainID(t, 16000+sequence)
	healthyDataSet := testOnChainID(t, 17000+sequence).SDK()
	var excludedFailedProvider atomic.Bool
	storageClient := &testutil.MockStorageClient{
		SelectUploadTargetsFunc: func(_ context.Context, opts storage.SelectUploadContextsOptions) ([]synapse.StorageTarget, error) {
			for _, excluded := range opts.ExcludeProviderIDs {
				if excluded.String() == failedProvider.String() {
					excludedFailedProvider.Store(true)
				}
			}
			clientDataSetID := testOnChainID(t, 18000+sequence).SDK()
			return []synapse.StorageTarget{&testutil.MockStorageTarget{
				ProviderIDValue: healthyProvider.SDK(), DataSetIDValue: &healthyDataSet,
				ClientDataSetIDValue: clientDataSetID,
			}}, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	bucket := &model.Bucket{
		Name: fmt.Sprintf("provision-recovery-%d", sequence), Status: model.BucketStatusProvisioning,
		DefaultCopies: 1, MinimumDurableCopies: 1,
	}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// The state the real failure path leaves behind: a generation that never
	// reached the chain, recorded through the same repository call.
	failed, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: failedProvider, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := runtime.repos.Contents.MarkDataSetFailed(t.Context(), failed.ID, "creation outcome unknown"); err != nil {
		t.Fatalf("MarkDataSetFailed: %v", err)
	}

	taskRow, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeBucketProvision, IdempotencyKey: bucketlifecycle.ProvisionKey(bucket.ID, bucket.DefaultCopies),
		Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID}, SubjectType: "bucket", SubjectKey: fmt.Sprint(bucket.ID),
	})
	if err != nil || !created {
		t.Fatalf("enqueue bucket provision task = %#v created=%v err=%v", taskRow, created, err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})

	stored, err := runtime.repos.Buckets.GetByID(t.Context(), bucket.ID)
	if err != nil || stored == nil || stored.Status != model.BucketStatusReady {
		t.Fatalf("bucket after recovery = %#v err=%v, want ready", stored, err)
	}
	if !excludedFailedProvider.Load() {
		t.Fatal("selection did not exclude the failed generation's provider")
	}
	bindings, err := runtime.repos.Contents.ListDataSetBindings(t.Context(), bucket.ID)
	if err != nil {
		t.Fatalf("list data set bindings: %v", err)
	}
	current := 0
	for i := range bindings {
		binding := &bindings[i]
		if !binding.IsCurrent {
			continue
		}
		current++
		if binding.ProviderID.String() != healthyProvider.String() || binding.Status != model.StorageDataSetStatusReady {
			t.Fatalf("current binding = provider:%s status:%s, want the healthy provider ready", binding.ProviderID, binding.Status)
		}
	}
	if current != 1 {
		t.Fatalf("current bindings = %d, want exactly 1", current)
	}
}

// A creation the chain refused leaves no data set, so the generation is removed
// and its provider becomes available to the bucket again. Keeping it would
// reserve that provider forever: a failed generation can neither be retired nor
// replaced, and the unique index covers every non-retired row.
func TestDataSetEnsureReleasesTheProviderWhenCreationIsRejected(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 26000+sequence)
	target := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		WaitDataSetFunc: func(context.Context, storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error) {
			return nil, pdp.ErrTxRejected
		},
	}
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return target, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	ctx := t.Context()
	bucket := &model.Bucket{
		Name: fmt.Sprintf("rejected-%d", sequence), Status: model.BucketStatusProvisioning,
		DefaultCopies: 1, MinimumDurableCopies: 1,
	}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	// The submission was recorded, so recovery resumes by waiting on it.
	clientDataSetID := testOnChainID(t, 27000+sequence)
	if err := runtime.repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID: binding.ID, TransactionID: "0xrejected", StatusURL: "https://provider.example/status",
		ClientDataSetID: &clientDataSetID,
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	ensureTask, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(binding.ID),
		Input:       storagepipeline.DataSetInput{DataSetID: binding.ID},
		SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(binding.ID),
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue data set ensure: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})

	ended, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || ended == nil || ended.Status != model.StorageDataSetStatusRetired {
		t.Fatalf("rejected generation = %#v err=%v, want it retired rather than deleted", ended, err)
	}
	// The provider is what retirement buys back.
	reused, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil || reused == nil || reused.ID == binding.ID {
		t.Fatalf("rebinding the freed provider = %#v err=%v", reused, err)
	}
}

// An unknown creation outcome must not terminate the copies bound to the
// generation. Retrying the ensure task rediscovers a data set the provider did
// create but never reported, and continuation only picks up copies that are
// still in transfer states — so failing them here would destroy the recovery
// that keeping the row is for.
func TestDataSetEnsureKeepsCopiesWhenTheCreationOutcomeIsUnknown(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 28000+sequence)
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return &testutil.MockStorageTarget{ProviderIDValue: providerID.SDK()}, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	ctx := t.Context()
	bucket := &model.Bucket{
		Name: fmt.Sprintf("unknown-%d", sequence), Status: model.BucketStatusActive,
		DefaultCopies: 1, MinimumDurableCopies: 1,
	}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 11,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("unknown-checksum-%d", sequence)), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("ensure content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "unknown.bin", ContentID: &content.ID,
		Size: 11, ETag: fmt.Sprintf("unknown-etag-%d", sequence), ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create object version: %v", err)
	}
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: content.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0, ProviderID: providerID,
		TransferMethod: model.StorageCopyTransferMethodIngress,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	ensureTask, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(binding.ID),
		Input:       storagepipeline.DataSetInput{DataSetID: binding.ID},
		SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(binding.ID),
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue data set ensure: %v", err)
	}
	// A creation was attempted long enough ago that the outcome is given up on,
	// and no transaction was ever learned.
	attempted := time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339Nano)
	if _, err := runtime.db.NewUpdate().
		Model((*model.TaskPayload)(nil)).
		Set("checkpoint_json = ?", fmt.Sprintf(`{"attempted_at":%q}`, attempted)).
		Where("task_id = ?", ensureTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("write attempted checkpoint: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "dataset_creation_unknown" {
		t.Fatalf("failure reason = %v, want dataset_creation_unknown", failed.FailureReason)
	}

	kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || kept == nil || kept.Status != model.StorageDataSetStatusFailed {
		t.Fatalf("unknown generation = %#v err=%v, want it kept as failed", kept, err)
	}
	// The copy stays in a transfer state, which is what continuation picks up.
	incomplete, err := runtime.repos.Contents.ListIncompleteCopiesForDataSet(ctx, binding.ID)
	if err != nil || len(incomplete) != 1 {
		t.Fatalf("incomplete copies = %#v err=%v, want the bound copy still recoverable", incomplete, err)
	}
}
