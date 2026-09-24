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

	"github.com/ethereum/go-ethereum"
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
	"github.com/strahe/synaps3/internal/observability"
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
	"github.com/strahe/synapse-go/piece"
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
	deletionState          func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error)
	wallet                 synapse.WalletOperator
	receipts               worker.WalletReceiptChecker
	walletBroadcastTimeout time.Duration
	walletReceiptTimeout   time.Duration
	terminator             synapse.ServiceTerminator
	epochs                 synapse.ChainEpochReader
	parkedPieces           synapse.ParkedPieceChecker
	uploadSpeedProbe       interface {
		Probe(context.Context, string) (time.Duration, error)
	}
	policy        cache.EvictionPolicy
	maxBytes      int64
	highPercent   int
	lowPercent    int
	concurrency   int
	maxRetries    *int
	leaseDuration time.Duration
	register      func(*worker.TaskHandlers, *taskengine.Registry) error
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
	if storageClient.DeletionStateFunc == nil {
		storageClient.DeletionStateFunc = options.deletionState
		if storageClient.DeletionStateFunc == nil {
			storageClient.DeletionStateFunc = func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
				return synapse.CleanupPieceState{}, nil
			}
		}
	}
	gate := cacheaccess.NewGate()
	tracker := cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, repos.Objects)
	maxRetries := 5
	if options.maxRetries != nil {
		maxRetries = *options.maxRetries
	}
	var observabilityService *observability.Service
	if options.uploadSpeedProbe != nil {
		observabilityService = observability.NewService(observability.ServiceOptions{Store: repos.Observability})
	}
	handlers, err := worker.NewTaskHandlers(worker.TaskHandlerDependencies{
		Repositories: repos, Events: options.events, Cache: cacheStore, CacheGate: gate, CacheTracker: tracker,
		Storage: storageClient, Wallet: options.wallet, Receipts: options.receipts,
		WalletBroadcastTimeout: options.walletBroadcastTimeout,
		WalletReceiptTimeout:   options.walletReceiptTimeout,
		Terminator:             options.terminator, Epochs: options.epochs,
		ParkedPieces:  options.parkedPieces,
		Observability: observabilityService, UploadSpeedProbe: options.uploadSpeedProbe,
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
	leaseDuration := options.leaseDuration
	if leaseDuration == 0 {
		leaseDuration = 300 * time.Millisecond
	}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: concurrency, PollInterval: 5 * time.Millisecond, LeaseDuration: leaseDuration,
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
	calls    atomic.Int64
	result   *synapse.TerminationResult
	err      error
	identity storage.ContextIdentity
	// notPaidFor makes the chain report the data set as another wallet's.
	notPaidFor atomic.Bool
}

func (t *testServiceTerminator) TerminateService(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
	t.calls.Add(1)
	return t.result, t.err
}

func (t *testServiceTerminator) VerifyServicePayer(context.Context, sdktypes.BigInt) error {
	if t.notPaidFor.Load() {
		return fmt.Errorf("data set is paid for by another wallet: %w", synapse.ErrServicePaidByAnother)
	}
	return nil
}

func (t *testServiceTerminator) ContextIdentity() storage.ContextIdentity {
	if t.identity != (storage.ContextIdentity{}) {
		return t.identity
	}
	return testutil.DefaultContextIdentity
}

type testEpochReader struct {
	epoch int64
	err   error
}

type parkedPieceCheckerFunc func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error)

func (f parkedPieceCheckerFunc) FindParkedPiece(ctx context.Context, serviceURL string, pieceCID cid.Cid) (synapse.ParkedPieceState, error) {
	return f(ctx, serviceURL, pieceCID)
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

func TestStoreResultMismatchRetainsCopyAndCheckpoint(t *testing.T) {
	events := &recordingWorkerEvents{events: make(chan recordedWorkerEvent, 8)}
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(strings.Repeat("s", 128))), &cache.ObjectInfo{Size: 128}, nil
	}}
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://terminal-store.example",
		StoreFunc: func(_ context.Context, _ io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
			if options == nil || options.OnProgress == nil || !options.PieceCID.Defined() {
				return nil, errors.New("store progress callback or intended piece identity is missing")
			}
			pieceInfo, err := piece.ParseV2(options.PieceCID)
			if err != nil || pieceInfo.RawSize != 128 {
				return nil, fmt.Errorf("store piece identity = %#v, err=%v", pieceInfo, err)
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
		leaseDuration: 5 * time.Second,
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
		BucketID: bucket.ID, ContentSize: 128,
		Checksum: testutil.StorageChecksum(fmt.Sprintf("terminal-%d", sequence)), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("ensure terminal content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "terminal.bin", ContentID: &content.ID, Size: 128,
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
	if !runtime.service.Retryable(failedTask) || len(failedTask.Checkpoint) == 0 {
		t.Fatal("Store result failure lost its recovery checkpoint")
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(ctx, copies[0].ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
		t.Fatalf("terminal copy = %#v, err=%v", copyRow, err)
	}
	// Ingress progress belongs to the transfer, so it survives on the copy.
	if copyRow.IngressStoreAttempt != 1 || copyRow.IngressBytesTransferred != 6 || copyRow.ProgressUpdatedAt == nil {
		t.Fatalf("terminal copy progress = attempt:%d bytes:%d at:%v", copyRow.IngressStoreAttempt, copyRow.IngressBytesTransferred, copyRow.ProgressUpdatedAt)
	}
	storedVersion, err := runtime.repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || storedVersion.State != model.ObjectStateUploading {
		t.Fatalf("terminal version = %#v, err=%v", storedVersion, err)
	}
}

type testCleanupContext struct {
	deletePiece func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error)
}

func (c testCleanupContext) DeletePieceByID(ctx context.Context, pieceID sdktypes.BigInt) (*sdktypes.WriteResult, error) {
	return c.deletePiece(ctx, pieceID)
}

// TestStorageCleanupContinuesPastUnsupportedCopy checks that remote cleanup
// skips a copy the provider cannot delete and still schedules the rest.
func TestStorageCleanupContinuesPastUnsupportedCopy(t *testing.T) {
	var deleteCalls atomic.Int64
	cleanupContext := testCleanupContext{
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
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: storageClient, deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
		return synapse.CleanupPieceState{Live: true}, nil
	}})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusUnsupported, model.StorageCleanupCopyStatusPending)
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

// TestStorageCleanupFinalizesContent checks that cleanup finishes past a
// deletion the provider cannot perform: it releases the cached bytes, deletes
// the content's current-state rows, and keeps the cleanup ledger.
func TestStorageCleanupFinalizesContent(t *testing.T) {
	var cacheDeletes atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return nil, errors.New("provider context must not be opened for an absent piece")
		}},
		cache: &testutil.MockCache{DeleteFunc: func(context.Context, string, string) error {
			cacheDeletes.Add(1)
			return nil
		}},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusUnsupported, model.StorageCleanupCopyStatusPending)
	now := time.Now()
	if _, err := runtime.db.NewInsert().Model(&model.ObjectCache{ContentID: content.ID, InCache: true, CreatedAt: now, UpdatedAt: now}).Exec(ctx); err != nil {
		t.Fatalf("create cache record: %v", err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	// An uncertain lease makes the engine run the handler again, and releasing
	// the cached bytes is idempotent, so the count is a lower bound.
	if cacheDeletes.Load() < 1 {
		t.Fatal("cleanup finished without releasing the cached bytes")
	}
	for _, rows := range []struct{ table, column string }{
		{"storage_contents", "id"}, {"object_cache", "content_id"}, {"storage_data_sets", "created_by_content_id"},
	} {
		var count int
		if err := runtime.db.NewRaw("SELECT count(*) FROM "+rows.table+" WHERE "+rows.column+" = ?", content.ID).Scan(ctx, &count); err != nil || count != 0 {
			t.Fatalf("%s rows naming finalized content = %d, err=%v", rows.table, count, err)
		}
	}
	var statuses []model.StorageCleanupCopyStatus
	if err := runtime.db.NewSelect().Model((*model.StorageCleanupCopy)(nil)).Column("status").
		Where("content_id = ?", content.ID).Order("copy_index").Scan(ctx, &statuses); err != nil {
		t.Fatalf("load cleanup ledger: %v", err)
	}
	if len(statuses) != 2 || statuses[0] != model.StorageCleanupCopyStatusUnsupported || statuses[1] != model.StorageCleanupCopyStatusRemoved {
		t.Fatalf("cleanup ledger statuses = %v", statuses)
	}
}

func TestStorageCleanupRecoveryIgnoresOwnUnacceptedCopy(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: removedPieceStorageClient()})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	cleanupCopy := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(cleanupCopy).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: cleanupCopy.StorageDataSetID, CopyIndex: 0,
		TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: cleanupCopy.ProviderID,
	}}); err != nil {
		t.Fatalf("create committed copy binding: %v", err)
	}
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, repository.MarkUploadCopyCommittedInput{
		ContentID: content.ID, CopyIndex: 0, PieceCID: cleanupCopy.PieceCID,
		PieceID: &cleanupCopy.PieceID, RetrievalURL: "https://provider.example/piece",
	})
	storedContent, err := runtime.repos.Contents.GetByID(ctx, content.ID)
	if err != nil || storedContent == nil || storedContent.AcceptedAt != nil {
		t.Fatalf("unaccepted cleanup content = %#v, %v", storedContent, err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Set("wait_reason = ?", "references").
		Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("resume cleanup from reference wait: %v", err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
}

func TestStorageCleanupKeepsCheckingScheduledDeletionBeforeDeadline(t *testing.T) {
	var pieceExists atomic.Bool
	pieceExists.Store(true)
	var pieceQueued atomic.Bool
	pieceQueued.Store(true)
	var receiptCalls atomic.Int64
	var deleteCalls atomic.Int64
	cleanupContext := testCleanupContext{
		deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
			deleteCalls.Add(1)
			return nil, errors.New("unexpected duplicate deletion")
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: &testutil.MockStorageClient{
		OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return cleanupContext, nil
		},
	}, deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
		return synapse.CleanupPieceState{Live: pieceExists.Load(), Queued: pieceQueued.Load()}, nil
	}, receipts: testReceiptChecker{check: func(context.Context, common.Hash) (*ethtypes.Receipt, error) {
		receiptCalls.Add(1)
		return nil, ethereum.NotFound
	}}})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	cleanupCopy := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(cleanupCopy).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, cleanupCopy.ID, common.HexToHash("0x1").Hex()); err != nil {
		t.Fatalf("schedule deletion: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-23*time.Hour)).Where("id = ?", cleanupCopy.ID).Exec(ctx); err != nil {
		t.Fatalf("age scheduled deletion: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("resume cleanup recovery: %v", err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	pending := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover &&
			task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
	})
	if pending.RetryCount != 0 || pending.AvailableAt.Before(time.Now().Add(45*time.Second)) {
		t.Fatalf("scheduled cleanup did not keep a low-frequency confirmation wait: %#v", pending)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("scheduled cleanup sent %d duplicate deletions", deleteCalls.Load())
	}
	copies, err := runtime.repos.StorageCleanup.AuthorizeTask(ctx, content.ID, 1, taskRow.ID)
	if err != nil || len(copies) != 1 || copies[0].Status != model.StorageCleanupCopyStatusDeleteScheduled {
		t.Fatalf("cleanup copies during confirmation wait = %#v, err=%v", copies, err)
	}
	pieceQueued.Store(false)
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("advance pending receipt poll: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return receiptCalls.Load() >= 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if deleteCalls.Load() != 0 {
		t.Fatalf("pending receipt sent %d duplicate deletions", deleteCalls.Load())
	}

	pieceExists.Store(false)
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("advance cleanup poll: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if deleteCalls.Load() != 0 {
		t.Fatalf("completed cleanup sent %d duplicate deletions", deleteCalls.Load())
	}
}

func TestStorageCleanupUnknownDeletionDeadline(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		withHash               bool
		missingTime            bool
		wantMissingTimeMessage bool
		recentScheduled        bool
		recentOtherCheckpoint  bool
	}{
		{name: "checkpoint takes precedence over recent scheduled time", withHash: true, recentScheduled: true},
		{name: "scheduled time is fallback", withHash: true, missingTime: true},
		{name: "unrecorded request"},
		{name: "checkpoint without timestamp", missingTime: true, wantMissingTimeMessage: true},
		{name: "another copy checkpoint is ignored", withHash: true, recentOtherCheckpoint: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var deleteCalls atomic.Int64
			var pieceQueued atomic.Bool
			pieceQueued.Store(true)
			newHash := common.HexToHash("0x2")
			cleanupContext := testCleanupContext{deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				deleteCalls.Add(1)
				return &sdktypes.WriteResult{Hash: newHash}, nil
			}}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return cleanupContext, nil
				}},
				deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					return synapse.CleanupPieceState{Live: true, Queued: pieceQueued.Load(), BlockNumber: 100}, nil
				},
				receipts: testReceiptChecker{check: func(context.Context, common.Hash) (*ethtypes.Receipt, error) {
					return nil, ethereum.NotFound
				}},
			})
			ctx := t.Context()
			content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
			copyRow := new(model.StorageCleanupCopy)
			if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
				t.Fatalf("load cleanup copy: %v", err)
			}
			attemptedAt := time.Now().Add(-25 * time.Hour)
			oldHash := common.HexToHash("0x1").Hex()
			if tc.withHash {
				if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRow.ID, oldHash); err != nil {
					t.Fatalf("schedule deletion: %v", err)
				}
				scheduledAt := attemptedAt
				if tc.recentScheduled {
					scheduledAt = time.Now().Add(-time.Hour)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
					Set("scheduled_at = ?", scheduledAt).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
					t.Fatalf("age scheduled deletion: %v", err)
				}
			}
			checkpointCopyID := copyRow.ID
			if tc.recentOtherCheckpoint {
				checkpointCopyID++
				attemptedAt = time.Now().Add(-time.Hour)
			}
			checkpoint := map[string]any{"copy_id": checkpointCopyID}
			if !tc.missingTime {
				checkpoint["attempted_at"] = attemptedAt
			}
			encodedCheckpoint, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatalf("encode cleanup checkpoint: %v", err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.TaskPayload)(nil)).
				Set("checkpoint_json = ?", encodedCheckpoint).Where("task_id = ?", taskRow.ID).Exec(ctx); err != nil {
				t.Fatalf("set cleanup checkpoint: %v", err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
				t.Fatalf("resume cleanup recovery: %v", err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
			})
			pieceQueued.Store(false)
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
				t.Fatalf("advance cleanup poll: %v", err)
			}
			failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed
			})
			if failed.FailureReason == nil || *failed.FailureReason != "cleanup_outcome_unknown" ||
				!runtime.service.Retryable(failed) || deleteCalls.Load() != 0 {
				t.Fatalf("timed-out cleanup = %#v, delete calls = %d", failed, deleteCalls.Load())
			}
			if failed.LastError == nil || !strings.Contains(*failed.LastError, "Recover may submit another paid request") {
				t.Fatalf("timed-out cleanup details = %v", failed.LastError)
			}
			if tc.wantMissingTimeMessage {
				if strings.Contains(*failed.LastError, "after 24 hours") {
					t.Fatalf("missing-time cleanup claims a completed wait: %q", *failed.LastError)
				}
			} else if !strings.Contains(*failed.LastError, "after 24 hours") {
				t.Fatalf("timed-out cleanup omits the elapsed wait: %q", *failed.LastError)
			}
			if err := runtime.db.NewSelect().Model(copyRow).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
				t.Fatalf("reload cleanup copy: %v", err)
			}
			if copyRow.Status != model.StorageCleanupCopyStatusFailed {
				t.Fatalf("timed-out cleanup copy = %#v", copyRow)
			}
			if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
				t.Fatalf("retry timed-out cleanup: %v", err)
			}
			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return deleteCalls.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
			})
			if err := runtime.db.NewSelect().Model(copyRow).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
				t.Fatalf("reload retried cleanup copy: %v", err)
			}
			if copyRow.Status != model.StorageCleanupCopyStatusDeleteScheduled || copyRow.DeleteTxHash == nil || *copyRow.DeleteTxHash != newHash.Hex() ||
				copyRow.ScheduledAt == nil || copyRow.ScheduledAt.Before(time.Now().Add(-time.Minute)) {
				t.Fatalf("retried cleanup copy = %#v", copyRow)
			}
		})
	}
}

func TestStorageCleanupConfirmsRemovalAfterDeadline(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: removedPieceStorageClient()})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	copyRow := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRow.ID, common.HexToHash("0x1").Hex()); err != nil {
		t.Fatalf("schedule deletion: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-25*time.Hour)).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatalf("age scheduled deletion: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("resume cleanup recovery: %v", err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
}

func TestStorageCleanupWaitsWhenDataSetIsNotLive(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: removedPieceStorageClient(),
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{}, errors.New("cleanup data set is not live")
		},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	copyRows, err := runtime.repos.StorageCleanup.AuthorizeTask(ctx, content.ID, 1, taskRow.ID)
	if err != nil || len(copyRows) != 1 {
		t.Fatalf("cleanup copies = %#v, err=%v", copyRows, err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRows[0].ID, common.HexToHash("0x1").Hex()); err != nil {
		t.Fatalf("schedule deletion: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-25*time.Hour)).Where("id = ?", copyRows[0].ID).Exec(ctx); err != nil {
		t.Fatalf("age scheduled deletion: %v", err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover &&
			task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
	})
	copyRows, err = runtime.repos.StorageCleanup.AuthorizeTask(ctx, content.ID, 1, taskRow.ID)
	if err != nil || len(copyRows) != 1 || copyRows[0].Status != model.StorageCleanupCopyStatusDeleteScheduled {
		t.Fatalf("cleanup copy before confirmation = %#v, err=%v", copyRows, err)
	}
	if stored, err := runtime.repos.Contents.GetByID(ctx, content.ID); err != nil || stored == nil {
		t.Fatalf("content before confirmation = %#v, err=%v", stored, err)
	}
}

func TestStorageCleanupReissuesOnlyAfterConfirmedFailureAndManualRetry(t *testing.T) {
	oldHash := common.HexToHash("0x1")
	newHash := common.HexToHash("0x2")
	var deleteCalls atomic.Int64
	cleanupContext := testCleanupContext{
		deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
			deleteCalls.Add(1)
			return &sdktypes.WriteResult{Hash: newHash}, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return cleanupContext, nil
		}},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Live: true, BlockNumber: 100}, nil
		},
		receipts: testReceiptChecker{check: func(_ context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
			if hash == oldHash {
				return &ethtypes.Receipt{Status: ethtypes.ReceiptStatusFailed, BlockNumber: big.NewInt(99)}, nil
			}
			return nil, ethereum.NotFound
		}},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	copyRow := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRow.ID, oldHash.Hex()); err != nil {
		t.Fatalf("schedule old request: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-25*time.Hour)).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatalf("age old request: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("resume cleanup recovery: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 3}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "cleanup_transaction_reverted" || !runtime.service.Retryable(failed) || deleteCalls.Load() != 0 {
		t.Fatalf("confirmed failure task = %#v, delete calls = %d", failed, deleteCalls.Load())
	}
	if failed.LastError == nil || !strings.Contains(*failed.LastError, "Recover may submit another paid request") {
		t.Fatalf("confirmed failure details = %v", failed.LastError)
	}
	if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
		t.Fatalf("retry confirmed failure: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 3 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if deleteCalls.Load() != 1 {
		t.Fatalf("retry sent %d deletion requests, want one", deleteCalls.Load())
	}
	if err := runtime.db.NewSelect().Model(copyRow).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
		t.Fatalf("reload cleanup copy: %v", err)
	}
	if copyRow.Status != model.StorageCleanupCopyStatusDeleteScheduled || copyRow.DeleteTxHash == nil || *copyRow.DeleteTxHash != newHash.Hex() {
		t.Fatalf("retry ledger = %#v", copyRow)
	}
}

func TestStorageCleanupUnknownRequestOutcomeDoesNotResend(t *testing.T) {
	var deleteCalls atomic.Int64
	cleanupContext := testCleanupContext{
		deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
			deleteCalls.Add(1)
			return nil, context.DeadlineExceeded
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return cleanupContext, nil
		}},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Live: true}, nil
		},
	})
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("advance cleanup poll: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if deleteCalls.Load() != 1 {
		t.Fatalf("unknown deletion sent %d requests, want one", deleteCalls.Load())
	}
	copies, err := runtime.repos.StorageCleanup.AuthorizeTask(t.Context(), content.ID, 1, taskRow.ID)
	if err != nil || len(copies) != 1 || copies[0].Status != model.StorageCleanupCopyStatusPending {
		t.Fatalf("unknown deletion ledger = %#v, err=%v", copies, err)
	}
}

func TestStorageCleanupManualRetryWithoutHashDoesNotAutomaticallyResend(t *testing.T) {
	oldHash := common.HexToHash("0x1")
	var deleteCalls atomic.Int64
	cleanupContext := testCleanupContext{
		deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
			deleteCalls.Add(1)
			return nil, context.DeadlineExceeded
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return cleanupContext, nil
		}},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Live: true, BlockNumber: 100}, nil
		},
		receipts: testReceiptChecker{check: func(context.Context, common.Hash) (*ethtypes.Receipt, error) {
			return &ethtypes.Receipt{Status: ethtypes.ReceiptStatusFailed, BlockNumber: big.NewInt(99)}, nil
		}},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	copyRow := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRow.ID, oldHash.Hex()); err != nil {
		t.Fatalf("schedule old request: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, "confirmed rejected"); err != nil {
		t.Fatalf("fail old request: %v", err)
	}
	oldAttempt := time.Now().Add(-25 * time.Hour)
	checkpoint, err := json.Marshal(map[string]any{"copy_id": copyRow.ID, "attempted_at": oldAttempt})
	if err != nil {
		t.Fatalf("encode cleanup checkpoint: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).
		Set("failure_reason = ?", "cleanup_outcome_unknown").
		Set("finished_at = ?", time.Now()).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("prepare failed cleanup task: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.TaskPayload)(nil)).
		Set("checkpoint_json = ?", checkpoint).
		Where("task_id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("record unconfirmed retry checkpoint: %v", err)
	}
	if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
		t.Fatalf("retry failed cleanup: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 3}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover &&
			task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
	})
	if deleteCalls.Load() != 1 {
		t.Fatalf("manual retry sent %d deletion requests, want one", deleteCalls.Load())
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("advance cleanup poll: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 3 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if deleteCalls.Load() != 1 {
		t.Fatalf("automatic recovery sent %d deletion requests, want one", deleteCalls.Load())
	}
	if err := runtime.db.NewSelect().Model(copyRow).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
		t.Fatalf("reload retried cleanup copy: %v", err)
	}
	if copyRow.Status != model.StorageCleanupCopyStatusPending || copyRow.DeleteTxHash != nil || copyRow.ScheduledAt != nil {
		t.Fatalf("unrecorded cleanup retry = %#v", copyRow)
	}
}

func TestStorageCleanupLegacyUnrecordedRetryWaitsForManualRetry(t *testing.T) {
	oldHash := common.HexToHash("0x1")
	newHash := common.HexToHash("0x2")
	var deleteCalls atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				deleteCalls.Add(1)
				return &sdktypes.WriteResult{Hash: newHash}, nil
			}}, nil
		}},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Live: true, BlockNumber: 100}, nil
		},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	copyRow := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRow.ID, oldHash.Hex()); err != nil {
		t.Fatalf("schedule old request: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, "previous request failed"); err != nil {
		t.Fatalf("fail old request: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
		Set("updated_at = ?", time.Now().Add(-time.Hour)).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatalf("age old failure: %v", err)
	}
	checkpoint, err := json.Marshal(map[string]any{
		"copy_id": copyRow.ID, "attempted_at": time.Now().Add(-30 * time.Minute), "retry_of_tx_hash": oldHash.Hex(),
	})
	if err != nil {
		t.Fatalf("encode old retry checkpoint: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.TaskPayload)(nil)).
		Set("checkpoint_json = ?", checkpoint).Where("task_id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("record old retry checkpoint: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("resume old retry: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 4}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
	})
	if deleteCalls.Load() != 0 {
		t.Fatalf("old unrecorded retry sent %d duplicate requests", deleteCalls.Load())
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
		Set("updated_at = ?", time.Now().Add(-26*time.Hour)).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatalf("age old failure further: %v", err)
	}
	agedCheckpoint, err := json.Marshal(map[string]any{
		"copy_id": copyRow.ID, "attempted_at": time.Now().Add(-25 * time.Hour), "retry_of_tx_hash": oldHash.Hex(),
	})
	if err != nil {
		t.Fatalf("encode aged retry checkpoint: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.TaskPayload)(nil)).
		Set("checkpoint_json = ?", agedCheckpoint).Where("task_id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("age old retry checkpoint: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("advance old retry poll: %v", err)
	}
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "cleanup_outcome_unknown" || !runtime.service.Retryable(failed) || deleteCalls.Load() != 0 {
		t.Fatalf("old retry deadline = %#v, delete calls = %d", failed, deleteCalls.Load())
	}
	if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
		t.Fatalf("retry timed-out old request: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 4 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if deleteCalls.Load() != 1 {
		t.Fatalf("manual retry sent %d deletion requests, want one", deleteCalls.Load())
	}
}

func TestStorageCleanupLegacyFailedCopyWithoutHashCanRetry(t *testing.T) {
	newHash := common.HexToHash("0x2")
	var deleteCalls atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				deleteCalls.Add(1)
				return &sdktypes.WriteResult{Hash: newHash}, nil
			}}, nil
		}},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Live: true, BlockNumber: 100}, nil
		},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	copyRow := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if err := runtime.repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, "remote removal was not confirmed"); err != nil {
		t.Fatalf("fail legacy cleanup copy: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).
		Set("failure_reason = ?", "cleanup_outcome_unknown").
		Set("finished_at = ?", time.Now()).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("prepare failed cleanup task: %v", err)
	}
	if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
		t.Fatalf("retry legacy cleanup: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if deleteCalls.Load() != 1 {
		t.Fatalf("legacy cleanup sent %d deletion requests, want one", deleteCalls.Load())
	}
	if err := runtime.db.NewSelect().Model(copyRow).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
		t.Fatalf("reload cleanup copy: %v", err)
	}
	if copyRow.Status != model.StorageCleanupCopyStatusDeleteScheduled || copyRow.DeleteTxHash == nil || *copyRow.DeleteTxHash != newHash.Hex() {
		t.Fatalf("legacy retry ledger = %#v", copyRow)
	}
}

func TestStorageCleanupManualRetryChecksChainBeforeSending(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state synapse.CleanupPieceState
	}{
		{name: "already removed", state: synapse.CleanupPieceState{Live: false}},
		{name: "already queued", state: synapse.CleanupPieceState{Live: true, Queued: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var deleteCalls atomic.Int64
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					deleteCalls.Add(1)
					return nil, errors.New("unexpected cleanup request")
				}},
				deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					return tc.state, nil
				},
			})
			ctx := t.Context()
			content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
			copyRow := new(model.StorageCleanupCopy)
			if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
				t.Fatalf("load cleanup copy: %v", err)
			}
			if err := runtime.repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, "unknown result"); err != nil {
				t.Fatalf("fail cleanup copy: %v", err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("status = ?", model.TaskStatusFailed).
				Set("failure_reason = ?", "cleanup_outcome_unknown").
				Set("finished_at = ?", time.Now()).
				Set("resume_mode = ?", model.TaskResumeModeRecover).
				Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
				t.Fatalf("prepare failed cleanup task: %v", err)
			}
			if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
				t.Fatalf("retry cleanup: %v", err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				if tc.state.Live {
					return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
				}
				return task.Status == model.TaskStatusCompleted
			})
			if deleteCalls.Load() != 0 {
				t.Fatalf("cleanup sent %d requests despite chain evidence", deleteCalls.Load())
			}
		})
	}
}

func TestStorageCleanupManualRetryCheckpointFailureDoesNotSend(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trigger string
	}{
		{name: "ledger reset", trigger: `CREATE TRIGGER fail_cleanup_retry BEFORE UPDATE OF status ON storage_cleanup_copies
			WHEN OLD.status = 'failed' AND NEW.status = 'pending'
			BEGIN SELECT RAISE(FAIL, 'injected cleanup retry failure'); END`},
		{name: "checkpoint write", trigger: `CREATE TRIGGER fail_cleanup_checkpoint BEFORE UPDATE OF checkpoint_json ON task_payloads
			BEGIN SELECT RAISE(FAIL, 'injected checkpoint failure'); END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			noRetries := 0
			var deleteCalls atomic.Int64
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				maxRetries: &noRetries,
				storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						deleteCalls.Add(1)
						return nil, nil
					}}, nil
				}},
				deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					return synapse.CleanupPieceState{Live: true}, nil
				},
			})
			ctx := t.Context()
			content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
			copyRow := new(model.StorageCleanupCopy)
			if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
				t.Fatalf("load cleanup copy: %v", err)
			}
			if err := runtime.repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, "unknown result"); err != nil {
				t.Fatalf("fail cleanup copy: %v", err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("status = ?", model.TaskStatusFailed).
				Set("failure_reason = ?", "cleanup_outcome_unknown").
				Set("finished_at = ?", time.Now()).
				Set("resume_mode = ?", model.TaskResumeModeRecover).
				Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
				t.Fatalf("prepare failed cleanup task: %v", err)
			}
			if _, err := runtime.db.ExecContext(ctx, tc.trigger); err != nil {
				t.Fatalf("install fault trigger: %v", err)
			}
			if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
				t.Fatalf("retry cleanup: %v", err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed
			})
			if failed.FailureReason == nil || *failed.FailureReason != "cleanup_not_started" || len(failed.Checkpoint) != 0 || deleteCalls.Load() != 0 {
				t.Fatalf("failed pre-request settlement = %#v, delete calls = %d", failed, deleteCalls.Load())
			}
			if err := runtime.db.NewSelect().Model(copyRow).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
				t.Fatalf("reload cleanup copy: %v", err)
			}
			if copyRow.Status != model.StorageCleanupCopyStatusFailed {
				t.Fatalf("failed pre-request cleanup copy = %#v", copyRow)
			}
		})
	}
}

func TestStorageCleanupUnrecordedReturnedHashWaitsForManualRetry(t *testing.T) {
	var deleteCalls atomic.Int64
	newHash := common.HexToHash("0x2")
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				deleteCalls.Add(1)
				return &sdktypes.WriteResult{Hash: newHash}, nil
			}}, nil
		}},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Live: true, BlockNumber: 100}, nil
		},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	copyRow := new(model.StorageCleanupCopy)
	if err := runtime.db.NewSelect().Model(copyRow).Where("content_id = ?", content.ID).Scan(ctx); err != nil {
		t.Fatalf("load cleanup copy: %v", err)
	}
	if _, err := runtime.db.ExecContext(ctx, `CREATE TRIGGER fail_cleanup_hash BEFORE UPDATE OF status ON storage_cleanup_copies
		WHEN OLD.status = 'pending' AND NEW.status = 'delete_scheduled'
		BEGIN SELECT RAISE(FAIL, 'injected cleanup hash failure'); END`); err != nil {
		t.Fatalf("install fault trigger: %v", err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	first := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if len(first.Checkpoint) == 0 || deleteCalls.Load() != 1 {
		t.Fatalf("unrecorded request task = %#v, delete calls = %d", first, deleteCalls.Load())
	}
	if err := runtime.db.NewSelect().Model(copyRow).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
		t.Fatalf("reload cleanup copy: %v", err)
	}
	if copyRow.Status != model.StorageCleanupCopyStatusPending || copyRow.DeleteTxHash != nil {
		t.Fatalf("unrecorded request ledger = %#v", copyRow)
	}
	agedCheckpoint, err := json.Marshal(map[string]any{"copy_id": copyRow.ID, "attempted_at": time.Now().Add(-25 * time.Hour)})
	if err != nil {
		t.Fatalf("encode aged checkpoint: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.TaskPayload)(nil)).
		Set("checkpoint_json = ?", agedCheckpoint).Where("task_id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("age cleanup checkpoint: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(ctx); err != nil {
		t.Fatalf("advance cleanup poll: %v", err)
	}
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "cleanup_outcome_unknown" || !runtime.service.Retryable(failed) || deleteCalls.Load() != 1 {
		t.Fatalf("unrecorded hash outcome = %#v, delete calls = %d", failed, deleteCalls.Load())
	}
}

// TestStorageCleanupRetriesCacheRelease checks that a failed cache release
// keeps the content bound to its cleanup, and a later run finishes it.
func TestStorageCleanupRetriesCacheRelease(t *testing.T) {
	var cacheDeletes atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: removedPieceStorageClient(),
		cache: &testutil.MockCache{DeleteFunc: func(context.Context, string, string) error {
			if cacheDeletes.Add(1) == 1 {
				return errors.New("cache volume is busy")
			}
			return nil
		}},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.RetryCount == 1
	})
	stored, err := runtime.repos.Contents.GetByID(ctx, content.ID)
	if err != nil || stored == nil || stored.CleanupTaskID == nil || *stored.CleanupTaskID != taskRow.ID {
		t.Fatalf("content after failed cache release = %#v, err=%v", stored, err)
	}
	if _, err := runtime.repos.Tasks.WakePending(ctx, []int64{taskRow.ID}); err != nil {
		t.Fatalf("WakePending: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if stored, err := runtime.repos.Contents.GetByID(ctx, content.ID); stored != nil {
		t.Fatalf("content after cleanup = %#v, err=%v", stored, err)
	}
	if cacheDeletes.Load() < 2 {
		t.Fatalf("cache deletes = %d, want the failed release and at least one retry", cacheDeletes.Load())
	}
}

// removedPieceStorageClient reports every cleanup piece as already gone.
func removedPieceStorageClient() *testutil.MockStorageClient {
	cleanupContext := testCleanupContext{
		deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
			return nil, errors.New("unexpected remote deletion")
		},
	}
	return &testutil.MockStorageClient{
		OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return cleanupContext, nil
		},
	}
}

// seedStorageCleanup stores content whose versions are gone, with one cleanup
// copy per status on its own provider, and binds a cleanup task to it.
func seedStorageCleanup(t *testing.T, runtime handlerTestRuntime, statuses ...model.StorageCleanupCopyStatus) (*model.StorageContent, *model.Task) {
	t.Helper()
	ctx := t.Context()
	sequence := storedObjectSequence.Add(1)
	copies := len(statuses)
	bucket := &model.Bucket{Name: fmt.Sprintf("cleanup-task-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: copies, MinimumDurableCopies: copies}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create cleanup bucket: %v", err)
	}
	content := &model.StorageContent{
		BucketID: bucket.ID, ContentSize: 1,
		Checksum:        testutil.StorageChecksum(fmt.Sprintf("cleanup-%d", sequence)),
		RequestedCopies: copies, CleanupGeneration: 1,
	}
	if _, err := runtime.db.NewInsert().Model(content).Exec(ctx); err != nil {
		t.Fatalf("create cleanup content: %v", err)
	}
	for copyIndex, status := range statuses {
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
		row := &model.StorageCleanupCopy{
			ContentID: content.ID, BucketID: bucket.ID, CopyIndex: copyIndex, ProviderID: providerID,
			StorageDataSetID: binding.ID, DataSetID: &dataSetID, ClientDataSetID: &clientDataSetID,
			PieceID:  testOnChainID(t, 12000+sequence*10+int64(copyIndex)),
			PieceCID: testPieceCID(t, fmt.Sprintf("cleanup-piece-%d-%d", sequence, copyIndex)).String(),
			Checksum: content.Checksum, Status: status,
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
	return content, taskRow
}

func TestStorageCleanupAdmissionFailureDoesNotScheduleDeletion(t *testing.T) {
	noRetries := 0
	var deleteCalls atomic.Int64
	cleanupContext := testCleanupContext{
		deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
			deleteCalls.Add(1)
			return nil, errors.New("unexpected remote deletion")
		},
	}
	storageClient := &testutil.MockStorageClient{
		OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return cleanupContext, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: storageClient, maxRetries: &noRetries, deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
		return synapse.CleanupPieceState{Live: true}, nil
	}})
	bucket := &model.Bucket{Name: "cleanup-admission", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create cleanup admission bucket: %v", err)
	}
	content := &model.StorageContent{
		BucketID: bucket.ID, ContentSize: 128, Checksum: testutil.StorageChecksum("cleanup-admission"),
		RequestedCopies: 1, CleanupGeneration: 1,
	}
	if _, err := runtime.db.NewInsert().Model(content).Exec(t.Context()); err != nil {
		t.Fatalf("create cleanup admission content: %v", err)
	}
	providerID := testOnChainID(t, 29101)
	dataSetID := testOnChainID(t, 29102)
	clientDataSetID := testOnChainID(t, 29103)
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: content.ID,
	})
	if err != nil {
		t.Fatalf("create cleanup admission binding: %v", err)
	}
	if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: binding.ID, ContentID: content.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
	}); err != nil {
		t.Fatalf("mark cleanup admission binding ready: %v", err)
	}
	cleanupCopy := &model.StorageCleanupCopy{
		ContentID: content.ID, BucketID: bucket.ID, CopyIndex: 0, ProviderID: providerID,
		StorageDataSetID: binding.ID, DataSetID: &dataSetID, ClientDataSetID: &clientDataSetID,
		PieceID: testOnChainID(t, 29104), PieceCID: testPieceCID(t, "cleanup-admission-piece").String(),
		Checksum: content.Checksum, Status: model.StorageCleanupCopyStatusPending,
	}
	if _, err := runtime.db.NewInsert().Model(cleanupCopy).Exec(t.Context()); err != nil {
		t.Fatalf("create cleanup admission copy: %v", err)
	}
	taskRow, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageCleanup, IdempotencyKey: storagecleanup.TaskKey(content.ID, 1),
		Input: storagecleanup.Input{ContentID: content.ID, Generation: 1}, SubjectType: "storage_content", SubjectKey: fmt.Sprint(content.ID),
	})
	if err != nil {
		t.Fatalf("enqueue cleanup admission task: %v", err)
	}
	if err := runtime.repos.StorageCleanup.BindTask(t.Context(), content.ID, 1, taskRow.ID); err != nil {
		t.Fatalf("bind cleanup admission task: %v", err)
	}
	failing := &validateFailureRepository{TaskRepository: runtime.repos.Tasks, err: errors.New("temporary database failure")}
	failing.remaining.Store(1)
	runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: failing, maximum: 1}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "cleanup_not_started" || !runtime.service.Retryable(failed) || len(failed.Checkpoint) != 0 {
		t.Fatalf("cleanup admission task = %#v", failed)
	}
	copies, err := runtime.repos.StorageCleanup.AuthorizeTask(t.Context(), content.ID, 1, taskRow.ID)
	if err != nil || len(copies) != 1 || copies[0].Status != model.StorageCleanupCopyStatusPending || copies[0].ScheduledAt != nil {
		t.Fatalf("cleanup copy after admission failure = %#v, err=%v", copies, err)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("cleanup admission performed %d remote deletions", deleteCalls.Load())
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

type validateFailureRepository struct {
	repository.TaskRepository
	remaining atomic.Int64
	err       error
}

func (r *validateFailureRepository) ValidateClaim(ctx context.Context, id, generation int64) error {
	if r.remaining.Add(-1) >= 0 {
		return r.err
	}
	return r.TaskRepository.ValidateClaim(ctx, id, generation)
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
		BucketID: bucket.ID, ContentSize: 128,
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

func TestInitialPeerPullWaitsForCommittedSourceEvenWithCache(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache:  &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return true }},
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "wait-for-source.bin",
		ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize,
		ETag: "wait-for-source", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Objects.SetVersionCachePresence(t.Context(), version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusRetired).Set("is_current = ?", false).
		Where("id = ?", pipeline.source.StorageDataSetID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	plan := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageTransferPlan)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, plan.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "source"
	})
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != plan.ID {
		t.Fatalf("peer copy after source wait = %#v, err=%v", copyRow, err)
	}
}

func seedReplacementPullTarget(t *testing.T, runtime handlerTestRuntime, cachePresent bool) (*model.StorageCopy, *testutil.MockStorageTarget, int64) {
	t.Helper()
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "migration-pull.bin",
		ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize,
		ETag: "migration-pull", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if cachePresent {
		if err := runtime.repos.Objects.SetVersionCachePresence(t.Context(), version.VersionID, true); err != nil {
			t.Fatal(err)
		}
	}
	replacement, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: pipeline.upload.BucketID, SourceDataSetID: pipeline.source.StorageDataSetID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: testOnChainID(t, 990001), ClientRequestID: "migration-pull-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	dataSetID, clientID := testOnChainID(t, 990002), testOnChainID(t, 990003)
	if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: replacement.TargetDataSetID, ContentID: pipeline.upload.ID,
		DataSetID: dataSetID, ClientDataSetID: &clientID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Replacements.Activate(t.Context(), replacement.ID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(t.Context(), pipeline.upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: replacement.TargetDataSetID, CopyIndex: 0,
		ProviderID: testOnChainID(t, 990001), TransferMethod: model.StorageCopyTransferMethodPeerPull,
	}}); err != nil {
		t.Fatal(err)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyForDataSet(t.Context(), pipeline.upload.ID, replacement.TargetDataSetID)
	if err != nil || copyRow == nil {
		t.Fatalf("replacement target copy = %#v, err=%v", copyRow, err)
	}
	item := &storagereplacement.Item{
		ReplacementID: replacement.ID, ContentID: pipeline.upload.ID,
		TargetDataSetID: replacement.TargetDataSetID, Status: storagereplacement.ItemStatusPending,
	}
	if _, err := runtime.db.NewInsert().Model(item).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	sdkDataSetID := dataSetID.SDK()
	target := &testutil.MockStorageTarget{
		ProviderIDValue:      testOnChainID(t, 990001).SDK(),
		DataSetIDValue:       &sdkDataSetID,
		ClientDataSetIDValue: clientID.SDK(),
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			return []byte{0xaa}, nil
		},
	}
	return copyRow, target, pipeline.source.StorageDataSetID
}

func TestReplacementPullFallsBackOnlyToRetainedCache(t *testing.T) {
	for _, tt := range []struct {
		name         string
		cachePresent bool
		noSource     bool
		pullErr      error
		wantMethod   model.StorageCopyTransferMethod
		wantStatus   model.TaskStatus
		wantReason   string
	}{
		{name: "terminal pull with cache", cachePresent: true, pullErr: &pdp.HTTPError{StatusCode: http.StatusBadRequest}, wantMethod: model.StorageCopyTransferMethodCacheRestore, wantStatus: model.TaskStatusCompleted},
		{name: "terminal pull without cache", pullErr: &pdp.HTTPError{StatusCode: http.StatusBadRequest}, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusFailed, wantReason: "migration_cache_missing"},
		{name: "temporary pull failure", cachePresent: true, pullErr: &pdp.HTTPError{StatusCode: http.StatusServiceUnavailable}, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusPending, wantReason: "provider_confirmation"},
		{name: "no source with cache", cachePresent: true, noSource: true, wantMethod: model.StorageCopyTransferMethodCacheRestore, wantStatus: model.TaskStatusCompleted},
		{name: "no source without cache", noSource: true, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusFailed, wantReason: "migration_cache_missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cacheStore := &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return tt.cachePresent }}
			storageClient := &testutil.MockStorageClient{}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				cache: cacheStore, storage: storageClient, policy: cache.EvictionPolicyNone,
				register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
					return handlers.RegisterStorage(registry)
				},
			})
			copyRow, target, sourceDataSetID := seedReplacementPullTarget(t, runtime, tt.cachePresent)
			if tt.noSource {
				if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).
					Set("status = ?", model.StorageDataSetStatusRetired).Set("is_current = ?", false).
					Where("id = ?", sourceDataSetID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			target.PullFunc = func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
				if tt.noSource {
					t.Error("Pull was called without a source")
				}
				return nil, tt.pullErr
			}
			storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				return target, nil
			}
			taskType := model.TaskTypeStoragePull
			if tt.noSource {
				taskType = model.TaskTypeStorageTransferPlan
			}
			taskRow := bindCopyTask(t, runtime, copyRow, taskType)
			limitedRepos := *runtime.repos
			limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{
				Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
				Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
			}, &limitedRepos, runtime.registry, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runEngine(t, engine)
			defer stopHandlerEngine(t, cancel, done)
			result := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				if task.Status != tt.wantStatus {
					return false
				}
				if tt.wantStatus == model.TaskStatusPending {
					return task.WaitReason != nil && *task.WaitReason == tt.wantReason
				}
				return true
			})
			if tt.wantStatus == model.TaskStatusFailed && (result.FailureReason == nil || *result.FailureReason != tt.wantReason) {
				t.Fatalf("failure reason = %v, want %s", result.FailureReason, tt.wantReason)
			}
			stored, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || stored.TransferMethod != tt.wantMethod {
				t.Fatalf("migration transfer = %#v, err=%v, want %s", stored, err, tt.wantMethod)
			}
			if tt.wantMethod == model.StorageCopyTransferMethodCacheRestore {
				if stored.ActiveTaskID == nil {
					t.Fatal("cache restore Store task was not bound")
				}
				next, err := runtime.repos.Tasks.GetByID(t.Context(), *stored.ActiveTaskID)
				if err != nil || next.Type != model.TaskTypeStorageStore {
					t.Fatalf("cache restore task = %#v, err=%v", next, err)
				}
			}
		})
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

func TestDataSetCreationAdmissionFailureRemainsSafeToRecover(t *testing.T) {
	noRetries := 0
	var createCalls atomic.Int64
	providerID := testOnChainID(t, 29001)
	provider := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			createCalls.Add(1)
			return nil, errors.New("unexpected data set creation")
		},
	}
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return provider, nil
		},
		FindMatchingDataSetFunc: func(context.Context, sdktypes.BigInt, map[string]string, bool) (*storage.DataSetRef, error) {
			return nil, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, maxRetries: &noRetries,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	bucket := &model.Bucket{Name: "data-set-admission", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create admission bucket: %v", err)
	}
	binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("create pending data set: %v", err)
	}
	taskRow, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(binding.ID),
		Input: storagepipeline.DataSetInput{DataSetID: binding.ID}, SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(binding.ID),
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue data set ensure: %v", err)
	}
	failing := &validateFailureRepository{TaskRepository: runtime.repos.Tasks, err: errors.New("temporary database failure")}
	failing.remaining.Store(1)
	limited := &limitedClaimRepository{TaskRepository: failing, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "dataset_creation_not_started" || !runtime.service.Retryable(failed) || len(failed.Checkpoint) != 0 {
		t.Fatalf("data set admission task = %#v", failed)
	}
	stored, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
	if err != nil || stored.Status != model.StorageDataSetStatusPending || stored.CreateTransactionID != nil || stored.CreateStatusURL != nil {
		t.Fatalf("data set after admission failure = %#v, err=%v", stored, err)
	}
	if createCalls.Load() != 0 {
		t.Fatalf("data set creation after admission failure = %d calls", createCalls.Load())
	}
	if err := runtime.service.Retry(t.Context(), taskRow.ID); err != nil {
		t.Fatalf("retry data set admission task: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if createCalls.Load() != 0 {
		t.Fatalf("data set recovery created %d services before returning to execute", createCalls.Load())
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
	var running, maximum, cacheOpens atomic.Int64
	entered := make(chan struct{}, 6)
	release := make(chan struct{})
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		cacheOpens.Add(1)
		return io.NopCloser(strings.NewReader(strings.Repeat("s", 128))), &cache.ObjectInfo{Size: 128}, nil
	}}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, policy: cache.EvictionPolicyNone, concurrency: 8,
		// The holders wait on the provider for as long as the test keeps them. A
		// lease they cannot lose keeps a slow race run from claiming them again
		// mid-wait, which is not what this test is about.
		leaseDuration: 5 * time.Second,
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
			ClientDataSetIDValue: clientID, ServiceURLValue: "https://store-limit.example",
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
	// Tasks beyond the limit give their workers back instead of blocking on the
	// gate, and they neither hash the bytes, checkpoint, nor spend retries.
	yielded := make(map[int64]bool)
	deadline := time.Now().Add(3 * time.Second)
	for len(yielded) != 2 && time.Now().Before(deadline) {
		clear(yielded)
		for _, taskRow := range tasks {
			stored, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
			if err != nil {
				close(release)
				t.Fatalf("load store task: %v", err)
			}
			if stored.Status == model.TaskStatusPending && stored.ResumeMode == model.TaskResumeModeExecute &&
				stored.WaitReason != nil && *stored.WaitReason == "resource" && stored.RetryCount == 0 && len(stored.Checkpoint) == 0 {
				yielded[stored.ID] = true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(yielded) != 2 || cacheOpens.Load() != 8 {
		close(release)
		t.Fatalf("store tasks beyond the limit = yielded:%d cache opens:%d, want 2 yielded and 8 opens", len(yielded), cacheOpens.Load())
	}
	close(release)
	for _, taskRow := range tasks {
		if yielded[taskRow.ID] {
			continue
		}
		waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
			return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
		})
	}
	for id := range yielded {
		if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), id).Exec(t.Context()); err != nil {
			t.Fatalf("wake yielded store task: %v", err)
		}
		waitForTask(t, runtime.repos, id, func(task *model.Task) bool {
			return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
		})
	}
	// Every task read its bytes twice after taking a slot, once to identify them
	// and once to send them. A store whose lease became uncertain while it waited
	// on the provider is claimed again and may read them again, so the total is
	// only a floor.
	if maximum.Load() != 4 || cacheOpens.Load() < 12 {
		t.Fatalf("store provider mutation = maximum:%d cache opens:%d, want 4 and at least 12", maximum.Load(), cacheOpens.Load())
	}
}

func TestStoreAdmissionFailureDoesNotStartUploadOrProgress(t *testing.T) {
	noRetries := 0
	payload := strings.Repeat("s", 128)
	var cacheOpens, storeCalls atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		cacheOpens.Add(1)
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: int64(len(payload))}, nil
	}}
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store-admission.example",
		StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
			storeCalls.Add(1)
			return nil, errors.New("unexpected Store call")
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, maxRetries: &noRetries,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	targetDataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &targetDataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	failing := &validateFailureRepository{TaskRepository: runtime.repos.Tasks, err: errors.New("temporary database failure")}
	failing.remaining.Store(1)
	limited := &limitedClaimRepository{TaskRepository: failing, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "store_not_started" || !runtime.service.Retryable(failed) || len(failed.Checkpoint) != 0 {
		t.Fatalf("store admission task = %#v", failed)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID || copyRow.IngressStoreAttempt != 0 {
		t.Fatalf("copy after store admission failure = %#v, err=%v", copyRow, err)
	}
	if storeCalls.Load() != 0 || cacheOpens.Load() != 0 {
		t.Fatalf("store admission calls = store:%d cache:%d, want 0/0", storeCalls.Load(), cacheOpens.Load())
	}
	if err := runtime.service.Retry(t.Context(), taskRow.ID); err != nil {
		t.Fatalf("retry store admission task: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if storeCalls.Load() != 0 || cacheOpens.Load() != 0 {
		t.Fatalf("store recovery calls = store:%d cache:%d, want no new calls", storeCalls.Load(), cacheOpens.Load())
	}
}

func TestStoreManualRetryRequiresUnsettledRecoveryEvidence(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	checkpoint := json.RawMessage(`{"attempted_at":"2026-09-09T00:00:00Z"}`)
	tests := []struct {
		name       string
		reason     string
		checkpoint json.RawMessage
		want       bool
	}{
		{name: "not started", reason: "store_not_started", want: true},
		{name: "not started with checkpoint", reason: "store_not_started", checkpoint: checkpoint},
		{name: "unknown outcome", reason: "store_outcome_unknown", checkpoint: checkpoint, want: true},
		{name: "unknown outcome without checkpoint", reason: "store_outcome_unknown"},
		{name: "checkpointed context failure", reason: "copy_context_failed", checkpoint: checkpoint, want: true},
		{name: "checkpointed owner missing", reason: "copy_owner_missing", checkpoint: checkpoint, want: true},
		{name: "context failure before checkpoint", reason: "copy_context_failed"},
		{name: "invalid checkpoint", reason: "invalid_checkpoint", checkpoint: checkpoint},
		{name: "store result failure", reason: "store_result_invalid", checkpoint: checkpoint, want: true},
		{name: "presign failure", reason: "commit_presign_failed", checkpoint: checkpoint, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason := tt.reason
			taskRow := &model.Task{
				Type: model.TaskTypeStorageStore, Status: model.TaskStatusFailed,
				FailureReason: &reason, Checkpoint: tt.checkpoint,
			}
			if got := runtime.service.Retryable(taskRow); got != tt.want {
				t.Fatalf("Retryable() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestStoreRecoveryRetriesMissingPieceAfterCheckpoint(t *testing.T) {
	payload := strings.Repeat("parked-store", 12)
	payload = payload[:128]
	info, err := piece.Calculate(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("calculate expected piece identity: %v", err)
	}
	var cacheOpens, storeCalls, parkedCalls atomic.Int64
	var parkedError atomic.Bool
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		cacheOpens.Add(1)
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: int64(len(payload))}, nil
	}}
	var parkedState atomic.Value
	parkedState.Store(synapse.ParkedPieceMissing)
	parked := parkedPieceCheckerFunc(func(_ context.Context, serviceURL string, pieceCID cid.Cid) (synapse.ParkedPieceState, error) {
		parkedCalls.Add(1)
		if serviceURL != "https://parked.example" || !pieceCID.Equals(info.CIDv2) {
			return "", fmt.Errorf("unexpected parked lookup %q %s", serviceURL, pieceCID)
		}
		if parkedError.Load() {
			return "", errors.New("temporary parked-piece lookup failure")
		}
		return parkedState.Load().(synapse.ParkedPieceState), nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://parked.example",
		StoreFunc: func(_ context.Context, reader io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
			storeCalls.Add(1)
			storedBytes, readErr := io.ReadAll(reader)
			if readErr != nil || string(storedBytes) != payload {
				return nil, fmt.Errorf("store reader = %d bytes, err=%v", len(storedBytes), readErr)
			}
			if options == nil || !options.PieceCID.Equals(info.CIDv2) {
				return nil, errors.New("store did not receive the intended PieceCIDv2")
			}
			return nil, errors.New("provider disconnected after accepting the upload")
		},
		PresignForCommitFunc: func(_ context.Context, pieces []storage.PieceInput) ([]byte, error) {
			if len(pieces) != 1 || !pieces[0].PieceCID.Equals(info.CIDv2) {
				return nil, errors.New("presign received the wrong piece")
			}
			return []byte{0xaa, 0xbb}, nil
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, parkedPieces: parked, policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("transfer_method = ?", model.StorageCopyTransferMethodCacheRestore).
		Where("id = ?", pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatalf("set cache restore transfer: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "parked-store.bin",
		ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize, ETag: "parked-store",
		ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatalf("create parked store version: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("piece_cid = ?", info.CIDv2.String()).Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
		t.Fatalf("align parked store content identity: %v", err)
	}
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	targetDataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &targetDataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	limitedRepos := *runtime.repos
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 6}
	limitedRepos.Tasks = limited
	runtime.repos.Tasks = limited
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
	}, &limitedRepos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatalf("new limited task engine: %v", err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)

	recovering := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover && len(task.Checkpoint) > 0
	})
	var checkpoint struct {
		AttemptedAt        time.Time `json:"attempted_at"`
		IntendedPieceCID   string    `json:"intended_piece_cid"`
		ProviderServiceURL string    `json:"provider_service_url"`
	}
	if err := json.Unmarshal(recovering.Checkpoint, &checkpoint); err != nil {
		t.Fatalf("decode store checkpoint: %v", err)
	}
	if checkpoint.IntendedPieceCID != info.CIDv2.String() || checkpoint.ProviderServiceURL != target.ServiceURL() {
		t.Fatalf("store checkpoint = %#v", checkpoint)
	}
	parkedState.Store(synapse.ParkedPieceProcessing)
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("wake processing store recovery: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	parkedError.Store(true)
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("wake failed store lookup: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 3 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	parkedError.Store(false)
	parkedState.Store(synapse.ParkedPieceMissing)
	checkpoint.AttemptedAt = time.Now().Add(-31 * time.Minute)
	checkpointJSON, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("encode old store checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`, checkpointJSON, taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("age store checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("wake store recovery: %v", err)
	}
	retrying := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 5 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if retrying.RetryCount != 1 || storeCalls.Load() != 2 {
		t.Fatalf("retransmitted store = retry count:%d calls:%d, want 1/2", retrying.RetryCount, storeCalls.Load())
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
		t.Fatalf("unknown store copy = %#v, err=%v", copyRow, err)
	}
	parkedState.Store(synapse.ParkedPieceReady)
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("wake retransmitted store recovery: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if storeCalls.Load() != 2 || cacheOpens.Load() != 4 || parkedCalls.Load() < 5 {
		t.Fatalf("store recovery calls = store:%d cache:%d parked:%d", storeCalls.Load(), cacheOpens.Load(), parkedCalls.Load())
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
	// A finished transfer schedules registration directly.
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow == nil || copyRow.ActiveTaskID == nil {
		t.Fatalf("target copy after pull = %#v, err=%v", copyRow, err)
	}
	next, err := runtime.repos.Tasks.GetByID(t.Context(), *copyRow.ActiveTaskID)
	if err != nil || next == nil || next.Type != model.TaskTypeStorageCommit {
		t.Fatalf("task after pull = %#v, err=%v, want storage_commit", next, err)
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
	// The source still holds a readable copy, so the content itself is fine.
	if content, err := runtime.repos.Contents.GetByID(t.Context(), pipeline.upload.ID); err != nil || content.ErrorMessage != nil {
		t.Fatalf("content after a failed copy next to a readable one = %#v, err=%v, want it not flagged", content, err)
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

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if runtime.service.Retryable(failed) {
		t.Fatal("wallet task with an uncertain broadcast is unexpectedly retryable")
	}
	if broadcasts.Load() != 1 {
		t.Fatalf("wallet broadcasts = %d, want 1", broadcasts.Load())
	}
	stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || stored.Status != model.WalletOperationStatusUnknown {
		t.Fatalf("wallet operation = %#v, err=%v, want unknown", stored, err)
	}
}

func TestWalletAdmissionFailureRemainsSafeToRecover(t *testing.T) {
	limit := 0
	var broadcasts atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		maxRetries: &limit,
		wallet: testWalletOperator{fund: func(context.Context, *big.Int) (string, error) {
			broadcasts.Add(1)
			return "", errors.New("unexpected wallet broadcast")
		}},
	})
	operation, taskRow := seedWalletTask(t, runtime, "wallet-admission-failure")
	failing := &validateFailureRepository{TaskRepository: runtime.repos.Tasks, err: errors.New("temporary database failure")}
	failing.remaining.Store(1)
	limited := &limitedClaimRepository{TaskRepository: failing, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "wallet_broadcast_not_started" || !runtime.service.Retryable(failed) {
		t.Fatalf("wallet admission task = %#v", failed)
	}
	storedOperation, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || storedOperation.Status != model.WalletOperationStatusPending || storedOperation.TaskID == nil || *storedOperation.TaskID != taskRow.ID || storedOperation.BroadcastAttemptedAt != nil {
		t.Fatalf("wallet operation after admission failure = %#v, err=%v", storedOperation, err)
	}
	if len(failed.Checkpoint) != 0 || broadcasts.Load() != 0 {
		t.Fatalf("wallet admission wrote evidence or called effect: checkpoint=%s broadcasts=%d", failed.Checkpoint, broadcasts.Load())
	}
	if err := runtime.service.Retry(t.Context(), taskRow.ID); err != nil {
		t.Fatalf("retry not-started wallet task: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if broadcasts.Load() != 0 {
		t.Fatalf("wallet recovery broadcast %d times before returning to execute", broadcasts.Load())
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

// A migration task that stopped but can still be retried keeps the replacement
// waiting for that retry; only a task that cannot be retried fails it.
func TestReplacementWaitsForARetryableFailedMigrationTask(t *testing.T) {
	tests := []struct {
		name              string
		reason            string
		checkpoint        string
		wantFailed        bool
		wantFenceReleased bool
	}{
		{name: "retryable", reason: "store_outcome_unknown", checkpoint: `{"attempted_at":"2026-09-11T00:00:00Z"}`},
		{name: "not retryable", reason: "store_result_invalid", wantFailed: true},
		{name: "missing migration cache", reason: "migration_cache_missing", wantFailed: true, wantFenceReleased: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
					if err := handlers.RegisterStorage(registry); err != nil {
						return err
					}
					return handlers.RegisterReplacement(registry)
				},
			})
			ctx := t.Context()
			sequence := storedObjectSequence.Add(1)
			id := func(offset int64) idtypes.OnChainID { return testOnChainID(t, 36000+sequence*10+offset) }
			bucket := &model.Bucket{
				Name: fmt.Sprintf("replacement-copy-retry-%d", sequence), Status: model.BucketStatusActive,
				DefaultCopies: 1, MinimumDurableCopies: 1,
			}
			if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
				t.Fatalf("create bucket: %v", err)
			}
			content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
				BucketID: bucket.ID, ContentSize: 11, Checksum: testutil.StorageChecksum(bucket.Name), RequestedCopies: 1,
			})
			if err != nil {
				t.Fatalf("ensure content: %v", err)
			}
			version := &model.ObjectVersion{
				VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "migrating.bin", Size: content.ContentSize,
				ETag: bucket.Name, ContentType: "application/octet-stream", ContentID: &content.ID,
			}
			if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
				t.Fatalf("create object version: %v", err)
			}
			source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: id(1), CopyIndex: 0, CreatedByContentID: content.ID,
			})
			if err != nil {
				t.Fatalf("create source: %v", err)
			}
			sourceClientID := id(3)
			if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID: source.ID, ContentID: content.ID, DataSetID: id(2), ClientDataSetID: &sourceClientID,
			}); err != nil {
				t.Fatalf("mark source ready: %v", err)
			}
			if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
				StorageDataSetID: source.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: id(1),
			}}); err != nil {
				t.Fatalf("create source copy: %v", err)
			}
			pieceID := id(4)
			testutil.CommitStorageCopy(t, runtime.db, runtime.repos, repository.MarkUploadCopyCommittedInput{
				ContentID: content.ID, CopyIndex: 0, PieceCID: testPieceCID(t, bucket.Name).String(),
				PieceID: &pieceID, RetrievalURL: "https://source.example/piece",
			})
			replacement, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: id(5), ClientRequestID: bucket.Name,
			})
			if err != nil {
				t.Fatalf("authorize replacement: %v", err)
			}
			targetClientID := id(7)
			if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID: replacement.TargetDataSetID, ContentID: content.ID, DataSetID: id(6), ClientDataSetID: &targetClientID,
			}); err != nil {
				t.Fatalf("mark target ready: %v", err)
			}
			if err := runtime.repos.Replacements.Activate(ctx, replacement.ID); err != nil {
				t.Fatalf("activate replacement: %v", err)
			}
			if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
				StorageDataSetID: replacement.TargetDataSetID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: id(5),
			}}); err != nil {
				t.Fatalf("create target copy: %v", err)
			}
			targetCopy, err := runtime.repos.Contents.GetUploadCopyForDataSet(ctx, content.ID, replacement.TargetDataSetID)
			if err != nil || targetCopy == nil {
				t.Fatalf("load target copy = %#v, err=%v", targetCopy, err)
			}
			item := &storagereplacement.Item{
				ReplacementID: replacement.ID, ContentID: content.ID, TargetDataSetID: replacement.TargetDataSetID,
				Status: storagereplacement.ItemStatusPending,
			}
			if _, err := runtime.db.NewInsert().Model(item).Exec(ctx); err != nil {
				t.Fatalf("insert replacement item: %v", err)
			}
			if _, err := runtime.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
				Set("seeding_complete = ?", true).Set("items_total = ?", 1).
				Where("id = ?", replacement.ID).Exec(ctx); err != nil {
				t.Fatalf("complete replacement seeding: %v", err)
			}
			// The migration task for the target copy has stopped.
			storeTask := bindCopyTask(t, runtime, targetCopy, model.TaskTypeStorageStore)
			claimed, err := runtime.repos.Tasks.ClaimNext(ctx, time.Minute)
			if err != nil || claimed == nil || claimed.ID != storeTask.ID {
				t.Fatalf("claim migration task = %#v, err=%v", claimed, err)
			}
			if tt.checkpoint != "" {
				if err := runtime.repos.Tasks.WriteCheckpoint(ctx, claimed.ID, claimed.ClaimGeneration, json.RawMessage(tt.checkpoint)); err != nil {
					t.Fatalf("write migration checkpoint: %v", err)
				}
			}
			reason, message := tt.reason, "migration stopped"
			if err := runtime.repos.Tasks.Settle(ctx, claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
				Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason, LastError: &message,
			}); err != nil {
				t.Fatalf("fail migration task: %v", err)
			}
			coordinateTask, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
				Type:           model.TaskTypeProviderReplacementCoordinate,
				IdempotencyKey: storagereplacement.CoordinateTaskKey(replacement.ID, replacement.TaskGeneration),
				Input:          storagereplacement.CoordinateInput{ReplacementID: replacement.ID, Generation: replacement.TaskGeneration},
				SubjectType:    "storage_replacement", SubjectKey: fmt.Sprint(replacement.ID),
			}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
				return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, taskRow.ID)
			})
			if err != nil {
				t.Fatalf("enqueue replacement coordinator: %v", err)
			}

			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			if tt.wantFailed {
				failed := waitForTask(t, runtime.repos, coordinateTask.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusFailed
				})
				stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
				if failed.FailureReason == nil || *failed.FailureReason != "replacement_copy_failed" || err != nil || stored.Status != storagereplacement.StatusFailed {
					t.Fatalf("coordinator = %#v, replacement = %#v err=%v, want the replacement failed", failed, stored, err)
				}
				copyAfterFailure, err := runtime.repos.Contents.GetUploadCopyByID(ctx, targetCopy.ID)
				if err != nil || (copyAfterFailure.ActiveTaskID == nil) != tt.wantFenceReleased {
					t.Fatalf("copy fence after attention = %#v, err=%v, want released=%v", copyAfterFailure, err, tt.wantFenceReleased)
				}
				return
			}
			waitForTask(t, runtime.repos, coordinateTask.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusPending && task.StatusMessage != nil &&
					*task.StatusMessage == "Waiting for a failed migration task to be retried"
			})
			stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored.Status == storagereplacement.StatusFailed {
				t.Fatalf("replacement = %#v err=%v, want it waiting for the migration retry", stored, err)
			}
		})
	}
}

func TestRetirementAdmissionFailureDoesNotEnterCleanupAttention(t *testing.T) {
	noRetries := 0
	terminator := &testServiceTerminator{err: errors.New("unexpected termination")}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		maxRetries: &noRetries, terminator: terminator, epochs: testEpochReader{epoch: 80},
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterReplacement(registry)
		},
	})
	ctx := t.Context()
	bucket := &model.Bucket{Name: "retirement-admission", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create retirement admission bucket: %v", err)
	}
	source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: testOnChainID(t, 29201), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("create retirement admission source: %v", err)
	}
	sourceClientID := testOnChainID(t, 29202)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: testOnChainID(t, 29203), ClientDataSetID: &sourceClientID,
	}); err != nil {
		t.Fatalf("mark retirement admission source ready: %v", err)
	}
	first, created, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: testOnChainID(t, 29204), ClientRequestID: "retirement-admission-first",
	})
	if err != nil || !created {
		t.Fatalf("authorize retirement admission replacement = %#v, created=%v, err=%v", first, created, err)
	}
	target, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, first.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("load retirement admission target = %#v, err=%v", target, err)
	}
	targetClientID := testOnChainID(t, 29205)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: target.ID, DataSetID: testOnChainID(t, 29206), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark retirement admission target ready: %v", err)
	}
	if _, created, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: testOnChainID(t, 29207), ClientRequestID: "retirement-admission-successor",
	}); err != nil || !created {
		t.Fatalf("authorize retirement admission successor: created=%v err=%v", created, err)
	}
	first, err = runtime.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || first.Status != storagereplacement.StatusSuperseded {
		t.Fatalf("superseded retirement admission replacement = %#v, err=%v", first, err)
	}
	generation, err := runtime.repos.Contents.NextDataSetRetirementGeneration(ctx, target.ID)
	if err != nil {
		t.Fatalf("next retirement admission generation: %v", err)
	}
	taskRow, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetRetire, IdempotencyKey: storagereplacement.RetireTaskKey(target.ID, generation),
		Input:       storagereplacement.RetireInput{ReplacementID: first.ID, DataSetID: target.ID, Generation: generation},
		SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(target.ID),
	})
	if err != nil {
		t.Fatalf("enqueue retirement admission task: %v", err)
	}
	if err := runtime.repos.Contents.BindDataSetRetirementTask(ctx, target.ID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind retirement admission task: %v", err)
	}
	failing := &validateFailureRepository{TaskRepository: runtime.repos.Tasks, err: errors.New("temporary database failure")}
	failing.remaining.Store(1)
	limited := &limitedClaimRepository{TaskRepository: failing, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "termination_not_started" || !runtime.service.Retryable(failed) || len(failed.Checkpoint) != 0 {
		t.Fatalf("retirement admission task = %#v", failed)
	}
	stored, err := runtime.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || stored.Status != storagereplacement.StatusSuperseded || stored.AbandonedTerminationEpoch != nil || stored.LastError != nil {
		t.Fatalf("replacement after retirement admission failure = %#v, err=%v", stored, err)
	}
	if terminator.calls.Load() != 0 {
		t.Fatalf("retirement admission called terminator %d times", terminator.calls.Load())
	}
	if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
		t.Fatalf("retry retirement admission task: %v", err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if terminator.calls.Load() != 0 {
		t.Fatalf("retirement recovery terminated service %d times before returning to execute", terminator.calls.Load())
	}
}

type sequencedTerminator struct {
	calls   atomic.Int64
	respond func(call int64) (*synapse.TerminationResult, error)
}

func (s *sequencedTerminator) TerminateService(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
	return s.respond(s.calls.Add(1))
}

func (s *sequencedTerminator) ContextIdentity() storage.ContextIdentity {
	return testutil.DefaultContextIdentity
}

func (s *sequencedTerminator) VerifyServicePayer(context.Context, sdktypes.BigInt) error {
	return nil
}

// seedAbandonedTargetRetirement leaves the ready target of a superseded
// replacement with a bound retirement task.
func seedAbandonedTargetRetirement(t *testing.T, runtime handlerTestRuntime, sequence int64) (*storagereplacement.Replacement, *model.Task) {
	t.Helper()
	ctx := t.Context()
	bucket := &model.Bucket{Name: fmt.Sprintf("abandoned-target-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	id := func(offset int64) idtypes.OnChainID { return testOnChainID(t, 35000+sequence*10+offset) }
	source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: id(1), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	sourceClientID := id(2)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: id(3), ClientDataSetID: &sourceClientID,
	}); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	first, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: id(4), ClientRequestID: fmt.Sprintf("abandoned-first-%d", sequence),
	})
	if err != nil {
		t.Fatalf("authorize replacement: %v", err)
	}
	targetClientID := id(5)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: first.TargetDataSetID, DataSetID: id(6), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark target ready: %v", err)
	}
	if _, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: id(7), ClientRequestID: fmt.Sprintf("abandoned-successor-%d", sequence),
	}); err != nil {
		t.Fatalf("authorize successor: %v", err)
	}
	generation, err := runtime.repos.Contents.NextDataSetRetirementGeneration(ctx, first.TargetDataSetID)
	if err != nil {
		t.Fatalf("next retirement generation: %v", err)
	}
	taskRow, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetRetire, IdempotencyKey: storagereplacement.RetireTaskKey(first.TargetDataSetID, generation),
		Input:       storagereplacement.RetireInput{ReplacementID: first.ID, DataSetID: first.TargetDataSetID, Generation: generation},
		SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(first.TargetDataSetID),
	})
	if err != nil {
		t.Fatalf("enqueue retirement: %v", err)
	}
	if err := runtime.repos.Contents.BindDataSetRetirementTask(ctx, first.TargetDataSetID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind retirement: %v", err)
	}
	return first, taskRow
}

// A termination whose outcome was not observed is not given up on. The next
// request goes out only after the adapter has read the chain, so it cannot end
// the service twice.
func TestRetirementWithUnobservedTerminationTriesAgain(t *testing.T) {
	terminator := &sequencedTerminator{respond: func(call int64) (*synapse.TerminationResult, error) {
		if call == 1 {
			return nil, errors.New("provider relay failed after submitting")
		}
		return &synapse.TerminationResult{TxHash: "0xretired", EndEpoch: 84}, nil
	}}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		terminator: terminator, epochs: testEpochReader{epoch: 90},
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterReplacement(registry)
		},
	})
	replacement, taskRow := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return terminator.calls.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	wakeTask(t, runtime, taskRow.ID)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return terminator.calls.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	wakeTask(t, runtime, taskRow.ID)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	stored, err := runtime.repos.Replacements.GetByID(t.Context(), replacement.ID)
	if err != nil || stored.AbandonedTerminationEpoch == nil || *stored.AbandonedTerminationEpoch != 84 || terminator.calls.Load() != 2 {
		t.Fatalf("replacement = %#v err=%v after %d requests, want the returned end epoch recorded", stored, err, terminator.calls.Load())
	}
}

// Payment debt is the operator's to settle, so it stops the task instead of
// being retried, and a retry stays available for after it is settled.
func TestRetirementBlockedByPaymentDebtWaitsForTheOperator(t *testing.T) {
	terminator := &sequencedTerminator{respond: func(int64) (*synapse.TerminationResult, error) {
		return nil, &synapse.TerminationBlockedError{Reason: "payment_debt", Shortfall: big.NewInt(42)}
	}}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		terminator: terminator, epochs: testEpochReader{epoch: 90},
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterReplacement(registry)
		},
	})
	_, taskRow := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "termination_blocked" || !runtime.service.Retryable(failed) || terminator.calls.Load() != 1 {
		t.Fatalf("blocked retirement = %#v after %d requests, want a retryable termination_blocked failure", failed, terminator.calls.Load())
	}
}

// seedSourceRetirement leaves a replaced source with a bound retirement task,
// its slot already handed to the target and the safety gate clear.
func seedSourceRetirement(t *testing.T, runtime handlerTestRuntime, sequence int64) (*storagereplacement.Replacement, *model.Task) {
	t.Helper()
	ctx := t.Context()
	bucket := &model.Bucket{Name: fmt.Sprintf("replaced-source-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	id := func(offset int64) idtypes.OnChainID { return testOnChainID(t, 36000+sequence*10+offset) }
	source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: id(1), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	sourceClientID := id(2)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: id(3), ClientDataSetID: &sourceClientID,
	}); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	replacement, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: id(4), ClientRequestID: fmt.Sprintf("replaced-source-%d", sequence),
	})
	if err != nil {
		t.Fatalf("authorize replacement: %v", err)
	}
	targetClientID := id(5)
	if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: replacement.TargetDataSetID, DataSetID: id(6), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark target ready: %v", err)
	}
	if err := runtime.repos.Replacements.Activate(ctx, replacement.ID); err != nil {
		t.Fatalf("activate replacement: %v", err)
	}
	if err := runtime.repos.Replacements.BeginRetirement(ctx, replacement.ID); err != nil {
		t.Fatalf("begin retirement: %v", err)
	}
	generation, err := runtime.repos.Contents.NextDataSetRetirementGeneration(ctx, source.ID)
	if err != nil {
		t.Fatalf("next retirement generation: %v", err)
	}
	taskRow, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetRetire, IdempotencyKey: storagereplacement.RetireTaskKey(source.ID, generation),
		Input:       storagereplacement.RetireInput{ReplacementID: replacement.ID, DataSetID: source.ID, Generation: generation},
		SubjectType: "storage_data_set", SubjectKey: fmt.Sprint(source.ID),
	})
	if err != nil {
		t.Fatalf("enqueue retirement: %v", err)
	}
	if err := runtime.repos.Contents.BindDataSetRetirementTask(ctx, source.ID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind retirement: %v", err)
	}
	return replacement, taskRow
}

// A data set ID is a number on one chain, paid for by one wallet. If either
// moved after a termination request went out, the recorded end epoch read here
// would describe someone else's service, so the task reads nothing, sends
// nothing and stops for an operator.
func TestRetirementStopsWhenTheSigningIdentityChanged(t *testing.T) {
	movedIdentity := testutil.DefaultContextIdentity
	movedIdentity.ChainID = testutil.DefaultContextIdentity.ChainID + 1
	tests := []struct {
		name       string
		seed       func(*testing.T, handlerTestRuntime, int64) (*storagereplacement.Replacement, *model.Task)
		wantStatus storagereplacement.Status
	}{
		// The replacement is still live, so it must show up as needing attention.
		{name: "replaced source", seed: seedSourceRetirement, wantStatus: storagereplacement.StatusCleanupAttention},
		// A superseded replacement is already finished; only the task stops.
		{name: "abandoned target", seed: seedAbandonedTargetRetirement, wantStatus: storagereplacement.StatusSuperseded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			terminator := &testServiceTerminator{identity: movedIdentity, result: &synapse.TerminationResult{TxHash: "0xretire", EndEpoch: 84}}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				terminator: terminator, epochs: testEpochReader{epoch: 90},
				register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
					return handlers.RegisterReplacement(registry)
				},
			})
			ctx := t.Context()
			replacement, taskRow := tt.seed(t, runtime, storedObjectSequence.Add(1))
			// One request went out under the original identity, and its outcome
			// was never observed.
			checkpoint, err := json.Marshal(map[string]any{
				"attempted_at": time.Now().UTC().Add(-time.Hour),
				"identity":     testutil.DefaultContextIdentity, "sends": 1,
			})
			if err != nil {
				t.Fatalf("encode checkpoint: %v", err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`, string(checkpoint), taskRow.ID).Exec(ctx); err != nil {
				t.Fatalf("write checkpoint: %v", err)
			}

			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed
			})
			if failed.FailureReason == nil || *failed.FailureReason != "termination_identity_changed" || terminator.calls.Load() != 0 {
				t.Fatalf("failure = %v after %d termination requests, want termination_identity_changed and no request",
					failed.FailureReason, terminator.calls.Load())
			}
			stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored == nil || stored.Status != tt.wantStatus {
				t.Fatalf("replacement = %#v err=%v, want status %s", stored, err, tt.wantStatus)
			}
		})
	}
}

// A data set another wallet pays for is never terminated. Ownership is read
// before anything is recorded, so once the original wallet is back a retry goes
// ahead instead of stopping on the identity of a request that was never sent.
func TestRetirementStopsOnADataSetAnotherWalletPaysFor(t *testing.T) {
	tests := []struct {
		name       string
		seed       func(*testing.T, handlerTestRuntime, int64) (*storagereplacement.Replacement, *model.Task)
		wantStatus storagereplacement.Status
	}{
		{name: "replaced source", seed: seedSourceRetirement, wantStatus: storagereplacement.StatusCleanupAttention},
		{name: "abandoned target", seed: seedAbandonedTargetRetirement, wantStatus: storagereplacement.StatusSuperseded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			terminator := &testServiceTerminator{result: &synapse.TerminationResult{TxHash: "0xretire", EndEpoch: 84}}
			terminator.notPaidFor.Store(true)
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				terminator: terminator, epochs: testEpochReader{epoch: 90},
				register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
					return handlers.RegisterReplacement(registry)
				},
			})
			ctx := t.Context()
			replacement, taskRow := tt.seed(t, runtime, storedObjectSequence.Add(1))

			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed
			})
			if failed.FailureReason == nil || *failed.FailureReason != "termination_payer_mismatch" ||
				terminator.calls.Load() != 0 || len(failed.Checkpoint) != 0 {
				t.Fatalf("failure = %v after %d termination requests with checkpoint %s, want termination_payer_mismatch with nothing sent or recorded",
					failed.FailureReason, terminator.calls.Load(), failed.Checkpoint)
			}
			stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored == nil || stored.Status != tt.wantStatus {
				t.Fatalf("replacement = %#v err=%v, want status %s", stored, err, tt.wantStatus)
			}

			// Retrying before the wallet is fixed stops the same way, instead of
			// leaving the task running against a replacement that already waits
			// for an operator.
			if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
				t.Fatalf("retry retirement: %v", err)
			}
			refused := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed
			})
			if refused.FailureReason == nil || *refused.FailureReason != "termination_payer_mismatch" || terminator.calls.Load() != 0 {
				t.Fatalf("second failure = %v after %d termination requests, want termination_payer_mismatch and none",
					refused.FailureReason, terminator.calls.Load())
			}

			// The original wallet is back, and the retirement runs to completion.
			terminator.notPaidFor.Store(false)
			if err := runtime.service.Retry(ctx, taskRow.ID); err != nil {
				t.Fatalf("retry retirement: %v", err)
			}
			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return terminator.calls.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
			})
			wakeTask(t, runtime, taskRow.ID)
			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusCompleted
			})
			wantFinished := storagereplacement.StatusCompleted
			if tt.wantStatus == storagereplacement.StatusSuperseded {
				wantFinished = storagereplacement.StatusSuperseded
			}
			finished, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || finished == nil || finished.Status != wantFinished || terminator.calls.Load() != 1 {
				t.Fatalf("replacement = %#v err=%v after %d termination requests, want status %s after one",
					finished, err, terminator.calls.Load(), wantFinished)
			}
		})
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
		WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
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
	// The only request sent for this ID was submitted, and the task resumes by
	// waiting on that submission.
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
	checkpoint, err := json.Marshal(map[string]any{
		"attempted_at": time.Now().UTC(), "client_data_set_id": clientDataSetID.String(),
		"identity": testutil.DefaultContextIdentity, "sends": 1,
		"transaction_id": "0xrejected", "status_url": "https://provider.example/status",
	})
	if err != nil {
		t.Fatalf("encode checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`, string(checkpoint), ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if runtime.service.Retryable(failed) {
		t.Fatal("a rejected creation offers a retry that could only fail again")
	}

	ended, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || ended == nil || ended.Status != model.StorageDataSetStatusRetired || ended.EnsureTaskID != nil {
		t.Fatalf("rejected generation = %#v err=%v, want it retired rather than deleted, with its creation fence released", ended, err)
	}
	// The provider is what retirement buys back.
	reused, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil || reused == nil || reused.ID == binding.ID {
		t.Fatalf("rebinding the freed provider = %#v err=%v", reused, err)
	}
}

type dataSetEnsureFixture struct {
	binding    *model.StorageDataSet
	ensureTask *model.Task
}

// seedDataSetEnsure prepares a pending generation with one bound copy and its
// ensure task, the state a first upload to a new provider leaves behind.
func seedDataSetEnsure(t *testing.T, runtime handlerTestRuntime, providerID idtypes.OnChainID, name string) dataSetEnsureFixture {
	t.Helper()
	ctx := t.Context()
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 11, Checksum: testutil.StorageChecksum(name), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("ensure content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: name + ".bin", ContentID: &content.ID,
		Size: 11, ETag: name, ContentType: "application/octet-stream",
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
	return dataSetEnsureFixture{binding: binding, ensureTask: ensureTask}
}

func wakeTask(t *testing.T, runtime handlerTestRuntime, taskID int64) {
	t.Helper()
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskID).Exec(t.Context()); err != nil {
		t.Fatalf("wake task %d: %v", taskID, err)
	}
}

// A create request whose outcome was lost is never given up on: the chain is
// checked for its client data set ID, the same ID is sent again while nothing
// is visible, and a later rejection is not taken as proof that the first
// request failed.
func TestDataSetCreationWithUnobservedOutcomeResendsTheSameID(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 31000+sequence)
	createdID := sdktypes.NewBigInt(uint64(32000 + sequence))
	var (
		mu      sync.Mutex
		sentIDs []string
		landed  atomic.Bool
	)
	sends := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(sentIDs)
	}
	target := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		CreateDataSetFunc: func(ctx context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			mu.Lock()
			sentIDs = append(sentIDs, opts.ClientDataSetID.String())
			first := len(sentIDs) == 1
			mu.Unlock()
			if first {
				return nil, errors.New("connection reset by provider")
			}
			opts.OnSubmitted(storage.CreateDataSetSubmission{
				ProviderID: providerID.SDK(), TransactionID: "0x" + strings.Repeat("ab", 32),
				StatusURL: "https://provider.example/pdp/data-sets/created/2", ClientDataSetID: opts.ClientDataSetID.Copy(),
			})
			<-ctx.Done()
			return nil, ctx.Err()
		},
		// The resent request loses to the first one, which did land after all.
		WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
			landed.Store(true)
			return nil, pdp.ErrTxRejected
		},
		FindDataSetByClientIDFunc: func(_ context.Context, clientID sdktypes.BigInt) (storage.DataSetRef, bool, error) {
			if !landed.Load() {
				return storage.DataSetRef{}, false, nil
			}
			ref, err := storage.NewDataSetRef(providerID.SDK(), createdID, clientID)
			return ref, err == nil, err
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{
			OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return target, nil
			},
		},
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("unobserved-%d", sequence))
	ctx := t.Context()
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
		return sends() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	recorded, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
	if err != nil || recorded.ClientDataSetID == nil || recorded.ClientDataSetID.String() != sentIDs[0] {
		t.Fatalf("generation after the first request = %#v err=%v, want its client data set ID recorded", recorded, err)
	}

	// Nothing is visible once the request has had time to land.
	stored, err := runtime.repos.Tasks.GetByID(ctx, fixture.ensureTask.ID)
	if err != nil {
		t.Fatalf("load ensure task: %v", err)
	}
	var checkpoint map[string]any
	if err := json.Unmarshal(stored.Checkpoint, &checkpoint); err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	checkpoint["attempted_at"] = time.Now().UTC().Add(-time.Hour)
	aged, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("encode checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`, string(aged), fixture.ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("age checkpoint: %v", err)
	}
	wakeTask(t, runtime, fixture.ensureTask.ID)
	waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
		return sends() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})

	wakeTask(t, runtime, fixture.ensureTask.ID)
	waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if sends() != 2 || sentIDs[0] != sentIDs[1] {
		t.Fatalf("create requests = %v, want the same client data set ID sent twice", sentIDs)
	}
	ready, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
	if err != nil || ready.Status != model.StorageDataSetStatusReady || ready.DataSetID == nil ||
		ready.DataSetID.String() != createdID.String() || ready.ClientDataSetID.String() != sentIDs[0] {
		t.Fatalf("generation = %#v err=%v, want the data set the first request created", ready, err)
	}
}

// A request is only looked up, waited on, or resent under the identity it was
// signed for, and an ID the chain resolves to some other data set is never used
// again. Both stop the task; only the conflict, which the chain settled, gives
// the generation up.
func TestDataSetCreationRecoveryStopsOnChangedIdentityOrConflict(t *testing.T) {
	otherIdentity := testutil.DefaultContextIdentity
	otherIdentity.Payer = common.HexToAddress("0x00000000000000000000000000000000000000c3")
	tests := []struct {
		name             string
		identity         storage.ContextIdentity
		submission       bool
		lookup           func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error)
		reason           string
		status           model.StorageDataSetStatus
		fenceHeld        bool
		retryable        bool
		incompleteCopies int
	}{
		{
			// Only this context cannot look the ID up. The data set the original
			// identity asked for may exist and be billing, so the generation, its
			// copies and its fence are all kept for an operator who restores the
			// configuration and retries.
			name: "changed identity", identity: otherIdentity, reason: "dataset_identity_changed",
			status: model.StorageDataSetStatusPending, fenceHeld: true, retryable: true, incompleteCopies: 1,
		},
		{
			// A recorded submission is no exception. Its status would report the
			// data set the original wallet's request created, which the wallet
			// signing now does not pay for.
			name: "changed identity after the submission was recorded", identity: otherIdentity, submission: true,
			reason: "dataset_identity_changed", status: model.StorageDataSetStatusCreating,
			fenceHeld: true, retryable: true, incompleteCopies: 1,
		},
		{
			// The chain resolved the ID to a record that is not ours, and said so
			// on two reads. Nothing will ever serve these copies.
			name: "correlation conflict", reason: "dataset_correlation_conflict",
			status: model.StorageDataSetStatusRetired, fenceHeld: false, retryable: false, incompleteCopies: 0,
			lookup: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
				return storage.DataSetRef{}, false, fmt.Errorf("lookup: %w", storage.ErrDataSetCorrelationConflict)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID := testOnChainID(t, 33000+sequence)
			clientID := testOnChainID(t, 34000+sequence)
			createdID := testOnChainID(t, 38000+sequence)
			var sends, waits atomic.Int64
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(), ContextIdentityValue: tt.identity, FindDataSetByClientIDFunc: tt.lookup,
				CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					sends.Add(1)
					return nil, errors.New("unexpected create request")
				},
				WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					waits.Add(1)
					// What the provider would report: the data set the original
					// wallet's request created.
					ref, err := storage.NewDataSetRef(providerID.SDK(), createdID.SDK(), clientID.SDK())
					if err != nil {
						return nil, err
					}
					return &storage.CreateDataSetResult{DataSet: ref}, nil
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						return target, nil
					},
				},
				policy: cache.EvictionPolicyNone,
				register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
					return handlers.RegisterStorage(registry)
				},
			})
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("recovery-stop-%d", sequence))
			ctx := t.Context()
			// One request was sent long ago under the original identity.
			if err := runtime.repos.Contents.RecordDataSetClientID(ctx, fixture.binding.ID, clientID); err != nil {
				t.Fatalf("record client data set ID: %v", err)
			}
			recorded := map[string]any{
				"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(),
				"identity": testutil.DefaultContextIdentity, "sends": 1,
			}
			if tt.submission {
				const transactionID, statusURL = "0xsubmitted", "https://provider.example/status"
				if err := runtime.repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
					ID: fixture.binding.ID, TransactionID: transactionID, StatusURL: statusURL, ClientDataSetID: &clientID,
				}); err != nil {
					t.Fatalf("mark creating: %v", err)
				}
				recorded["transaction_id"], recorded["status_url"] = transactionID, statusURL
			}
			checkpoint, err := json.Marshal(recorded)
			if err != nil {
				t.Fatalf("encode checkpoint: %v", err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`, string(checkpoint), fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatalf("write checkpoint: %v", err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = ? WHERE id = ?`, model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatalf("resume in recovery: %v", err)
			}

			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			failed := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed
			})
			if failed.FailureReason == nil || *failed.FailureReason != tt.reason || sends.Load() != 0 || waits.Load() != 0 {
				t.Fatalf("failure = %v after %d create requests and %d waits, want %s with neither",
					failed.FailureReason, sends.Load(), waits.Load(), tt.reason)
			}
			if runtime.service.Retryable(failed) != tt.retryable {
				t.Fatalf("retryable = %v, want %v", runtime.service.Retryable(failed), tt.retryable)
			}
			kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
			if err != nil || kept.Status != tt.status || (kept.EnsureTaskID != nil) != tt.fenceHeld {
				t.Fatalf("generation = %#v err=%v, want status %s with fence held=%v", kept, err, tt.status, tt.fenceHeld)
			}
			incomplete, err := runtime.repos.Contents.ListIncompleteCopiesForDataSet(ctx, fixture.binding.ID)
			if err != nil || len(incomplete) != tt.incompleteCopies {
				t.Fatalf("incomplete copies = %#v err=%v, want %d", incomplete, err, tt.incompleteCopies)
			}
		})
	}
}

// TestDataSetCreationRecoversFromAnUnusableCheckpointThroughTheRow checks that a
// checkpoint that can no longer name its request is resolved through the ID the
// row recorded before the provider call, and that nothing is sent when the chain
// holds nothing under it.
func TestDataSetCreationRecoversFromAnUnusableCheckpointThroughTheRow(t *testing.T) {
	for _, tt := range []struct {
		name      string
		found     bool
		status    model.StorageDataSetStatus
		fenceHeld bool
	}{
		{name: "chain holds the data set", found: true, status: model.StorageDataSetStatusReady, fenceHeld: false},
		{name: "chain holds nothing", found: false, status: model.StorageDataSetStatusPending, fenceHeld: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID := testOnChainID(t, 41000+sequence)
			clientID := testOnChainID(t, 42000+sequence)
			createdID := testOnChainID(t, 43000+sequence)
			var sends, lookups atomic.Int64
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				FindDataSetByClientIDFunc: func(_ context.Context, asked sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					lookups.Add(1)
					if asked.String() != clientID.SDK().String() {
						t.Errorf("looked up %s, want the recorded %s", asked.String(), clientID.String())
					}
					if !tt.found {
						return storage.DataSetRef{}, false, nil
					}
					ref, err := storage.NewDataSetRef(providerID.SDK(), createdID.SDK(), asked)
					if err != nil {
						t.Errorf("new data set ref: %v", err)
						return storage.DataSetRef{}, false, err
					}
					return ref, true, nil
				},
				CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					sends.Add(1)
					return nil, errors.New("unexpected create request")
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						return target, nil
					},
				},
				policy: cache.EvictionPolicyNone,
				register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
					return handlers.RegisterStorage(registry)
				},
			})
			ctx := t.Context()
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("unnamed-creation-%d", sequence))
			if err := runtime.repos.Contents.RecordDataSetClientID(ctx, fixture.binding.ID, clientID); err != nil {
				t.Fatalf("record client data set ID: %v", err)
			}
			// A checkpoint this build cannot decode: the ID arrives as a number
			// where the record expects text.
			if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`,
				`{"client_data_set_id": 12}`, fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatalf("write checkpoint: %v", err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = ? WHERE id = ?`, model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatalf("resume in recovery: %v", err)
			}

			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			settled := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
				if tt.found {
					return task.Status == model.TaskStatusCompleted
				}
				return task.Status == model.TaskStatusFailed
			})
			if !tt.found && (settled.FailureReason == nil || *settled.FailureReason != "invalid_checkpoint") {
				t.Fatalf("failure = %v, want invalid_checkpoint", settled.FailureReason)
			}
			if sends.Load() != 0 || lookups.Load() == 0 {
				t.Fatalf("sends = %d lookups = %d, want the recorded ID looked up and nothing sent", sends.Load(), lookups.Load())
			}
			kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
			if err != nil || kept.Status != tt.status || (kept.EnsureTaskID != nil) != tt.fenceHeld {
				t.Fatalf("generation = %#v err=%v, want status %s with fence held=%v", kept, err, tt.status, tt.fenceHeld)
			}
		})
	}
}

// TestDataSetEnsureGivesUpAGenerationThatNeverSent checks that a task that
// cannot run gives up its generation — and its creation fence — only because the
// row proves no request ever went out.
func TestDataSetEnsureGivesUpAGenerationThatNeverSent(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 44000+sequence)
	target := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		// An identity missing its chain and record keeper cannot sign, so the
		// task fails before anything is sent.
		ContextIdentityValue: storage.ContextIdentity{Payer: common.HexToAddress("0x00000000000000000000000000000000000000d4")},
		CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			t.Error("a creation was sent without a complete signing identity")
			return nil, errors.New("unexpected create request")
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{
			OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return target, nil
			},
		},
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	ctx := t.Context()
	fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("never-sent-%d", sequence))

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "dataset_creation_unsent" || runtime.service.Retryable(failed) {
		t.Fatalf("failure = %v retryable=%v, want dataset_creation_unsent with no retry offered", failed.FailureReason, runtime.service.Retryable(failed))
	}
	kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
	if err != nil || kept.Status != model.StorageDataSetStatusRetired || kept.EnsureTaskID != nil {
		t.Fatalf("generation = %#v err=%v, want it retired with its creation fence released", kept, err)
	}
}

// TestDataSetCreationLooksUpARejectedResendInsteadOfGivingUp checks that a
// rejection arriving after an unobserved outcome proves nothing: the request
// before it may have created the data set, so the submission is dropped, the ID
// is looked up, and the generation keeps its fence.
func TestDataSetCreationLooksUpARejectedResendInsteadOfGivingUp(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 46000+sequence)
	clientID := testOnChainID(t, 47000+sequence)
	var lookups atomic.Int64
	target := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
			return nil, fmt.Errorf("submission: %w", synapse.ErrProviderTransactionRejected)
		},
		FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
			lookups.Add(1)
			return storage.DataSetRef{}, false, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{
			OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return target, nil
			},
		},
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	ctx := t.Context()
	fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("dropped-submission-%d", sequence))
	if err := runtime.repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID: fixture.binding.ID, TransactionID: "0xdead", StatusURL: "https://provider.example/status",
		ClientDataSetID: &clientID,
	}); err != nil {
		t.Fatalf("mark creating: %v", err)
	}
	// An earlier request with this ID had no observed outcome, so the rejection
	// only means that one may have created the data set first.
	checkpoint, err := json.Marshal(map[string]any{
		"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(),
		"identity": testutil.DefaultContextIdentity, "sends": 2,
		"transaction_id": "0xdead", "status_url": "https://provider.example/status",
	})
	if err != nil {
		t.Fatalf("encode checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`, string(checkpoint), fixture.ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = ? WHERE id = ?`, model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("resume in recovery: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(*model.Task) bool {
		return lookups.Load() > 0
	})
	kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
	if err != nil || kept.Status != model.StorageDataSetStatusCreating || kept.EnsureTaskID == nil {
		t.Fatalf("generation = %#v err=%v, want it kept with its creation fence", kept, err)
	}
	stored, err := runtime.repos.Tasks.GetByID(ctx, fixture.ensureTask.ID)
	if err != nil || stored == nil || strings.Contains(string(stored.Checkpoint), `"transaction_id"`) {
		t.Fatalf("task = %#v err=%v, want the rejected submission dropped from its checkpoint", stored, err)
	}
}
