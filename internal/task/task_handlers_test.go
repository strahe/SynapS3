package task_test

import (
	"bytes"
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
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/task/dataset"
	"github.com/strahe/synaps3/internal/task/wallet"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synaps3/internal/walletoperation"
	taskengine "github.com/strahe/synaps3/internal/worker"
	sdkcosts "github.com/strahe/synapse-go/costs"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/uptrace/bun"
)

const (
	handlerTestPollInterval  = testutil.TaskPollInterval
	handlerTestLeaseDuration = testutil.TaskLeaseDuration
)

type handlerTestRuntime struct {
	db                    *bun.DB
	repos                 *repository.Repositories
	cache                 cache.Cache
	gate                  *cacheaccess.Gate
	tracker               *cacheaccess.Tracker
	storage               *testutil.MockStorageClient
	ready                 *taskengine.Messenger
	entryPoints           task.EntryPoints
	registry              *taskengine.Registry
	service               *taskengine.Service
	retries               *taskengine.Scheduler
	engine                *taskengine.Engine
	testMaxAttempts       *int
	testDependencyRetries *int
	testBackoff           *taskengine.BackoffPolicy
	schedules             []taskengine.ScheduleDefinition
}

type handlerRuntimeOptions struct {
	providerTier              providerselect.Tier
	logger                    *slog.Logger
	cache                     cache.Cache
	events                    interface{ Publish(string, map[string]any) }
	storage                   *testutil.MockStorageClient
	deletionState             func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error)
	wallet                    synapse.WalletOperator
	receipts                  wallet.WalletReceiptChecker
	walletBroadcastTimeout    time.Duration
	walletReceiptTimeout      time.Duration
	commitMaxWait             time.Duration
	commitMaxPieces           int
	commitSealOnCachePressure bool
	removalChain              synapse.RemovalChainReader
	removalMaxWait            time.Duration
	removalMaxPieces          int
	terminator                synapse.ServiceTerminator
	epochs                    synapse.ChainEpochReader
	parkedPieces              synapse.ParkedPieceChecker
	commitNonces              synapse.CommitNonceReader
	uploadSpeedProbe          providerbenchmark.UploadProbe
	policy                    cache.EvictionPolicy
	maxBytes                  int64
	maxWriteBytes             int64
	highPercent               int
	lowPercent                int
	concurrency               int
	uploadConcurrency         int
	maxAttempts               *int
	dependencyRetries         *int
	testBackoff               *taskengine.BackoffPolicy
	leaseDuration             time.Duration
	observabilityChecker      observability.RefreshChecker
	retireOnly                bool
	extraHandlers             []taskengine.Handler
}

func newHandlerTestRuntime(t *testing.T, options handlerRuntimeOptions) handlerTestRuntime {
	t.Helper()
	db := testutil.NewTestDB(t)
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
				return synapse.CleanupPieceState{Status: synapse.CleanupPieceRemoved}, nil
			}
		}
	}
	if options.removalChain == nil {
		options.removalChain = testRemovalChain{}
	}
	gate := cacheaccess.NewGate()
	tracker := cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, repos.Objects)
	observabilityService := observability.NewService(observability.ServiceOptions{Store: repos.Observability})
	if options.observabilityChecker != nil {
		observabilityService = observability.NewService(observability.ServiceOptions{
			Store: repos.Observability, Checker: options.observabilityChecker,
			LocalDataSets: observability.LocalDataSetSourceFunc(func(context.Context) ([]observability.LocalDataSet, error) { return nil, nil }),
		})
	} else if options.uploadSpeedProbe != nil {
		observabilityService = observability.NewService(observability.ServiceOptions{Store: repos.Observability})
	}
	logger := options.logger
	if logger == nil {
		logger = slog.Default()
	}
	registry := taskengine.NewRegistry()
	service, err := taskengine.NewService(registry, repos)
	if err != nil {
		t.Fatalf("new task service: %v", err)
	}
	var ready *taskengine.Messenger
	var entryPoints task.EntryPoints
	var retries *taskengine.Scheduler
	if options.retireOnly {
		handler, err := dataset.NewRetireHandler(dataset.RetireDependencies{
			Repositories: repos, Terminator: options.terminator, Epochs: options.epochs,
			Logger: logger,
		})
		if err != nil {
			t.Fatalf("new retirement handler: %v", err)
		}
		if err := registry.Register(handler); err != nil {
			t.Fatalf("register retirement: %v", err)
		}
	} else {
		ready, err = registry.Messenger("test.ready", nil, []string{storagepipeline.MessageDataSetReady})
		if err != nil {
			t.Fatalf("ready messenger: %v", err)
		}
		entryPoints, err = task.Register(registry, service, task.Dependencies{
			AnchorProviderTier: options.providerTier,
			Repositories:       repos, Events: options.events, Cache: cacheStore, CacheGate: gate, CacheTracker: tracker,
			Storage: storageClient, Wallet: options.wallet, Receipts: options.receipts,
			WalletBroadcastTimeout: options.walletBroadcastTimeout,
			WalletReceiptTimeout:   options.walletReceiptTimeout,
			Terminator:             options.terminator, Epochs: options.epochs,
			ParkedPieces: options.parkedPieces, CommitNonces: options.commitNonces,
			Observability: observabilityService, UploadSpeedProbe: options.uploadSpeedProbe,
			UploadConcurrency: options.uploadConcurrency,
			EvictionPolicy:    options.policy, MaxCacheBytes: options.maxBytes, MaxWriteBytes: options.maxWriteBytes,
			LRUHighPercent: options.highPercent, LRULowPercent: options.lowPercent,
			DefaultCopies: 2, Logger: logger,
			CommitMaxWait:             options.commitMaxWait,
			CommitMaxPieces:           options.commitMaxPieces,
			CommitSealOnCachePressure: options.commitSealOnCachePressure,
			RemovalChain:              options.removalChain,
			RemovalMaxWait:            options.removalMaxWait,
			RemovalMaxPieces:          options.removalMaxPieces,
		})
		if err != nil {
			t.Fatalf("register handlers: %v", err)
		}
		retries, err = service.Scheduler("test.coordinator", model.TaskTypeStorageStore)
		if err != nil {
			t.Fatalf("coordinator scheduler: %v", err)
		}
	}
	for _, handler := range options.extraHandlers {
		if err := registry.Register(handler); err != nil {
			t.Fatalf("register extra handler: %v", err)
		}
	}
	concurrency := max(options.concurrency, 1)
	leaseDuration := options.leaseDuration
	if leaseDuration == 0 {
		leaseDuration = handlerTestLeaseDuration
	}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: concurrency, PollInterval: handlerTestPollInterval, LeaseDuration: leaseDuration,
	}, repos, registry, slog.Default())
	if err != nil {
		t.Fatalf("new task engine: %v", err)
	}
	runtime := handlerTestRuntime{
		db: db, repos: repos, cache: cacheStore, gate: gate, tracker: tracker,
		storage: storageClient, ready: ready, entryPoints: entryPoints, registry: registry, service: service, retries: retries, engine: engine, testMaxAttempts: options.maxAttempts,
		testBackoff: options.testBackoff, testDependencyRetries: options.dependencyRetries,
	}
	runtime.engine, err = taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: concurrency, PollInterval: handlerTestPollInterval, LeaseDuration: leaseDuration,
	}, repos, handlerExecutionRegistry(t, runtime), slog.Default())
	if err != nil {
		t.Fatalf("new task engine with test policies: %v", err)
	}
	return runtime
}

type testWalletOperator struct {
	identity walletoperation.ExecutionIdentity
	fund     func(context.Context, *big.Int) (synapse.WalletSubmission, error)
	withdraw func(context.Context, *big.Int) (synapse.WalletSubmission, error)
	replay   func(context.Context, model.WalletOperationType, *big.Int, synapse.WalletSubmission) error
}

func (o testWalletOperator) FundUSDFC(ctx context.Context, amount *big.Int) (synapse.WalletSubmission, error) {
	if o.fund == nil {
		return synapse.WalletSubmission{}, errors.New("unexpected fund request")
	}
	return o.fund(ctx, amount)
}

func (o testWalletOperator) WithdrawUSDFC(ctx context.Context, amount *big.Int) (synapse.WalletSubmission, error) {
	if o.withdraw != nil {
		return o.withdraw(ctx, amount)
	}
	return synapse.WalletSubmission{}, errors.New("unexpected withdraw request")
}

func (testWalletOperator) ApproveFWSS(context.Context) (synapse.WalletSubmission, error) {
	return synapse.WalletSubmission{}, errors.New("unexpected approval request")
}

func (o testWalletOperator) RebroadcastSignedTransaction(ctx context.Context, operationType model.WalletOperationType, amount *big.Int, submission synapse.WalletSubmission, _ time.Duration) error {
	if o.replay == nil {
		return errors.New("unexpected wallet replay")
	}
	return o.replay(ctx, operationType, amount, submission)
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

func (t *testServiceTerminator) TerminateService(_ context.Context, _ sdktypes.BigInt, onSubmitted func(string) error) (*synapse.TerminationResult, error) {
	t.calls.Add(1)
	err := t.err
	if t.result != nil && t.result.TxHash != "" && onSubmitted != nil {
		err = errors.Join(err, onSubmitted(t.result.TxHash))
	}
	return t.result, err
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

func taskTestMaxAttempts(retries *int) *int {
	if retries == nil {
		return nil
	}
	return new(*retries + 1)
}

func handlerTaskMaxAttempts(t *testing.T, runtime handlerTestRuntime, row *model.Task) int {
	t.Helper()
	policy, ok := runtime.service.Policy(row.Type)
	if !ok {
		t.Fatalf("task type %s has no policy", row.Type)
	}
	return policy.MaxAttempts
}

func handlerTaskRetryable(t *testing.T, runtime handlerTestRuntime, row *model.Task) bool {
	t.Helper()
	eligible, err := runtime.service.RetryableContext(t.Context(), row)
	if err != nil {
		t.Fatal(err)
	}
	return eligible
}

func handlerTaskFixture(t *testing.T, runtime handlerTestRuntime, row *model.Task) *model.Task {
	t.Helper()
	definition, _ := runtime.registry.Definition(row.Type)
	if definition.Identity != nil && row.SubjectType == "" {
		if identity, err := definition.Identity(row.Input); err == nil {
			row.SubjectType, row.SubjectKey, row.Generation = identity.SubjectType, identity.SubjectKey, identity.Generation
		}
	}
	return row
}

func settleHandlerTaskFixture(t *testing.T, runtime handlerTestRuntime, taskID int64, status model.TaskStatus, reason *string) {
	t.Helper()
	row, err := runtime.repos.Tasks.GetByID(t.Context(), taskID)
	if err != nil || row == nil {
		t.Fatalf("get task for fixture settlement: %v", err)
	}
	generation := row.ClaimGeneration + 1
	now := time.Now()
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusRunning).Set("claim_generation = ?", generation).
		Set("lease_until = ?", now.Add(time.Minute)).
		Where("id = ?", taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Tasks.Settle(t.Context(), taskID, generation, repository.TaskTransition{Status: status, ResumeMode: model.TaskResumeModeRecover, FailureReason: reason}); err != nil {
		t.Fatal(err)
	}
}

func ageHandlerOperation(t *testing.T, runtime handlerTestRuntime, taskID int64, age time.Duration) {
	t.Helper()
	row, err := runtime.repos.Tasks.GetByID(t.Context(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal(row.Runtime, &state); err != nil {
		t.Fatal(err)
	}
	if state["operation_key"] == nil || state["operation_key"] == "" {
		t.Fatal("operation observation has not started")
	}
	state["operation_started_at"] = time.Now().UTC().Add(-age)
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("runtime_json = ?", json.RawMessage(raw)).Where("id = ?", taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type handlerPolicyFixture struct {
	taskengine.Handler
	definition taskengine.Definition
}

func (h handlerPolicyFixture) Definition() taskengine.Definition { return h.definition }

func handlerExecutionRegistry(t *testing.T, runtime handlerTestRuntime) *taskengine.Registry {
	t.Helper()
	registry := taskengine.NewRegistry()
	for _, taskType := range runtime.registry.Types() {
		handler, _ := runtime.registry.Handler(taskType)
		definition, _ := runtime.registry.Definition(taskType)
		definition.Policy.ObservationInterval = handlerTestPollInterval
		if runtime.testMaxAttempts != nil {
			definition.Policy.MaxAttempts = *runtime.testMaxAttempts
		}
		if runtime.testDependencyRetries != nil {
			definition.Policy.DependencyRetries = *runtime.testDependencyRetries
		}
		definition.Policy.Backoff = taskengine.BackoffPolicy{
			InitialDelay: 50 * time.Millisecond, Multiplier: 2, MaximumDelay: 50 * time.Millisecond,
		}
		if runtime.testBackoff != nil {
			definition.Policy.Backoff = *runtime.testBackoff
		}
		if definition.Policy.ResendBackoff != nil && taskType != model.TaskTypeStorageCommit && taskType != model.TaskTypeStorageRemoval {
			definition.Policy.ResendBackoff = &taskengine.BackoffPolicy{
				InitialDelay: 50 * time.Millisecond, Multiplier: 2, MaximumDelay: 50 * time.Millisecond,
			}
		}
		if err := registry.Register(handlerPolicyFixture{Handler: handler, definition: definition}); err != nil {
			t.Fatal(err)
		}
	}
	for _, schedule := range runtime.schedules {
		if err := registry.RegisterSchedule(schedule); err != nil {
			t.Fatal(err)
		}
	}
	return registry
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

func waitForTask(t *testing.T, repos *repository.Repositories, id int64, predicate func(*model.Task) bool, timeout ...time.Duration) *model.Task {
	t.Helper()
	duration := 3 * time.Second
	if len(timeout) > 0 {
		duration = timeout[0]
	}
	deadline := time.Now().Add(duration)
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
	t.Fatalf("task %d did not reach expected state: status=%s resume=%s reason=%s error=%s message=%s",
		id, taskField(row, func(r *model.Task) *string { s := string(r.Status); return &s }),
		taskField(row, func(r *model.Task) *string { s := string(r.ResumeMode); return &s }),
		taskField(row, func(r *model.Task) *string { return r.FailureReason }),
		taskField(row, func(r *model.Task) *string { return r.LastError }),
		taskField(row, func(r *model.Task) *string { return r.StatusMessage }))
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
	bucket := &model.Bucket{Name: fmt.Sprintf("cache-task-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
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
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
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
	opened, err := runtime.gate.Open(t.Context(), cacheKey, func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 11}, nil
	})
	if err != nil {
		t.Fatalf("open protected cache entry: %v", err)
	}
	planner, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type:  model.TaskTypeCacheCapacityReconcile,
		Input: taskengine.CycleInput{Cycle: 1},
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
	if eviction.WorkStartedAt != nil {
		t.Fatal("waiting for the cache lock started deletion timing")
	}
	if err := runtime.tracker.RecordAccess(t.Context(), *version.ContentID, version.CacheAccessedAt); err != nil {
		t.Fatalf("record newer cache access: %v", err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("close protected cache entry: %v", err)
	}
	cancelled := waitForTask(t, runtime.repos, eviction.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCancelled })
	if cancelled.WorkStartedAt != nil {
		t.Fatal("cancelled eviction invented a deletion start")
	}
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

type failingDataSetRefreshChecker struct{ dataSetAttempts atomic.Int64 }

func (c *failingDataSetRefreshChecker) CheckProviders(context.Context, time.Time, []observability.LocalDataSet) ([]observability.ProviderState, error) {
	id, _ := idtypes.ParseOnChainID("provider_id", "101")
	return []observability.ProviderState{{ProviderID: id, Status: observability.StatusAvailable}}, nil
}

func (c *failingDataSetRefreshChecker) CheckDataSets(context.Context, time.Time, []observability.LocalDataSet) ([]observability.DataSetState, error) {
	c.dataSetAttempts.Add(1)
	return nil, errors.New("data set refresh failed")
}

func TestObservabilityTaskRetriesAfterPartialCatalogSuccess(t *testing.T) {
	checker := &failingDataSetRefreshChecker{}
	events := &recordingWorkerEvents{events: make(chan recordedWorkerEvent, 2)}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{events: events, observabilityChecker: checker})
	taskRow, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type:  model.TaskTypeObservabilityRefresh,
		Input: taskengine.CycleInput{Cycle: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(row *model.Task) bool {
		return row.Status == model.TaskStatusPending && row.RetryCount > 0 && row.FailureReason != nil && *row.FailureReason == "observability_refresh_failed"
	})
	if checker.dataSetAttempts.Load() == 0 {
		t.Fatal("data set refresh was not attempted")
	}
	select {
	case event := <-events.events:
		if event.topic != "provider_catalog_updated" {
			t.Fatalf("event = %q, want provider_catalog_updated", event.topic)
		}
	default:
		t.Fatal("provider catalog event was not published")
	}
	page, err := runtime.repos.Observability.ListProviderStates(t.Context(), observability.ListOptions{})
	if err != nil || len(page.Items) != 1 || page.Items[0].ProviderID.String() != "101" {
		t.Fatalf("committed provider states = %#v, error=%v", page.Items, err)
	}
}

func TestStoreResultMismatchRetainsCopyAndCheckpoint(t *testing.T) {
	events := &recordingWorkerEvents{events: make(chan recordedWorkerEvent, 8)}
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(strings.Repeat("s", 128))), &cache.ObjectInfo{Size: 128}, nil
	}}
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://terminal-store.example",
		UploadStoreSessionFunc: func(_ context.Context, _ string, _ string, _ io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
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
	})
	ctx := t.Context()
	sequence := storedObjectSequence.Add(1)
	bucket := &model.Bucket{Name: fmt.Sprintf("terminal-copy-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create terminal copy bucket: %v", err)
	}
	content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 128,
		Checksum: testutil.StorageChecksum(strings.Repeat("s", 128)), RequestedCopies: 1,
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
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
	if !handlerTaskRetryable(t, runtime, failedTask) || len(failedTask.Checkpoint) == 0 {
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
	deletePiece  func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error)
	deletePieces func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error)
}

func (c testCleanupContext) DeletePiecesByID(ctx context.Context, pieceIDs []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
	if c.deletePieces != nil {
		return c.deletePieces(ctx, pieceIDs)
	}
	if len(pieceIDs) != 1 || c.deletePiece == nil {
		return nil, errors.New("unexpected batch deletion")
	}
	return c.deletePiece(ctx, pieceIDs[0])
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
		return synapse.CleanupPieceState{Status: synapse.CleanupPiecePresent}, nil
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
	if deleteCalls.Load() != 0 {
		t.Fatalf("content coordination performed %d remote deletions", deleteCalls.Load())
	}
	copies, err := runtime.repos.StorageCleanup.AuthorizeTask(ctx, content.ID, 1, taskRow.ID)
	if err != nil || len(copies) != 2 || copies[1].Status != model.StorageCleanupCopyStatusPending || copies[1].RemovalRequestID == nil {
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
	copyRow := &model.StorageCopy{
		ContentID: content.ID, BucketID: content.BucketID, ContentSize: content.ContentSize,
		StorageDataSetID: cleanupCopy.StorageDataSetID, CopyIndex: 0,
		TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: cleanupCopy.ProviderID,
		Status: model.StorageCopyStatusPending,
	}
	if _, err := runtime.db.NewInsert().Model(copyRow).Exec(ctx); err != nil {
		t.Fatalf("seed cleanup's committed copy: %v", err)
	}
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
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

// A cleanup status this version does not know may record a paid request, so
// the copy is neither checked, deleted, nor rewritten.
func TestStorageCleanupLeavesUnknownCopyStatusUntouched(t *testing.T) {
	var stateChecks, deleteCalls atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePiece: func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				deleteCalls.Add(1)
				return &sdktypes.WriteResult{Hash: common.HexToHash("0x1")}, nil
			}}, nil
		}},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			stateChecks.Add(1)
			return synapse.CleanupPieceState{Status: synapse.CleanupPiecePresent}, nil
		},
	})
	ctx := t.Context()
	content, taskRow := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	unknown := model.StorageCleanupCopyStatus("removal_queued")
	// A newer schema can admit a status this handler does not recognize.
	if _, err := runtime.db.ExecContext(ctx, `ALTER TABLE storage_cleanup_copies
		DROP CONSTRAINT chk_storage_cleanup_copies_status,
		ADD CONSTRAINT chk_storage_cleanup_copies_status
		CHECK (status IN ('pending', 'delete_scheduled', 'removed', 'failed', 'unsupported', 'removal_queued'))`); err != nil {
		t.Fatalf("extend fixture cleanup vocabulary: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
		Set("status = ?", unknown).Where("content_id = ?", content.ID).Exec(ctx); err != nil {
		t.Fatalf("store unknown cleanup status: %v", err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "cleanup_status"
	})
	if stateChecks.Load() != 0 || deleteCalls.Load() != 0 {
		t.Fatalf("state checks = %d, delete calls = %d, want none", stateChecks.Load(), deleteCalls.Load())
	}
	copyRows, err := runtime.repos.StorageCleanup.AuthorizeTask(ctx, content.ID, 1, taskRow.ID)
	if err != nil || len(copyRows) != 1 || copyRows[0].Status != unknown {
		t.Fatalf("cleanup copy after waiting = %#v, err=%v", copyRows, err)
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

	releaseFailed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.RetryCount == 1
	})
	if releaseFailed.WorkStartedAt == nil {
		t.Fatal("local cache release failure lost its actual start time")
	}
	stored, err := runtime.repos.Contents.GetByID(ctx, content.ID)
	if err != nil || stored == nil || stored.CleanupTaskID == nil || *stored.CleanupTaskID != taskRow.ID {
		t.Fatalf("content after failed cache release = %#v, err=%v", stored, err)
	}
	if _, err := runtime.repos.Tasks.WakePending(ctx, []int64{taskRow.ID}); err != nil {
		t.Fatalf("WakePending: %v", err)
	}
	releaseCompleted := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if releaseCompleted.WorkStartedAt == nil || !releaseCompleted.WorkStartedAt.Equal(*releaseFailed.WorkStartedAt) {
		t.Fatal("local cache release retry reset its actual start time")
	}
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
		if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
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
		Type:  model.TaskTypeStorageCleanup,
		Input: storagecleanup.Input{ContentID: content.ID, Generation: 1},
	})
	if err != nil {
		t.Fatalf("enqueue cleanup task: %v", err)
	}
	if err := runtime.repos.StorageCleanup.BindTask(ctx, content.ID, 1, taskRow.ID); err != nil {
		t.Fatalf("bind cleanup task: %v", err)
	}
	return content, taskRow
}

func TestCacheCapacityCompletesHourlyCycleWhenCleanupDisabled(t *testing.T) {
	for _, policy := range []cache.EvictionPolicy{cache.EvictionPolicyNone, cache.EvictionPolicyAfterUpload} {
		t.Run(string(policy), func(t *testing.T) {
			runtime := newScheduledCapacityRuntime(t, handlerRuntimeOptions{policy: policy})
			planner, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
				Type: model.TaskTypeCacheCapacityReconcile, Input: taskengine.CycleInput{Cycle: 1},
			})
			if err != nil || !created {
				t.Fatalf("enqueue capacity = %#v created=%v err=%v", planner, created, err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			stored := waitForTask(t, runtime.repos, planner.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
			if stored.FinishedAt == nil || stored.RetryCount != 0 || stored.WaitReason != nil {
				t.Fatalf("disabled capacity round = %#v", stored)
			}
			schedule, err := runtime.repos.TaskSchedules.GetForUpdate(t.Context(), "capacity-policy")
			if err != nil || schedule == nil || schedule.NextRunAt.Before(stored.FinishedAt.Add(59*time.Minute)) {
				t.Fatalf("disabled capacity schedule = %#v, err=%v", schedule, err)
			}
		})
	}
}

func TestCacheCapacityTaskEvictsLRUItemsOnlyToCleanupTarget(t *testing.T) {
	// A 30-byte cache with watermarks at 27 and 18 bytes holds three 11-byte items.
	tests := []struct {
		name           string
		used           int64
		reserved       int64
		writeRefused   bool
		failCheckpoint bool
		maxWriteBytes  int64
		wantEvicted    int
	}{
		{name: "usage at high watermark", used: 33, wantEvicted: 2},
		{name: "reservations reach high watermark", used: 22, reserved: 6, wantEvicted: 1},
		{name: "write refused below high watermark", used: 22, writeRefused: true, wantEvicted: 1},
		{name: "write refusal survives checkpoint failure", used: 22, writeRefused: true, failCheckpoint: true, wantEvicted: 1},
		// A 25-byte write leaves room for only 5 bytes of cached data, below the
		// low watermark.
		{name: "cleanup keeps room for the largest write", used: 12, writeRefused: true, maxWriteBytes: 25, wantEvicted: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var used atomic.Int64
			used.Store(tt.used)
			var refused atomic.Bool
			refused.Store(tt.writeRefused)
			var deletedMu sync.Mutex
			var deleted []string
			cacheStore := &testutil.MockCache{
				UsedBytesFunc: used.Load,
				CapacitySnapshotFunc: func() cache.CapacitySnapshot {
					return cache.CapacitySnapshot{UsedBytes: used.Load(), ReservedBytes: tt.reserved, MaxBytes: 30}
				},
				ConsumeRefusedWriteBytesFunc: func() int64 {
					if refused.Swap(false) {
						return max(tt.maxWriteBytes, 10)
					}
					return 0
				},
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
				maxWriteBytes: tt.maxWriteBytes, highPercent: 90, lowPercent: 60, concurrency: 1,
			})
			base := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Microsecond)
			versions := []*model.ObjectVersion{
				seedStoredCacheObject(t, runtime, 11, base),
				seedStoredCacheObject(t, runtime, 11, base.Add(time.Hour)),
				seedStoredCacheObject(t, runtime, 11, base.Add(2*time.Hour)),
			}
			planner, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
				Type:  model.TaskTypeCacheCapacityReconcile,
				Input: taskengine.CycleInput{Cycle: 1},
			})
			if err != nil || !created {
				t.Fatalf("enqueue cache capacity task = %#v created=%v err=%v", planner, created, err)
			}
			restoreFailCapacityCheckpoint := func() {}
			if tt.failCheckpoint {
				restoreFailCapacityCheckpoint = testutil.FailWrites(t, runtime.db, "tasks", "UPDATE OF checkpoint_json", "", "injected checkpoint failure")
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			if tt.failCheckpoint {
				waitForTask(t, runtime.repos, planner.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusPending && task.RetryCount > 0 &&
						task.LastError != nil && strings.Contains(*task.LastError, "injected checkpoint failure")
				})
				restoreFailCapacityCheckpoint()
				wakeTask(t, runtime, planner.ID)
			}

			deadline := time.Now().Add(3 * time.Second)
			var page repository.TaskPage
			for time.Now().Before(deadline) {
				page, err = runtime.repos.Tasks.List(t.Context(), repository.TaskListFilter{
					Scope: repository.TaskScopeHistory, Type: model.TaskTypeCacheEvict, Status: model.TaskStatusCompleted, Limit: 10,
				})
				if err != nil {
					t.Fatalf("list completed cache tasks: %v", err)
				}
				if len(page.Tasks) >= tt.wantEvicted {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if len(page.Tasks) != tt.wantEvicted {
				t.Fatalf("completed cache tasks = %#v, want %d", page.Tasks, tt.wantEvicted)
			}
			wantUsed := tt.used - int64(tt.wantEvicted)*11
			if used.Load() != wantUsed {
				t.Fatalf("cache usage = %d, want %d", used.Load(), wantUsed)
			}
			deletedMu.Lock()
			gotDeleted := append([]string(nil), deleted...)
			deletedMu.Unlock()
			if len(gotDeleted) != tt.wantEvicted {
				t.Fatalf("deleted cache keys = %#v, want the %d least recently used", gotDeleted, tt.wantEvicted)
			}
			for index, key := range gotDeleted {
				if key != versions[index].CacheKey() {
					t.Fatalf("deleted cache keys = %#v, want the %d least recently used", gotDeleted, tt.wantEvicted)
				}
			}
			for index, version := range versions {
				stored, err := runtime.repos.Objects.GetVersionByID(t.Context(), version.VersionID)
				if err != nil || stored == nil {
					t.Fatalf("load version %d: %#v err=%v", index, stored, err)
				}
				if evicted := index < tt.wantEvicted; stored.InCache == evicted {
					t.Fatalf("version %d in cache = %v, want %v", index, stored.InCache, !evicted)
				}
			}
		})
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
		Type:  model.TaskTypeCacheEvict,
		Input: cacheeviction.EvictInput{ContentID: *version.ContentID, Generation: generation},
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
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
	}, &limitedRepos, handlerExecutionRegistry(t, runtime), slog.Default())
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
		Type:  model.TaskTypeCacheEvict,
		Input: cacheeviction.EvictInput{ContentID: *version.ContentID, Generation: generation},
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
		Type:  model.TaskTypeCacheEvict,
		Input: cacheeviction.EvictInput{ContentID: contentID, Generation: firstGeneration},
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
		Type:        model.TaskTypeCacheEvict,
		Input:       cacheeviction.EvictInput{ContentID: contentID, Generation: reservation.Generation},
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

func TestCacheEvictionPersistentDeleteFailureRetainsOwner(t *testing.T) {
	for _, failRecord := range []bool{false, true} {
		t.Run(fmt.Sprintf("record failure=%v", failRecord), func(t *testing.T) {
			deleteErr := errors.New("cache deletion failed")
			cacheStore := &testutil.MockCache{
				DeleteFunc: func(context.Context, string, string) error {
					if failRecord {
						return nil
					}
					return deleteErr
				},
			}
			noRetries := 0
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				cache: cacheStore, policy: cache.EvictionPolicyNone, maxAttempts: taskTestMaxAttempts(&noRetries),
			})
			version := seedStoredCacheObject(t, runtime, 11, time.Now().UTC().Add(-time.Hour))
			contentID := *version.ContentID
			if failRecord {
				testutil.FailWrites(t, runtime.db, "object_cache", "UPDATE OF in_cache", "NOT NEW.in_cache", "cache deletion failed")
			}
			generation, err := runtime.repos.CacheEvictions.NextEvictionGeneration(t.Context(), contentID)
			if err != nil {
				t.Fatalf("next eviction generation: %v", err)
			}
			taskRow, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
				Type:  model.TaskTypeCacheEvict,
				Input: cacheeviction.EvictInput{ContentID: contentID, Generation: generation},
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
			if stored.FailureReason == nil || *stored.FailureReason != "attempts_exhausted" {
				t.Fatalf("failure reason = %v, want attempts_exhausted", stored.FailureReason)
			}
			if stored.LastError == nil || !strings.Contains(*stored.LastError, deleteErr.Error()) {
				t.Fatalf("last error = %v, want %q", stored.LastError, deleteErr)
			}
			if stored.WorkStartedAt == nil {
				t.Fatal("failed cache deletion lost its actual start time")
			}
			entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), contentID)
			if err != nil || entry == nil {
				t.Fatalf("load cache entry after failed eviction = %#v, err=%v", entry, err)
			}
			if entry.CacheActiveTaskID == nil || *entry.CacheActiveTaskID != taskRow.ID || !entry.InCache {
				t.Fatalf("cache entry after failed eviction = %#v, want retained owner and cache", entry)
			}
		})
	}
}

type limitedClaimRepository struct {
	repository.TaskRepository
	maximum int64
	claims  atomic.Int64
	// countType, when set, counts only claims of that task type.
	countType       model.TaskType
	excludedTaskIDs []int64
}

type validateFailureRepository struct {
	repository.TaskRepository
	remaining atomic.Int64
	err       error
}

func rejectTaskCheckpoint(t *testing.T, db *bun.DB, taskID int64) {
	t.Helper()
	testutil.FailWrites(t, db, "tasks", "UPDATE OF checkpoint_json", fmt.Sprintf("NEW.id = %d", taskID), "temporary database failure")
}

func (r *validateFailureRepository) ValidateClaim(ctx context.Context, id, generation int64) error {
	if r.remaining.Add(-1) >= 0 {
		return r.err
	}
	return r.TaskRepository.ValidateClaim(ctx, id, generation)
}

func (r *limitedClaimRepository) ClaimNext(ctx context.Context, lease time.Duration, filter repository.TaskClaimFilter) (*model.Task, error) {
	if r.claims.Load() >= r.maximum {
		return nil, nil
	}
	filter.ExcludedTaskIDs = append(filter.ExcludedTaskIDs, r.excludedTaskIDs...)
	claimed, err := r.TaskRepository.ClaimNext(ctx, lease, filter)
	if claimed != nil && err == nil && (r.countType == "" || claimed.Type == r.countType) {
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
		Checksum: testutil.StorageChecksum(strings.Repeat("s", 128)), RequestedCopies: 2,
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
		if readyErr := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
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
	pieceInfo, err := piece.Calculate(strings.NewReader(strings.Repeat("s", 128)))
	if err != nil {
		t.Fatalf("calculate copy piece CID: %v", err)
	}
	pieceCID := pieceInfo.CIDv2
	pieceID := testOnChainID(t, 8000+sequence)
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
		StorageCopyID: copies[0].ID, ContentID: upload.ID, CopyIndex: 0, PieceCID: pieceCID.String(),
		PieceID: &pieceID, RetrievalURL: "https://source.example/piece/" + pieceCID.String(),
	})
	if targetStatus == model.StorageCopyStatusPieceReady {
		if err := runtime.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageCopyID: copies[1].ID, ContentID: upload.ID, CopyIndex: 1,
			PieceCID: pieceCID.String(), RetrievalURL: "https://target.example/piece/" + pieceCID.String(),
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
	for _, taskType := range []model.TaskType{model.TaskTypeStorageTransferPlan, model.TaskTypeStoragePull} {
		t.Run(string(taskType), func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				cache:  &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return true }},
				policy: cache.EvictionPolicyNone,
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
			target := &testutil.MockStorageTarget{
				ProviderIDValue: pipeline.targetSet.ProviderID.SDK(), ClientDataSetIDValue: pipeline.targetClient,
			}
			dataSetID := pipeline.targetSet.DataSetID.SDK()
			target.DataSetIDValue = &dataSetID
			runtime.storage.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				return target, nil
			}
			plan := bindCopyTask(t, runtime, pipeline.target, taskType)
			before := time.Now()
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			waiting := waitForTask(t, runtime.repos, plan.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "source"
			})
			interval := handlerTestPollInterval
			if taskType == model.TaskTypeStorageTransferPlan {
				interval = 30 * time.Second
			}
			if waiting.AvailableAt.Before(before.Add(interval)) || waiting.AvailableAt.After(time.Now().Add(interval)) || waiting.RetryCount != 0 {
				t.Fatalf("source dependency wait = %#v", waiting)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != plan.ID {
				t.Fatalf("peer copy after source wait = %#v, err=%v", copyRow, err)
			}
		})
	}
}

// A transfer method this version does not know may not be a store, so the copy
// waits even though its bytes are cached and could be uploaded.
func TestTransferPlanWaitsOnUnknownTransferMethod(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache:  &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return true }},
		policy: cache.EvictionPolicyNone,
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "unknown-method.bin",
		ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize,
		ETag: "unknown-method", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Objects.SetVersionCachePresence(t.Context(), version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	unknown := model.StorageCopyTransferMethod("repair")
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).
		Set("transfer_method = ?", unknown).Where("id = ?", pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	plan := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageTransferPlan)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, plan.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "transfer_method"
	})
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.TransferMethod != unknown || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != plan.ID {
		t.Fatalf("copy after waiting = %#v, err=%v", copyRow, err)
	}
}

func seedReplacementPullTarget(t *testing.T, runtime handlerTestRuntime, cachePresent bool) (*model.StorageCopy, *testutil.MockStorageTarget, seededCopyPipeline) {
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
	if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{
		ID:        replacement.TargetDataSetID,
		DataSetID: dataSetID, ClientDataSetID: &clientID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := activateReplacement(t, runtime, replacement.ID); err != nil {
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
			return testutil.CommitExtraData(7), nil
		},
	}
	return copyRow, target, pipeline
}

func TestReplacementPullFallsBackOnlyToRetainedCache(t *testing.T) {
	for _, tt := range []struct {
		name         string
		cachePresent bool
		noSource     bool
		pullErr      error
		pullStatus   storage.PullStatus
		taskType     model.TaskType
		retry        bool
		wantMethod   model.StorageCopyTransferMethod
		wantStatus   model.TaskStatus
		wantReason   string
	}{
		{name: "terminal pull with cache", cachePresent: true, wantMethod: model.StorageCopyTransferMethodCacheRestore, wantStatus: model.TaskStatusCompleted},
		{name: "terminal pull without cache", retry: true, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusFailed, wantReason: "migration_cache_missing"},
		{name: "confirmed missing piece without cache", pullStatus: storage.PullStatusComplete, retry: true, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusFailed, wantReason: "migration_cache_missing"},
		{name: "temporary pull failure", cachePresent: true, pullErr: &pdp.HTTPError{StatusCode: http.StatusServiceUnavailable}, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusPending, wantReason: "pull_request_failed"},
		{name: "no source with cache", cachePresent: true, noSource: true, taskType: model.TaskTypeStorageTransferPlan, wantMethod: model.StorageCopyTransferMethodCacheRestore, wantStatus: model.TaskStatusCompleted},
		{name: "no source without cache", noSource: true, taskType: model.TaskTypeStorageTransferPlan, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusFailed, wantReason: "migration_cache_missing"},
		{name: "no source without cache (pull)", noSource: true, taskType: model.TaskTypeStoragePull, retry: true, wantMethod: model.StorageCopyTransferMethodPeerPull, wantStatus: model.TaskStatusFailed, wantReason: "migration_cache_missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cacheStore := &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return tt.cachePresent }}
			storageClient := &testutil.MockStorageClient{}
			pieceState := synapse.ParkedPieceMissing
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				cache: cacheStore, storage: storageClient, policy: cache.EvictionPolicyNone,
				parkedPieces: parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
					return pieceState, nil
				}),
			})
			copyRow, target, pipeline := seedReplacementPullTarget(t, runtime, tt.cachePresent)
			var signatures, calls atomic.Uint64
			target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
				return testutil.CommitExtraData(6 + signatures.Add(1)), nil
			}
			if tt.noSource {
				if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).
					Set("status = ?", model.StorageDataSetStatusRetired).Set("is_current = ?", false).
					Where("id = ?", pipeline.source.StorageDataSetID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
				calls.Add(1)
				if tt.noSource {
					t.Error("Pull was called without a source")
				}
				if tt.pullErr != nil {
					return nil, tt.pullErr
				}
				status := tt.pullStatus
				if status == "" {
					status = storage.PullStatusFailed
				}
				return pullStatusResult(request, status), nil
			}
			storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				return target, nil
			}
			taskType := tt.taskType
			if taskType == "" {
				taskType = model.TaskTypeStoragePull
			}
			taskRow := bindCopyTask(t, runtime, copyRow, taskType)
			result := runOneStorageTask(t, runtime, taskRow, tt.wantStatus)
			if tt.wantStatus == model.TaskStatusPending && (result.RetryCount == 0 || result.ResumeMode != model.TaskResumeModeRecover) {
				t.Fatalf("temporary Pull failure did not retain recovery: %#v", result)
			}
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
			if !tt.retry {
				return
			}
			if tt.noSource {
				if len(result.Checkpoint) != 0 {
					t.Fatalf("Pull without a source persisted a checkpoint: %s", result.Checkpoint)
				}
				successor, err := runtime.service.Retry(t.Context(), taskRow.ID)
				if err != nil {
					t.Fatal(err)
				}
				if len(successor.Checkpoint) != 0 {
					t.Fatalf("Pull retry without a source changed its empty checkpoint: %s", successor.Checkpoint)
				}
				owned, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
				if err != nil || owned.ActiveTaskID == nil || *owned.ActiveTaskID != successor.ID {
					t.Fatalf("Pull retry without a source did not transfer ownership: %#v, err=%v", owned, err)
				}
				return
			}
			var initialCheckpoint struct {
				AttemptID string `json:"attempt_id"`
			}
			if err := json.Unmarshal(result.Checkpoint, &initialCheckpoint); err != nil {
				t.Fatal(err)
			}
			initialAttempt, err := runtime.repos.Contents.GetPullAttempt(t.Context(), initialCheckpoint.AttemptID, copyRow.ContentID, copyRow.StorageDataSetID)
			if err != nil || initialAttempt.Status != storagepull.AttemptStatusAbandoned || initialAttempt.ResolvedAt == nil || initialAttempt.LastError == nil {
				t.Fatalf("initial Pull attempt was not abandoned: %v", err)
			}
			secondURL := "https://second.example/piece/" + pipeline.pieceCID.String()
			secondPieceID := testOnChainID(t, 990004)
			testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
				StorageCopyID: pipeline.target.ID, ContentID: copyRow.ContentID, CopyIndex: pipeline.target.CopyIndex,
				PieceCID: pipeline.pieceCID.String(), PieceID: &secondPieceID, RetrievalURL: secondURL,
			})
			pieceState = synapse.ParkedPieceReady
			target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
				calls.Add(1)
				if request.From(pipeline.pieceCID) != secondURL || !bytes.Equal(request.ExtraData, testutil.CommitExtraData(8)) {
					t.Error("Pull retry did not use the second source and a fresh authorization")
				}
				return pullStatusResult(request, storage.PullStatusComplete), nil
			}
			successor, err := runtime.service.Retry(t.Context(), taskRow.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(successor.Checkpoint) != 0 {
				t.Fatalf("Pull successor inherited an abandoned attempt: %s", successor.Checkpoint)
			}
			completed := runStorageTaskClaims(t, runtime, successor, model.TaskStatusCompleted, 2)
			var nextCheckpoint struct {
				AttemptID string `json:"attempt_id"`
			}
			if err := json.Unmarshal(completed.Checkpoint, &nextCheckpoint); err != nil || nextCheckpoint.AttemptID == "" || nextCheckpoint.AttemptID == initialCheckpoint.AttemptID {
				t.Fatalf("Pull retry did not persist a fresh attempt identity: %v", err)
			}
			var attempts []storagepull.Attempt
			if err := runtime.db.NewSelect().Model(&attempts).
				Where("content_id = ? AND storage_data_set_id = ?", copyRow.ContentID, copyRow.StorageDataSetID).
				Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 2 || signatures.Load() != 2 || calls.Load() != 2 {
				t.Fatalf("Pull retry attempts=%d signatures=%d calls=%d, want two of each", len(attempts), signatures.Load(), calls.Load())
			}
			for _, attempt := range attempts {
				if attempt.AttemptID == initialCheckpoint.AttemptID {
					if attempt.Status != storagepull.AttemptStatusAbandoned || attempt.ResolvedAt == nil || !attempt.ResolvedAt.Equal(*initialAttempt.ResolvedAt) ||
						attempt.LastError == nil || *attempt.LastError != *initialAttempt.LastError || attempt.ExtraDataHex != initialAttempt.ExtraDataHex {
						t.Fatal("Pull retry changed the abandoned attempt evidence")
					}
					continue
				}
				if attempt.AttemptID != nextCheckpoint.AttemptID || attempt.Status != storagepull.AttemptStatusAttempted || attempt.ResolvedAt == nil ||
					!attempt.SourceProviderID.Equal(pipeline.target.ProviderID) || !attempt.SourceDataSetID.Equal(*pipeline.targetSet.DataSetID) || attempt.SourceRetrievalURL != secondURL {
					t.Fatal("Pull retry did not resolve the second source attempt")
				}
			}
			stored, err = runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || stored.Status != model.StorageCopyStatusPieceReady {
				t.Fatalf("Pull retry copy = %#v, err=%v, want piece_ready", stored, err)
			}
			history, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
			if err != nil || history.Status != model.TaskStatusFailed || history.SupersededAt == nil || !bytes.Equal(history.Checkpoint, result.Checkpoint) {
				t.Fatalf("Pull retry changed source task evidence: %v", err)
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
	taskRow, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: taskType, Input: input,
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
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	generation, err := runtime.repos.Contents.NextCopyWorkGeneration(t.Context(), pipeline.target.ID)
	if err != nil {
		t.Fatalf("next copy generation: %v", err)
	}
	input := storagepipeline.CopyGenerationInput{CopyID: pipeline.target.ID, Generation: generation}
	bind := func(taskType model.TaskType) (*model.Task, error) {
		returnTask, _, enqueueErr := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
			Type: taskType, Input: input,
		}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
			return repos.Contents.BindCopyTask(ctx, pipeline.target.ID, generation, taskRow.ID)
		})
		return returnTask, enqueueErr
	}

	winner, err := bind(model.TaskTypeStorageTransferPlan)
	if err != nil {
		t.Fatalf("bind winning copy task: %v", err)
	}
	if _, err := bind(model.TaskTypeStoragePull); !errors.Is(err, repository.ErrConflict) {
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
		OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, opts storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			if opts.DataSetMetadata["bucket"] == "" {
				t.Fatal("bucket metadata missing")
			}
			for _, target := range targets {
				if target.ProviderID().Equal(id) {
					return target.(*testutil.MockStorageTarget), nil
				}
			}
			return nil, errors.New("unknown provider")
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
	})
	seedProviderSelection(t, runtime, targets[0].ProviderID(), targets[1].ProviderID())
	bucket := &model.Bucket{Name: fmt.Sprintf("provision-%d", sequence), Status: model.BucketStatusProvisioning, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	taskRow, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type:  model.TaskTypeBucketProvision,
		Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID, Copies: bucket.DefaultCopies},
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

func TestStoragePlanningFailuresConsumeBudgetAndKeepSanitizedErrors(t *testing.T) {
	selectionErr := &synapse.NoProviderCandidatesError{Cause: errors.Join(
		storage.ErrNoHealthyProviders,
		fmt.Errorf("provider 2: GET https://provider.example/pdp/ping?token=secret-token: %w: provider.example resolves to 198.18.0.1", storage.ErrPrivateNetwork),
	)}
	fundingErr := &synapse.ProviderUnavailableError{Cause: fmt.Errorf(
		"reading upload costs: Post https://secret-user:secret-key@rpc.example/v1/rpc?token=secret-token: %w", context.DeadlineExceeded,
	)}
	for _, taskType := range []model.TaskType{model.TaskTypeBucketProvision, model.TaskTypeUploadPlan} {
		for _, phase := range []struct {
			reason string
			err    error
		}{{"providers", selectionErr}, {"funding", fundingErr}} {
			t.Run(string(taskType)+"/"+phase.reason, func(t *testing.T) {
				logFile, err := os.CreateTemp(t.TempDir(), "storage-wait-*.log")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = logFile.Close() })
				var dependencyStage atomic.Int32
				changedErr := fmt.Errorf("dependency changed: %w", phase.err)
				dependencyError := func() error {
					switch dependencyStage.Load() {
					case 0:
						return phase.err
					case 1:
						return changedErr
					default:
						return nil
					}
				}
				sequence := storedObjectSequence.Add(1)
				providerID := testOnChainID(t, 12000+sequence).SDK()
				dataSetID := testOnChainID(t, 13000+sequence).SDK()
				target := &testutil.MockStorageTarget{
					ProviderIDValue: providerID, DataSetIDValue: &dataSetID,
					ClientDataSetIDValue: testOnChainID(t, 14000+sequence).SDK(),
				}
				runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
					logger: slog.New(slog.NewJSONHandler(logFile, nil)),
					storage: &testutil.MockStorageClient{
						OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
							if err := dependencyError(); err != nil && phase.reason == "providers" {
								return nil, err
							}
							return target, nil
						},
						PrepareUploadFunc: func(context.Context, uint64, []synapse.StorageTarget) (*sdkcosts.MultiContextCosts, error) {
							if err := dependencyError(); err != nil && phase.reason == "funding" {
								return nil, err
							}
							return &sdkcosts.MultiContextCosts{Ready: true}, nil
						},
					},
				})
				seedProviderSelection(t, runtime, providerID)
				bucket := &model.Bucket{
					Name: fmt.Sprintf("planning-wait-%d", sequence), Status: model.BucketStatusProvisioning,
					DefaultCopies: 1, MinimumDurableCopies: 1,
				}
				if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
					t.Fatal(err)
				}
				request := taskengine.EnqueueRequest{
					Type:  taskType,
					Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID, Copies: bucket.DefaultCopies},
				}
				if taskType == model.TaskTypeUploadPlan {
					content, err := runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
						BucketID: bucket.ID, ContentSize: 11, RequestedCopies: 1,
						Checksum: testutil.StorageChecksum(fmt.Sprintf("planning-wait-%d", sequence)),
					})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), &model.ObjectVersion{
						VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "object.bin", ContentID: &content.ID, Size: 11,
						ETag: "etag", ContentType: "application/octet-stream",
					}); err != nil {
						t.Fatal(err)
					}
					request.Input = storagepipeline.UploadPlanInput{ContentID: content.ID}
				}
				taskRow, _, err := runtime.service.Enqueue(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 4}
				cancel, done := runHandlerEngine(t, runtime)
				defer stopHandlerEngine(t, cancel, done)
				waiting := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusPending && task.ClaimGeneration == 1 && task.LastError != nil
				})
				wantError := errtext.Format(phase.err)
				if waiting.LastError == nil || *waiting.LastError != wantError {
					t.Fatalf("waiting task last_error = %v, want %q", waiting.LastError, wantError)
				}
				if waiting.RetryCount != 1 || waiting.ResumeMode != model.TaskResumeModeExecute {
					t.Fatalf("waiting task changed retry behavior: %#v", waiting)
				}
				logged, err := os.ReadFile(logFile.Name())
				if err != nil {
					t.Fatal(err)
				}
				var entry struct {
					TaskID     int64          `json:"task_id"`
					TaskType   model.TaskType `json:"task_type"`
					WaitReason string         `json:"wait_reason"`
					Error      string         `json:"error"`
				}
				if err := json.Unmarshal(logged, &entry); err != nil {
					t.Fatalf("read wait diagnostic: %v", err)
				}
				if entry.TaskID != taskRow.ID || entry.TaskType != taskType || entry.WaitReason != phase.reason || entry.Error != wantError {
					t.Fatalf("wait diagnostic = %#v", entry)
				}
				if strings.Contains(wantError, "secret") || strings.Contains(string(logged), "secret") {
					t.Fatal("waiting task or log exposed endpoint credentials")
				}
				if phase.reason == "providers" && (!strings.Contains(wantError, "https://provider.example/pdp/ping") || !strings.Contains(wantError, "provider.example resolves to 198.18.0.1")) {
					t.Fatal("waiting task omitted the blocked provider address")
				}
				wakeTask(t, runtime, taskRow.ID)
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
					return task.ClaimGeneration == 2 && task.Status == model.TaskStatusPending && task.LastError != nil && *task.LastError == wantError
				})
				repeated, err := os.ReadFile(logFile.Name())
				if err != nil || string(repeated) != string(logged) {
					t.Fatalf("unchanged dependency produced another warning: %s, err=%v", repeated, err)
				}
				dependencyStage.Store(1)
				wakeTask(t, runtime, taskRow.ID)
				wantChangedError := errtext.Format(changedErr)
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
					return task.ClaimGeneration == 3 && task.Status == model.TaskStatusPending && task.LastError != nil && *task.LastError == wantChangedError
				})
				changed, err := os.ReadFile(logFile.Name())
				if err != nil {
					t.Fatal(err)
				}
				entries := strings.Split(strings.TrimSpace(string(changed)), "\n")
				if len(entries) != 2 {
					t.Fatalf("changed dependency warnings = %d, want 2: %s", len(entries), changed)
				}
				if err := json.Unmarshal([]byte(entries[1]), &entry); err != nil || entry.Error != wantChangedError || entry.TaskID != taskRow.ID {
					t.Fatalf("changed dependency diagnostic = %#v, err=%v", entry, err)
				}
				if strings.Contains(string(changed), "secret") {
					t.Fatal("changed dependency exposed endpoint credentials")
				}
				dependencyStage.Store(2)
				wakeTask(t, runtime, taskRow.ID)
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
					return task.ClaimGeneration == 4 && task.Status != model.TaskStatusRunning && task.LastError == nil
				})
				bindings, err := runtime.repos.Contents.ListDataSetBindings(t.Context(), bucket.ID)
				if err != nil || len(bindings) != 1 {
					t.Fatalf("planning did not resume after dependency recovered: bindings=%d err=%v", len(bindings), err)
				}
			})
		}
	}
}

func TestDataSetEnsureSchedulesBoundCopyWorkWhenReady(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing_task_%t", existing), func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyNone})
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
			future := time.Now().Add(time.Hour)
			var waiting *model.Task
			if existing {
				waiting = bindCopyTask(t, runtime, copyRow, model.TaskTypeStorageTransferPlan)
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
					Set("available_at = ?", future).Set("wait_reason = ?", "dataset").
					Where("id = ?", waiting.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			ensureTask, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
				Type:  model.TaskTypeStorageDataSetEnsure,
				Input: storagepipeline.DataSetInput{ExecutionIdentity: testStorageExecutionIdentity(), DataSetID: binding.ID},
			}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
				return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
			})
			if err != nil {
				t.Fatalf("enqueue data set ensure: %v", err)
			}
			dataSetID := testOnChainID(t, 10000+sequence)
			clientDataSetID := testOnChainID(t, 11000+sequence)
			if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{
				ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
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
			if existing && (transferTask.ID != waiting.ID || transferTask.AvailableAt.After(time.Now()) || transferTask.RetryCount != waiting.RetryCount) {
				t.Fatalf("waiting transfer was not woken: %#v", transferTask)
			}
		})
	}
}

func TestDataSetCreationDoesNotRequireMetadataDiscovery(t *testing.T) {
	var createCalls, discoveryCalls atomic.Int64
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 9000+sequence)
	dataSetID := testOnChainID(t, 10000+sequence)
	provider := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
			createCalls.Add(1)
			ref, err := storage.NewDataSetRef(providerID.SDK(), dataSetID.SDK(), *options.ClientDataSetID)
			return &storage.CreateDataSetResult{DataSet: ref, TransactionID: "0xsubmitted", ConfirmedTransactionID: "0xconfirmed"}, errors.Join(err, errors.New("provider response ended after confirmation"))
		},
	}
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return provider, nil
		},
		FindMatchingDataSetFunc: func(context.Context, sdktypes.BigInt, map[string]string, bool) (*storage.DataSetRef, error) {
			discoveryCalls.Add(1)
			return nil, &synapse.ProviderUnavailableError{Cause: errors.New("data set discovery unavailable")}
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
	})
	fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("ensure-task-%d", sequence))
	runOneStorageTask(t, runtime, fixture.ensureTask, model.TaskStatusCompleted)
	binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
	if err != nil || binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || !binding.DataSetID.Equal(dataSetID) || binding.ClientDataSetID == nil || binding.ClientDataSetID.IsZero() {
		t.Fatalf("created data set = %#v err=%v", binding, err)
	}
	if binding.TxHash == nil || *binding.TxHash != "0xsubmitted" || binding.ConfirmedTxHash == nil || *binding.ConfirmedTxHash != "0xconfirmed" {
		t.Fatalf("creation evidence=%#v", binding)
	}
	if createCalls.Load() != 1 || discoveryCalls.Load() != 0 {
		t.Fatalf("creation calls=%d metadata discovery calls=%d", createCalls.Load(), discoveryCalls.Load())
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
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, maxAttempts: taskTestMaxAttempts(&noRetries),
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
		Type:  model.TaskTypeStorageDataSetEnsure,
		Input: storagepipeline.DataSetInput{ExecutionIdentity: testStorageExecutionIdentity(), DataSetID: binding.ID},
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue data set ensure: %v", err)
	}
	rejectTaskCheckpoint(t, runtime.db, taskRow.ID)
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" || !handlerTaskRetryable(t, runtime, failed) || len(failed.Checkpoint) != 0 {
		t.Fatalf("data set admission task = %#v", failed)
	}
	stored, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
	if err != nil || stored.Status != model.StorageDataSetStatusPending || stored.TxHash != nil || stored.CreateStatusURL != nil {
		t.Fatalf("data set after admission failure = %#v, err=%v", stored, err)
	}
	if createCalls.Load() != 0 {
		t.Fatalf("data set creation after admission failure = %d calls", createCalls.Load())
	}
	{
		var retryErr error
		taskRow, retryErr = runtime.service.Retry(t.Context(), taskRow.ID)
		if retryErr != nil {
			t.Fatalf("retry data set admission task: %v", retryErr)
		}
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if createCalls.Load() != 0 {
		t.Fatalf("data set recovery created %d services before returning to execute", createCalls.Load())
	}
}

func TestTaskHistorySurvivesReleasedStorageOwner(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		policy: cache.EvictionPolicyNone,
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageTransferPlan)
	claimed, err := runtime.repos.Tasks.ClaimNext(t.Context(), time.Minute, repository.TaskClaimFilter{})
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("claim copy task = %#v, err=%v", claimed, err)
	}
	if err := runtime.repos.Contents.CompleteCopyTask(t.Context(), pipeline.target.ID, pipeline.target.WorkGeneration+1, taskRow.ID); err != nil {
		t.Fatalf("clear copy task fence: %v", err)
	}
	if err := runtime.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover,
	}); err != nil {
		t.Fatalf("complete retained task: %v", err)
	}
	if stored, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID); err != nil || stored == nil || stored.Status != model.TaskStatusCompleted || len(stored.Input) == 0 {
		t.Fatalf("saved task history = %#v, err=%v", stored, err)
	}
	if upload, err := runtime.repos.Contents.GetByID(t.Context(), pipeline.upload.ID); err != nil || upload == nil {
		t.Fatalf("storage upload evidence = %#v, err=%v", upload, err)
	}
	if copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID); err != nil || copyRow == nil {
		t.Fatalf("storage copy evidence = %#v, err=%v", copyRow, err)
	}
}

func TestStoreTasksReachAndRespectTypeConcurrency(t *testing.T) {
	for _, configuredLimit := range []int{0, 2} {
		t.Run(fmt.Sprintf("configured=%d", configuredLimit), func(t *testing.T) {
			limit := configuredLimit
			if limit == 0 {
				limit = 4
			}
			var running, maximum, cacheOpens atomic.Int64
			entered := make(chan struct{}, limit+2)
			release := make(chan struct{})
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				cacheOpens.Add(1)
				return io.NopCloser(strings.NewReader(strings.Repeat("s", 128))), &cache.ObjectInfo{Size: 128}, nil
			}}
			storageClient := &testutil.MockStorageClient{}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				cache: cacheStore, storage: storageClient, policy: cache.EvictionPolicyNone, concurrency: 8, uploadConcurrency: configuredLimit,
				// Keep blocked provider calls fenced under the race detector.
				leaseDuration: 5 * time.Second,
			})
			clients := make(map[string]sdktypes.BigInt)
			tasks := make([]*model.Task, 0, limit+2)
			for range limit + 2 {
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
					UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
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
			for range limit {
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					close(release)
					t.Fatal("store tasks did not reach upload concurrency")
				}
			}
			// Capacity waits never claim or prepare bytes; their original due time stays intact.
			waiting := make(map[int64]bool)
			for _, taskRow := range tasks {
				stored, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
				if err != nil {
					close(release)
					t.Fatalf("load store task: %v", err)
				}
				if stored.Status == model.TaskStatusPending && stored.ClaimGeneration == 0 &&
					stored.WaitReason == nil && stored.RetryCount == 0 && len(stored.Checkpoint) == 0 &&
					stored.WorkStartedAt == nil && stored.AvailableAt.Equal(taskRow.AvailableAt) {
					waiting[stored.ID] = true
				}
			}
			if len(waiting) != 2 || cacheOpens.Load() != int64(2*limit) {
				close(release)
				t.Fatalf("store tasks beyond the limit = waiting:%d cache opens:%d, want 2 unclaimed and %d opens", len(waiting), cacheOpens.Load(), 2*limit)
			}
			close(release)
			for _, taskRow := range tasks {
				if waiting[taskRow.ID] {
					continue
				}
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
				})
			}
			for id := range waiting {
				waitForTask(t, runtime.repos, id, func(task *model.Task) bool {
					return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
				})
			}
			// Identification and upload each read the cache; recovery may read it again.
			if maximum.Load() != int64(limit) || cacheOpens.Load() < int64(2*len(tasks)) {
				t.Fatalf("store upload concurrency = maximum:%d cache opens:%d, want %d and at least %d", maximum.Load(), cacheOpens.Load(), limit, 2*len(tasks))
			}
		})
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
		UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
			storeCalls.Add(1)
			return nil, errors.New("unexpected Store call")
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, maxAttempts: taskTestMaxAttempts(&noRetries),
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
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" || !handlerTaskRetryable(t, runtime, failed) || len(failed.Checkpoint) != 0 {
		t.Fatalf("store admission task = %#v", failed)
	}
	if failed.WorkStartedAt != nil {
		t.Fatal("store admission failure invented an upload start")
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID || copyRow.IngressStoreAttempt != 0 {
		t.Fatalf("copy after store admission failure = %#v, err=%v", copyRow, err)
	}
	if storeCalls.Load() != 0 || cacheOpens.Load() != 0 {
		t.Fatalf("store admission calls = store:%d cache:%d, want 0/0", storeCalls.Load(), cacheOpens.Load())
	}
	{
		var retryErr error
		taskRow, retryErr = runtime.service.Retry(t.Context(), taskRow.ID)
		if retryErr != nil {
			t.Fatalf("retry store admission task: %v", retryErr)
		}
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	if storeCalls.Load() != 0 || cacheOpens.Load() != 0 {
		t.Fatalf("store recovery calls = store:%d cache:%d, want no new calls", storeCalls.Load(), cacheOpens.Load())
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
		ServiceURLValue:          "https://parked.example",
		FinalizeStoreSessionFunc: func(context.Context, string, string, cid.Cid) error { return synapse.ErrStoreSessionIncomplete },
		UploadStoreSessionFunc: func(_ context.Context, _ string, _ string, reader io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
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
			return testutil.CommitExtraData(7), nil
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, parkedPieces: parked, policy: cache.EvictionPolicyNone,
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
		Set("piece_cid = ?", info.CIDv2.String()).Set("checksum = ?", testutil.StorageChecksum(payload)).Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
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
	recovering := runOneStorageTask(t, runtime, taskRow, model.TaskStatusPending)
	if recovering.ResumeMode != model.TaskResumeModeRecover || len(recovering.Checkpoint) == 0 || storeCalls.Load() != 1 {
		t.Fatalf("interrupted store = %#v calls:%d, want checkpointed recovery after one Store", recovering, storeCalls.Load())
	}
	var checkpoint struct {
		storagepipeline.ExecutionIdentity
		VerifiedChecksum     string    `json:"verified_checksum,omitempty"`
		AttemptedAt          time.Time `json:"attempted_at"`
		IntendedPieceCID     string    `json:"intended_piece_cid"`
		ProviderServiceURL   string    `json:"provider_service_url"`
		IngressAttempt       int       `json:"ingress_attempt"`
		SessionID            string    `json:"session_id"`
		Stage                string    `json:"stage"`
		ObservationStartedAt time.Time `json:"observation_started_at"`
	}
	if err := json.Unmarshal(recovering.Checkpoint, &checkpoint); err != nil {
		t.Fatalf("decode store checkpoint: %v", err)
	}
	if checkpoint.IntendedPieceCID != info.CIDv2.String() || checkpoint.ProviderServiceURL != target.ServiceURL() {
		t.Fatalf("store checkpoint = %#v", checkpoint)
	}
	parkedState.Store(synapse.ParkedPieceProcessing)
	wakeTask(t, runtime, taskRow.ID)
	processing := runOneStorageTask(t, runtime, recovering, model.TaskStatusPending)
	if processing.ResumeMode != model.TaskResumeModeRecover || processing.RetryCount != 1 || storeCalls.Load() != 1 {
		t.Fatalf("processing recovery = %#v calls:%d, want recovery without retransmission", processing, storeCalls.Load())
	}
	parkedError.Store(true)
	wakeTask(t, runtime, taskRow.ID)
	lookupFailed := runOneStorageTask(t, runtime, processing, model.TaskStatusPending)
	if lookupFailed.ResumeMode != model.TaskResumeModeRecover || lookupFailed.RetryCount != 2 || storeCalls.Load() != 1 {
		t.Fatalf("failed lookup recovery = %#v calls:%d, want recovery without retransmission", lookupFailed, storeCalls.Load())
	}
	parkedError.Store(false)
	parkedState.Store(synapse.ParkedPieceMissing)
	checkpoint.AttemptedAt = time.Now().Add(-31 * time.Minute)
	checkpointJSON, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("encode old store checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, json.RawMessage(checkpointJSON), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("age store checkpoint: %v", err)
	}
	wakeTask(t, runtime, taskRow.ID)
	replayable := runOneStorageTask(t, runtime, lookupFailed, model.TaskStatusPending)
	if replayable.ResumeMode != model.TaskResumeModeExecute || storeCalls.Load() != 1 {
		t.Fatalf("missing piece recovery = %#v calls:%d, want Execute without retransmission in Recover", replayable, storeCalls.Load())
	}
	wakeTask(t, runtime, taskRow.ID)
	retrying := runOneStorageTask(t, runtime, replayable, model.TaskStatusPending)
	if retrying.ResumeMode != model.TaskResumeModeRecover || retrying.RetryCount != 3 || storeCalls.Load() != 2 {
		t.Fatalf("retransmitted store = retry count:%d calls:%d, want 3/2", retrying.RetryCount, storeCalls.Load())
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
		t.Fatalf("unknown store copy = %#v, err=%v", copyRow, err)
	}
	parkedState.Store(synapse.ParkedPieceReady)
	wakeTask(t, runtime, taskRow.ID)
	runOneStorageTask(t, runtime, retrying, model.TaskStatusCompleted)
	if storeCalls.Load() != 2 || cacheOpens.Load() != 4 || parkedCalls.Load() < 5 {
		t.Fatalf("store recovery calls = store:%d cache:%d parked:%d", storeCalls.Load(), cacheOpens.Load(), parkedCalls.Load())
	}
}

func TestPullRecoverObservesThenRepeatsIdenticalRequestInExecute(t *testing.T) {
	for _, lostCheckpoint := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost checkpoint=%v", lostCheckpoint), func(t *testing.T) {
			testPullRequestReplay(t, lostCheckpoint)
		})
	}
}

func testPullRequestReplay(t *testing.T, lostCheckpoint bool) {
	var pullCalls, statusCalls, presignCalls atomic.Int64
	type observedPull struct {
		piece string
		extra string
		url   string
	}
	var observedMu sync.Mutex
	var observed []observedPull
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			presignCalls.Add(1)
			return testutil.CommitExtraData(7), nil
		},
		SubmitPullFunc: func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
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
			return pullStatusResult(request, storage.PullStatusComplete), nil
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		parkedPieces: parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
			statusCalls.Add(1)
			if pullCalls.Load() >= 2 {
				return synapse.ParkedPieceReady, nil
			}
			return synapse.ParkedPieceMissing, nil
		}),
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
	recovering := runOneStorageTask(t, runtime, taskRow, model.TaskStatusPending)
	if recovering.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("pull after ambiguous response = %#v", recovering)
	}
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
	if attempt.ExtraDataHex == "" || attempt.ResolvedAt != nil {
		t.Fatalf("attempt before replay = %#v", attempt)
	}
	if count, err := runtime.db.NewSelect().Model((*storagecommit.Request)(nil)).Where("storage_data_set_id = ?", pipeline.targetSet.ID).Count(t.Context()); err != nil || count != 0 {
		t.Fatalf("Commit requests before Pull completes = %d, %v", count, err)
	}
	if lostCheckpoint {
		if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = NULL").Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ?, work_started_at = NULL WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("make pull recovery ready: %v", err)
	}
	replayed := runOneStorageTask(t, runtime, recovering, model.TaskStatusPending)
	if replayed.ResumeMode != model.TaskResumeModeExecute || statusCalls.Load() != 1 || pullCalls.Load() != 1 {
		t.Fatalf("pull observation mode=%s observations=%d submissions=%d", replayed.ResumeMode, statusCalls.Load(), pullCalls.Load())
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("make repeated pull ready: %v", err)
	}
	completed := runOneStorageTask(t, runtime, replayed, model.TaskStatusCompleted)
	if completed.WorkStartedAt == nil || !completed.WorkStartedAt.After(attempt.AttemptedAt) {
		t.Fatalf("pull recovery start = %v, want %v", completed.WorkStartedAt, attempt.AttemptedAt)
	}
	if pullCalls.Load() != 2 || presignCalls.Load() != 1 {
		t.Fatalf("pull calls = %d, presign calls = %d; want 2 pulls with one authorization", pullCalls.Load(), presignCalls.Load())
	}
	// A finished transfer joins an unsigned collecting request.
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow == nil || copyRow.CommitRequestID == nil || copyRow.WorkTaskID() == nil ||
		copyRow.Status != model.StorageCopyStatusPieceReady || copyRow.CommitSealed() {
		t.Fatalf("target copy after pull = %#v, err=%v", copyRow, err)
	}
	next, err := runtime.repos.Tasks.GetByID(t.Context(), *copyRow.WorkTaskID())
	if err != nil || next == nil || next.Type != model.TaskTypeStorageCommit {
		t.Fatalf("task after pull = %#v, err=%v, want storage_commit", next, err)
	}
	settled, err := runtime.repos.Contents.GetPullAttempt(t.Context(), attempt.AttemptID, pipeline.target.ContentID, pipeline.target.StorageDataSetID)
	if err != nil || settled.ResolvedAt == nil || settled.ExtraDataHex != attempt.ExtraDataHex {
		t.Fatalf("settled attempt = %#v, %v", settled, err)
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observed) != 2 || observed[0] != observed[1] {
		t.Fatalf("pull requests = %#v, want two identical requests", observed)
	}
}

func TestPullProviderFailureAtomicallyAbandonsAttemptAndCopy(t *testing.T) {
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return testutil.CommitExtraData(7), nil },
		SubmitPullFunc: func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
			return pullStatusResult(request, storage.PullStatusFailed), nil
		},
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
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
	return newPullTask(t, func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
		pullCalls.Add(1)
		return nil, pullErr
	}, nil, maxRetries)
}

func newPullTask(
	t *testing.T,
	submit func(context.Context, storage.PullRequest) (*storage.PullResult, error),
	parked synapse.ParkedPieceChecker,
	maxRetries *int,
) (handlerTestRuntime, seededCopyPipeline, *model.Task) {
	t.Helper()
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			return testutil.CommitExtraData(7), nil
		},
		SubmitPullFunc: submit,
	}
	if parked == nil {
		parked = parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
			return synapse.ParkedPieceMissing, nil
		})
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone, maxAttempts: taskTestMaxAttempts(maxRetries), parkedPieces: parked,
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

func assertProtectedPullFailure(
	t *testing.T,
	runtime handlerTestRuntime,
	pipeline seededCopyPipeline,
	taskRow *model.Task,
	wantReason string,
) *model.Task {
	t.Helper()
	failed := runOneStorageTask(t, runtime, taskRow, model.TaskStatusFailed)
	if failed.FailureReason == nil || *failed.FailureReason != wantReason || !handlerTaskRetryable(t, runtime, failed) {
		t.Fatalf("pull task failure = %#v, want retryable %s", failed, wantReason)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageCopyStatusPending ||
		copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
		t.Fatalf("protected pull copy = %#v, err=%v", copyRow, err)
	}
	var attempts []storagepull.Attempt
	if err := runtime.db.NewSelect().Model(&attempts).Where("content_id = ?", pipeline.target.ContentID).Scan(t.Context()); err != nil {
		t.Fatalf("load pull attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != storagepull.AttemptStatusAttempted || attempts[0].ResolvedAt != nil {
		t.Fatalf("pull attempts = %#v, want one unresolved attempt", attempts)
	}
	return failed
}

func TestPullClientErrorsRetainAttemptAndCopy(t *testing.T) {
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
			assertProtectedPullFailure(t, runtime, pipeline, taskRow, storagepull.FailureOutcomeUnknown)
			if pullCalls.Load() != 1 {
				t.Fatalf("pull calls = %d, want 1", pullCalls.Load())
			}
		})
	}
}

func TestPullRetryableProviderErrorKeepsCopyOpen(t *testing.T) {
	limit := 1
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
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
	}, &limitedRepos, handlerExecutionRegistry(t, runtime), slog.Default())
	if err != nil {
		t.Fatalf("new limited task engine: %v", err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)
	pending := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return pullCalls.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if pending.RetryCount != 1 {
		t.Fatalf("retry count = %d, want 1 for retryable provider failure", pending.RetryCount)
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
	failed := assertProtectedPullFailure(t, runtime, pipeline, taskRow, "attempts_exhausted")
	if pullCalls.Load() != 1 {
		t.Fatalf("pull calls = %d, want 1", pullCalls.Load())
	}
	versions := make([]*model.ObjectVersion, 2)
	for i := range versions {
		versions[i] = &model.ObjectVersion{
			VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID,
			Key: fmt.Sprintf("protected-pull-%d", i), ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize,
			ETag: "protected-pull", ContentType: "application/octet-stream",
		}
		if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), versions[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i, version := range versions {
		_, err := runtime.repos.Objects.DeleteObjectVersionPermanently(t.Context(), repository.DeleteObjectVersionInput{
			BucketID: version.BucketID, Key: version.Key, VersionID: version.VersionID,
		})
		if i == 0 && err != nil {
			t.Fatalf("delete shared reference: %v", err)
		}
		if i == 1 && !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
			t.Fatalf("delete last reference to an unresolved pull = %v", err)
		}
	}
	{
		var retryErr error
		taskRow, retryErr = runtime.service.Retry(t.Context(), taskRow.ID)
		if retryErr != nil {
			t.Fatal(retryErr)
		}
	}
	retried, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
	if err != nil || retried.Status != model.TaskStatusPending || retried.ResumeMode != model.TaskResumeModeRecover ||
		retried.RetryCount != 0 || !bytes.Equal(retried.Checkpoint, failed.Checkpoint) {
		t.Fatalf("manual pull retry = %#v, %v", retried, err)
	}
}

func TestPullRecoverWithoutCheckpointReturnsToExecute(t *testing.T) {
	var presignCalls, pullCalls, statusCalls atomic.Int64
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			presignCalls.Add(1)
			return testutil.CommitExtraData(7), nil
		},
		SubmitPullFunc: func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
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
	claimed, err := runtime.repos.Tasks.ClaimNext(t.Context(), 100*time.Millisecond, repository.TaskClaimFilter{})
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

func TestTransfersWaitForCommitBacklog(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeStorageStore, model.TaskTypeStoragePull} {
		t.Run(string(taskType), func(t *testing.T) {
			target := &testutil.MockStorageTarget{
				PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
					t.Error("a backlogged transfer signed a commit request")
					return nil, errors.New("unexpected presign")
				},
			}
			storageClient := &testutil.MockStorageClient{
				OpenDataSetTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
					return target, nil
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: storageClient, policy: cache.EvictionPolicyNone,
			})
			pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
			target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
			dataSetID := pipeline.targetSet.DataSetID.SDK()
			target.DataSetIDValue = &dataSetID
			target.ClientDataSetIDValue = pipeline.targetClient
			runtime.repos.Contents = &readyCopyCountRepo{StorageContentRepository: runtime.repos.Contents, count: config.DefaultCommitMaxBacklog}
			taskRow := bindCopyTask(t, runtime, pipeline.target, taskType)
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)

			waiting := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "commit_backlog"
			})
			if waiting.ResumeMode != model.TaskResumeModeExecute || len(waiting.Checkpoint) != 0 {
				t.Fatalf("waiting task = %#v, want an execute wait without a transfer checkpoint", waiting)
			}
		})
	}
}

type readyCopyCountRepo struct {
	repository.StorageContentRepository
	count int
}

func (r *readyCopyCountRepo) CountCommitBacklog(context.Context, int64) (int, error) {
	return r.count, nil
}

func waitForCopy(t *testing.T, runtime handlerTestRuntime, copyID int64, predicate func(*model.StorageCopy) bool) *model.StorageCopy {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyID)
		if err != nil {
			t.Fatalf("load copy %d: %v", copyID, err)
		}
		if predicate(copyRow) {
			return copyRow
		}
		if time.Now().After(deadline) {
			t.Fatalf("copy %d did not reach the expected state: %#v", copyID, copyRow)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWalletRecoveryObservesTransactionWithoutRebroadcast(t *testing.T) {
	var broadcasts, observations atomic.Int64
	transactionHash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		wallet: testWalletOperator{fund: func(_ context.Context, amount *big.Int) (synapse.WalletSubmission, error) {
			broadcasts.Add(1)
			if amount.String() != "100" {
				t.Fatalf("wallet amount = %s, want 100", amount)
			}
			return synapse.WalletSubmission{TxHash: transactionHash}, nil
		}},
		receipts: testReceiptChecker{check: func(_ context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
			observations.Add(1)
			if hash != common.HexToHash(transactionHash) {
				t.Fatalf("receipt hash = %s, want %s", hash, transactionHash)
			}
			return &ethtypes.Receipt{Status: ethtypes.ReceiptStatusSuccessful, TxHash: hash}, nil
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
			Type:  model.TaskTypeWalletOperation,
			Input: walletoperation.Input{ExecutionIdentity: testutil.DefaultWalletExecutionIdentity(), OperationID: operation.ID},
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
	broadcast, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || broadcast.BroadcastAttemptedAt == nil {
		t.Fatalf("broadcast timing = %#v, %v", broadcast, err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ?, work_started_at = NULL WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("make wallet recovery ready: %v", err)
	}
	completed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 2 && task.Status == model.TaskStatusCompleted
	})
	if completed.WorkStartedAt == nil || !completed.WorkStartedAt.After(*broadcast.BroadcastAttemptedAt) {
		t.Fatalf("wallet recovery start = %v, want %v", completed.WorkStartedAt, broadcast.BroadcastAttemptedAt)
	}
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
		wallet: testWalletOperator{fund: func(ctx context.Context, _ *big.Int) (synapse.WalletSubmission, error) {
			broadcasts.Add(1)
			<-ctx.Done()
			return synapse.WalletSubmission{}, context.Cause(ctx)
		}},
	})
	operation, taskRow := seedWalletTask(t, runtime, "wallet-broadcast-timeout")
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if handlerTaskRetryable(t, runtime, failed) {
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
		maxAttempts: taskTestMaxAttempts(&limit),
		wallet: testWalletOperator{fund: func(context.Context, *big.Int) (synapse.WalletSubmission, error) {
			broadcasts.Add(1)
			return synapse.WalletSubmission{}, errors.New("unexpected wallet broadcast")
		}},
	})
	operation, taskRow := seedWalletTask(t, runtime, "wallet-admission-failure")
	rejectTaskCheckpoint(t, runtime.db, taskRow.ID)
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" || !handlerTaskRetryable(t, runtime, failed) {
		t.Fatalf("wallet admission task = %#v", failed)
	}
	storedOperation, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || storedOperation.Status != model.WalletOperationStatusPending || storedOperation.TaskID == nil || *storedOperation.TaskID != taskRow.ID || storedOperation.BroadcastAttemptedAt != nil {
		t.Fatalf("wallet operation after admission failure = %#v, err=%v", storedOperation, err)
	}
	if len(failed.Checkpoint) != 0 || broadcasts.Load() != 0 {
		t.Fatalf("wallet admission wrote evidence or called effect: checkpoint=%s broadcasts=%d", failed.Checkpoint, broadcasts.Load())
	}
	{
		var retryErr error
		taskRow, retryErr = runtime.service.Retry(t.Context(), taskRow.ID)
		if retryErr != nil {
			t.Fatalf("retry not-started wallet task: %v", retryErr)
		}
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
		wallet:               testWalletOperator{},
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
	checkpoint, err := json.Marshal(walletoperation.Checkpoint{ExecutionIdentity: testutil.DefaultWalletExecutionIdentity(), BroadcastAttempted: true, TransactionHash: transactionHash})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(checkpoint)).Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	waiting := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return limited.claims.Load() == 1 && task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	if observations.Load() != 1 {
		t.Fatalf("wallet receipt observations = %d, want 1", observations.Load())
	}
	if waiting.LastError == nil || *waiting.LastError != context.DeadlineExceeded.Error() {
		t.Fatalf("waiting task last_error = %v, want why the receipt lookup failed", waiting.LastError)
	}
}

func TestWalletRecoveryRequiresConsistentTransactionEvidence(t *testing.T) {
	transactionHash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").Hex()
	for _, scenario := range []string{"missing checkpoint", "conflicting checkpoint", "checkpoint only", "checkpoint only retry", "wrong receipt hash", "wrong receipt status"} {
		t.Run(scenario, func(t *testing.T) {
			var observations atomic.Int64
			var operationID int64
			var runtime handlerTestRuntime
			runtime = newHandlerTestRuntime(t, handlerRuntimeOptions{
				wallet: testWalletOperator{},
				receipts: testReceiptChecker{check: func(_ context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
					observations.Add(1)
					stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operationID)
					if err != nil || stored.TxHash == nil || *stored.TxHash != transactionHash || stored.Status != model.WalletOperationStatusSubmitted {
						t.Errorf("RPC observed before transaction was recorded: %#v %v", stored, err)
					}
					if scenario == "wrong receipt hash" {
						hash = common.HexToHash("0x9999")
					}
					status := uint64(ethtypes.ReceiptStatusSuccessful)
					if scenario == "wrong receipt status" {
						status = 2
					}
					return &ethtypes.Receipt{TxHash: hash, Status: status}, nil
				}},
			})
			operation, row := seedWalletTask(t, runtime, scenario)
			operationID = operation.ID
			if err := runtime.repos.WalletOperations.MarkBroadcastAttempted(t.Context(), operation.ID, row.ID); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(scenario, "checkpoint only") {
				if err := runtime.repos.WalletOperations.MarkSubmitted(t.Context(), operation.ID, row.ID, transactionHash); err != nil {
					t.Fatal(err)
				}
			}
			if scenario != "missing checkpoint" {
				hash := transactionHash
				if scenario == "conflicting checkpoint" {
					hash = common.HexToHash("0x9999").Hex()
				}
				checkpoint, err := json.Marshal(walletoperation.Checkpoint{ExecutionIdentity: testutil.DefaultWalletExecutionIdentity(), BroadcastAttempted: true, TransactionHash: hash})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(checkpoint)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "checkpoint only retry" {
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).
					Set("failure_reason = ?", "handler_panic").Set("last_error = ?", "interrupted handler").Set("finished_at = ?", time.Now()).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				var err error
				row, err = runtime.service.Retry(t.Context(), row.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			want := model.TaskStatusFailed
			if strings.HasPrefix(scenario, "checkpoint only") {
				want = model.TaskStatusCompleted
			}
			finished := runOneStorageTask(t, runtime, row, want)
			stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(scenario, "checkpoint only") {
				if observations.Load() != 1 || stored.Status != model.WalletOperationStatusConfirmed || stored.TaskID != nil {
					t.Fatalf("checkpoint recovery did not confirm original transaction: %#v", stored)
				}
				return
			}
			wantObservations := int64(0)
			if scenario == "wrong receipt hash" || scenario == "wrong receipt status" {
				wantObservations = 1
			}
			if observations.Load() != wantObservations || finished.FailureReason == nil || *finished.FailureReason != "wallet_evidence_conflict" || stored.TaskID == nil || *stored.TaskID != row.ID || stored.Status != model.WalletOperationStatusSubmitted {
				t.Fatalf("conflicting transaction evidence was not retained: task=%#v operation=%#v observations=%d", finished, stored, observations.Load())
			}
		})
	}
}

func TestWalletRetryRejectsUnverifiableTransactionEvidence(t *testing.T) {
	transactionHash := common.HexToHash("0x1234").Hex()
	for _, scenario := range []string{"missing hash", "conflicting identity", "conflicting hash", "invalid signed transaction", "invalid checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{wallet: testWalletOperator{}})
			operation, row := seedWalletTask(t, runtime, scenario)
			if err := runtime.repos.WalletOperations.MarkBroadcastAttempted(t.Context(), operation.ID, row.ID); err != nil {
				t.Fatal(err)
			}
			checkpoint := walletoperation.Checkpoint{ExecutionIdentity: testutil.DefaultWalletExecutionIdentity(), BroadcastAttempted: true}
			if scenario != "missing hash" {
				checkpoint.TransactionHash = transactionHash
				var raw []byte
				if scenario == "invalid signed transaction" {
					raw = []byte{1, 2, 3}
				}
				if err := runtime.repos.WalletOperations.RecordSubmission(t.Context(), operation.ID, row.ID, transactionHash, raw, ""); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "conflicting identity":
				identity := *checkpoint.Identity
				identity.Payer = common.HexToAddress("0x9999")
				checkpoint.Identity = &identity
			case "conflicting hash":
				checkpoint.TransactionHash = common.HexToHash("0x9999").Hex()
			}
			var err error
			row.Checkpoint, err = json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "invalid checkpoint" {
				row.Checkpoint = json.RawMessage(`{"broadcast_attempted":"invalid"}`)
			}
			row.Status = model.TaskStatusFailed
			row.FailureReason = new("handler_panic")
			if err := runtime.service.InspectRetry(t.Context(), row); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("unverifiable evidence should reject Retry: %v", err)
			}
			if retryable, err := runtime.service.RetryableContext(t.Context(), row); err != nil || retryable {
				t.Fatalf("unverifiable evidence should be unavailable without a server error: retryable=%v err=%v", retryable, err)
			}
		})
	}
}

func TestWalletInvalidAmountSettlesOnlyBeforeBroadcast(t *testing.T) {
	for _, scenario := range []string{"no broadcast evidence", "checkpoint only", "broadcast attempted", "submitted transaction"} {
		t.Run(scenario, func(t *testing.T) {
			var broadcasts atomic.Int64
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{wallet: testWalletOperator{
				fund: func(context.Context, *big.Int) (synapse.WalletSubmission, error) {
					broadcasts.Add(1)
					return synapse.WalletSubmission{}, errors.New("unexpected broadcast")
				},
			}})
			operation, row := seedWalletTask(t, runtime, scenario)
			amount := new(big.Int).Lsh(big.NewInt(1), 256).String()
			if _, err := runtime.db.NewUpdate().Model((*model.WalletOperation)(nil)).Set("amount = ?", amount).Where("id = ?", operation.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if scenario == "broadcast attempted" || scenario == "submitted transaction" {
				if err := runtime.repos.WalletOperations.MarkBroadcastAttempted(t.Context(), operation.ID, row.ID); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "checkpoint only" {
				checkpoint, err := json.Marshal(walletoperation.Checkpoint{ExecutionIdentity: testutil.DefaultWalletExecutionIdentity(), BroadcastAttempted: true, TransactionHash: common.HexToHash("0x1234").Hex()})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(checkpoint)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "submitted transaction" {
				if err := runtime.repos.WalletOperations.RecordSubmission(t.Context(), operation.ID, row.ID, common.HexToHash("0x1234").Hex(), []byte{1, 2, 3}, ""); err != nil {
					t.Fatal(err)
				}
			}
			finished := runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if broadcasts.Load() != 0 || finished.FailureReason == nil || *finished.FailureReason != "wallet_amount_invalid" {
				t.Fatalf("invalid amount invoked wallet or failed incorrectly: broadcasts=%d task=%#v", broadcasts.Load(), finished)
			}
			if scenario == "no broadcast evidence" {
				if stored.Status != model.WalletOperationStatusFailed || stored.TaskID != nil || stored.CompletedAt == nil {
					t.Fatalf("unsent invalid operation remains unresolved: %#v", stored)
				}
				if err := runtime.service.InspectRetry(t.Context(), finished); !errors.Is(err, repository.ErrConflict) {
					t.Fatalf("invalid amount should not allow another wallet attempt: %v", err)
				}
				return
			}
			wantStatus := model.WalletOperationStatusPending
			if scenario == "submitted transaction" {
				wantStatus = model.WalletOperationStatusSubmitted
			}
			if stored.Status != wantStatus || stored.TaskID == nil || *stored.TaskID != row.ID || stored.CompletedAt != nil {
				t.Fatalf("unresolved broadcast evidence was discarded: %#v", stored)
			}
		})
	}
}

func TestWalletInvalidBroadcastEvidencePreservesCandidateHash(t *testing.T) {
	transactionHash := common.HexToHash("0x1234").Hex()
	raw := bytes.Repeat([]byte{0x82}, 32)
	for _, scenario := range []string{"invalid returned submission", "adapter rejected evidence"} {
		t.Run(scenario, func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{wallet: testWalletOperator{
				fund: func(context.Context, *big.Int) (synapse.WalletSubmission, error) {
					var err error
					if scenario == "adapter rejected evidence" {
						err = fmt.Errorf("%w: transaction mismatch", synapse.ErrWalletReplayInvalidEvidence)
					}
					return synapse.WalletSubmission{TxHash: transactionHash, SignedTransaction: raw}, err
				},
			}})
			operation, row := seedWalletTask(t, runtime, scenario)
			finished := runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			if finished.FailureReason == nil || *finished.FailureReason != "wallet_evidence_conflict" || finished.LastError == nil || !strings.Contains(*finished.LastError, transactionHash) || strings.Contains(*finished.LastError, hex.EncodeToString(raw)) {
				t.Fatalf("conflicting submission should retain only its candidate hash in diagnostics: %#v", finished)
			}
			stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
			if err != nil || stored.Status != model.WalletOperationStatusPending || stored.TaskID == nil || *stored.TaskID != row.ID || stored.BroadcastAttemptedAt == nil || stored.TxHash != nil || len(stored.SignedTransaction) != 0 {
				t.Fatalf("conflicting submission was accepted or its owner lost: %#v %v", stored, err)
			}
			var checkpoint walletoperation.Checkpoint
			if err := json.Unmarshal(finished.Checkpoint, &checkpoint); err != nil || checkpoint.TransactionHash != "" {
				t.Fatalf("conflicting submission entered checkpoint: %#v %v", checkpoint, err)
			}
		})
	}
}

type walletRPCDiagnosticError string

func (e walletRPCDiagnosticError) Error() string { return string(e) }
func (walletRPCDiagnosticError) ErrorCode() int  { return 3 }
func (walletRPCDiagnosticError) ErrorData() any  { return "0xdeadbeef" }

func TestWalletSignedTransactionObservationAndManualRetry(t *testing.T) {
	key, err := crypto.HexToECDSA(strings.Repeat("01", 32))
	if err != nil {
		t.Fatal(err)
	}
	identity := testutil.DefaultWalletExecutionIdentity()
	contextIdentity := *identity.Identity
	contextIdentity.Payer = crypto.PubkeyToAddress(key.PublicKey)
	identity.Identity = &contextIdentity
	contract, err := abi.JSON(strings.NewReader(`[{"type":"function","name":"withdraw","inputs":[{"type":"address"},{"type":"uint256"}]}]`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := contract.Pack("withdraw", *identity.USDFC, big.NewInt(100))
	if err != nil {
		t.Fatal(err)
	}
	chainID := identity.Identity.ChainID.BigInt()
	tx, err := ethtypes.SignNewTx(key, ethtypes.LatestSignerForChainID(chainID), &ethtypes.DynamicFeeTx{
		ChainID: chainID, Nonce: 7, To: identity.Payments, Value: new(big.Int), Gas: 100000,
		GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2), Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	submission := synapse.WalletSubmission{TxHash: tx.Hash().Hex(), SignedTransaction: raw}
	var broadcasts, replays atomic.Int64
	var confirmed atomic.Bool
	attempts := 2
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		maxAttempts: &attempts,
		wallet: testWalletOperator{
			identity: identity,
			withdraw: func(context.Context, *big.Int) (synapse.WalletSubmission, error) {
				broadcasts.Add(1)
				return submission, &sdktypes.BroadcastError{Transaction: tx, Cause: walletRPCDiagnosticError("execution reverted during broadcast: https://rpc.example/broadcast?token=private-token")}
			},
			replay: func(_ context.Context, operationType model.WalletOperationType, amount *big.Int, got synapse.WalletSubmission) error {
				replays.Add(1)
				if operationType != model.WalletOperationTypeWithdraw || amount.Cmp(big.NewInt(100)) != 0 || got.TxHash != submission.TxHash || !bytes.Equal(got.SignedTransaction, raw) {
					t.Errorf("replayed transaction or request differs")
				}
				return fmt.Errorf("%w: %w", synapse.ErrWalletReplayBroadcast, walletRPCDiagnosticError("execution reverted during replay: https://rpc.example/broadcast?token=private-token"))
			},
		},
		receipts: testReceiptChecker{check: func(_ context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
			if confirmed.Load() {
				return &ethtypes.Receipt{TxHash: hash, Status: ethtypes.ReceiptStatusSuccessful}, nil
			}
			return nil, ethereum.NotFound
		}},
	})
	operation, row := seedWalletTaskWithIdentity(t, runtime, "signed-observation", model.WalletOperationTypeWithdraw, identity)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if row.RetryCount != 0 || row.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("first signed submission consumed an attempt: %#v", row)
	}
	stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || stored.Status != model.WalletOperationStatusSubmitted || !bytes.Equal(stored.SignedTransaction, raw) || stored.LastError == nil {
		t.Fatalf("signed broadcast evidence not preserved: %#v %v", stored, err)
	}
	for _, want := range []string{"execution reverted during broadcast", "RPC code: 3", "Revert data: 0xdeadbeef"} {
		if !strings.Contains(*stored.LastError, want) {
			t.Errorf("broadcast diagnostic = %q, missing %q", *stored.LastError, want)
		}
	}
	if strings.Contains(*stored.LastError, "private-token") {
		t.Fatalf("broadcast diagnostic exposed URL secret: %q", *stored.LastError)
	}
	firstRuntime := bytes.Clone(row.Runtime)
	row = runOneStorageTask(t, runtime, readyWalletTask(t, runtime, row, false), model.TaskStatusPending)
	if row.RetryCount != 0 || replays.Load() != 0 || !testutil.JSONEqual(row.Runtime, firstRuntime) {
		t.Fatalf("restart observation consumed budget, replayed, or reset its clock: %#v", row)
	}
	row = runOneStorageTask(t, runtime, readyWalletTask(t, runtime, row, true), model.TaskStatusPending)
	if row.RetryCount != 1 || row.ResumeMode != model.TaskResumeModeExecute || replays.Load() != 0 {
		t.Fatalf("expired observation did not schedule native execute retry: %#v", row)
	}
	row = runOneStorageTask(t, runtime, readyWalletTask(t, runtime, row, false), model.TaskStatusPending)
	if row.RetryCount != 1 || replays.Load() != 1 || row.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("replay did not enter a fresh free observation: %#v replays=%d", row, replays.Load())
	}
	stored, err = runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || stored.LastError == nil {
		t.Fatalf("replay diagnostic was not persisted: %#v %v", stored, err)
	}
	for _, want := range []string{"execution reverted during replay", "RPC code: 3", "Revert data: 0xdeadbeef"} {
		if !strings.Contains(*stored.LastError, want) {
			t.Errorf("replay diagnostic = %q, missing %q", *stored.LastError, want)
		}
	}
	if strings.Contains(*stored.LastError, "private-token") {
		t.Fatalf("replay diagnostic exposed URL secret: %q", *stored.LastError)
	}
	row = runOneStorageTask(t, runtime, readyWalletTask(t, runtime, row, true), model.TaskStatusFailed)
	if row.FailureReason == nil || *row.FailureReason != "attempts_exhausted" {
		t.Fatalf("exhausted transaction task: %#v", row)
	}
	stored, err = runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || stored.Status != model.WalletOperationStatusSubmitted || stored.TaskID == nil || *stored.TaskID != row.ID || !bytes.Equal(stored.SignedTransaction, raw) || stored.LastError == nil || !strings.Contains(*stored.LastError, "after 2 attempts") {
		t.Fatalf("exhaustion discarded transaction evidence or owner: %#v %v", stored, err)
	}
	confirmed.Store(true)
	next, err := runtime.service.Retry(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	var nextRuntime model.TaskRuntime
	if err := json.Unmarshal(next.Runtime, &nextRuntime); err != nil || nextRuntime.Observation != nil || nextRuntime.OperationKey != "" || next.RetryCount != 0 {
		t.Fatalf("manual retry did not reset runtime and budget: %#v %v", next, err)
	}
	runOneStorageTask(t, runtime, next, model.TaskStatusCompleted)
	stored, err = runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
	if err != nil || stored.Status != model.WalletOperationStatusConfirmed || stored.TaskID != nil || !bytes.Equal(stored.SignedTransaction, raw) || broadcasts.Load() != 1 || replays.Load() != 1 {
		t.Fatalf("manual retry did not settle the original transaction: %#v %v broadcasts=%d replays=%d", stored, err, broadcasts.Load(), replays.Load())
	}
	source, err := runtime.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || source.Status != model.TaskStatusFailed || source.RetryCount != row.RetryCount || !bytes.Equal(source.Checkpoint, row.Checkpoint) {
		t.Fatalf("manual retry rewrote its source: %#v %v", source, err)
	}
}

func readyWalletTask(t *testing.T, runtime handlerTestRuntime, row *model.Task, expire bool) *model.Task {
	t.Helper()
	query := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", row.ID)
	if expire {
		var state model.TaskRuntime
		if err := json.Unmarshal(row.Runtime, &state); err != nil || state.Observation == nil {
			t.Fatalf("observation state missing: %s %v", row.Runtime, err)
		}
		state.Observation.StartedAt = time.Now().Add(-10 * time.Minute)
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		query = query.Set("runtime_json = ?", json.RawMessage(raw))
	}
	if _, err := query.Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	ready, err := runtime.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func seedWalletTask(t *testing.T, runtime handlerTestRuntime, requestID string) (*model.WalletOperation, *model.Task) {
	return seedWalletTaskWithIdentity(t, runtime, requestID, model.WalletOperationTypeFund, testutil.DefaultWalletExecutionIdentity())
}

func seedWalletTaskWithIdentity(t *testing.T, runtime handlerTestRuntime, requestID string, operationType model.WalletOperationType, identity walletoperation.ExecutionIdentity) (*model.WalletOperation, *model.Task) {
	t.Helper()
	var operation *model.WalletOperation
	var taskRow *model.Task
	if err := runtime.repos.WithTx(t.Context(), func(repos *repository.Repositories) error {
		var err error
		operation, _, err = repos.WalletOperations.CreateOrGet(t.Context(), repository.CreateWalletOperationInput{
			Type: operationType, ClientRequestID: requestID, Amount: "100",
		})
		if err != nil {
			return err
		}
		taskRow, _, err = runtime.service.EnqueueInTransaction(t.Context(), repos, taskengine.EnqueueRequest{
			Type:  model.TaskTypeWalletOperation,
			Input: walletoperation.Input{ExecutionIdentity: identity, OperationID: operation.ID},
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
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: testOnChainID(t, 7403), ClientDataSetID: &sourceClientID,
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: target.ID, DataSetID: testOnChainID(t, 7406), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark target ready: %v", err)
	}
	if err := activateReplacement(t, runtime, replacement.ID); err != nil {
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
		Where("id = ?", replacement.ID).
		Exec(ctx); err != nil {
		t.Fatalf("complete replacement seeding: %v", err)
	}
	coordinateTask, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
		Type: model.TaskTypeProviderReplacementCoordinate,
		Input: storagereplacement.CoordinateInput{
			ReplacementID: replacement.ID, Generation: replacement.TaskGeneration,
		},
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue replacement coordinator: %v", err)
	}

	limitedRepos := *runtime.repos
	limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
	}, &limitedRepos, handlerExecutionRegistry(t, runtime), slog.Default())
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

func TestReplacementRecoversFailedMigrationWithinCoordinatorBudget(t *testing.T) {
	dependencyLimit := 1
	for _, tt := range []struct {
		name                   string
		contents               int
		invalidStoreCheckpoint bool
		blockedReason          string
		exhaustedFirst         bool
		dependencyRetries      *int
	}{
		{name: "retryable", contents: 1},
		{name: "retryable batch", contents: 7},
		{name: "exhausted dependency leaves others running", contents: 2, exhaustedFirst: true},
		{name: "invalid Store checkpoint", contents: 1, invalidStoreCheckpoint: true},
		{name: "identity mismatch requires operator", contents: 1, blockedReason: "storage_identity_changed"},
		{name: "content mismatch requires operator", contents: 1, blockedReason: "store_identity_mismatch"},
		{name: "dependency limit and manual reset", contents: 1, dependencyRetries: &dependencyLimit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime, replacement, coordinator := replacementBatchFixture(t, tt.contents, true, 128)
			runtime.testDependencyRetries = tt.dependencyRetries
			ctx := t.Context()
			var contents []model.StorageContent
			if err := runtime.db.NewSelect().Model(&contents).Where("bucket_id = ?", replacement.BucketID).OrderExpr("id ASC").Scan(ctx); err != nil {
				t.Fatal(err)
			}
			source, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, replacement.SourceDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			target, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			var copies []*model.StorageCopy
			var stopped []*model.Task
			var checkpoints []json.RawMessage
			for i, content := range contents {
				identity, err := piece.Calculate(bytes.NewReader(bytes.Repeat([]byte{byte(i + 1)}, int(content.ContentSize))))
				if err != nil {
					t.Fatal(err)
				}
				if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
					StorageDataSetID: source.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: source.ProviderID,
				}}); err != nil {
					t.Fatal(err)
				}
				pieceID := testOnChainID(t, int64(90000+i))
				testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
					ContentID: content.ID, CopyIndex: 0, PieceCID: identity.CIDv2.String(), PieceID: &pieceID, RetrievalURL: "https://source.example/piece",
				})
				if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
					StorageDataSetID: target.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: target.ProviderID,
				}}); err != nil {
					t.Fatal(err)
				}
				copyRow, err := runtime.repos.Contents.GetUploadCopyForDataSet(ctx, content.ID, target.ID)
				if err != nil || copyRow == nil {
					t.Fatalf("target copy = %#v, %v", copyRow, err)
				}
				item := &storagereplacement.Item{ReplacementID: replacement.ID, ContentID: content.ID, TargetDataSetID: target.ID, Status: storagereplacement.ItemStatusPending}
				if _, err := runtime.db.NewInsert().Model(item).Exec(ctx); err != nil {
					t.Fatal(err)
				}
				storeTask := bindCopyTask(t, runtime, copyRow, model.TaskTypeStorageStore)
				claimed, err := runtime.repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{ExcludedTaskIDs: []int64{coordinator.ID}})
				if err != nil || claimed == nil || claimed.ID != storeTask.ID {
					t.Fatalf("claim migration task = %#v, %v", claimed, err)
				}
				reason := "store_outcome_unknown"
				if tt.blockedReason != "" {
					reason = tt.blockedReason
				}
				var checkpoint json.RawMessage
				if tt.invalidStoreCheckpoint {
					reason = "invalid_checkpoint"
				} else {
					checkpoint = json.RawMessage(fmt.Sprintf(`{"attempted_at":"2026-09-11T00:00:00Z","intended_piece_cid":%q,"provider_service_url":"https://target.example","session_id":"00000000-0000-4000-8000-000000000001","stage":"upload","ingress_attempt":0}`, identity.CIDv2.String()))
					if err := runtime.repos.Tasks.WriteCheckpoint(ctx, claimed.ID, claimed.ClaimGeneration, checkpoint); err != nil {
						t.Fatal(err)
					}
				}
				message := "migration stopped"
				if err := runtime.repos.Tasks.Settle(ctx, claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason, LastError: &message}); err != nil {
					t.Fatal(err)
				}
				copies = append(copies, copyRow)
				stopped = append(stopped, storeTask)
				checkpoints = append(checkpoints, checkpoint)
			}
			if _, err := runtime.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("seeding_complete = ?", true).Where("id = ?", replacement.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			if tt.exhaustedFirst {
				checkpoint, err := json.Marshal(map[string]any{"dependency_retries": map[string]int{fmt.Sprintf("storage_copy:%d", copies[0].ID): 5}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?::jsonb", string(checkpoint)).Where("id = ?", coordinator.ID).Exec(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if tt.invalidStoreCheckpoint || tt.blockedReason != "" {
				coordinator = runStorageTaskClaims(t, runtime, coordinator, model.TaskStatusFailed, 2)
				stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
				copyAfter, copyErr := runtime.repos.Contents.GetUploadCopyByID(ctx, copies[0].ID)
				var item storagereplacement.Item
				if err := runtime.db.NewSelect().Model(&item).Where("replacement_id = ?", replacement.ID).Scan(ctx); err != nil {
					t.Fatal(err)
				}
				if err != nil || stored.Status != storagereplacement.StatusFailed || stored.LastError == nil || *stored.LastError != "Some stored content could not be copied to the new provider. Retry the replacement to try it again." ||
					coordinator.FailureReason == nil || *coordinator.FailureReason != "replacement_item_attention" || item.Status != storagereplacement.ItemStatusAttention || copyErr != nil || copyAfter.ActiveTaskID == nil {
					t.Fatalf("attention = task %#v, replacement %#v, item %#v, copy %#v, errors %v %v", coordinator, stored, item, copyAfter, err, copyErr)
				}
				count, err := runtime.db.NewSelect().Model((*model.Task)(nil)).Where("retry_of_task_id = ?", stopped[0].ID).Count(ctx)
				if err != nil || count != 0 {
					t.Fatalf("blocked dependency created %d successors: %v", count, err)
				}
				return
			}
			coordinator = runOneStorageTask(t, runtime, coordinator, model.TaskStatusPending)
			if coordinator.RetryCount != 0 || coordinator.FailureReason != nil {
				t.Fatalf("dependency recovery consumed coordinator budget: %#v", coordinator)
			}
			var progress struct {
				DependencyRetries map[string]int `json:"dependency_retries"`
			}
			if err := json.Unmarshal(coordinator.Checkpoint, &progress); err != nil {
				t.Fatal(err)
			}
			var successor *model.Task
			for i, taskRow := range stopped {
				successor, err = runtime.repos.Tasks.GetDirectSuccessor(ctx, taskRow.ID)
				if tt.exhaustedFirst && i == 0 {
					var item storagereplacement.Item
					if queryErr := runtime.db.NewSelect().Model(&item).Where("replacement_id = ? AND content_id = ?", replacement.ID, copies[0].ContentID).Scan(ctx); queryErr != nil {
						t.Fatal(queryErr)
					}
					if err != nil || successor != nil || item.Status != storagereplacement.ItemStatusAttention {
						t.Fatalf("exhausted dependency = successor %#v, item %#v, %v", successor, item, err)
					}
					continue
				}
				if err != nil || successor == nil || successor.RetryOfTaskID == nil || *successor.RetryOfTaskID != taskRow.ID || successor.RetryCount != 0 || handlerTaskMaxAttempts(t, runtime, successor) != 10 ||
					progress.DependencyRetries[fmt.Sprintf("storage_copy:%d", copies[i].ID)] != 1 {
					t.Fatalf("dependent recovery = successor %#v, checkpoint %s, wait=%s, %v", successor, coordinator.Checkpoint, taskField(coordinator, func(row *model.Task) *string { return row.WaitReason }), err)
				}
				history, err := runtime.repos.Tasks.GetByID(ctx, taskRow.ID)
				if err != nil || history.Status != model.TaskStatusFailed || history.SupersededAt == nil || !testutil.JSONEqual(history.Checkpoint, checkpoints[i]) {
					t.Fatalf("failed source history = %#v, %v", history, err)
				}
			}
			stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored.Status != storagereplacement.StatusMigrating {
				t.Fatalf("replacement = %#v, %v", stored, err)
			}
			if tt.dependencyRetries == nil {
				if tt.contents == 1 {
					testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{StorageCopyID: copies[0].ID})
					coordinator = runOneStorageTask(t, runtime, coordinator, model.TaskStatusPending)
					var item storagereplacement.Item
					if err := runtime.db.NewSelect().Model(&item).Where("replacement_id = ?", replacement.ID).Scan(ctx); err != nil || item.Status != storagereplacement.ItemStatusCopied {
						t.Fatalf("completed item = %#v, %v", item, err)
					}
					progress.DependencyRetries = nil
					if err := json.Unmarshal(coordinator.Checkpoint, &progress); err != nil {
						t.Fatal(err)
					}
					if _, exists := progress.DependencyRetries[fmt.Sprintf("storage_copy:%d", copies[0].ID)]; exists {
						t.Fatalf("completed copy retained its dependency retry count: %s", coordinator.Checkpoint)
					}
				}
				return
			}
			var claimed *model.Task
			deadline := time.After(handlerTestLeaseDuration)
			for claimed == nil {
				claimed, err = runtime.repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{ExcludedTaskIDs: []int64{coordinator.ID}})
				if err != nil {
					t.Fatal(err)
				}
				if claimed == nil {
					select {
					case <-deadline:
						t.Fatal("dependent retry did not become available")
					case <-time.After(handlerTestPollInterval):
					}
				}
			}
			if claimed.ID != successor.ID {
				t.Fatalf("claimed retry = %#v, want %d", claimed, successor.ID)
			}
			reason := "store_outcome_unknown"
			if err := runtime.repos.Tasks.Settle(ctx, claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason}); err != nil {
				t.Fatal(err)
			}
			coordinator = runOneStorageTask(t, runtime, coordinator, model.TaskStatusPending)
			if next, err := runtime.repos.Tasks.GetDirectSuccessor(ctx, successor.ID); err != nil || next != nil {
				t.Fatalf("exhausted dependency successor = %#v, %v", next, err)
			}
			var item storagereplacement.Item
			if err := runtime.db.NewSelect().Model(&item).Where("replacement_id = ?", replacement.ID).Scan(ctx); err != nil || item.Status != storagereplacement.ItemStatusAttention {
				t.Fatalf("exhausted item = %#v, %v", item, err)
			}
			coordinator = runOneStorageTask(t, runtime, coordinator, model.TaskStatusFailed)
			stored, err = runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored.Status != storagereplacement.StatusFailed || coordinator.FailureReason == nil || *coordinator.FailureReason != "replacement_item_attention" || stored.LastError == nil || *stored.LastError != "Some stored content could not be copied to the new provider. Retry the replacement to try it again." {
				t.Fatalf("exhausted replacement = %#v, coordinator %#v, %v", stored, coordinator, err)
			}
			coordinator, err = runtime.service.Retry(ctx, coordinator.ID)
			if err != nil {
				t.Fatal(err)
			}
			coordinator = runOneStorageTask(t, runtime, coordinator, model.TaskStatusPending)
			if next, err := runtime.repos.Tasks.GetDirectSuccessor(ctx, successor.ID); err != nil || next == nil {
				t.Fatalf("manual coordinator retry did not restore the dependency budget: %#v, %v", next, err)
			}
			if err := json.Unmarshal(coordinator.Checkpoint, &progress); err != nil || progress.DependencyRetries[fmt.Sprintf("storage_copy:%d", copies[0].ID)] != 1 || coordinator.RetryCount != 0 {
				t.Fatalf("manual retry checkpoint = %s, %v", coordinator.Checkpoint, err)
			}
		})
	}
	for _, manualFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared commit manual first %t", manualFirst), func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
			ctx := t.Context()
			copyRow, _, request := seedSealedReplacementBatch(t, runtime, 2, storagecommit.RequestStatusReady)
			replacement, err := runtime.repos.Replacements.GetByClientRequestID(ctx, copyRow.BucketID, "migration-pull-test")
			if err != nil || replacement == nil {
				t.Fatalf("replacement = %#v, %v", replacement, err)
			}
			var copies []model.StorageCopy
			if err := runtime.db.NewSelect().Model(&copies).Where("commit_request_id = ?", request.RequestID).OrderExpr("id ASC").Scan(ctx); err != nil || len(copies) != 2 {
				t.Fatalf("shared batch copies = %#v, %v", copies, err)
			}
			for _, row := range copies {
				item := &storagereplacement.Item{ReplacementID: replacement.ID, ContentID: row.ContentID, TargetDataSetID: row.StorageDataSetID, Status: storagereplacement.ItemStatusPending}
				if _, err := runtime.db.NewInsert().Model(item).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := runtime.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("seeding_complete = ?", true).Where("id = ?", replacement.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			commitTask, err := runtime.repos.Tasks.GetByID(ctx, *request.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			reason := "attempts_exhausted"
			settleHandlerTaskFixture(t, runtime, commitTask.ID, model.TaskStatusFailed, &reason)
			coordinator, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
				Type: model.TaskTypeProviderReplacementCoordinate, Input: storagereplacement.CoordinateInput{ReplacementID: replacement.ID, Generation: replacement.TaskGeneration},
			}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
				return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, row.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			var manualSuccessor *model.Task
			if manualFirst {
				registry := taskengine.NewRegistry()
				for _, taskType := range runtime.registry.Types() {
					handler, _ := runtime.registry.Handler(taskType)
					if taskType == coordinator.Type {
						handler = revokeAfterReplacementPlan{Handler: handler, revoke: func() {
							var retryErr error
							manualSuccessor, retryErr = runtime.service.Retry(ctx, commitTask.ID)
							if retryErr != nil {
								t.Errorf("manual commit retry: %v", retryErr)
							}
						}}
					}
					if err := registry.Register(handler); err != nil {
						t.Fatal(err)
					}
				}
				runtime.registry = registry
			}
			coordinator = runOneStorageTask(t, runtime, coordinator, model.TaskStatusPending)
			successor, err := runtime.repos.Tasks.GetDirectSuccessor(ctx, commitTask.ID)
			if err != nil || successor == nil || (manualFirst && (manualSuccessor == nil || manualSuccessor.ID != successor.ID)) {
				t.Fatalf("shared commit successor = %#v, manual %#v, %v", successor, manualSuccessor, err)
			}
			assertProgress := func() {
				t.Helper()
				var progress struct {
					DependencyRetries map[string]int `json:"dependency_retries"`
				}
				if err := json.Unmarshal(coordinator.Checkpoint, &progress); err != nil {
					t.Fatal(err)
				}
				wantRetries := 1
				if manualFirst {
					wantRetries = 0
					if _, counted := progress.DependencyRetries["storage_commit_request:"+request.RequestID]; counted {
						t.Fatalf("manual commit retry was counted as automatic: %s", coordinator.Checkpoint)
					}
				}
				if coordinator.RetryCount != 0 || coordinator.FailureReason != nil || progress.DependencyRetries["storage_commit_request:"+request.RequestID] != wantRetries {
					t.Fatalf("shared commit budget = task %#v, counts %#v", coordinator, progress.DependencyRetries)
				}
				var items []storagereplacement.Item
				if err := runtime.db.NewSelect().Model(&items).Where("replacement_id = ?", replacement.ID).Scan(ctx); err != nil || len(items) != 2 {
					t.Fatalf("shared items = %#v, %v", items, err)
				}
				for _, item := range items {
					if item.Status != storagereplacement.ItemStatusPending {
						t.Fatalf("shared item = %#v", item)
					}
				}
				count, err := runtime.db.NewSelect().Model((*model.Task)(nil)).Where("retry_of_task_id = ?", commitTask.ID).Count(ctx)
				if err != nil || count != 1 {
					t.Fatalf("shared successors = %d, %v", count, err)
				}
			}
			assertProgress()
			if !manualFirst {
				coordinator = runOneStorageTask(t, runtime, coordinator, model.TaskStatusPending)
				assertProgress()
			}
		})
	}
	for _, scenario := range []struct {
		name        string
		exhausted   bool
		readFailure bool
	}{
		{name: "pull retry"},
		{name: "pull attention retry", exhausted: true},
		{name: "pull evidence read failure", readFailure: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			storageClient := &testutil.MockStorageClient{}
			options := handlerRuntimeOptions{
				storage: storageClient, policy: cache.EvictionPolicyNone,
				parkedPieces: parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
					return synapse.ParkedPieceReady, nil
				}),
			}
			if scenario.exhausted {
				limit := 0
				options.dependencyRetries = &limit
			}
			runtime := newHandlerTestRuntime(t, options)
			ctx := t.Context()
			copyRow, target, pipeline := seedReplacementPullTarget(t, runtime, false)
			var presigns, calls atomic.Int64
			target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
				return testutil.CommitExtraData(uint64(6 + presigns.Add(1))), nil
			}
			secondURL := "https://second.example/piece/" + pipeline.pieceCID.String()
			target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
				if calls.Add(1) == 1 {
					return pullStatusResult(request, storage.PullStatusFailed), nil
				}
				if got := request.From(pipeline.pieceCID); got != secondURL || !bytes.Equal(request.ExtraData, testutil.CommitExtraData(8)) {
					t.Errorf("restarted pull = source %q, authorization %x, want the second source and new authorization", got, request.ExtraData)
				}
				return pullStatusResult(request, storage.PullStatusComplete), nil
			}
			storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				return target, nil
			}
			pullTask := bindCopyTask(t, runtime, copyRow, model.TaskTypeStoragePull)
			failed := runOneStorageTask(t, runtime, pullTask, model.TaskStatusFailed)
			if failed.FailureReason == nil || *failed.FailureReason != "migration_cache_missing" {
				t.Fatalf("initial pull failure = %#v", failed)
			}
			var checkpoint struct {
				AttemptID string `json:"attempt_id"`
			}
			if err := json.Unmarshal(failed.Checkpoint, &checkpoint); err != nil || checkpoint.AttemptID == "" {
				t.Fatalf("initial pull checkpoint = %s, %v", failed.Checkpoint, err)
			}
			oldCheckpoint := append(json.RawMessage(nil), failed.Checkpoint...)
			oldAttempt, err := runtime.repos.Contents.GetPullAttempt(ctx, checkpoint.AttemptID, copyRow.ContentID, copyRow.StorageDataSetID)
			if err != nil || oldAttempt.Status != storagepull.AttemptStatusAbandoned || oldAttempt.ResolvedAt == nil {
				t.Fatalf("initial pull attempt = %#v, %v", oldAttempt, err)
			}
			reason := storagepull.FailureOutcomeUnknown
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("failure_reason = ?", reason).
				Where("id = ? AND status = ?", pullTask.ID, model.TaskStatusFailed).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
				StorageCopyID: pipeline.target.ID, ContentID: pipeline.upload.ID, CopyIndex: pipeline.target.CopyIndex,
				PieceCID: pipeline.pieceCID.String(), RetrievalURL: secondURL,
			})
			replacement, err := runtime.repos.Replacements.GetByClientRequestID(ctx, copyRow.BucketID, "migration-pull-test")
			if err != nil || replacement == nil {
				t.Fatalf("load pull replacement = %#v, %v", replacement, err)
			}
			if _, err := runtime.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
				Set("seeding_complete = ?", true).Where("id = ?", replacement.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			coordinateTask, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
				Type:  model.TaskTypeProviderReplacementCoordinate,
				Input: storagereplacement.CoordinateInput{ReplacementID: replacement.ID, Generation: replacement.TaskGeneration},
			}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
				return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, taskRow.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			itemStatus := func() storagereplacement.ItemStatus {
				var item storagereplacement.Item
				if err := runtime.db.NewSelect().Model(&item).
					Where("replacement_id = ? AND content_id = ?", replacement.ID, copyRow.ContentID).Scan(ctx); err != nil {
					t.Fatal(err)
				}
				return item.Status
			}
			wantRetries := 0
			if scenario.exhausted {
				coordinateTask = runStorageTaskClaims(t, runtime, coordinateTask, model.TaskStatusFailed, 2)
				stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
				if err != nil || stored.Status != storagereplacement.StatusFailed || itemStatus() != storagereplacement.ItemStatusAttention ||
					coordinateTask.FailureReason == nil || *coordinateTask.FailureReason != "replacement_item_attention" {
					t.Fatalf("exhausted replacement = %#v, task=%#v, err=%v", stored, coordinateTask, err)
				}
				if _, err := runtime.service.Retry(ctx, pullTask.ID); !errors.Is(err, repository.ErrConflict) {
					t.Fatalf("attention pull retry = %v, want conflict", err)
				}
				if child, err := runtime.repos.Tasks.GetDirectSuccessor(ctx, pullTask.ID); err != nil || child != nil {
					t.Fatalf("attention pull successor = %#v, %v", child, err)
				}
				runtime.testDependencyRetries = nil
				coordinateTask, err = runtime.service.Retry(ctx, coordinateTask.ID)
				if err != nil || itemStatus() != storagereplacement.ItemStatusPending {
					t.Fatalf("replacement retry = %#v, %v", coordinateTask, err)
				}
			}
			if scenario.readFailure {
				faults := &pullEvidenceFailureRepository{StorageContentRepository: runtime.repos.Contents, stage: "attempt"}
				faults.remaining.Store(1)
				runtime.repos.Contents = faults
				coordinateTask = runOneStorageTask(t, runtime, coordinateTask, model.TaskStatusPending)
				runtime.repos.Contents = faults.StorageContentRepository
				stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
				if err != nil || stored.Status == storagereplacement.StatusFailed || itemStatus() != storagereplacement.ItemStatusPending ||
					coordinateTask.RetryCount != 1 || coordinateTask.FailureReason == nil || *coordinateTask.FailureReason != "replacement_retry_eligibility_failed" {
					t.Fatalf("eligibility read failure = task %#v, replacement %#v, err=%v", coordinateTask, stored, err)
				}
				if child, err := runtime.repos.Tasks.GetDirectSuccessor(ctx, pullTask.ID); err != nil || child != nil || calls.Load() != 1 {
					t.Fatalf("eligibility failure started recovery: child=%#v, calls=%d, err=%v", child, calls.Load(), err)
				}
				wantRetries++
			}
			coordinateTask = runOneStorageTask(t, runtime, coordinateTask, model.TaskStatusPending)
			if coordinateTask.RetryCount != wantRetries || coordinateTask.FailureReason != nil {
				t.Fatalf("pull retry coordinator = %#v, want retry count %d", coordinateTask, wantRetries)
			}
			successor, err := runtime.repos.Tasks.GetDirectSuccessor(ctx, pullTask.ID)
			if err != nil || successor == nil || successor.RetryOfTaskID == nil || *successor.RetryOfTaskID != pullTask.ID || len(successor.Checkpoint) != 0 {
				t.Fatalf("pull recovery successor = %#v, %v", successor, err)
			}
			runStorageTaskClaims(t, runtime, successor, model.TaskStatusCompleted, 2)
			copyAfter, err := runtime.repos.Contents.GetUploadCopyByID(ctx, copyRow.ID)
			if err != nil || copyAfter.Status != model.StorageCopyStatusPieceReady || copyAfter.CommitRequestID == nil ||
				calls.Load() != 2 || presigns.Load() != 2 {
				t.Fatalf("restarted pull = copy %#v, calls=%d, presigns=%d, err=%v", copyAfter, calls.Load(), presigns.Load(), err)
			}
			var attempts []storagepull.Attempt
			if err := runtime.db.NewSelect().Model(&attempts).
				Where("content_id = ? AND storage_data_set_id = ?", copyRow.ContentID, copyRow.StorageDataSetID).
				OrderExpr("attempted_at ASC, attempt_id ASC").Scan(ctx); err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 2 || attempts[0].AttemptID != oldAttempt.AttemptID || attempts[0].Status != storagepull.AttemptStatusAbandoned ||
				attempts[0].ResolvedAt == nil || attempts[0].LastError == nil || oldAttempt.LastError == nil || *attempts[0].LastError != *oldAttempt.LastError ||
				attempts[1].AttemptID == oldAttempt.AttemptID || attempts[1].Status != storagepull.AttemptStatusAttempted || attempts[1].ResolvedAt == nil ||
				!attempts[1].SourceProviderID.Equal(pipeline.target.ProviderID) || attempts[1].SourceRetrievalURL != secondURL {
				t.Fatalf("migration pull attempts = %#v", attempts)
			}
			sourceHistory, err := runtime.repos.Tasks.GetByID(ctx, pullTask.ID)
			if err != nil || sourceHistory.Status != model.TaskStatusFailed || sourceHistory.SupersededAt == nil || !bytes.Equal(sourceHistory.Checkpoint, oldCheckpoint) {
				t.Fatalf("original pull evidence = %#v, %v", sourceHistory, err)
			}
			stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored.Status == storagereplacement.StatusFailed {
				t.Fatalf("replacement after pull recovery = %#v, %v", stored, err)
			}
		})
	}
}

// replacementSourceFixture replaces the only replica of a bucket while the old
// provider still holds unfinished writes.
type replacementSourceFixture struct {
	runtime handlerTestRuntime
	bucket  *model.Bucket
	source  *model.StorageDataSet
	id      func(int64) idtypes.OnChainID
}

func newReplacementSourceFixture(t *testing.T, options handlerRuntimeOptions) replacementSourceFixture {
	t.Helper()
	runtime := newHandlerTestRuntime(t, options)
	sequence := storedObjectSequence.Add(1)
	id := func(offset int64) idtypes.OnChainID { return testOnChainID(t, 37000+sequence*10+offset) }
	bucket := &model.Bucket{
		Name: fmt.Sprintf("replacement-source-write-%d", sequence), Status: model.BucketStatusActive,
		DefaultCopies: 1, MinimumDurableCopies: 1,
	}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	source, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: id(1), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{ID: source.ID, DataSetID: id(2)}); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	return replacementSourceFixture{runtime: runtime, bucket: bucket, source: source, id: id}
}

// write creates content whose only copy is still being written to the old
// provider; live gives the content a live version.
func (f replacementSourceFixture) write(t *testing.T, key string, live bool) *model.StorageCopy {
	t.Helper()
	ctx := t.Context()
	content, err := f.runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: f.bucket.ID, ContentSize: 128, Checksum: testutil.StorageChecksum(f.bucket.Name + key), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("ensure content: %v", err)
	}
	if live {
		version := &model.ObjectVersion{
			VersionID: model.NewVersionID(), BucketID: f.bucket.ID, Key: key, Size: content.ContentSize,
			ETag: key, ContentType: "application/octet-stream", ContentID: &content.ID,
		}
		if _, err := f.runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("create object version: %v", err)
		}
	}
	if err := f.runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: f.source.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: f.id(1),
	}}); err != nil {
		t.Fatalf("create source copy: %v", err)
	}
	copyRow, err := f.runtime.repos.Contents.GetUploadCopyForDataSet(ctx, content.ID, f.source.ID)
	if err != nil || copyRow == nil {
		t.Fatalf("load source copy = %#v, err=%v", copyRow, err)
	}
	return copyRow
}

// stop claims the task and settles it as the old provider left it.
func (f replacementSourceFixture) stop(t *testing.T, task *model.Task, checkpoint string, transition repository.TaskTransition) {
	t.Helper()
	claimed, err := f.runtime.repos.Tasks.ClaimNext(t.Context(), time.Minute, repository.TaskClaimFilter{})
	if err != nil || claimed == nil || claimed.ID != task.ID {
		t.Fatalf("claim old write = %#v, err=%v", claimed, err)
	}
	if checkpoint != "" {
		if err := f.runtime.repos.Tasks.WriteCheckpoint(t.Context(), claimed.ID, claimed.ClaimGeneration, json.RawMessage(checkpoint)); err != nil {
			t.Fatalf("write old checkpoint: %v", err)
		}
	}
	if err := f.runtime.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, transition); err != nil {
		t.Fatalf("settle old write: %v", err)
	}
}

func (f replacementSourceFixture) replace(t *testing.T) (*storagereplacement.Replacement, *model.Task) {
	t.Helper()
	ctx := t.Context()
	replacement, _, err := f.runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: f.bucket.ID, SourceDataSetID: f.source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: f.id(5), ClientRequestID: f.bucket.Name,
	})
	if err != nil {
		t.Fatalf("authorize replacement: %v", err)
	}
	if err := markTestDataSetReady(t, f.runtime, ctx, repository.MarkDataSetReadyInput{ID: replacement.TargetDataSetID, DataSetID: f.id(6)}); err != nil {
		t.Fatalf("mark target ready: %v", err)
	}
	coordinateTask, _, err := f.runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
		Type:  model.TaskTypeProviderReplacementCoordinate,
		Input: storagereplacement.CoordinateInput{ReplacementID: replacement.ID, Generation: replacement.TaskGeneration},
	}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, taskRow.ID)
	})
	if err != nil {
		t.Fatalf("enqueue replacement coordinator: %v", err)
	}
	return replacement, coordinateTask
}

func TestReplacementHandsOverUnsignedSourceWrites(t *testing.T) {
	pieceInfo, err := piece.Calculate(strings.NewReader(strings.Repeat("r", 128)))
	if err != nil {
		t.Fatal(err)
	}
	stale := time.Now().UTC().Add(-21 * time.Hour).Format(time.RFC3339Nano)
	failed := func(reason string) repository.TaskTransition {
		message := "write stopped"
		return repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason, LastError: &message}
	}
	waiting := "provider"
	for _, tt := range []struct {
		name       string
		checkpoint string
		transition repository.TaskTransition
		history    int
	}{
		{
			name: "stopped with an expired confirmation window",
			checkpoint: fmt.Sprintf(`{"attempted_at":"2026-09-11T00:00:00Z","intended_piece_cid":%q,"provider_service_url":"https://source.example","session_id":"00000000-0000-4000-8000-000000000001","stage":"upload","ingress_attempt":10,"observation_started_at":%q}`,
				pieceInfo.CIDv2.String(), stale),
			transition: failed("store_processing_timeout"),
		},
		{name: "engine failure", transition: failed("handler_panic")},
		{name: "invalid checkpoint", transition: failed("invalid_checkpoint")},
		{name: "waiting for the provider", history: 128, transition: repository.TaskTransition{
			Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now().Add(time.Hour), WaitReason: &waiting,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newReplacementSourceFixture(t, handlerRuntimeOptions{})
			ctx := t.Context()
			for i := range tt.history {
				if _, err := f.runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
					BucketID: f.bucket.ID, ContentSize: 128, RequestedCopies: 1,
					Checksum: testutil.StorageChecksum(fmt.Sprintf("%s-history-%d", f.bucket.Name, i)),
				}); err != nil {
					t.Fatal(err)
				}
			}
			write := f.write(t, "writing.bin", true)
			storeTask := bindCopyTask(t, f.runtime, write, model.TaskTypeStorageStore)
			f.stop(t, storeTask, tt.checkpoint, tt.transition)
			replacement, coordinateTask := f.replace(t)

			runStorageTaskClaims(t, f.runtime, coordinateTask, model.TaskStatusPending, 2)
			stored, err := f.runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored.Status != storagereplacement.StatusMigrating {
				t.Fatalf("replacement = %#v err=%v, want migrating", stored, err)
			}
			if tt.history > 0 && stored.SeedingComplete {
				t.Fatal("expected the history scan to remain incomplete")
			}
			target, err := f.runtime.repos.Contents.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
			if err != nil || !target.IsCurrent {
				t.Fatalf("target = %#v err=%v, want it receiving new writes", target, err)
			}
			old, err := f.runtime.repos.Contents.GetUploadCopyByID(ctx, write.ID)
			if err != nil || old.Status != model.StorageCopyStatusFailed || old.ActiveTaskID != nil {
				t.Fatalf("old copy = %#v err=%v, want it handed over", old, err)
			}
			moved, err := f.runtime.repos.Contents.GetUploadCopyForDataSet(ctx, write.ContentID, replacement.TargetDataSetID)
			if err != nil || moved == nil || moved.Status != model.StorageCopyStatusPending || moved.ActiveTaskID == nil {
				t.Fatalf("replacement copy = %#v err=%v, want transfer scheduled", moved, err)
			}
			if migration, err := f.runtime.repos.Contents.IsPendingReplacementCopy(ctx, moved.ID); err != nil || !migration {
				t.Fatalf("replacement copy migration = %v, err=%v, want cache recovery available", migration, err)
			}
			if successor, err := f.runtime.repos.Tasks.GetDirectSuccessor(ctx, storeTask.ID); err != nil || successor != nil {
				t.Fatalf("old write successor = %#v err=%v, want no retry on the old provider", successor, err)
			}
			current, err := f.runtime.repos.Tasks.GetByID(ctx, storeTask.ID)
			if err != nil {
				t.Fatal(err)
			}
			if current.Status != model.TaskStatusPending {
				return
			}
			if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now()).Where("id = ?", current.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			runOneStorageTask(t, f.runtime, current, model.TaskStatusCancelled)
		})
	}
}

// Writes the old provider holds a signed authorization for are never handed
// over. Activation and migration go on, but retirement waits for them, and a
// write that stopped is retried within the coordinator's budget with its
// evidence.
func TestReplacementHoldsRetirementForSignedSourceWrites(t *testing.T) {
	for _, tt := range []struct {
		name              string
		stopped           bool
		attempts          int
		dependencyRetries int
		want              model.TaskStatus
	}{
		{name: "waiting", attempts: 1, dependencyRetries: 1, want: model.TaskStatusPending},
		{name: "recovered", stopped: true, attempts: 2, dependencyRetries: 1, want: model.TaskStatusPending},
		{name: "exhausted", stopped: true, attempts: 1, want: model.TaskStatusFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newReplacementSourceFixture(t, handlerRuntimeOptions{maxAttempts: &tt.attempts, dependencyRetries: &tt.dependencyRetries})
			ctx := t.Context()
			// The content has no live version, so the whole migration is this one
			// signed write.
			member := f.write(t, "signed.bin", false)
			if err := f.runtime.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
				StorageCopyID: member.ID, ContentID: member.ContentID, CopyIndex: 0, PieceCID: "piece-signed",
			}); err != nil {
				t.Fatalf("mark piece ready: %v", err)
			}
			requestID := f.bucket.Name
			commitTask, _, err := f.runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
				Type: model.TaskTypeStorageCommit, Input: storagepipeline.CommitRequestInput{RequestID: requestID},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.repos.Contents.CreateCollectingCommitRequest(ctx, repository.CreateCommitRequestInput{
				RequestID: requestID, TaskID: commitTask.ID, StorageDataSetID: f.source.ID, CopyIDs: []int64{member.ID},
			}); err != nil {
				t.Fatalf("collect: %v", err)
			}
			if _, err := f.runtime.repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
				RequestID: requestID, TaskID: commitTask.ID, ExtraDataHex: "abcd",
				Members: []repository.SealMember{{CopyID: member.ID, ContentID: member.ContentID, PieceCID: "piece-signed"}},
			}); err != nil {
				t.Fatalf("seal: %v", err)
			}
			checkpoint := `{"request_id":"` + requestID + `","sends":1}`
			if tt.stopped {
				reason := "attempts_exhausted"
				f.stop(t, commitTask, checkpoint, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason})
			}
			replacement, coordinateTask := f.replace(t)

			// Activation, hand-over with seeding, then the retirement check.
			coordinator := runStorageTaskClaims(t, f.runtime, coordinateTask, tt.want, 3)
			stored, err := f.runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil {
				t.Fatal(err)
			}
			source, err := f.runtime.repos.Contents.GetDataSetBindingByID(ctx, f.source.ID)
			if err != nil || source.IsCurrent || source.Status != model.StorageDataSetStatusDraining {
				t.Fatalf("source = %#v err=%v, want it draining, not retiring", source, err)
			}
			if held, err := f.runtime.repos.Contents.GetUploadCopyByID(ctx, member.ID); err != nil || held.Status != model.StorageCopyStatusCommitting {
				t.Fatalf("signed write = %#v err=%v, want it left to its batch", held, err)
			}
			successor, err := f.runtime.repos.Tasks.GetDirectSuccessor(ctx, commitTask.ID)
			if err != nil {
				t.Fatal(err)
			}
			request, err := f.runtime.repos.Contents.GetCommitRequest(ctx, requestID)
			if err != nil {
				t.Fatal(err)
			}
			switch tt.name {
			case "waiting":
				if coordinator.WaitReason == nil || *coordinator.WaitReason != "source_writes" || stored.Status != storagereplacement.StatusMigrating || successor != nil {
					t.Fatalf("coordinator = %#v replacement = %#v, want both waiting for the signed write", coordinator, stored)
				}
			case "recovered":
				var progress struct {
					DependencyRetries map[string]int `json:"dependency_retries"`
				}
				if err := json.Unmarshal(coordinator.Checkpoint, &progress); err != nil || progress.DependencyRetries["storage_commit_request:"+requestID] != 1 {
					t.Fatalf("source write retry counts = %s, %v", coordinator.Checkpoint, err)
				}
				if successor == nil || successor.Type != model.TaskTypeStorageCommit || successor.ResumeMode != model.TaskResumeModeRecover ||
					successor.RetryCount != 0 || !testutil.JSONEqual(successor.Checkpoint, []byte(checkpoint)) ||
					request.TaskID == nil || *request.TaskID != successor.ID || coordinator.RetryCount != 0 || stored.Status != storagereplacement.StatusMigrating {
					t.Fatalf("batch recovery lost its evidence or budget: coordinator=%#v request=%#v successor=%#v", coordinator, request, successor)
				}
			case "exhausted":
				if successor != nil || request.TaskID == nil || *request.TaskID != commitTask.ID || stored.Status != storagereplacement.StatusFailed || stored.LastError == nil ||
					*stored.LastError != "The old provider did not finish writes it had already accepted, so it cannot be shut down yet. Retry the replacement after the old provider recovers." {
					t.Fatalf("replacement = %#v request=%#v successor=%#v, want it stopped with the old provider named", stored, request, successor)
				}
			}
		})
	}
}

func TestEngineFailedCoordinatorLeavesTheReplacementRetryable(t *testing.T) {
	for _, internalFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("internal error %t", internalFailure), func(t *testing.T) {
			limit := 1
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{maxAttempts: &limit})
			ctx := t.Context()
			sequence := storedObjectSequence.Add(1)
			id := func(offset int64) idtypes.OnChainID { return testOnChainID(t, 38000+sequence*10+offset) }
			bucket := &model.Bucket{
				Name: fmt.Sprintf("engine-failed-coordinator-%d", sequence), Status: model.BucketStatusActive,
				DefaultCopies: 1, MinimumDurableCopies: 1,
			}
			if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
				t.Fatalf("create bucket: %v", err)
			}
			source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: id(1), CopyIndex: 0,
			})
			if err != nil {
				t.Fatalf("create source: %v", err)
			}
			if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{ID: source.ID, DataSetID: id(2)}); err != nil {
				t.Fatalf("mark source ready: %v", err)
			}
			replacement, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: id(5), ClientRequestID: bucket.Name,
			})
			if err != nil {
				t.Fatalf("authorize replacement: %v", err)
			}
			inputVersion := 99
			if internalFailure {
				inputVersion = 1
			}
			coordinator, _, err := runtime.repos.Tasks.Enqueue(ctx, handlerTaskFixture(t, runtime, &model.Task{
				Type:         model.TaskTypeProviderReplacementCoordinate,
				InputVersion: inputVersion, Input: []byte(fmt.Sprintf(`{"replacement_id":%d,"generation":%d}`, replacement.ID, replacement.TaskGeneration)),
				Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
			}))
			if err != nil {
				t.Fatalf("enqueue coordinator: %v", err)
			}
			if err := runtime.repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, coordinator.ID); err != nil {
				t.Fatalf("bind coordinator: %v", err)
			}

			contents := runtime.repos.Contents
			if internalFailure {
				runtime.repos.Contents = replacementBindingFailureRepository{StorageContentRepository: contents}
			}
			failed := runOneStorageTask(t, runtime, coordinator, model.TaskStatusFailed)
			runtime.repos.Contents = contents
			stored, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			wantReason := "input_version_unsupported"
			if internalFailure {
				wantReason = "attempts_exhausted"
			}
			if failed.FailureReason == nil || *failed.FailureReason != wantReason || err != nil || !stored.Status.Retryable() {
				t.Fatalf("coordinator = %#v, replacement = %#v err=%v, want the replacement retryable", failed, stored, err)
			}
			if internalFailure {
				if stored.LastError == nil || *stored.LastError != "Replacement work stopped because of an internal error. Retry the replacement; if it stops again, check its task on the Tasks page." {
					t.Fatalf("replacement last error = %v", stored.LastError)
				}
				if eligible, err := runtime.service.RetryableContext(ctx, failed); err != nil || !eligible {
					t.Fatalf("coordinator retry eligible=%t, %v", eligible, err)
				}
				if successor, err := runtime.service.Retry(ctx, failed.ID); err != nil || successor == nil {
					t.Fatalf("coordinator retry = %#v, %v", successor, err)
				}
			}
		})
	}
}

type replacementBindingFailureRepository struct {
	repository.StorageContentRepository
}

func (replacementBindingFailureRepository) GetDataSetBindingByID(context.Context, int64) (*model.StorageDataSet, error) {
	return nil, errors.New("data set lookup unavailable")
}

func TestRetirementAdmissionFailureDoesNotEnterCleanupAttention(t *testing.T) {
	noRetries := 0
	terminator := &testServiceTerminator{err: errors.New("unexpected termination")}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		maxAttempts: taskTestMaxAttempts(&noRetries), terminator: terminator, epochs: testEpochReader{epoch: 80},
		retireOnly: true,
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: target.ID, DataSetID: testOnChainID(t, 29206), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark retirement admission target ready: %v", err)
	}
	if err := runtime.repos.Replacements.MarkFailed(t.Context(), first.ID, nil, "replacement stopped"); err != nil {
		t.Fatal(err)
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
		Type:  model.TaskTypeStorageDataSetRetire,
		Input: storagereplacement.RetireInput{ReplacementID: first.ID, DataSetID: target.ID, Generation: generation},
	})
	if err != nil {
		t.Fatalf("enqueue retirement admission task: %v", err)
	}
	if err := runtime.repos.Contents.BindDataSetRetirementTask(ctx, target.ID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind retirement admission task: %v", err)
	}
	rejectTaskCheckpoint(t, runtime.db, taskRow.ID)
	limited := &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	runtime.repos.Tasks = limited
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" || !handlerTaskRetryable(t, runtime, failed) || len(failed.Checkpoint) != 0 {
		t.Fatalf("retirement admission task = %#v", failed)
	}
	stored, err := runtime.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || stored.Status != storagereplacement.StatusSuperseded || stored.AbandonedTerminationEpoch != nil || stored.LastError == nil || *stored.LastError != "replacement stopped" {
		t.Fatalf("replacement after retirement admission failure = %#v, err=%v", stored, err)
	}
	if terminator.calls.Load() != 0 {
		t.Fatalf("retirement admission called terminator %d times", terminator.calls.Load())
	}
	{
		var retryErr error
		taskRow, retryErr = runtime.service.Retry(ctx, taskRow.ID)
		if retryErr != nil {
			t.Fatalf("retry retirement admission task: %v", retryErr)
		}
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

func (s *sequencedTerminator) TerminateService(_ context.Context, _ sdktypes.BigInt, onSubmitted func(string) error) (*synapse.TerminationResult, error) {
	result, err := s.respond(s.calls.Add(1))
	if result != nil && result.TxHash != "" && onSubmitted != nil {
		err = errors.Join(err, onSubmitted(result.TxHash))
	}
	return result, err
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: first.TargetDataSetID, DataSetID: id(6), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark target ready: %v", err)
	}
	if err := runtime.repos.Replacements.MarkFailed(t.Context(), first.ID, nil, "replacement stopped"); err != nil {
		t.Fatal(err)
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
		Type:  model.TaskTypeStorageDataSetRetire,
		Input: storagereplacement.RetireInput{ReplacementID: first.ID, DataSetID: first.TargetDataSetID, Generation: generation},
	})
	if err != nil {
		t.Fatalf("enqueue retirement: %v", err)
	}
	if err := runtime.repos.Contents.BindDataSetRetirementTask(ctx, first.TargetDataSetID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind retirement: %v", err)
	}
	return first, taskRow
}

func TestRetirementWithUnknownSubmissionDoesNotResend(t *testing.T) {
	terminator := &observingRetirementTerminator{testServiceTerminator: testServiceTerminator{err: context.DeadlineExceeded}}
	budget := 2
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true, maxAttempts: &budget,
	})
	replacement, row := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if row.RetryCount != 1 || terminator.calls.Load() != 1 {
		t.Fatalf("unknown submission did not retain its observation budget: task=%#v sends=%d", row, terminator.calls.Load())
	}
	row = expireRetirementObservation(t, runtime, row)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
	if row.FailureReason == nil || *row.FailureReason != "attempts_exhausted" || terminator.calls.Load() != 1 {
		t.Fatalf("unknown submission was resent after its observation window: task=%#v sends=%d", row, terminator.calls.Load())
	}
	attempts, err := runtime.repos.Replacements.ListTerminationAttempts(t.Context(), []int64{replacement.ID})
	if err != nil || len(attempts) != 1 {
		t.Fatalf("unknown submission lost its admitted attempt: %#v err=%v", attempts, err)
	}
	attempt := attempts[0]
	if attempt.TaskID == nil || *attempt.TaskID != row.ID || attempt.AttemptedAt == nil || attempt.TxHash != nil || attempt.ConfirmedTxHash != nil || attempt.ReceiptStatus != nil {
		t.Fatalf("unknown submission evidence was fabricated or discarded: %#v", attempt)
	}
	next, err := runtime.service.Retry(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	runOneStorageTask(t, runtime, next, model.TaskStatusPending)
	if terminator.calls.Load() != 1 {
		t.Fatalf("manual recovery resent an unknown transaction: %d sends", terminator.calls.Load())
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
		retireOnly: true,
	})
	_, taskRow := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "termination_blocked" || !handlerTaskRetryable(t, runtime, failed) || terminator.calls.Load() != 1 {
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: replacement.TargetDataSetID, DataSetID: id(6), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark target ready: %v", err)
	}
	if err := activateReplacement(t, runtime, replacement.ID); err != nil {
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
		Type:  model.TaskTypeStorageDataSetRetire,
		Input: storagereplacement.RetireInput{ReplacementID: replacement.ID, DataSetID: source.ID, Generation: generation},
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
	movedIdentity.ChainID = 314
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
			terminator := &testServiceTerminator{identity: movedIdentity, result: &synapse.TerminationResult{TxHash: "0xretire", EndEpoch: new(int64(84))}}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				terminator: terminator, epochs: testEpochReader{epoch: 90},
				retireOnly: true,
			})
			ctx := t.Context()
			replacement, taskRow := tt.seed(t, runtime, storedObjectSequence.Add(1))
			// One request went out under the original identity, and its outcome
			// was never observed.
			checkpoint, err := json.Marshal(map[string]any{
				"attempted_at": time.Now().UTC().Add(-time.Hour),
				"identity":     testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress, "sends": 1,
			})
			if err != nil {
				t.Fatalf("encode checkpoint: %v", err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, string(checkpoint), taskRow.ID).Exec(ctx); err != nil {
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
			terminator := &testServiceTerminator{result: &synapse.TerminationResult{TxHash: "0xretire", EndEpoch: new(int64(84))}}
			terminator.notPaidFor.Store(true)
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				terminator: terminator, epochs: testEpochReader{epoch: 90},
				retireOnly: true,
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
			{
				var retryErr error
				taskRow, retryErr = runtime.service.Retry(ctx, taskRow.ID)
				if retryErr != nil {
					t.Fatalf("retry retirement: %v", retryErr)
				}
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
			{
				var retryErr error
				taskRow, retryErr = runtime.service.Retry(ctx, taskRow.ID)
				if retryErr != nil {
					t.Fatalf("retry retirement: %v", retryErr)
				}
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

func TestRetirementSubmissionWriteFailureRetainsReturnedEvidence(t *testing.T) {
	terminator := &testServiceTerminator{result: &synapse.TerminationResult{TxHash: "0xretire", EndEpoch: new(int64(84))}}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		terminator: terminator,
		epochs:     testEpochReader{epoch: 80},
		retireOnly: true,
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
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
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
		ID: target.ID, DataSetID: testOnChainID(t, 7302), ClientDataSetID: &targetClientID,
	}); err != nil {
		t.Fatalf("mark retirement target ready: %v", err)
	}
	if err := runtime.repos.Replacements.MarkFailed(t.Context(), first.ID, nil, "replacement stopped"); err != nil {
		t.Fatal(err)
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
		Type: model.TaskTypeStorageDataSetRetire,
		Input: storagereplacement.RetireInput{
			ReplacementID: first.ID, DataSetID: target.ID, Generation: generation,
		},
	})
	if err != nil {
		t.Fatalf("enqueue retirement task: %v", err)
	}
	if err := runtime.repos.Contents.BindDataSetRetirementTask(ctx, target.ID, generation, retirementTask.ID); err != nil {
		t.Fatalf("bind retirement task: %v", err)
	}
	if _, err := runtime.db.ExecContext(ctx, "CREATE SEQUENCE retirement_evidence_fault"); err != nil {
		t.Fatal(err)
	}
	testutil.FailWrites(t, runtime.db, "storage_data_set_terminations", "INSERT", "NEW.role = 'abandoned_target' AND nextval('retirement_evidence_fault') = 1", "injected retirement evidence failure")
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	stored := waitForTask(t, runtime.repos, retirementTask.ID, func(task *model.Task) bool {
		first, err = runtime.repos.Replacements.GetByID(ctx, first.ID)
		return err == nil && first != nil && first.AbandonedTerminationEpoch != nil && *first.AbandonedTerminationEpoch == 84 && task.Status == model.TaskStatusPending
	})
	if terminator.calls.Load() != 1 || first.AbandonedTerminationTxHash == nil || *first.AbandonedTerminationTxHash != "0xretire" || !strings.Contains(string(stored.Checkpoint), `"transaction_hash": "0xretire"`) {
		t.Fatalf("termination calls=%d evidence=%#v checkpoint=%s", terminator.calls.Load(), first, stored.Checkpoint)
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
	selection := make(chan storage.NewProviderContextOptions, 4)
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, opts storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			select {
			case selection <- opts:
			default:
			}
			return &testutil.MockStorageTarget{ProviderIDValue: id, DataSetIDValue: &dataSetID, ClientDataSetIDValue: clientDataSetID}, nil
		},
	}

	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
	})
	seedProviderSelection(t, runtime, providerID)
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
		Type:  model.TaskTypeUploadPlan,
		Input: storagepipeline.UploadPlanInput{ContentID: content.ID},
	})
	if err != nil || !created {
		t.Fatalf("enqueue upload plan = %#v created=%v err=%v", taskRow, created, err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	var opts storage.NewProviderContextOptions
	select {
	case opts = <-selection:
	case <-time.After(5 * time.Second):
		stopHandlerEngine(t, cancel, done)
		t.Fatal("upload plan never asked for storage targets")
	}
	stopHandlerEngine(t, cancel, done)

	if opts.DataSetMetadata["bucket"] != bucket.Name {
		t.Fatalf("bucket metadata = %q, want %q", opts.DataSetMetadata["bucket"], bucket.Name)
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
	var openedFailedProvider atomic.Bool
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			if id.String() == failedProvider.String() {
				openedFailedProvider.Store(true)
				return nil, errors.New("failed provider selected")
			}
			clientDataSetID := testOnChainID(t, 18000+sequence).SDK()
			return &testutil.MockStorageTarget{ProviderIDValue: healthyProvider.SDK(), DataSetIDValue: &healthyDataSet, ClientDataSetIDValue: clientDataSetID}, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
	})
	seedProviderSelection(t, runtime, failedProvider.SDK(), healthyProvider.SDK())
	bucket := &model.Bucket{
		Name: fmt.Sprintf("provision-recovery-%d", sequence), Status: model.BucketStatusProvisioning,
		DefaultCopies: 1, MinimumDurableCopies: 1,
	}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// An unfinished creation retains its provider identity outside the current slot.
	failed, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: failedProvider, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("status = ?", model.StorageDataSetStatusCreating).Set("is_current = ?", false).Where("id = ?", failed.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}

	taskRow, created, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type:  model.TaskTypeBucketProvision,
		Input: bucketlifecycle.ProvisionInput{BucketID: bucket.ID, Copies: bucket.DefaultCopies},
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
	if openedFailedProvider.Load() {
		t.Fatal("selection opened the failed generation's provider")
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

func TestDataSetEnsurePreservesTheFailedGenerationWhenCreationIsRejected(t *testing.T) {
	sequence := storedObjectSequence.Add(1)
	providerID := testOnChainID(t, 26000+sequence)
	target := &testutil.MockStorageTarget{
		ProviderIDValue: providerID.SDK(),
		WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
			return nil, pdp.ErrTxRejected
		},
		FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
			return storage.DataSetRef{}, false, nil
		},
	}
	storageClient := &testutil.MockStorageClient{
		OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return target, nil
		},
	}
	budget := 1
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone, maxAttempts: &budget,
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
		ID: binding.ID, TxHash: "0xrejected", StatusURL: "https://provider.example/status",
		ClientDataSetID: &clientDataSetID,
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	ensureTask, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{
		Type:  model.TaskTypeStorageDataSetEnsure,
		Input: storagepipeline.DataSetInput{ExecutionIdentity: testStorageExecutionIdentity(), DataSetID: binding.ID},
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, row.ID)
	})
	if err != nil {
		t.Fatalf("enqueue data set ensure: %v", err)
	}
	checkpoint, err := json.Marshal(map[string]any{
		"attempted_at": time.Now().UTC(), "client_data_set_id": clientDataSetID.String(),
		"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress, "sends": 1,
		"transaction_id": "0xrejected", "status_url": "https://provider.example/status",
	})
	if err != nil {
		t.Fatalf("encode checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, string(checkpoint), ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" || !handlerTaskRetryable(t, runtime, failed) {
		t.Fatalf("failure=%v retryable=%v, want an exhausted round with manual recovery", failed.FailureReason, handlerTaskRetryable(t, runtime, failed))
	}

	ended, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || ended == nil || ended.Status != model.StorageDataSetStatusCreating || ended.EnsureTaskID == nil || *ended.EnsureTaskID != failed.ID || !ended.IsCurrent || ended.ClientDataSetID == nil || !ended.ClientDataSetID.Equal(clientDataSetID) {
		t.Fatalf("rejected generation = %#v err=%v, want its request and current owner preserved", ended, err)
	}
	if ended.TxHash == nil || *ended.TxHash != "0xrejected" || ended.CreateStatusURL == nil || *ended.CreateStatusURL != "https://provider.example/status" {
		t.Fatalf("rejected submission evidence was lost: %#v", ended)
	}
	events, err := runtime.repos.Tasks.ListEvents(ctx, failed.ID, 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	preserved := false
	for _, event := range events {
		if event.Type == "dataset_transaction_rejected" && strings.Contains(string(event.Details), "0xrejected") && strings.Contains(string(event.Details), clientDataSetID.String()) {
			preserved = true
		}
	}
	var recovery struct {
		TxHash   string `json:"transaction_id"`
		Rejected bool   `json:"transaction_rejected"`
	}
	if err := json.Unmarshal(failed.Checkpoint, &recovery); err != nil || !preserved || recovery.TxHash != "0xrejected" || !recovery.Rejected {
		t.Fatalf("rejected submission recovery=%#v err=%v events=%v", recovery, err, events)
	}
	if _, local, err := runtime.repos.Replacements.SourceEligibility(ctx, ended.ID); err != nil || !local {
		t.Fatalf("failed source eligibility = %v, %v", local, err)
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
		Type:  model.TaskTypeStorageDataSetEnsure,
		Input: storagepipeline.DataSetInput{ExecutionIdentity: testStorageExecutionIdentity(), DataSetID: binding.ID},
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

// A lost create response is recovered with the same request identity.
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
	if recorded.LastError == nil || *recorded.LastError != "connection reset by provider" {
		t.Fatalf("generation last_error = %v, want why the request failed", recorded.LastError)
	}

	// Nothing is visible once the request has had time to land.
	stored, err := runtime.repos.Tasks.GetByID(ctx, fixture.ensureTask.ID)
	if err != nil {
		t.Fatalf("load ensure task: %v", err)
	}
	if stored.LastError == nil || *stored.LastError != "connection reset by provider" {
		t.Fatalf("waiting task last_error = %v, want why the request failed", stored.LastError)
	}
	var checkpoint map[string]any
	if err := json.Unmarshal(stored.Checkpoint, &checkpoint); err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	checkpoint["next_resend_at"] = time.Now().UTC().Add(-time.Second)
	aged, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("encode checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, string(aged), fixture.ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("age checkpoint: %v", err)
	}
	wakeTask(t, runtime, fixture.ensureTask.ID)
	waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusCompleted
	})
	if sends() != 2 || sentIDs[0] != sentIDs[1] {
		t.Fatalf("create requests = %v, want the same client data set ID sent twice", sentIDs)
	}
	ready, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
	if err != nil || ready.Status != model.StorageDataSetStatusReady || ready.DataSetID == nil ||
		ready.DataSetID.String() != createdID.String() || ready.ClientDataSetID.String() != sentIDs[0] ||
		ready.LastError != nil {
		t.Fatalf("generation = %#v err=%v, want the data set the first request created", ready, err)
	}
}

func TestDataSetCreationRecoveryRecordsIndependentRoundStart(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sends         int
		unknownTime   bool
		existingStart bool
	}{
		{name: "first submission", sends: 1},
		{name: "existing start", sends: 1, existingStart: true},
		{name: "resubmitted request", sends: 2},
		{name: "unknown send count"},
		{name: "unknown attempted time", sends: 1, unknownTime: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID := testOnChainID(t, 51000+sequence)
			clientID := testOnChainID(t, 52000+sequence)
			createdID := testOnChainID(t, 53000+sequence)
			ref, err := storage.NewDataSetRef(providerID.SDK(), createdID.SDK(), clientID.SDK())
			if err != nil {
				t.Fatal(err)
			}
			var ready atomic.Bool
			var sends atomic.Int64
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					sends.Add(1)
					return nil, errors.New("unexpected create request")
				},
				WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					if !ready.Load() {
						return nil, errors.New("submission status is temporarily unavailable")
					}
					return &storage.CreateDataSetResult{DataSet: ref, TransactionID: "0xsubmitted", ConfirmedTransactionID: "0xconfirmed"}, nil
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						return target, nil
					},
				},
				policy: cache.EvictionPolicyNone,
			})
			ctx := t.Context()
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("restore-start-%d", sequence))
			const transactionID, statusURL = "0xsubmitted", "https://provider.example/status"
			if err := runtime.repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
				ID: fixture.binding.ID, TxHash: transactionID, StatusURL: statusURL, ClientDataSetID: &clientID,
			}); err != nil {
				t.Fatal(err)
			}
			baseTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
			attemptedAt := baseTime
			if tc.unknownTime {
				attemptedAt = time.Time{}
			}
			checkpoint, err := json.Marshal(map[string]any{
				"attempted_at": attemptedAt, "client_data_set_id": clientID.String(),
				"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress, "sends": tc.sends,
				"transaction_id": transactionID, "status_url": statusURL,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, string(checkpoint), fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			var existingStart, expectedStart *time.Time

			if tc.existingStart {
				knownStart := baseTime.Add(-time.Minute)
				existingStart, expectedStart = &knownStart, &knownStart
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = ?, work_started_at = ?, created_at = ? WHERE id = ?`,
				model.TaskResumeModeRecover, existingStart, baseTime.Add(-30*time.Minute), fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			assertStart := func(row *model.Task) {
				t.Helper()
				if expectedStart == nil {
					if row.WorkStartedAt == nil || !row.WorkStartedAt.After(baseTime) {
						t.Fatalf("round recovery start = %v, must be after persisted submission %v", row.WorkStartedAt, baseTime)
					}
				} else if row.WorkStartedAt == nil || !row.WorkStartedAt.Equal(*expectedStart) {
					t.Fatalf("work start = %v, want %v", row.WorkStartedAt, expectedStart)
				}
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			pending := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusPending && row.RetryCount > 0
			})
			assertStart(pending)
			ready.Store(true)
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			completed := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusCompleted
			})
			assertStart(completed)
			stored, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
			if err != nil || stored.TxHash == nil || *stored.TxHash != "0xsubmitted" || stored.ConfirmedTxHash == nil || *stored.ConfirmedTxHash != "0xconfirmed" {
				t.Fatalf("creation evidence=%#v error=%v", stored, err)
			}
			if sends.Load() != 0 {
				t.Fatalf("recovery sent %d duplicate create requests", sends.Load())
			}
		})
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
			name: "correlation conflict", reason: "dataset_correlation_conflict",
			status: model.StorageDataSetStatusPending, fenceHeld: true, retryable: false, incompleteCopies: 1,
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
			})
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("recovery-stop-%d", sequence))
			ctx := t.Context()
			// One request was sent long ago under the original identity.
			if err := runtime.repos.Contents.RecordDataSetClientID(ctx, fixture.binding.ID, fixture.ensureTask.ID, clientID); err != nil {
				t.Fatalf("record client data set ID: %v", err)
			}
			recorded := map[string]any{
				"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(),
				"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress, "sends": 1,
			}
			if tt.submission {
				const transactionID, statusURL = "0xsubmitted", "https://provider.example/status"
				if err := runtime.repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
					ID: fixture.binding.ID, TxHash: transactionID, StatusURL: statusURL, ClientDataSetID: &clientID,
				}); err != nil {
					t.Fatalf("mark creating: %v", err)
				}
				recorded["transaction_id"], recorded["status_url"] = transactionID, statusURL
			}
			checkpoint, err := json.Marshal(recorded)
			if err != nil {
				t.Fatalf("encode checkpoint: %v", err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, string(checkpoint), fixture.ensureTask.ID).Exec(ctx); err != nil {
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
			if handlerTaskRetryable(t, runtime, failed) != tt.retryable {
				t.Fatalf("retryable = %v, want %v", handlerTaskRetryable(t, runtime, failed), tt.retryable)
			}
			if failed.WorkStartedAt != nil {
				t.Fatalf("invalid request identity restored work start %v", failed.WorkStartedAt)
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

func TestDataSetCreationWithUnusableCheckpointPreservesConfirmedOrPendingFacts(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		t.Run(fmt.Sprintf("confirmed=%t", confirmed), func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID := testOnChainID(t, 41000+sequence)
			clientID := testOnChainID(t, 42000+sequence)
			var calls atomic.Int64
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					calls.Add(1)
					return storage.DataSetRef{}, false, errors.New("unexpected observation without original identity")
				},
				CreateDataSetFunc: func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					calls.Add(1)
					return nil, errors.New("unexpected create request")
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{
					OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
						return target, nil
					},
				}, policy: cache.EvictionPolicyNone,
			})
			ctx := t.Context()
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("unnamed-creation-%d", sequence))
			if err := runtime.repos.Contents.RecordDataSetClientID(ctx, fixture.binding.ID, fixture.ensureTask.ID, clientID); err != nil {
				t.Fatal(err)
			}
			if confirmed {
				if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{
					ID: fixture.binding.ID, DataSetID: testOnChainID(t, 43000+sequence), ClientDataSetID: &clientID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = ?, resume_mode = ? WHERE id = ?",
				`{"client_data_set_id":12}`, model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			settled := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(row *model.Task) bool {
				if confirmed {
					return row.Status == model.TaskStatusCompleted
				}
				return row.Status == model.TaskStatusFailed
			})
			if !confirmed && (settled.FailureReason == nil || *settled.FailureReason != "invalid_checkpoint") {
				t.Fatalf("failure=%v, want invalid_checkpoint", settled.FailureReason)
			}
			if calls.Load() != 0 {
				t.Fatalf("external calls=%d, want zero", calls.Load())
			}
			kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
			expectedStatus := model.StorageDataSetStatusPending
			if confirmed {
				expectedStatus = model.StorageDataSetStatusReady
			}
			if err != nil || kept.Status != expectedStatus || (kept.EnsureTaskID != nil) == confirmed || !kept.IsCurrent || !kept.ClientDataSetID.Equal(clientID) {
				t.Fatalf("generation=%#v err=%v", kept, err)
			}
		})
	}
}

func TestDataSetEnsurePreservesAnUnsentGenerationWithIncompleteIdentity(t *testing.T) {
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
	})
	ctx := t.Context()
	fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("never-sent-%d", sequence))

	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusFailed
	})
	if failed.FailureReason == nil || *failed.FailureReason != "storage_identity_changed" || !handlerTaskRetryable(t, runtime, failed) {
		t.Fatalf("failure = %v retryable=%v, want recovery after restoring the signing identity", failed.FailureReason, handlerTaskRetryable(t, runtime, failed))
	}
	kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
	if err != nil || kept.Status != model.StorageDataSetStatusPending || kept.EnsureTaskID == nil || *kept.EnsureTaskID != failed.ID || !kept.IsCurrent || kept.ClientDataSetID != nil {
		t.Fatalf("generation = %#v err=%v, want its unsent owner and current slot preserved", kept, err)
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
	})
	ctx := t.Context()
	fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("dropped-submission-%d", sequence))
	if err := runtime.repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID: fixture.binding.ID, TxHash: "0xdead", StatusURL: "https://provider.example/status",
		ClientDataSetID: &clientID,
	}); err != nil {
		t.Fatalf("mark creating: %v", err)
	}
	// An earlier request with this ID had no observed outcome, so the rejection
	// only means that one may have created the data set first.
	checkpoint, err := json.Marshal(map[string]any{
		"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(),
		"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress, "sends": 2,
		"transaction_id": "0xdead", "status_url": "https://provider.example/status",
	})
	if err != nil {
		t.Fatalf("encode checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, string(checkpoint), fixture.ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = ? WHERE id = ?`, model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(ctx); err != nil {
		t.Fatalf("resume in recovery: %v", err)
	}

	pending := runOneStorageTask(t, runtime, fixture.ensureTask, model.TaskStatusPending)
	if lookups.Load() != 1 || pending.RetryCount != 1 {
		t.Fatalf("rejected lookup: task=%#v lookups=%d", pending, lookups.Load())
	}
	kept, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, fixture.binding.ID)
	if err != nil || kept.Status != model.StorageDataSetStatusCreating || kept.EnsureTaskID == nil || kept.TxHash == nil || *kept.TxHash != "0xdead" {
		t.Fatalf("generation = %#v err=%v, want it kept with its creation fence", kept, err)
	}
	var recovery struct {
		TxHash   string `json:"transaction_id"`
		Rejected bool   `json:"transaction_rejected"`
	}
	if err := json.Unmarshal(pending.Checkpoint, &recovery); err != nil || recovery.TxHash != "0xdead" || !recovery.Rejected {
		t.Fatalf("rejected submission = %#v err=%v", recovery, err)
	}
}

func seedProviderSelection(t *testing.T, runtime handlerTestRuntime, providers ...sdktypes.BigInt) {
	t.Helper()
	active, pdp, url := true, true, "https://provider.example"
	var ids []idtypes.OnChainID
	for _, provider := range providers {
		id := idtypes.OnChainIDFromSDK(provider)
		ids = append(ids, id)
		err := runtime.repos.Observability.UpsertProviderObservation(t.Context(), time.Now().UTC(), observability.ProviderState{ProviderID: id, Status: observability.StatusAvailable, Active: &active, HasPDP: &pdp, ServiceURL: &url, Profile: &observability.ProviderProfile{ProviderID: id, Active: true, ServiceURL: url, RegistrySnapshot: json.RawMessage(`{"version":1}`)}})
		if err != nil {
			t.Fatal(err)
		}
		seedProviderSelectionSpeed(t, runtime, id)
	}
	if _, err := runtime.repos.Observability.RecordApprovedProviders(t.Context(), time.Now().UTC(), ids); err != nil {
		t.Fatal(err)
	}
}

func seedProviderSelectionSpeed(t *testing.T, runtime handlerTestRuntime, providers ...idtypes.OnChainID) {
	t.Helper()
	for _, id := range providers {
		now := time.Now().UTC()
		row := &providerbenchmark.Result{
			ProviderID: id.String(), State: providerbenchmark.StateSucceeded,
			ServiceURLHash: providerbenchmark.URLHash("https://provider.example"), SampleBytes: providerbenchmark.SampleBytes,
			DurationMS: new(int64(1000)), BytesPerSecond: new(int64(providerbenchmark.SampleBytes)), TestedAt: &now,
			CreatedAt: now, UpdatedAt: now,
		}
		if _, err := runtime.db.NewInsert().Model(row).On("CONFLICT (provider_id) DO NOTHING").Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func activateReplacement(t *testing.T, runtime handlerTestRuntime, id int64) error {
	t.Helper()
	row, err := runtime.repos.Replacements.GetByID(t.Context(), id)
	if err != nil {
		return err
	}
	if row.TaskID != nil {
		return runtime.repos.Replacements.Activate(t.Context(), id, row.TaskGeneration, *row.TaskID)
	}
	task, _, err := runtime.repos.Tasks.Enqueue(t.Context(), handlerTaskFixture(t, runtime, &model.Task{Type: model.TaskTypeProviderReplacementCoordinate, InputVersion: 1, Input: json.RawMessage(fmt.Sprintf(`{"replacement_id":%d,"generation":%d}`, row.ID, row.TaskGeneration)), SubjectType: "storage_replacement", SubjectKey: fmt.Sprint(row.ID)}))
	if err != nil {
		return err
	}
	if err := runtime.repos.Replacements.BindTask(t.Context(), id, row.TaskGeneration, task.ID); err != nil {
		return err
	}
	if err := runtime.repos.Replacements.Activate(t.Context(), id, row.TaskGeneration, task.ID); err != nil {
		return err
	}
	if err := runtime.repos.Replacements.CompleteTask(t.Context(), id, row.TaskGeneration, task.ID); err != nil {
		return err
	}
	settleHandlerTaskFixture(t, runtime, task.ID, model.TaskStatusCancelled, nil)
	return nil
}

func (o testWalletOperator) WalletIdentity() walletoperation.ExecutionIdentity {
	if o.identity.Identity != nil {
		return o.identity
	}
	return testutil.DefaultWalletExecutionIdentity()
}

func (*testServiceTerminator) PDPVerifierAddress() common.Address {
	return testutil.DefaultPDPVerifierAddress
}

func (*sequencedTerminator) PDPVerifierAddress() common.Address {
	return testutil.DefaultPDPVerifierAddress
}

func TestDataSetCreationRetainsPartialResultOnError(t *testing.T) {
	for _, tt := range []struct {
		name              string
		callbackOnly      bool
		found             bool
		wantConfirmedHash string
	}{
		{name: "partial result resolved", found: true, wantConfirmedHash: "0xconfirmed"},
		{name: "partial result not visible", wantConfirmedHash: "0xconfirmed"},
		{name: "submission callback resolved", callbackOnly: true, found: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			providerID := testOnChainID(t, 90101)
			dataSetID := sdktypes.NewBigInt(90102)
			cause := errors.New("provider confirmation response has no data set identity")
			var creates, lookups, statusPolls atomic.Int64
			provider := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					creates.Add(1)
					if tt.callbackOnly {
						options.OnSubmitted(storage.CreateDataSetSubmission{TransactionID: "0xsubmitted"})
						return nil, cause
					}
					return &storage.CreateDataSetResult{TransactionID: "0xsubmitted", ConfirmedTransactionID: tt.wantConfirmedHash}, cause
				},
				FindDataSetByClientIDFunc: func(_ context.Context, clientID sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					lookups.Add(1)
					if !tt.found {
						return storage.DataSetRef{}, false, nil
					}
					ref, err := storage.NewDataSetRef(providerID.SDK(), dataSetID, clientID)
					return ref, err == nil, err
				},
				WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					statusPolls.Add(1)
					return nil, errors.New("cannot poll without a status URL")
				},
			}
			budget := 2
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return provider, nil
			}}, policy: cache.EvictionPolicyNone, maxAttempts: &budget})
			fixture := seedDataSetEnsure(t, runtime, providerID, "partial-creation-result")
			waiting := runOneStorageTask(t, runtime, fixture.ensureTask, model.TaskStatusPending)
			row, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
			if err != nil {
				t.Fatal(err)
			}
			if row.Status != model.StorageDataSetStatusCreating || row.DataSetID != nil {
				t.Fatalf("partial result marked data set ready: %#v", row)
			}
			if row.TxHash == nil || *row.TxHash != "0xsubmitted" {
				t.Fatalf("submitted hash = %v, want 0xsubmitted", row.TxHash)
			}
			confirmedHash := ""
			if row.ConfirmedTxHash != nil {
				confirmedHash = *row.ConfirmedTxHash
			}
			if got := confirmedHash; got != tt.wantConfirmedHash {
				t.Fatalf("confirmed hash = %q, want %q", got, tt.wantConfirmedHash)
			}
			if row.LastError == nil || *row.LastError != cause.Error() {
				t.Fatalf("creation error = %v, want %q", row.LastError, cause)
			}

			var checkpoint map[string]any
			if err := json.Unmarshal(waiting.Checkpoint, &checkpoint); err != nil {
				t.Fatal(err)
			}
			checkpoint["next_resend_at"] = time.Now().UTC().Add(-time.Hour)
			encoded, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("checkpoint_json = ?", json.RawMessage(encoded)).
				Set("resume_mode = ?", model.TaskResumeModeExecute).
				Where("id = ?", waiting.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if tt.found {
				runOneStorageTask(t, runtime, waiting, model.TaskStatusCompleted)
			} else {
				waiting = runOneStorageTask(t, runtime, waiting, model.TaskStatusPending)
				if waiting.RetryCount != 1 {
					t.Fatalf("observation consumed a retry: %d", waiting.RetryCount)
				}
				waiting = setDataSetConfirmationStart(t, runtime, waiting, time.Now().UTC().Add(-31*time.Minute))
				failed := runOneStorageTask(t, runtime, waiting, model.TaskStatusFailed)
				if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" {
					t.Fatalf("unresolved transaction did not exhaust its budget: %#v", failed)
				}
			}
			row, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tt.found && (row.Status != model.StorageDataSetStatusReady || row.DataSetID == nil || row.DataSetID.String() != dataSetID.String()) {
				t.Fatalf("client ID lookup did not resolve data set: %#v", row)
			}
			confirmedHash = ""
			if row.ConfirmedTxHash != nil {
				confirmedHash = *row.ConfirmedTxHash
			}
			if row.TxHash == nil || *row.TxHash != "0xsubmitted" || confirmedHash != tt.wantConfirmedHash {
				t.Fatalf("recovery lost transaction hashes: %#v", row)
			}
			if lookups.Load() == 0 {
				t.Fatal("recovery did not query the client data set ID")
			}
			if creates.Load() != 1 {
				t.Fatalf("recovery submitted %d create requests, want 1", creates.Load())
			}
			if statusPolls.Load() != 0 {
				t.Fatalf("polled a missing status URL %d times", statusPolls.Load())
			}
		})
	}
}
