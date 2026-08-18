package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const fakeSubmittedCommitTxHash = "0x7890abcdef1234567890abcdef1234567890abcdef1234567890abcdef123456"

func testCID(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("test-data"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("creating test multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func createContextProviderIDEqual(opts *storage.CreateContextOptions, want sdktypes.BigInt) bool {
	return opts.ProviderID != nil && opts.ProviderID.Equal(want)
}

func createContextDataSetIDEqual(opts *storage.CreateContextOptions, want sdktypes.BigInt) bool {
	return opts.DataSetID != nil && opts.DataSetID.Equal(want)
}

func copyCommitSubmittedForTest(copyRow *model.StorageUploadCopy) bool {
	return copyRow != nil && copyRow.Status == model.StorageUploadCopyStatusCommitting && copyRow.CommitTransactionID != nil && *copyRow.CommitTransactionID != ""
}

// seedCachedObject creates a bucket, writes a file into the filesystem cache,
// and inserts an object in "cached" state.
func seedCachedObject(t *testing.T, env *testWorkerEnv) (*model.Bucket, int64, string) {
	t.Helper()
	ctx := context.Background()

	bucket := &model.Bucket{Name: fmt.Sprintf("b-%d", time.Now().UnixNano()), Status: model.BucketStatusActive}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	key := "hello.txt"
	data := []byte("hello world")
	versionID := model.NewVersionID()
	cacheKey := ".versions/" + versionID

	info, err := env.cache.Put(ctx, bucket.Name, cacheKey, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("cache put: %v", err)
	}

	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         key,
		Size:        int64(len(data)),
		ETag:        info.ETag,
		Checksum:    info.Checksum,
		ContentType: "text/plain",
		CacheKey:    cacheKey,
	}
	objID, err := env.repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("creating object version: %v", err)
	}
	return bucket, objID, versionID
}

// seedObjectInDB inserts a bucket and object into the DB only (no cache write).
func seedObjectInDB(t *testing.T, env *testWorkerEnv, bucketStatus model.BucketStatus) (*model.Bucket, int64, string) {
	t.Helper()
	ctx := context.Background()

	bucket := &model.Bucket{Name: fmt.Sprintf("b-%d", time.Now().UnixNano()), Status: bucketStatus}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	versionID := model.NewVersionID()
	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         "hello.txt",
		Size:        11,
		ETag:        "abc123",
		Checksum:    "sha256-test",
		ContentType: "text/plain",
		CacheKey:    ".versions/" + versionID,
	}
	objID, err := env.repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("creating object version: %v", err)
	}
	return bucket, objID, versionID
}

// seedTask creates a queued task of the given type.
func seedTask(t *testing.T, env *testWorkerEnv, taskType model.TaskType, refID int64, versionID string, maxRetries, retryCount int) *model.Task {
	t.Helper()
	ctx := context.Background()
	task := &model.Task{
		Type:           taskType,
		RefType:        "object",
		RefID:          refID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("%s:%s", taskType, versionID),
		Status:         model.TaskStatusQueued,
		RetryCount:     retryCount,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	}
	switch taskType {
	case model.TaskTypeUpload:
		task.Payload = map[string]interface{}{"stage": ""}
	case model.TaskTypeEvictCache:
		stage := cacheeviction.StageAfterUpload
		task.Stage = &stage
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("creating task: %v", err)
	}
	return task
}

func seedStagedUploadTask(t *testing.T, env *testWorkerEnv, refID int64, versionID string, maxRetries int) *model.Task {
	t.Helper()
	stage := "prepare_upload"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          refID,
		RefVersionID:   versionID,
		IdempotencyKey: "upload:" + versionID,
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("creating staged task: %v", err)
	}
	return task
}

func waitForObjectState(t *testing.T, env *testWorkerEnv, versionID string, state model.ObjectState, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
			if err != nil {
				t.Fatalf("timed out waiting for version %s state %s; last lookup error: %v", versionID, state, err)
			}
			if version == nil {
				t.Fatalf("timed out waiting for version %s state %s; version not found", versionID, state)
			}
			t.Fatalf("timed out waiting for version %s state %s; current state %s", versionID, state, version.State)
		case <-ticker.C:
			version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
			if err != nil || version == nil {
				continue
			}
			if version.State == state {
				return
			}
		}
	}
}

// runWorkerUntilTask runs a worker and waits until the given task
// leaves active queue states, or times out.
func runWorkerUntilTask(t *testing.T, env *testWorkerEnv, w worker.Worker, taskID int64, timeout time.Duration) *model.Task {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseTasks := env.repos.Tasks
	env.repos.Tasks = stopAfterTaskRepo{TaskRepository: baseTasks, stopTaskID: taskID}
	defer func() { env.repos.Tasks = baseTasks }()

	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()

	deadline := time.After(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("timed out waiting for task %d to be processed", taskID)
		case <-ticker.C:
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err != nil {
				continue
			}
			if task != nil && task.Status != model.TaskStatusQueued && task.Status != model.TaskStatusScheduled && task.Status != model.TaskStatusWaiting && task.Status != model.TaskStatusRunning {
				cancel()
				<-done
				return task
			}
		}
	}
}

type stopAfterTaskRepo struct {
	repository.TaskRepository
	stopTaskID int64
}

func (r stopAfterTaskRepo) ClaimReady(ctx context.Context, taskType model.TaskType, leaseDuration time.Duration) (*model.Task, error) {
	task, err := r.GetByID(ctx, r.stopTaskID)
	if err != nil {
		return nil, err
	}
	if task != nil && !taskStatusActive(task.Status) {
		return nil, nil
	}
	return r.TaskRepository.ClaimReady(ctx, taskType, leaseDuration)
}

func taskStatusActive(status model.TaskStatus) bool {
	switch status {
	case model.TaskStatusQueued, model.TaskStatusScheduled, model.TaskStatusWaiting, model.TaskStatusRunning:
		return true
	default:
		return false
	}
}

// runWorkerUntilTaskRetryCount runs a worker until the task has recorded at
// least the requested retry count, then cancels the worker.
func runWorkerUntilTaskRetryCount(t *testing.T, env *testWorkerEnv, w worker.Worker, taskID int64, retryCount int, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()

	deadline := time.After(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("timed out waiting for task %d retry_count >= %d", taskID, retryCount)
		case <-ticker.C:
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err != nil {
				continue
			}
			if task != nil && task.RetryCount >= retryCount && task.Status != model.TaskStatusRunning && task.Status != model.TaskStatusFailed {
				cancel()
				<-done
				return
			}
		}
	}
}

func waitForSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitForTaskStatus(t *testing.T, env *testWorkerEnv, taskID int64, status model.TaskStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err != nil {
				t.Fatalf("timed out waiting for task %d to reach %s; last lookup error: %v", taskID, status, err)
			}
			if task == nil {
				t.Fatalf("timed out waiting for task %d to reach %s; task not found", taskID, status)
			}
			t.Fatalf("timed out waiting for task %d to reach %s; current status %s", taskID, status, task.Status)
		case <-ticker.C:
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err != nil || task == nil {
				continue
			}
			if task.Status == status {
				return
			}
		}
	}
}

type publishedAdminEvent struct {
	topic   string
	payload map[string]any
}

type fakeAdminEventPublisher struct {
	mu     sync.Mutex
	events []publishedAdminEvent
}

func (p *fakeAdminEventPublisher) Publish(topic string, payload map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, publishedAdminEvent{topic: topic, payload: payload})
}

func (p *fakeAdminEventPublisher) hasTopic(topic string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, event := range p.events {
		if event.topic == topic {
			return true
		}
	}
	return false
}

type fakeUploadContext struct {
	providerID           sdktypes.BigInt
	dataSetID            sdktypes.BigInt
	dataSetMu            sync.RWMutex
	boundDataSet         *sdktypes.BigInt
	pieceID              sdktypes.BigInt
	pieceCID             cid.Cid
	clientDataID         sdktypes.BigInt
	pullEntered          chan struct{}
	releasePull          chan struct{}
	pullOnce             sync.Once
	pullErr              error
	createCalls          *atomic.Int32
	waitCalls            *atomic.Int32
	skipCreateSubmission bool
	createErr            error
	waitErr              error
	storeErr             error
	storeProgress        []int64
	storeCalls           atomic.Int32
	commitErr            error
	serviceURL           string
	presignCalls         atomic.Int32
	commitCalls          atomic.Int32
	commitMu             sync.Mutex
	commitExtras         [][]byte
	pullCalls            atomic.Int32
}

func newFakeUploadContext(providerID sdktypes.BigInt, dataSetID sdktypes.BigInt, pieceID sdktypes.BigInt, pieceCID cid.Cid) *fakeUploadContext {
	dataSetUint64, _ := dataSetID.Uint64()
	return &fakeUploadContext{
		providerID:   providerID,
		dataSetID:    dataSetID,
		pieceID:      pieceID,
		pieceCID:     pieceCID,
		clientDataID: sdktypes.NewBigInt(dataSetUint64 + 10000),
	}
}

func (f *fakeUploadContext) ProviderID() sdktypes.BigInt { return f.providerID.Copy() }

func (f *fakeUploadContext) DataSetID() *sdktypes.BigInt {
	f.dataSetMu.RLock()
	defer f.dataSetMu.RUnlock()
	if f.boundDataSet == nil {
		return nil
	}
	id := f.boundDataSet.Copy()
	return &id
}

func (f *fakeUploadContext) GetProviderInfo() storage.Provider {
	return storage.Provider{
		ID:         f.providerID.Copy(),
		ServiceURL: f.ServiceURL(),
	}
}

func (f *fakeUploadContext) WithCDN() bool { return false }

func (f *fakeUploadContext) PieceURL(piece cid.Cid) string {
	return fmt.Sprintf("https://provider-%s.example/piece/%s", f.providerID.String(), piece.String())
}

func (f *fakeUploadContext) ServiceURL() string {
	if f.serviceURL != "" {
		return f.serviceURL
	}
	return fmt.Sprintf("https://provider-%s.example", f.providerID.String())
}

func (f *fakeUploadContext) CreateDataSet(_ context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	if f.createCalls != nil {
		f.createCalls.Add(1)
	}
	submission := storage.CreateDataSetSubmission{
		TransactionID:   fmt.Sprintf("0xcreate%s", f.dataSetID.String()),
		StatusURL:       fmt.Sprintf("https://provider-%s.example/status/create", f.providerID.String()),
		ClientDataSetID: sdkBigIntTestPtr(f.clientDataID),
	}
	if !f.skipCreateSubmission && opts != nil && opts.OnSubmitted != nil {
		opts.OnSubmitted(submission)
	}
	if f.createErr != nil {
		return nil, f.createErr
	}
	createdDataSetID := f.dataSetID.Copy()
	f.dataSetMu.Lock()
	f.boundDataSet = &createdDataSetID
	f.dataSetMu.Unlock()
	return &storage.CreateDataSetResult{
		TransactionID:   submission.TransactionID,
		DataSetID:       f.dataSetID.Copy(),
		ClientDataSetID: f.clientDataID.Copy(),
	}, nil
}

func (f *fakeUploadContext) WaitForDataSetCreated(_ context.Context, submission storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error) {
	if f.waitCalls != nil {
		f.waitCalls.Add(1)
	}
	if f.waitErr != nil {
		return nil, f.waitErr
	}
	createdDataSetID := f.dataSetID.Copy()
	f.dataSetMu.Lock()
	f.boundDataSet = &createdDataSetID
	f.dataSetMu.Unlock()
	return &storage.CreateDataSetResult{
		TransactionID:   submission.TransactionID,
		DataSetID:       f.dataSetID.Copy(),
		ClientDataSetID: submission.ClientDataSetID.Copy(),
	}, nil
}

func (f *fakeUploadContext) Store(_ context.Context, r io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
	f.storeCalls.Add(1)
	if _, err := io.ReadAll(r); err != nil {
		return nil, err
	}
	if opts != nil && opts.OnProgress != nil {
		for _, n := range f.storeProgress {
			opts.OnProgress(n)
		}
	}
	if f.storeErr != nil {
		return nil, f.storeErr
	}
	return &storage.StoreResult{PieceCID: f.pieceCID, Size: 11}, nil
}

func (f *fakeUploadContext) PresignForCommit(_ context.Context, _ []storage.PieceInput) ([]byte, error) {
	f.presignCalls.Add(1)
	return []byte(fmt.Sprintf("extra-%s", f.providerID.String())), nil
}

func (f *fakeUploadContext) Pull(ctx context.Context, _ storage.PullRequest) (*storage.PullResult, error) {
	f.pullCalls.Add(1)
	if f.pullEntered != nil {
		f.pullOnce.Do(func() { close(f.pullEntered) })
	}
	if f.releasePull != nil {
		select {
		case <-f.releasePull:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	return &storage.PullResult{Status: storage.PullStatusComplete}, nil
}

func (f *fakeUploadContext) Commit(_ context.Context, req storage.CommitRequest) (*storage.CommitResult, error) {
	f.commitCalls.Add(1)
	f.commitMu.Lock()
	f.commitExtras = append(f.commitExtras, append([]byte(nil), req.ExtraData...))
	f.commitMu.Unlock()
	if req.OnSubmitted != nil {
		req.OnSubmitted(fakeSubmittedCommitTxHash)
	}
	if f.commitErr != nil {
		return nil, f.commitErr
	}
	return &storage.CommitResult{
		TransactionID: fakeSubmittedCommitTxHash,
		DataSetID:     f.dataSetID.Copy(),
		PieceIDs:      []sdktypes.BigInt{f.pieceID.Copy()},
	}, nil
}

func sdkBigIntTestPtr(id sdktypes.BigInt) *sdktypes.BigInt {
	cp := id.Copy()
	return &cp
}

func newFakeUploadContexts(t *testing.T, copies int, base uint64) []synapse.UploadContext {
	t.Helper()
	contexts := make([]synapse.UploadContext, 0, copies)
	for i := 0; i < copies; i++ {
		offset := base + uint64(i)
		contexts = append(contexts, newFakeUploadContext(
			sdktypes.NewBigInt(100+offset),
			sdktypes.NewBigInt(1000+offset),
			sdktypes.NewBigInt(2000+offset),
			testCID(t),
		))
	}
	return contexts
}

func seedReadyPrimaryStoreTask(t *testing.T, env *testWorkerEnv) (*model.StorageUpload, *model.Task, *fakeUploadContext) {
	t.Helper()
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:              binding.ID,
		UploadID:        upload.ID,
		DataSetID:       onChainID(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "11001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	stage := "ingress_store"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ingress_store:%d", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create primary store task: %v", err)
	}
	primaryCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	dataSetID := sdktypes.NewBigInt(1001)
	primaryCtx.boundDataSet = &dataSetID
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)) {
			return primaryCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	return upload, task, primaryCtx
}

func TestUploader_WaitsPollIntervalBeforeInitialClaim(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	uploadStarted := make(chan struct{})
	var closeStarted sync.Once
	pollInterval := 100 * time.Millisecond

	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		closeStarted.Do(func() { close(uploadStarted) })
		contexts := make([]synapse.UploadContext, 0, opts.Copies)
		for i := 0; i < opts.Copies; i++ {
			contexts = append(contexts, newFakeUploadContext(
				sdktypes.NewBigInt(uint64(100+i)),
				sdktypes.NewBigInt(uint64(1000+i)),
				sdktypes.NewBigInt(uint64(2000+i)),
				testCID(t),
			))
		}
		return contexts, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, pollInterval, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()

	select {
	case <-uploadStarted:
		t.Fatal("uploader claimed upload task before initial poll interval elapsed")
	case <-time.After(pollInterval / 2):
	}

	waitForTaskStatus(t, env, task.ID, model.TaskStatusCompleted, time.Second)
}

func TestUploader_CompletesRetryWhenObjectIsAlreadyStored(t *testing.T) {
	env := newTestWorkerEnv(t)
	fixture := seedReadableUploadWithPendingPeer(t, env)
	stage := "peer_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          fixture.objID,
		RefVersionID:   fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:peer_commit:%d:1", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("create peer commit task: %v", err)
	}

	claimed, err := env.repos.Tasks.ClaimReady(context.Background(), model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimReady: task=%v err=%v", claimed, err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(context.Background(), repository.MarkUploadCopyCommittedInput{
		UploadID:     fixture.upload.ID,
		CopyIndex:    1,
		PieceCID:     testCID(t).String(),
		PieceID:      onChainIDPtr(t, "302"),
		RetrievalURL: "https://peer.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	finalized, _, err := env.repos.Uploads.FinalizeUploadIfTargetCopiesMet(context.Background(), repository.FinalizeUploadInput{UploadID: fixture.upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet: %v", err)
	}
	if !finalized {
		t.Fatal("FinalizeUploadIfTargetCopiesMet did not finalize the upload")
	}
	status, err := env.repos.Tasks.ScheduleRetryRunning(context.Background(), claimed, "late worker failure", 0)
	if err != nil {
		t.Fatalf("ScheduleRetryRunning: %v", err)
	}
	if status != model.TaskStatusScheduled {
		t.Fatalf("retry status = %s, want scheduled", status)
	}

	var storageCalls atomic.Int32
	env.storage.CreateContextsFunc = func(context.Context, *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		storageCalls.Add(1)
		return nil, errors.New("storage must not be called for a stored object")
	}
	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 10*time.Millisecond, slog.Default())
	got := runWorkerUntilTask(t, env, uploader, task.ID, time.Second)

	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("task status = %s, want completed", got.Status)
	}
	if got.RetryCount != 1 {
		t.Fatalf("retry count = %d, want 1", got.RetryCount)
	}
	if calls := storageCalls.Load(); calls != 0 {
		t.Fatalf("storage calls = %d, want 0", calls)
	}
}

func TestUploader_ClaimsLaterPendingTaskWhileAnotherUploadRuns(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, firstObjID, firstVersionID := seedCachedObject(t, env)
	firstTask := seedStagedUploadTask(t, env, firstObjID, firstVersionID, 5)

	firstUploadEntered := make(chan struct{})
	releaseFirstUpload := make(chan struct{})
	var releaseOnce sync.Once
	var uploadCalls atomic.Int32

	env.storage.CreateContextsFunc = func(ctx context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		call := uploadCalls.Add(1)
		if call == 1 {
			close(firstUploadEntered)
			select {
			case <-releaseFirstUpload:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		contexts := make([]synapse.UploadContext, 0, opts.Copies)
		for i := 0; i < opts.Copies; i++ {
			offset := uint64(call*100 + int32(i))
			contexts = append(contexts, newFakeUploadContext(
				sdktypes.NewBigInt(100+offset),
				sdktypes.NewBigInt(1000+offset),
				sdktypes.NewBigInt(2000+offset),
				testCID(t),
			))
		}
		return contexts, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 2, 20*time.Millisecond, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(ctx)
		close(done)
	}()
	defer func() {
		releaseOnce.Do(func() { close(releaseFirstUpload) })
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()

	waitForSignal(t, firstUploadEntered, time.Second, "first upload to start")

	_, secondObjID, secondVersionID := seedCachedObject(t, env)
	secondTask := seedStagedUploadTask(t, env, secondObjID, secondVersionID, 5)

	waitForTaskStatus(t, env, secondTask.ID, model.TaskStatusCompleted, 500*time.Millisecond)

	got, err := env.repos.Tasks.GetByID(context.Background(), firstTask.ID)
	if err != nil {
		t.Fatalf("get first task: %v", err)
	}
	if got.Status != model.TaskStatusRunning {
		t.Fatalf("first task status = %s, want running while second task completed", got.Status)
	}
}

func TestUploader_HealthyWhileUploadTaskIsActive(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	_ = seedStagedUploadTask(t, env, objID, versionID, 5)

	uploadEntered := make(chan struct{})
	releaseUpload := make(chan struct{})
	var releaseOnce sync.Once
	pollInterval := 20 * time.Millisecond

	env.storage.CreateContextsFunc = func(ctx context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		close(uploadEntered)
		select {
		case <-releaseUpload:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		contexts := make([]synapse.UploadContext, 0, opts.Copies)
		for i := 0; i < opts.Copies; i++ {
			contexts = append(contexts, newFakeUploadContext(
				sdktypes.NewBigInt(uint64(100+i)),
				sdktypes.NewBigInt(uint64(1000+i)),
				sdktypes.NewBigInt(uint64(2000+i)),
				testCID(t),
			))
		}
		return contexts, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, pollInterval, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(ctx)
		close(done)
	}()
	defer func() {
		releaseOnce.Do(func() { close(releaseUpload) })
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()

	waitForSignal(t, uploadEntered, time.Second, "upload to start")
	time.Sleep(4 * pollInterval)

	if !uploader.Healthy() {
		t.Fatal("uploader should remain healthy while upload task is active")
	}
}

func TestUploader_StagedPrimaryStoreRecordsTransferProgress(t *testing.T) {
	env := newTestWorkerEnv(t)
	upload, task, primaryCtx := seedReadyPrimaryStoreTask(t, env)
	primaryCtx.storeProgress = []int64{5, 11}
	publisher := &fakeAdminEventPublisher{}
	before, err := env.repos.Objects.GetVersionByID(context.Background(), task.RefVersionID)
	if err != nil || before == nil || before.CacheAccessedAt == nil {
		t.Fatalf("GetVersionByID before upload: version=%v err=%v", before, err)
	}
	objects := &cacheAccessCountingObjectRepo{ObjectRepository: env.repos.Objects}
	env.repos.Objects = objects

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default(), worker.WithEventPublisher(publisher))
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, err := env.repos.Uploads.GetByID(context.Background(), upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID(upload): upload=%v err=%v", got, err)
	}
	if got.IngressStoreAttempt != 1 || got.IngressBytesTransferred != 11 || got.ProgressUpdatedAt == nil {
		t.Fatalf("staged progress = bytes:%d attempt:%d updated:%v, want completed primary transfer", got.IngressBytesTransferred, got.IngressStoreAttempt, got.ProgressUpdatedAt)
	}
	if !publisher.hasTopic("upload_progress_updated") {
		t.Fatal("expected upload_progress_updated event")
	}
	if objects.writes != 0 {
		t.Fatalf("background upload cache access writes = %d, want 0", objects.writes)
	}
	after, err := env.repos.Objects.GetVersionByID(context.Background(), task.RefVersionID)
	if err != nil || after == nil || after.CacheAccessedAt == nil {
		t.Fatalf("GetVersionByID after upload: version=%v err=%v", after, err)
	}
	if !after.CacheAccessedAt.Equal(*before.CacheAccessedAt) {
		t.Fatalf("cache_accessed_at changed from %v to %v during background upload", before.CacheAccessedAt, after.CacheAccessedAt)
	}
}

type cacheAccessCountingObjectRepo struct {
	repository.ObjectRepository
	writes int
}

func (r *cacheAccessCountingObjectRepo) RecordVersionCacheAccess(ctx context.Context, versionID string, accessedAt time.Time) error {
	r.writes++
	return r.ObjectRepository.RecordVersionCacheAccess(ctx, versionID, accessedAt)
}

func TestUploader_StagedPrimaryCommitKeepsCacheUntilAllCopiesCommitted(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	pieceCID := testCID(t)
	peerPullEntered := make(chan struct{})
	releaseSecondaryPull := make(chan struct{})
	var releaseSecondaryPullOnce sync.Once

	primary := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), pieceCID)
	secondary := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), pieceCID)
	secondary.pullEntered = peerPullEntered
	secondary.releasePull = releaseSecondaryPull
	contextsByProvider := map[string]*fakeUploadContext{
		primary.providerID.String():   primary,
		secondary.providerID.String(): secondary,
	}
	contextsByDataSet := map[string]*fakeUploadContext{
		primary.dataSetID.String():   primary,
		secondary.dataSetID.String(): secondary,
	}

	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 2 {
			t.Fatalf("CreateContexts copies = %d, want 2", opts.Copies)
		}
		return []synapse.UploadContext{primary, secondary}, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if opts.ProviderID != nil {
			return contextsByProvider[opts.ProviderID.String()], nil
		}
		if opts.DataSetID != nil {
			return contextsByDataSet[opts.DataSetID.String()], nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		releaseSecondaryPullOnce.Do(func() { close(releaseSecondaryPull) })
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()

	waitForObjectState(t, env, versionID, model.ObjectStateReplicating, 3*time.Second)
	waitForSignal(t, peerPullEntered, time.Second, "secondary pull to start")

	obj, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || obj == nil {
		t.Fatalf("GetVersionByID: obj=%v err=%v", obj, err)
	}
	if obj.StorageUploadID == nil || !obj.InFilecoin {
		t.Fatalf("replicating object storage = upload:%v in_filecoin:%v, want readable upload", obj.StorageUploadID, obj.InFilecoin)
	}
	if !obj.InCache {
		t.Fatal("replicating object in_cache = false, want cache retained before all copies are committed")
	}
	upload, err := env.repos.Uploads.GetByID(context.Background(), *obj.StorageUploadID)
	if err != nil || upload == nil {
		t.Fatalf("GetByID(upload): upload=%v err=%v", upload, err)
	}
	if upload.Status != model.StorageUploadStatusReadable {
		t.Fatalf("upload status = %s, want readable while peer copy is blocked", upload.Status)
	}
	evict, err := env.repos.Tasks.ClaimReady(context.Background(), model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady(evict): %v", err)
	}
	if evict != nil {
		t.Fatalf("unexpected evict task before all copies are committed: %#v", evict)
	}

	releaseSecondaryPullOnce.Do(func() { close(releaseSecondaryPull) })
	waitForObjectState(t, env, versionID, model.ObjectStateStored, 3*time.Second)

	evict, err = env.repos.Tasks.ClaimReady(context.Background(), model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady(evict after stored): %v", err)
	}
	if evict == nil {
		t.Fatal("expected evict task after all copies are committed")
	}
	if evict.RefID != objID || evict.RefVersionID != versionID {
		t.Fatalf("evict task refs after stored = (%d,%s), want (%d,%s)", evict.RefID, evict.RefVersionID, objID, versionID)
	}
	if task.ID == 0 || bucket.ID == 0 {
		t.Fatal("seeded task and bucket should be persisted")
	}
}

func TestUploader_EmptyPayloadStartsStagedPrepare(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: "upload:" + versionID,
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if _, err := env.db.NewInsert().Model(task).Exec(context.Background()); err != nil {
		t.Fatalf("creating upload task without payload: %v", err)
	}

	var legacyUploadCalled atomic.Bool
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		legacyUploadCalled.Store(true)
		return nil, errors.New("legacy upload should not be called")
	}
	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		if opts.Copies != 3 {
			t.Fatalf("CreateContexts copies = %d, want 3", opts.Copies)
		}
		return []synapse.UploadContext{
			newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t)),
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 100*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	if legacyUploadCalled.Load() {
		t.Fatal("empty upload payload used legacy upload path")
	}
	got, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID: got=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateUploading || got.StorageUploadID != nil || got.InFilecoin {
		t.Fatalf("version after prepare = state:%s upload:%v in_filecoin:%v, want uploading without FOC binding", got.State, got.StorageUploadID, got.InFilecoin)
	}
	var uploads []model.StorageUpload
	if err := env.db.NewSelect().Model(&uploads).Where("source_version_id = ?", versionID).Scan(context.Background()); err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].Status != model.StorageUploadStatusRunning || uploads[0].RequestedCopies != 3 {
		t.Fatalf("uploads after prepare = %#v, want one running upload with three requested copies", uploads)
	}
	tasks, _, err := env.repos.Tasks.List(context.Background(), string(model.TaskTypeUpload), "", "", 10, 0)
	if err != nil {
		t.Fatalf("list upload tasks: %v", err)
	}
	foundEnsurePrimary := false
	for _, task := range tasks {
		if task.RefVersionID == versionID && strings.Contains(task.IdempotencyKey, "ensure_dataset") && strings.HasSuffix(task.IdempotencyKey, ":0") {
			if task.Stage == nil || *task.Stage != "ensure_dataset" {
				t.Fatalf("ensure dataset task stage = %#v, want ensure_dataset", task.Stage)
			}
			if _, ok := task.Payload["stage"]; ok {
				t.Fatalf("ensure dataset task payload kept stage: %#v", task.Payload)
			}
			foundEnsurePrimary = true
		}
	}
	if !foundEnsurePrimary {
		t.Fatalf("upload tasks = %#v, want primary ensure_dataset task", tasks)
	}
}

func TestUploader_StagedPrepareUsesConfiguredCopyCount(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 4 {
			t.Fatalf("CreateContexts copies = %d, want 4", opts.Copies)
		}
		return []synapse.UploadContext{
			newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(404), sdktypes.NewBigInt(4004), sdktypes.NewBigInt(5001), testCID(t)),
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 4, 1, 100*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	var uploads []model.StorageUpload
	if err := env.db.NewSelect().Model(&uploads).Where("source_version_id = ?", versionID).Scan(ctx); err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].RequestedCopies != 4 {
		t.Fatalf("uploads after prepare = %#v, want one upload with four requested copies", uploads)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, uploads[0].ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 4 {
		t.Fatalf("copy rows = %d, want 4", len(copies))
	}
}

func TestUploader_StagedPrepareUsesBucketCopyOverride(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	ctx := context.Background()
	bucketCopies := 4
	if err := env.repos.Buckets.SetDefaultCopies(ctx, bucket.Name, &bucketCopies); err != nil {
		t.Fatalf("SetDefaultCopies: %v", err)
	}

	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 4 {
			t.Fatalf("CreateContexts copies = %d, want 4", opts.Copies)
		}
		contexts := make([]synapse.UploadContext, 0, opts.Copies)
		for i := 0; i < opts.Copies; i++ {
			contexts = append(contexts, newFakeUploadContext(
				sdktypes.NewBigInt(uint64(100+i)),
				sdktypes.NewBigInt(uint64(1000+i)),
				sdktypes.NewBigInt(uint64(2000+i)),
				testCID(t),
			))
		}
		return contexts, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 100*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	var uploads []model.StorageUpload
	if err := env.db.NewSelect().Model(&uploads).Where("source_version_id = ?", versionID).Scan(ctx); err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].RequestedCopies != 4 {
		t.Fatalf("uploads after prepare = %#v, want one upload with four requested copies", uploads)
	}
}

func TestUploader_StagedPrepareReusesExistingUploadRequestedCopies(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	contextsByDataSet := make(map[string]*fakeUploadContext, 4)
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 4,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	for copyIndex := 0; copyIndex < 4; copyIndex++ {
		providerID := onChainID(t, fmt.Sprintf("%d", 100+copyIndex))
		binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID:          bucket.ID,
			ProviderID:        providerID,
			CopyIndex:         copyIndex,
			CreatedByUploadID: upload.ID,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
		}
		dataSetID := onChainID(t, fmt.Sprintf("%d", 1000+copyIndex))
		if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID:        binding.ID,
			UploadID:  upload.ID,
			DataSetID: dataSetID,
		}); err != nil {
			t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
		}
		storageCtx := newFakeUploadContext(
			providerID.SDK(),
			dataSetID.SDK(),
			sdktypes.NewBigInt(uint64(2000+copyIndex)),
			testCID(t),
		)
		boundDataSetID := dataSetID.SDK()
		storageCtx.boundDataSet = &boundDataSetID
		contextsByDataSet[dataSetID.String()] = storageCtx
		transferMethod := model.StorageCopyTransferMethodPeerPull
		if copyIndex == 0 {
			transferMethod = model.StorageCopyTransferMethodIngress
		}
		if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: binding.ID,
			CopyIndex:        copyIndex,
			TransferMethod:   transferMethod,
			ProviderID:       providerID,
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings(%d): %v", copyIndex, err)
		}
	}

	newBucketCopies := 7
	if err := env.repos.Buckets.SetDefaultCopies(ctx, bucket.Name, &newBucketCopies); err != nil {
		t.Fatalf("SetDefaultCopies after upload: %v", err)
	}
	var createContextsCopies []int
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCopies = append(createContextsCopies, opts.Copies)
		return newFakeUploadContexts(t, opts.Copies, 10000), nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if opts.DataSetID != nil {
			if uploadCtx := contextsByDataSet[opts.DataSetID.String()]; uploadCtx != nil {
				return uploadCtx, nil
			}
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	stage := "prepare_upload"
	retryTask := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:retry", versionID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, retryTask); err != nil {
		t.Fatalf("create retry prepare task: %v", err)
	}
	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 100*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, retryTask.ID, 5*time.Second)

	if len(createContextsCopies) != 0 {
		t.Fatalf("CreateContexts copies after retry = %v, want none", createContextsCopies)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		t.Fatalf("ListCopies after retry: %v", err)
	}
	if len(copies) != 4 {
		t.Fatalf("copy rows after retry = %d, want historical 4", len(copies))
	}
}

func TestUploader_StagedPrepareCapsConfiguredCopyCount(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 8 {
			t.Fatalf("CreateContexts copies = %d, want 8", opts.Copies)
		}
		contexts := make([]synapse.UploadContext, 0, opts.Copies)
		for i := 0; i < opts.Copies; i++ {
			contexts = append(contexts, newFakeUploadContext(
				sdktypes.NewBigInt(uint64(100+i)),
				sdktypes.NewBigInt(uint64(1000+i)),
				sdktypes.NewBigInt(uint64(2000+i)),
				testCID(t),
			))
		}
		return contexts, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 99, 1, 100*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	var uploads []model.StorageUpload
	if err := env.db.NewSelect().Model(&uploads).Where("source_version_id = ?", versionID).Scan(ctx); err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].RequestedCopies != 8 {
		t.Fatalf("uploads after prepare = %#v, want one upload with eight requested copies", uploads)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, uploads[0].ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 8 {
		t.Fatalf("copy rows = %d, want 8", len(copies))
	}
}

func TestUploader_EnsureDatasetUsesExistingResolvedDataset(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:0", versionID, upload.ID),
		Payload: map[string]interface{}{
			"stage":      "ensure_dataset",
			"upload_id":  upload.ID,
			"copy_index": 0,
		},
		Status:      model.TaskStatusQueued,
		MaxRetries:  5,
		ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create ensure task: %v", err)
	}

	var createCalls atomic.Int32
	existingID := sdktypes.NewBigInt(13236)
	providerCtx := newFakeUploadContext(sdktypes.NewBigInt(101), existingID, sdktypes.NewBigInt(2001), testCID(t))
	providerCtx.boundDataSet = &existingID
	providerCtx.createCalls = &createCalls
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return providerCtx, nil
		}
		if createContextDataSetIDEqual(opts, existingID) {
			return providerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	if got := createCalls.Load(); got != 0 {
		t.Fatalf("CreateDataSet calls = %d, want 0 when provider context already resolves a dataset", got)
	}
	gotBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.Status != model.StorageDataSetStatusReady || onChainIDPtrString(gotBinding.DataSetID) != "13236" {
		t.Fatalf("binding after ensure = status:%s dataSet:%v, want ready/13236", gotBinding.Status, gotBinding.DataSetID)
	}
}

func TestUploader_EnsureDatasetContextTimeoutKeepsBindingPendingForRetry(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:0", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create ensure task: %v", err)
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return nil, context.DeadlineExceeded
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	gotBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.Status != model.StorageDataSetStatusPending {
		t.Fatalf("binding status after context timeout = %s, want pending", gotBinding.Status)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%v err=%v", copyRow, err)
	}
	if copyRow.Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("copy status after context timeout = %s, want pending", copyRow.Status)
	}
}

func TestUploader_EnsureDatasetSubmittedProviderUnavailableWaitsWithoutRetry(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:0", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create ensure task: %v", err)
	}

	var createCalls atomic.Int32
	var waitCalls atomic.Int32
	providerCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	providerCtx.createCalls = &createCalls
	providerCtx.waitCalls = &waitCalls
	providerCtx.createErr = &synapse.ProviderUnavailableError{Cause: context.DeadlineExceeded}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return providerCtx, nil
		}
		if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)) {
			return providerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("submitted create task = %#v err=%v, want dependency wait without retry", gotTask, err)
	}

	gotBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex after create error: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.Status != model.StorageDataSetStatusCreating || gotBinding.CreateTransactionID == nil || gotBinding.CreateStatusURL == nil {
		t.Fatalf("binding after submitted create error = %#v, want creating with submission", gotBinding)
	}

	providerCtx.createErr = nil
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule task retry: %v", err)
	}
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	if createCalls.Load() != 1 || waitCalls.Load() != 1 {
		t.Fatalf("create calls=%d wait calls=%d, want create once then wait once", createCalls.Load(), waitCalls.Load())
	}
	gotBinding, err = env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex after retry: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.Status != model.StorageDataSetStatusReady || onChainIDPtrString(gotBinding.DataSetID) != "1001" {
		t.Fatalf("binding after retry = %#v, want ready dataset 1001", gotBinding)
	}
}

func TestUploader_EnsureDatasetCreatingProviderUnavailableWaitsWithoutRetry(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID:              binding.ID,
		UploadID:        upload.ID,
		TransactionID:   "0xcreate3e9",
		StatusURL:       "https://provider.example/status/create",
		ClientDataSetID: onChainIDPtr(t, "11001"),
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:0", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create ensure task: %v", err)
	}

	var createCalls atomic.Int32
	var waitCalls atomic.Int32
	providerCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	providerCtx.createCalls = &createCalls
	providerCtx.waitCalls = &waitCalls
	providerCtx.waitErr = &synapse.ProviderUnavailableError{Cause: context.DeadlineExceeded}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return providerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("creating data set task = %#v err=%v, want dependency wait without retry", gotTask, err)
	}

	gotBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.Status != model.StorageDataSetStatusCreating {
		t.Fatalf("binding status after wait error = %s, want creating", gotBinding.Status)
	}
	if gotBinding.CreateTransactionID == nil || *gotBinding.CreateTransactionID != "0xcreate3e9" {
		t.Fatalf("binding transaction after wait error = %v, want original transaction", gotBinding.CreateTransactionID)
	}
	if createCalls.Load() != 0 || waitCalls.Load() != 1 {
		t.Fatalf("create calls=%d wait calls=%d, want only one wait", createCalls.Load(), waitCalls.Load())
	}
}

func TestUploader_EnsureDatasetCreatingRejectedMarksBindingFailed(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID:              binding.ID,
		UploadID:        upload.ID,
		TransactionID:   "0xcreate3e9",
		StatusURL:       "https://provider.example/status/create",
		ClientDataSetID: onChainIDPtr(t, "11001"),
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:0", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create ensure task: %v", err)
	}

	var createCalls atomic.Int32
	var waitCalls atomic.Int32
	providerCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	providerCtx.createCalls = &createCalls
	providerCtx.waitCalls = &waitCalls
	providerCtx.waitErr = fmt.Errorf("wait rejected: %w", pdp.ErrTxRejected)
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return providerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	gotBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.Status != model.StorageDataSetStatusFailed {
		t.Fatalf("binding status after rejected wait = %s, want failed", gotBinding.Status)
	}
	if createCalls.Load() != 0 || waitCalls.Load() != 1 {
		t.Fatalf("create calls=%d wait calls=%d, want only one wait", createCalls.Load(), waitCalls.Load())
	}

	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule rejected create task: %v", err)
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(workerCtx)
		close(done)
	}()
	waitForTaskStatus(t, env, task.ID, model.TaskStatusWaiting, 5*time.Second)
	cancel()
	waitForSignal(t, done, time.Second, "uploader shutdown")

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask == nil {
		t.Fatalf("GetByID task after retry: task=%#v err=%v", gotTask, err)
	}
	if gotTask.RetryCount != 1 {
		t.Fatalf("retry count after evidence-backed failure = %d, want 1", gotTask.RetryCount)
	}
	if createCalls.Load() != 0 || waitCalls.Load() != 1 {
		t.Fatalf("create calls=%d wait calls=%d after retry, want no repeated service creation", createCalls.Load(), waitCalls.Load())
	}
}

func TestUploader_EnsureDatasetCreationRejectionPreservesEstablishedEvidence(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID:              binding.ID,
		UploadID:        upload.ID,
		TransactionID:   "0xcreate3e9",
		StatusURL:       "https://provider.example/status/create",
		ClientDataSetID: onChainIDPtr(t, "11001"),
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:0", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0},
		Status:         model.TaskStatusQueued,
		MaxRetries:     1,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create ensure task: %v", err)
	}

	providerCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	providerCtx.waitErr = fmt.Errorf("wait rejected: %w", pdp.ErrTxRejected)
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return providerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	gotTask, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if gotTask.Status != model.TaskStatusExhausted {
		t.Fatalf("task status = %s, want exhausted", gotTask.Status)
	}
	gotVersion, _ := env.repos.Objects.GetVersionByID(ctx, versionID)
	if gotVersion.State != model.ObjectStateUploading || gotVersion.StorageUploadID != nil {
		t.Fatalf("version after rejected dataset creation = state:%s upload:%v, want recoverable uploading state", gotVersion.State, gotVersion.StorageUploadID)
	}
	gotUpload, err := env.repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || gotUpload == nil {
		t.Fatalf("GetByID(upload): upload=%v err=%v", gotUpload, err)
	}
	if gotUpload.Status != model.StorageUploadStatusFailed {
		t.Fatalf("upload status = %s, want failed creation attempt retained for diagnosis", gotUpload.Status)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%v err=%v", copyRow, err)
	}
	if copyRow.Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("primary copy status = %s, want failed", copyRow.Status)
	}
	provenance, err := env.repos.Uploads.GetUploadProvenance(ctx, upload.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance: %v", err)
	}
	if provenance == nil || len(provenance.Failures) != 1 {
		t.Fatalf("provenance failures = %#v, want one primary failure", provenance)
	}
	failure := provenance.Failures[0]
	if failure.ProviderID == nil || failure.ProviderID.String() != "101" || failure.TransferMethod != string(model.StorageCopyTransferMethodIngress) || failure.Stage == nil || *failure.Stage != "wait dataset" || failure.ErrorMessage == nil || *failure.ErrorMessage != "wait rejected: pdp: transaction rejected" {
		t.Fatalf("failure = %#v, want provider 101 ingress wait dataset failure", failure)
	}
	gotBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.Status != model.StorageDataSetStatusFailed || gotBinding.CreateTransactionID == nil || gotBinding.ClientDataSetID == nil {
		t.Fatalf("binding after rejected creation = %#v, want failed binding with service evidence retained", gotBinding)
	}
	_, prepareTotal, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", "", 10, 0)
	if err != nil {
		t.Fatalf("List prepare tasks: %v", err)
	}
	if prepareTotal != 0 {
		t.Fatalf("prepare tasks = %d, want no automatic provider replacement", prepareTotal)
	}
}

func TestUploader_PrimaryCommitSubmittedErrorDoesNotFailObject(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("mark committing: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:              primary.ID,
		UploadID:        upload.ID,
		DataSetID:       onChainID(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "9001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	pieceCID := testCID(t)
	if err := env.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID.String(),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	stage := "ingress_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ingress_commit:%d", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     1,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create primary commit task: %v", err)
	}
	primaryCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), pieceCID)
	primaryDataSetID := sdktypes.NewBigInt(1001)
	primaryCtx.boundDataSet = &primaryDataSetID
	primaryCtx.commitErr = errors.New("commit status poll timeout")
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)) {
			return primaryCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(
		env.repos, env.cache, env.storage, nil, env.sm,
		cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default(),
		worker.WithPDPStatusChecker(synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{AllowPrivateNetworks: true})),
	)
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask == nil || gotTask.Status != model.TaskStatusExhausted {
		t.Fatalf("submitted commit task = %#v err=%v, want exhausted without failing content", gotTask, err)
	}

	gotVersion, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || gotVersion == nil {
		t.Fatalf("GetVersionByID after commit error: version=%v err=%v", gotVersion, err)
	}
	if gotVersion.State != model.ObjectStateCommitting || gotVersion.FailedAtState != nil || gotVersion.LastError != nil {
		t.Fatalf("version after submitted commit error = state:%s failed_at:%v last_error:%v, want committing without failure", gotVersion.State, gotVersion.FailedAtState, gotVersion.LastError)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%v err=%v", copyRow, err)
	}
	if copyRow.Status != model.StorageUploadCopyStatusCommitting || copyRow.CommitTransactionID == nil || *copyRow.CommitTransactionID == "" {
		t.Fatalf("primary copy after submitted commit error = %#v, want committing with tx", copyRow)
	}
	if copyRow.CommitExtraDataHex == nil || *copyRow.CommitExtraDataHex == "" {
		t.Fatalf("primary copy commit extra data = %#v, want persisted payload", copyRow.CommitExtraDataHex)
	}

	primaryCtx.commitErr = nil
	statusRequests := atomic.Int32{}
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		statusRequests.Add(1)
		if r.URL.Path != "/pdp/data-sets/1001/pieces/added/"+fakeSubmittedCommitTxHash {
			t.Fatalf("commit status path = %q, want submitted transaction status path", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"piecesAdded":true,"confirmedPieceIds":[2001]}`, fakeSubmittedCommitTxHash)
	}))
	defer statusServer.Close()
	primaryCtx.serviceURL = statusServer.URL
	if err := env.repos.Tasks.RetryExhausted(ctx, task.ID); err != nil {
		t.Fatalf("RetryExhausted: %v", err)
	}
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	primaryCtx.commitMu.Lock()
	commitExtras := append([][]byte(nil), primaryCtx.commitExtras...)
	primaryCtx.commitMu.Unlock()
	if primaryCtx.presignCalls.Load() != 1 {
		t.Fatalf("presign calls = %d, want one persisted extra data payload reused", primaryCtx.presignCalls.Load())
	}
	if primaryCtx.commitCalls.Load() != 1 {
		t.Fatalf("commit calls = %d, want retry to wait on submitted transaction without resubmitting", primaryCtx.commitCalls.Load())
	}
	if statusRequests.Load() != 1 {
		t.Fatalf("status requests = %d, want retry to poll submitted transaction once", statusRequests.Load())
	}
	if len(commitExtras) != 1 || string(commitExtras[0]) != "extra-101" {
		t.Fatalf("commit extras = %q, want only original commit payload", commitExtras)
	}
	copyRow, err = env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy after retry: copy=%v err=%v", copyRow, err)
	}
	if copyRow.Status != model.StorageUploadCopyStatusCommitted {
		t.Fatalf("primary copy after retry = %s, want committed", copyRow.Status)
	}
}

func TestUploader_RejectedSubmittedIngressCommitIsResubmitted(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("mark committing: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:              primary.ID,
		UploadID:        upload.ID,
		DataSetID:       onChainID(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "9001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: primary.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	pieceCID := testCID(t)
	if err := env.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID.String(),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID:            upload.ID,
		CopyIndex:           0,
		CommitExtraDataHex:  "01",
		CommitTransactionID: fakeSubmittedCommitTxHash,
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitting: %v", err)
	}
	stage := "ingress_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ingress_commit:%d", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     2,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	statusRequests := atomic.Int32{}
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		statusRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"rejected","dataSetId":1001,"pieceCount":1,"piecesAdded":false}`, fakeSubmittedCommitTxHash)
	}))
	defer statusServer.Close()
	primaryCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), pieceCID)
	primaryDataSetID := sdktypes.NewBigInt(1001)
	primaryCtx.boundDataSet = &primaryDataSetID
	primaryCtx.serviceURL = statusServer.URL
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, primaryDataSetID) {
			return primaryCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(
		env.repos, env.cache, env.storage, nil, env.sm,
		cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default(),
		worker.WithPDPStatusChecker(synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{AllowPrivateNetworks: true})),
	)
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow.Status != model.StorageUploadCopyStatusPieceReady || copyRow.CommitTransactionID != nil || copyRow.CommitExtraDataHex != nil {
		t.Fatalf("copy after rejected status = %#v err=%v, want resubmittable piece", copyRow, err)
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule rejected commit task: %v", err)
	}
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 1 {
		t.Fatalf("rejected commit task = %#v, want completed after one retry", gotTask)
	}
	if statusRequests.Load() != 1 || primaryCtx.commitCalls.Load() != 1 || primaryCtx.presignCalls.Load() != 1 {
		t.Fatalf("rejected commit recovery calls = status:%d commit:%d presign:%d, want 1/1/1", statusRequests.Load(), primaryCtx.commitCalls.Load(), primaryCtx.presignCalls.Load())
	}
	copyRow, err = env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow.Status != model.StorageUploadCopyStatusCommitted {
		t.Fatalf("copy after rejected commit retry = %#v err=%v, want committed", copyRow, err)
	}
	gotVersion, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || gotVersion == nil || gotVersion.State != model.ObjectStateStored {
		t.Fatalf("version after rejected commit retry = %#v err=%v, want stored", gotVersion, err)
	}
}

func TestUploader_SubmittedPeerMismatchedStatusRemainsRecoverableAfterExhaustion(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	pieceCID := testCID(t)
	if err := env.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     fixture.upload.ID,
		CopyIndex:    1,
		PieceCID:     pieceCID.String(),
		RetrievalURL: "https://peer.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID:            fixture.upload.ID,
		CopyIndex:           1,
		CommitExtraDataHex:  "02",
		CommitTransactionID: fakeSubmittedCommitTxHash,
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitting: %v", err)
	}
	stage := "peer_commit"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: fixture.objID, RefVersionID: fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:peer_commit:%d:1", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued, MaxRetries: 1, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create peer commit task: %v", err)
	}

	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":9999,"pieceCount":1,"piecesAdded":true,"confirmedPieceIds":[302]}`, fakeSubmittedCommitTxHash)
	}))
	defer statusServer.Close()
	peerCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(302), pieceCID)
	peerDataSetID := sdktypes.NewBigInt(2002)
	peerCtx.boundDataSet = &peerDataSetID
	peerCtx.serviceURL = statusServer.URL
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, peerDataSetID) {
			return peerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(
		env.repos, env.cache, env.storage, nil, env.sm,
		cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default(),
		worker.WithPDPStatusChecker(synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{AllowPrivateNetworks: true})),
	)
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusExhausted || gotTask.RetryCount != 1 {
		t.Fatalf("peer commit task = %#v, want exhausted after one bounded attempt", gotTask)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || !copyCommitSubmittedForTest(copyRow) || *copyRow.CommitTransactionID != fakeSubmittedCommitTxHash {
		t.Fatalf("peer copy after mismatched status = %#v err=%v, want recoverable submitted commit", copyRow, err)
	}
	if err := env.repos.Uploads.MarkUploadCopyFailed(ctx, fixture.upload.ID, 1, "late generic failure"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkUploadCopyFailed submitted peer error = %v, want conflict", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, fixture.versionID)
	if err != nil || version == nil || version.State != model.ObjectStateReplicating {
		t.Fatalf("version after submitted peer uncertainty = %#v err=%v, want replicating", version, err)
	}
}

func TestUploader_UnavailablePeerKeepsObjectReadableAndWaitsForInPlaceRepair(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	_ = seedStagedUploadTask(t, env, objID, versionID, 1)

	pieceCID := testCID(t)
	ingress := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), pieceCID)
	peer := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), pieceCID)
	peer.pullErr = &synapse.ProviderUnavailableError{Cause: errors.New("provider pull failed")}
	contextsByProvider := map[string]*fakeUploadContext{
		ingress.providerID.String(): ingress,
		peer.providerID.String():    peer,
	}
	contextsByDataSet := map[string]*fakeUploadContext{
		ingress.dataSetID.String(): ingress,
		peer.dataSetID.String():    peer,
	}
	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		if opts.Copies == 2 {
			return []synapse.UploadContext{ingress, peer}, nil
		}
		t.Fatalf("unexpected CreateContexts copies = %d; established slots must not be replaced", opts.Copies)
		return nil, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if opts.ProviderID != nil {
			return contextsByProvider[opts.ProviderID.String()], nil
		}
		if opts.DataSetID != nil {
			return contextsByDataSet[opts.DataSetID.String()], nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()

	var obj *model.ObjectVersion
	var repairTask *model.Task
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for peer failure to enter in-place repair")
		case <-ticker.C:
			got, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
			if err != nil || got == nil || got.StorageUploadID == nil {
				continue
			}
			upload, err := env.repos.Uploads.GetByID(context.Background(), *got.StorageUploadID)
			if err != nil || upload == nil {
				continue
			}
			copies, err := env.repos.Uploads.ListCopies(context.Background(), upload.ID)
			if err != nil {
				continue
			}
			exhaustedTasks, err := env.repos.Tasks.ListExhausted(context.Background(), 10)
			if err != nil || len(exhaustedTasks) != 0 {
				continue
			}
			repairTasks, _, err := env.repos.Tasks.List(context.Background(), string(model.TaskTypeUpload), "repair_replica", string(model.TaskStatusWaiting), 10, 0)
			if err != nil {
				continue
			}
			for i := range repairTasks {
				if taskPayloadInt64ForTest(repairTasks[i].Payload, "storage_data_set_id") > 0 {
					repairTask = &repairTasks[i]
					break
				}
			}
			if got.State == model.ObjectStateReplicating && upload.Status == model.StorageUploadStatusReadable && len(copies) == 2 && repairTask != nil {
				obj = got
				goto doneWaiting
			}
		}
	}

doneWaiting:
	if obj == nil || obj.StorageUploadID == nil || !obj.InFilecoin {
		t.Fatalf("object after secondary failure = %#v, want readable replicating version", obj)
	}
	copies, err := env.repos.Uploads.ListCopies(context.Background(), *obj.StorageUploadID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 2 || copies[0].Status != model.StorageUploadCopyStatusCommitted || copies[1].Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("copies after peer failure = %#v, want committed ingress and pending original peer", copies)
	}
	if repairTask.RefType != "bucket" || taskPayloadInt64ForTest(repairTask.Payload, "storage_upload_copy_id") != copies[1].ID {
		t.Fatalf("repair task = %#v, want exact original peer copy %d", repairTask, copies[1].ID)
	}
	binding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(context.Background(), obj.BucketID, 1)
	if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusUnavailable {
		t.Fatalf("peer binding after outage = %#v err=%v, want unavailable", binding, err)
	}
	evict, err := env.repos.Tasks.ClaimReady(context.Background(), model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady(evict): %v", err)
	}
	if evict != nil {
		t.Fatalf("unexpected evict task for partial upload: %#v", evict)
	}
	peer.pullErr = nil
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", repairTask.ID).
		Exec(context.Background()); err != nil {
		t.Fatalf("reschedule replica repair: %v", err)
	}
	waitForObjectState(t, env, versionID, model.ObjectStateStored, 5*time.Second)
	repairedCopy, err := env.repos.Uploads.GetUploadCopy(context.Background(), *obj.StorageUploadID, 1)
	if err != nil || repairedCopy == nil || repairedCopy.Status != model.StorageUploadCopyStatusCommitted {
		t.Fatalf("repaired copy = %#v err=%v, want committed original slot", repairedCopy, err)
	}
	recoveredBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(context.Background(), obj.BucketID, 1)
	if err != nil || recoveredBinding == nil || recoveredBinding.Status != model.StorageDataSetStatusReady {
		t.Fatalf("recovered binding = %#v err=%v, want ready", recoveredBinding, err)
	}
	if got := createContextsCalls.Load(); got != 1 {
		t.Fatalf("CreateContexts calls = %d, want only initial slot provisioning", got)
	}
}

func TestUploader_ReplicaRepairDoesNotRecoverDataSetBeforeStorageOperationSucceeds(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	peerCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), testCID(t))
	dataSetID := sdktypes.NewBigInt(2002)
	peerCtx.boundDataSet = &dataSetID
	peerCtx.pullErr = errors.New("unexpected pull response")
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, dataSetID) {
			return peerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.upload.BucketID,
		RefVersionID: fixture.versionID, IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload: map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": copyRow.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask.Status != model.TaskStatusScheduled || gotTask.RetryCount != 1 {
		t.Fatalf("repair task = %#v err=%v, want bounded retry", gotTask, err)
	}
	binding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.peer.ID)
	if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusUnavailable {
		t.Fatalf("binding before successful storage operation = %#v err=%v, want unavailable", binding, err)
	}
	gotCopy, err := env.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
	if err != nil || gotCopy == nil || gotCopy.Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("copy after failed repair operation = %#v err=%v, want pending", gotCopy, err)
	}
}

func TestUploader_ReplicaRepairResubmitsRejectedCommit(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	pieceCID := testCID(t)
	if err := env.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     fixture.upload.ID,
		CopyIndex:    1,
		PieceCID:     pieceCID.String(),
		RetrievalURL: "https://peer.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID:            fixture.upload.ID,
		CopyIndex:           1,
		CommitExtraDataHex:  "02",
		CommitTransactionID: fakeSubmittedCommitTxHash,
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitting: %v", err)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.upload.BucketID,
		RefVersionID: fixture.versionID, IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload: map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": copyRow.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 2, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}

	statusRequests := atomic.Int32{}
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		statusRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"rejected","dataSetId":2002,"pieceCount":1,"piecesAdded":false}`, fakeSubmittedCommitTxHash)
	}))
	defer statusServer.Close()
	peerCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(302), pieceCID)
	peerDataSetID := sdktypes.NewBigInt(2002)
	peerCtx.boundDataSet = &peerDataSetID
	peerCtx.serviceURL = statusServer.URL
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, peerDataSetID) {
			return peerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(
		env.repos, env.cache, env.storage, nil, env.sm,
		cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default(),
		worker.WithPDPStatusChecker(synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{AllowPrivateNetworks: true})),
	)
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)
	copyRow, err = env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageUploadCopyStatusPieceReady || copyRow.CommitTransactionID != nil || copyRow.CommitExtraDataHex != nil {
		t.Fatalf("repair copy after rejected commit = %#v err=%v, want resubmittable piece", copyRow, err)
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule repair task: %v", err)
	}
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted {
		t.Fatalf("repair task = %#v, want completed after rejected commit retry", gotTask)
	}
	if statusRequests.Load() != 1 || peerCtx.commitCalls.Load() != 1 {
		t.Fatalf("repair recovery calls = status:%d commit:%d, want 1/1", statusRequests.Load(), peerCtx.commitCalls.Load())
	}
	copyRow, err = env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageUploadCopyStatusCommitted {
		t.Fatalf("repair copy after retry = %#v err=%v, want committed", copyRow, err)
	}
	binding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.peer.ID)
	if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusReady {
		t.Fatalf("data set after rejected repair retry = %#v err=%v, want ready", binding, err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, fixture.versionID)
	if err != nil || version == nil || version.State != model.ObjectStateStored {
		t.Fatalf("version after rejected repair retry = %#v err=%v, want stored", version, err)
	}
}

func TestUploader_ReplicaRepairResumesAfterCopyCommitBeforeDataSetRecovery(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copyRow.ID,
		UploadID:            fixture.upload.ID,
		CopyIndex:           1,
		PieceCID:            testCID(t).String(),
		PieceID:             onChainIDPtr(t, "302"),
		RetrievalURL:        "https://peer.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.upload.BucketID,
		IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload:        map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": copyRow.ID},
		Status:         model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	peerBinding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.peer.ID)
	if err != nil || peerBinding == nil || peerBinding.DataSetID == nil {
		t.Fatalf("GetDataSetBindingByID: binding=%#v err=%v", peerBinding, err)
	}
	peerCtx := newFakeUploadContext(peerBinding.ProviderID.SDK(), peerBinding.DataSetID.SDK(), sdktypes.NewBigInt(302), testCID(t))
	boundDataSetID := peerBinding.DataSetID.SDK()
	peerCtx.boundDataSet = &boundDataSetID
	var contextCalls atomic.Int32
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		contextCalls.Add(1)
		if !createContextDataSetIDEqual(opts, boundDataSetID) {
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
		return peerCtx, nil
	}
	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
		t.Fatalf("repair task = %#v, want idempotent completion", gotTask)
	}
	binding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.peer.ID)
	if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusReady {
		t.Fatalf("recovered data set = %#v err=%v, want ready", binding, err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, fixture.versionID)
	if err != nil || version == nil || version.State != model.ObjectStateStored {
		t.Fatalf("finalized version = %#v err=%v, want stored", version, err)
	}
	if contextCalls.Load() != 1 || peerCtx.storeCalls.Load() != 0 || peerCtx.commitCalls.Load() != 0 {
		t.Fatalf("committed resume calls: context=%d store=%d commit=%d, want one context validation without rewriting", contextCalls.Load(), peerCtx.storeCalls.Load(), peerCtx.commitCalls.Load())
	}
}

func TestUploader_DataSetRecoveryFinalizesOtherCommittedUploads(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	ingress, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, fixture.upload.BucketID, 0)
	if err != nil || ingress == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex ingress: binding=%#v err=%v", ingress, err)
	}
	otherUpload, otherVersionID := seedCommittedReplicatingUploadOnBindings(t, env, fixture.upload.BucketID, ingress, fixture.peer)

	currentCopy, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || currentCopy == nil {
		t.Fatalf("GetUploadCopy current: copy=%#v err=%v", currentCopy, err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: currentCopy.ID,
		UploadID:            fixture.upload.ID,
		CopyIndex:           1,
		PieceCID:            testCID(t).String(),
		PieceID:             onChainIDPtr(t, "302"),
		RetrievalURL:        "https://peer.example/current-piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted current: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.upload.BucketID,
		RefVersionID: fixture.versionID, IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload: map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": currentCopy.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	peerDataSetID := sdktypes.NewBigInt(2002)
	peerCtx := newFakeUploadContext(sdktypes.NewBigInt(202), peerDataSetID, sdktypes.NewBigInt(302), testCID(t))
	peerCtx.boundDataSet = &peerDataSetID
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, peerDataSetID) {
			return peerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
		t.Fatalf("repair task = %#v, want one coordinator to finish every affected upload", gotTask)
	}
	for _, versionID := range []string{fixture.versionID, otherVersionID} {
		version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || version == nil || version.State != model.ObjectStateStored {
			t.Fatalf("version %s after data set recovery = %#v err=%v, want stored", versionID, version, err)
		}
	}
	other, err := env.repos.Uploads.GetByID(ctx, otherUpload.ID)
	if err != nil || other == nil || other.Status != model.StorageUploadStatusComplete {
		t.Fatalf("other upload after data set recovery = %#v err=%v, want complete", other, err)
	}
}

func TestUploader_ReplicaRepairFinalizesCommittedCopyWhenDataSetStartsDraining(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	if _, err := env.db.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER drain_repair_data_set_after_commit
		AFTER UPDATE OF status ON storage_upload_copies
		WHEN NEW.id = %d AND NEW.status = '%s'
		BEGIN
			UPDATE storage_data_sets
			SET status = '%s', updated_at = CURRENT_TIMESTAMP
			WHERE id = %d;
		END`, copyRow.ID, model.StorageUploadCopyStatusCommitted, model.StorageDataSetStatusDraining, fixture.peer.ID)); err != nil {
		t.Fatalf("create draining race trigger: %v", err)
	}

	peerBinding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.peer.ID)
	if err != nil || peerBinding == nil || peerBinding.DataSetID == nil {
		t.Fatalf("GetDataSetBindingByID: binding=%#v err=%v", peerBinding, err)
	}
	peerCtx := newFakeUploadContext(peerBinding.ProviderID.SDK(), peerBinding.DataSetID.SDK(), sdktypes.NewBigInt(302), testCID(t))
	boundDataSetID := peerBinding.DataSetID.SDK()
	peerCtx.boundDataSet = &boundDataSetID
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if !createContextDataSetIDEqual(opts, boundDataSetID) {
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
		return peerCtx, nil
	}
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.upload.BucketID,
		IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload:        map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": copyRow.ID},
		Status:         model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
		t.Fatalf("repair task = %#v, want completed without retry", gotTask)
	}
	binding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.peer.ID)
	if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusDraining {
		t.Fatalf("data set after concurrent transition = %#v err=%v, want draining", binding, err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, fixture.versionID)
	if err != nil || version == nil || version.State != model.ObjectStateStored {
		t.Fatalf("finalized version = %#v err=%v, want stored", version, err)
	}
}

func TestUploader_CommittedReplicaRepairWaitsForLiveProviderEvidence(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copyRow.ID,
		UploadID:            fixture.upload.ID,
		CopyIndex:           1,
		PieceCID:            testCID(t).String(),
		PieceID:             onChainIDPtr(t, "302"),
		RetrievalURL:        "https://peer.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.upload.BucketID,
		IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload:        map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": copyRow.ID},
		Status:         model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	env.storage.CreateContextFunc = func(_ context.Context, _ *storage.CreateContextOptions) (synapse.UploadContext, error) {
		return nil, &synapse.ProviderUnavailableError{Cause: context.DeadlineExceeded}
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("repair task = %#v err=%v, want dependency wait without retry", gotTask, err)
	}
	binding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.peer.ID)
	if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusUnavailable {
		t.Fatalf("data set without live evidence = %#v err=%v, want unavailable", binding, err)
	}
}

func TestUploader_ReplicaRepairUsesRetainedCacheWhenNoRemoteCopyIsReadable(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, _, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark version uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%#v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  upload.ID,
		DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, binding.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: bucket.ID,
		RefVersionID: versionID, IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", binding.ID),
		Payload: map[string]interface{}{"storage_data_set_id": binding.ID, "storage_upload_copy_id": copyRow.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	repairedCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	dataSetID := sdktypes.NewBigInt(1001)
	repairedCtx.boundDataSet = &dataSetID
	var createContextCalls atomic.Int32
	var createContextsCalls atomic.Int32
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		createContextCalls.Add(1)
		if !createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) || !createContextDataSetIDEqual(opts, dataSetID) {
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
		return repairedCtx, nil
	}
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		return nil, errors.New("replica repair must not select a new provider")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
		t.Fatalf("repair task = %#v, want completed without retry", gotTask)
	}
	if got := createContextCalls.Load(); got != 1 {
		t.Fatalf("CreateContext calls = %d, want one exact original context", got)
	}
	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls = %d, want no provider selection", got)
	}
	if got := repairedCtx.storeCalls.Load(); got != 1 {
		t.Fatalf("Store calls = %d, want one retained-cache upload", got)
	}
	if got := repairedCtx.pullCalls.Load(); got != 0 {
		t.Fatalf("Pull calls = %d, want no remote source pull", got)
	}
	repairedCopy, err := env.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
	if err != nil || repairedCopy == nil || repairedCopy.Status != model.StorageUploadCopyStatusCommitted {
		t.Fatalf("repaired copy = %#v err=%v, want committed original copy", repairedCopy, err)
	}
	recoveredBinding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || recoveredBinding == nil || recoveredBinding.Status != model.StorageDataSetStatusReady {
		t.Fatalf("recovered binding = %#v err=%v, want ready", recoveredBinding, err)
	}
	storedVersion, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || storedVersion == nil || storedVersion.State != model.ObjectStateStored {
		t.Fatalf("stored version = %#v err=%v, want stored", storedVersion, err)
	}
}

func TestUploader_PeerTransientFailureKeepsCopyRetryable(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()

	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	ingress, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("ingress binding: %v", err)
	}
	peer, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("peer binding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: ingress.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("MarkDataSetReady ingress: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: peer.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002")}); err != nil {
		t.Fatalf("MarkDataSetReady peer: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: ingress.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: peer.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     testCID(t).String(),
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://ingress.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := env.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}

	peerCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), testCID(t))
	peerDataSetID := sdktypes.NewBigInt(2002)
	peerCtx.boundDataSet = &peerDataSetID
	peerCtx.pullErr = errors.New("temporary provider pull failed")
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(2002)) {
			return peerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	stage := "peer_pull"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:peer_pull:%d:1", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create peer task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 1)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy peer: copy=%v err=%v", copyRow, err)
	}
	if copyRow.Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("peer copy status = %s, want pending while task remains retryable", copyRow.Status)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 2 {
		t.Fatalf("copy count = %d, want no replacement before retry exhaustion", len(copies))
	}
}

type readableUploadWithPendingPeerFixture struct {
	objID     int64
	versionID string
	upload    *model.StorageUpload
	peer      *model.StorageDataSet
}

func seedReadableUploadWithPendingPeer(t *testing.T, env *testWorkerEnv) readableUploadWithPendingPeerFixture {
	t.Helper()
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()

	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	ingress, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("ingress binding: %v", err)
	}
	peer, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("peer binding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: ingress.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("MarkDataSetReady ingress: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: peer.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002")}); err != nil {
		t.Fatalf("MarkDataSetReady peer: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: ingress.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: peer.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     testCID(t).String(),
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://ingress.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := env.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}
	return readableUploadWithPendingPeerFixture{
		objID:     objID,
		versionID: versionID,
		upload:    upload,
		peer:      peer,
	}
}

func seedCommittedReplicatingUploadOnBindings(
	t *testing.T,
	env *testWorkerEnv,
	bucketID int64,
	ingress *model.StorageDataSet,
	peer *model.StorageDataSet,
) (*model.StorageUpload, string) {
	t.Helper()
	ctx := context.Background()
	versionID := model.NewVersionID()
	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucketID,
		Key:         "other.txt",
		Size:        12,
		ETag:        "other-etag",
		Checksum:    "other-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/" + versionID,
	}
	if _, err := env.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(other): %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark other upload uploading: %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("mark other upload committing: %v", err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucketID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt(other): %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: ingress.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: ingress.ProviderID},
		{StorageDataSetID: peer.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: peer.ProviderID},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(other): %v", err)
	}
	pieceCID := testCID(t).String()
	for copyIndex, input := range []struct {
		pieceID      string
		retrievalURL string
	}{
		{pieceID: "401", retrievalURL: "https://ingress.example/other-piece"},
		{pieceID: "402", retrievalURL: "https://peer.example/other-piece"},
	} {
		if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID:     upload.ID,
			CopyIndex:    copyIndex,
			PieceCID:     pieceCID,
			PieceID:      onChainIDPtr(t, input.pieceID),
			RetrievalURL: input.retrievalURL,
		}); err != nil {
			t.Fatalf("MarkUploadCopyCommitted(other %d): %v", copyIndex, err)
		}
	}
	if _, err := env.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent(other): %v", err)
	}
	return upload, versionID
}

func TestUploader_PeerUsesRetainedCacheWhenReadableProviderBecomesUnavailable(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	source, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, fixture.upload.BucketID, 0)
	if err != nil || source == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex source: binding=%#v err=%v", source, err)
	}
	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, source.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable source: %v", err)
	}

	pieceCID := testCID(t)
	peerCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3002), pieceCID)
	peerDataSetID := sdktypes.NewBigInt(2002)
	peerCtx.boundDataSet = &peerDataSetID
	env.storage.CreateContextsFunc = func(context.Context, *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return nil, errors.New("existing replica slots must not select another provider")
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(202)) && createContextDataSetIDEqual(opts, sdktypes.NewBigInt(2002)) {
			return peerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	stage := "peer_pull"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: fixture.objID, RefVersionID: fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:peer_pull:%d:1", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create peer task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()

	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for cache-backed peer commit")
		case <-ticker.C:
			copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
			if err == nil && copyRow != nil && copyRow.Status == model.StorageUploadCopyStatusCommitted {
				goto committed
			}
		}
	}

committed:
	if peerCtx.storeCalls.Load() != 1 || peerCtx.pullCalls.Load() != 0 {
		t.Fatalf("peer transfer calls = store:%d pull:%d, want cache store only", peerCtx.storeCalls.Load(), peerCtx.pullCalls.Load())
	}
	exhausted, err := env.repos.Tasks.ListExhausted(ctx, 10)
	if err != nil {
		t.Fatalf("ListExhausted: %v", err)
	}
	if len(exhausted) != 0 {
		t.Fatalf("exhausted tasks = %#v, want none", exhausted)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, fixture.versionID)
	if err != nil || version == nil || version.State != model.ObjectStateReplicating {
		t.Fatalf("version after one available replica = %#v err=%v, want strict replicating", version, err)
	}
}

func TestUploader_QueuedPeerStageHandsUnavailableDataSetToInPlaceRepair(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)

	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "provider dataset retired"); err != nil {
		t.Fatalf("MarkDataSetUnavailable peer: %v", err)
	}

	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		return nil, errors.New("established slots must not select another provider")
	}

	stage := "peer_pull"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          fixture.objID,
		RefVersionID:   fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:peer_pull:%d:1", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create ensure task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID(task): %v", err)
	}
	if gotTask.Status != model.TaskStatusCompleted {
		t.Fatalf("ensure task status = %s, want completed", gotTask.Status)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 2 || copies[1].CopyIndex != 1 || copies[1].Status != model.StorageUploadCopyStatusPending || copies[1].StorageDataSetID == nil || *copies[1].StorageDataSetID != fixture.peer.ID {
		t.Fatalf("copies after unavailable peer ensure = %#v, want original pending peer copy", copies)
	}
	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls = %d, want no provider replacement", got)
	}
	repairTasks, total, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
	if err != nil {
		t.Fatalf("List repair tasks: %v", err)
	}
	if total != 1 || len(repairTasks) != 1 || repairTasks[0].RefVersionID != fixture.versionID || taskPayloadInt64ForTest(repairTasks[0].Payload, "storage_data_set_id") != fixture.peer.ID || taskPayloadInt64ForTest(repairTasks[0].Payload, "storage_upload_copy_id") != copies[1].ID {
		t.Fatalf("repair tasks = %#v, want one exact in-place repair", repairTasks)
	}
}

func TestUploader_ReplicaRepairWaitsForRunningPeerOperation(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, fixture.peer.CopyIndex)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}

	dataSetID := sdktypes.NewBigInt(2002)
	peerCtx := newFakeUploadContext(sdktypes.NewBigInt(202), dataSetID, sdktypes.NewBigInt(3002), testCID(t))
	peerCtx.boundDataSet = &dataSetID
	peerCtx.pullEntered = make(chan struct{})
	peerCtx.releasePull = make(chan struct{})
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if !createContextDataSetIDEqual(opts, dataSetID) {
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
		return peerCtx, nil
	}

	peerStage := "peer_pull"
	peerTask := &model.Task{
		Type: model.TaskTypeUpload, Stage: &peerStage, RefType: "object", RefID: fixture.objID, RefVersionID: fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:peer_pull:%d:1", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": fixture.peer.CopyIndex, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, peerTask); err != nil {
		t.Fatalf("Create peer task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 2, 10*time.Millisecond, slog.Default())
	workerCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(workerCtx)
		close(done)
	}()
	released := false
	defer func() {
		if !released {
			close(peerCtx.releasePull)
		}
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()
	waitForSignal(t, peerCtx.pullEntered, 5*time.Second, "ordinary peer pull")

	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	repairStage := "repair_replica"
	repairTask := &model.Task{
		Type: model.TaskTypeUpload, Stage: &repairStage, RefType: "bucket", RefID: fixture.upload.BucketID,
		RefVersionID: fixture.versionID, IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload: map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": copyRow.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, repairTask); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	waitForTaskStatus(t, env, repairTask.ID, model.TaskStatusWaiting, 5*time.Second)
	if got := peerCtx.pullCalls.Load(); got != 1 {
		t.Fatalf("pull calls while ordinary task is running = %d, want 1", got)
	}

	close(peerCtx.releasePull)
	released = true
	waitForTaskStatus(t, env, peerTask.ID, model.TaskStatusCompleted, 5*time.Second)
	peerCommitDeadline := time.After(5 * time.Second)
	peerCommitTicker := time.NewTicker(10 * time.Millisecond)
	defer peerCommitTicker.Stop()
	for {
		select {
		case <-peerCommitDeadline:
			t.Fatal("timed out waiting for peer commit handoff to finish")
		case <-peerCommitTicker.C:
			_, total, listErr := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "peer_commit", string(model.TaskStatusCompleted), 10, 0)
			if listErr == nil && total == 1 {
				goto peerCommitFinished
			}
		}
	}

peerCommitFinished:
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", repairTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule replica repair: %v", err)
	}
	waitForTaskStatus(t, env, repairTask.ID, model.TaskStatusCompleted, 5*time.Second)

	if got := peerCtx.pullCalls.Load(); got != 1 {
		t.Fatalf("total pull calls = %d, want one transfer for the concrete copy", got)
	}
	if got := peerCtx.commitCalls.Load(); got != 1 {
		t.Fatalf("total commit calls = %d, want one commit for the concrete copy", got)
	}
}

func TestUploader_PeerOperationDefersWhileRecoveredDataSetHasActiveRepair(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, fixture.peer.CopyIndex)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	repairStage := "repair_replica"
	repairTask := &model.Task{
		Type: model.TaskTypeUpload, Stage: &repairStage, RefType: "bucket", RefID: fixture.upload.BucketID,
		RefVersionID: fixture.versionID, IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", fixture.peer.ID),
		Payload: map[string]interface{}{"storage_data_set_id": fixture.peer.ID, "storage_upload_copy_id": copyRow.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, repairTask); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	claimedRepair, err := env.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Hour)
	if err != nil || claimedRepair == nil || claimedRepair.ID != repairTask.ID {
		t.Fatalf("ClaimReady repair: task=%#v err=%v", claimedRepair, err)
	}
	peerStage := "peer_pull"
	peerTask := &model.Task{
		Type: model.TaskTypeUpload, Stage: &peerStage, RefType: "object", RefID: fixture.objID, RefVersionID: fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:peer_pull:%d:1", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": fixture.peer.CopyIndex, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, peerTask); err != nil {
		t.Fatalf("Create peer task: %v", err)
	}
	var createContextCalls atomic.Int32
	env.storage.CreateContextFunc = func(context.Context, *storage.CreateContextOptions) (synapse.UploadContext, error) {
		createContextCalls.Add(1)
		return nil, errors.New("ordinary peer operation must defer to active repair")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, peerTask.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, peerTask.ID)
	if err != nil || gotTask == nil || gotTask.RetryCount != 0 {
		t.Fatalf("deferred peer task = %#v err=%v, want dependency wait without retry", gotTask, err)
	}
	if got := createContextCalls.Load(); got != 0 {
		t.Fatalf("CreateContext calls = %d, want no competing storage operation", got)
	}
}

func TestUploader_IngressOperationDefersWhileDataSetHasActiveRepair(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	upload, ingressTask, _ := seedReadyPrimaryStoreTask(t, env)
	binding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, upload.BucketID, 0)
	if err != nil || binding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: binding=%#v err=%v", binding, err)
	}
	copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%#v err=%v", copyRow, err)
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(time.Hour)).
		Where("id = ?", ingressTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("defer ingress task claim: %v", err)
	}
	repairStage := "repair_replica"
	repairTask := &model.Task{
		Type: model.TaskTypeUpload, Stage: &repairStage, RefType: "bucket", RefID: upload.BucketID,
		RefVersionID: upload.SourceVersionID, IdempotencyKey: fmt.Sprintf("upload:repair-data-set:%d", binding.ID),
		Payload: map[string]interface{}{"storage_data_set_id": binding.ID, "storage_upload_copy_id": copyRow.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, repairTask); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	claimedRepair, err := env.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Hour)
	if err != nil || claimedRepair == nil || claimedRepair.ID != repairTask.ID {
		t.Fatalf("ClaimReady repair: task=%#v err=%v", claimedRepair, err)
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", ingressTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("release ingress task claim: %v", err)
	}
	var createContextCalls atomic.Int32
	env.storage.CreateContextFunc = func(context.Context, *storage.CreateContextOptions) (synapse.UploadContext, error) {
		createContextCalls.Add(1)
		return nil, errors.New("ordinary ingress operation must defer to active repair")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 3, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, ingressTask.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, ingressTask.ID)
	if err != nil || gotTask == nil || gotTask.RetryCount != 0 {
		t.Fatalf("deferred ingress task = %#v err=%v, want dependency wait without retry", gotTask, err)
	}
	if got := createContextCalls.Load(); got != 0 {
		t.Fatalf("CreateContext calls = %d, want no competing storage operation", got)
	}
}

func TestUploader_RepairPreparePreservesAssignedPeerSlots(t *testing.T) {
	cases := []struct {
		name       string
		mark       func(context.Context, *testing.T, *testWorkerEnv, readableUploadWithPendingPeerFixture)
		wantRepair bool
	}{
		{
			name: "failed copy",
			mark: func(ctx context.Context, t *testing.T, env *testWorkerEnv, fixture readableUploadWithPendingPeerFixture) {
				t.Helper()
				if err := env.repos.Uploads.MarkUploadCopyFailed(ctx, fixture.upload.ID, 1, "peer pull: provider failed"); err != nil {
					t.Fatalf("MarkUploadCopyFailed: %v", err)
				}
			},
		},
		{
			name:       "unavailable dataset",
			wantRepair: true,
			mark: func(ctx context.Context, t *testing.T, env *testWorkerEnv, fixture readableUploadWithPendingPeerFixture) {
				t.Helper()
				if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "provider dataset retired"); err != nil {
					t.Fatalf("MarkDataSetUnavailable peer: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestWorkerEnv(t)
			ctx := context.Background()
			fixture := seedReadableUploadWithPendingPeer(t, env)
			tc.mark(ctx, t, env, fixture)

			var createContextsCalls atomic.Int32
			env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
				createContextsCalls.Add(1)
				return nil, errors.New("assigned slots must not select another provider")
			}

			stage := "prepare_upload"
			task := &model.Task{
				Type:           model.TaskTypeUpload,
				Stage:          &stage,
				RefType:        "object",
				RefID:          fixture.objID,
				RefVersionID:   fixture.versionID,
				IdempotencyKey: fmt.Sprintf("upload:%s:prepare_upload:%d:repair", fixture.versionID, fixture.upload.ID),
				Payload:        map[string]interface{}{"upload_id": fixture.upload.ID},
				Status:         model.TaskStatusQueued,
				MaxRetries:     5,
				ScheduledAt:    time.Now(),
			}
			if err := env.repos.Tasks.Create(ctx, task); err != nil {
				t.Fatalf("Create repair task: %v", err)
			}

			uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 10*time.Millisecond, slog.Default())
			runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

			gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
			if err != nil {
				t.Fatalf("GetByID(task): %v", err)
			}
			if gotTask.Status != model.TaskStatusCompleted {
				t.Fatalf("repair task status = %s, want completed", gotTask.Status)
			}
			copies, err := env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
			if err != nil {
				t.Fatalf("ListCopies: %v", err)
			}
			if len(copies) != 2 || copies[1].CopyIndex != 1 {
				t.Fatalf("copies after repair prepare = %#v, want original assigned slots only", copies)
			}
			if got := createContextsCalls.Load(); got != 0 {
				t.Fatalf("CreateContexts calls = %d, want no provider replacement", got)
			}
			repairTasks, total, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
			if err != nil {
				t.Fatalf("List repair tasks: %v", err)
			}
			if tc.wantRepair {
				if total != 1 || len(repairTasks) != 1 || taskPayloadInt64ForTest(repairTasks[0].Payload, "storage_data_set_id") != fixture.peer.ID {
					t.Fatalf("repair tasks = %#v, want one in-place repair", repairTasks)
				}
			} else if total != 0 {
				t.Fatalf("repair tasks = %#v, want failed copy to remain operator-visible", repairTasks)
			}
		})
	}
}

func TestUploader_FailedNewPeerDataSetWithServiceEvidenceIsNotReselected(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	if _, err := env.db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("requested_copies = ?", 3).
		Where("id = ?", fixture.upload.ID).
		Exec(ctx); err != nil {
		t.Fatalf("set requested copies: %v", err)
	}
	failedProviderID := onChainID(t, "303")
	failedBinding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          fixture.upload.BucketID,
		ProviderID:        failedProviderID,
		CopyIndex:         2,
		CreatedByUploadID: fixture.upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding failed candidate: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID:              failedBinding.ID,
		UploadID:        fixture.upload.ID,
		TransactionID:   "0xcreate303",
		StatusURL:       "https://provider-303.example/status/create",
		ClientDataSetID: onChainIDPtr(t, "10303"),
	}); err != nil {
		t.Fatalf("MarkDataSetCreating failed candidate: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, fixture.upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: failedBinding.ID,
		CopyIndex:        2,
		TransferMethod:   model.StorageCopyTransferMethodPeerPull,
		ProviderID:       failedProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings failed candidate: %v", err)
	}

	failedCtx := newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t))
	failedCtx.waitErr = fmt.Errorf("wait rejected: %w", pdp.ErrTxRejected)
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, failedCtx.providerID) {
			return failedCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		return nil, errors.New("service evidence must prevent provider reselection")
	}

	stage := "ensure_dataset"
	ensureTask := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          fixture.objID,
		RefVersionID:   fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:2", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 2, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     1,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, ensureTask); err != nil {
		t.Fatalf("Create ensure task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, ensureTask.ID, 5*time.Second)

	repairTasks, repairTotal, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", "", 10, 0)
	if err != nil {
		t.Fatalf("list repair tasks: %v", err)
	}
	if repairTotal != 0 || len(repairTasks) != 0 {
		t.Fatalf("repair tasks = %#v, want no automatic provider replacement", repairTasks)
	}
	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls = %d, want 0", got)
	}
	retained, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, fixture.upload.BucketID, 2)
	if err != nil || retained == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: binding=%v err=%v", retained, err)
	}
	if retained.ProviderID.String() != failedProviderID.String() || retained.Status != model.StorageDataSetStatusFailed || retained.CreateTransactionID == nil || retained.ClientDataSetID == nil {
		t.Fatalf("retained binding = %#v, want failed provider 303 with service evidence", retained)
	}

	copies, err := env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 3 || copies[2].CopyIndex != 2 || copies[2].ProviderID == nil || copies[2].ProviderID.String() != failedProviderID.String() || copies[2].Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("copies after rejection = %#v, want failed original provider copy retained", copies)
	}
	provenance, err := env.repos.Uploads.GetUploadProvenance(ctx, fixture.upload.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance: %v", err)
	}
	if len(provenance.Failures) != 1 || provenance.Failures[0].ProviderID == nil || provenance.Failures[0].ProviderID.String() != failedProviderID.String() {
		t.Fatalf("provenance failures = %#v, want failed provider attempt retained", provenance.Failures)
	}
}

func TestUploader_RepairReusesAuthorizedBindingWithoutUploadCopy(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	if _, err := env.db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("requested_copies = ?", 3).
		Where("id = ?", fixture.upload.ID).
		Exec(ctx); err != nil {
		t.Fatalf("set requested copies: %v", err)
	}

	replacementProviderID := onChainID(t, "404")
	if _, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          fixture.upload.BucketID,
		ProviderID:        replacementProviderID,
		CopyIndex:         2,
		CreatedByUploadID: fixture.upload.ID,
	}); err != nil {
		t.Fatalf("EnsureDataSetBinding replacement: %v", err)
	}

	replacement := newFakeUploadContext(sdktypes.NewBigInt(404), sdktypes.NewBigInt(4004), sdktypes.NewBigInt(5001), testCID(t))
	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		return []synapse.UploadContext{replacement}, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, replacement.providerID) {
			return replacement, nil
		}
		if createContextDataSetIDEqual(opts, replacement.dataSetID) {
			return replacement, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	stage := "prepare_upload"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          fixture.objID,
		RefVersionID:   fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:prepare_upload:%d:repair", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls = %d, want 0 when an authorized slot binding exists", got)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 3 || copies[2].CopyIndex != 2 || copies[2].ProviderID == nil || copies[2].ProviderID.String() != "404" {
		t.Fatalf("copies after repair = %#v, want reused replacement copy index 2 from provider 404", copies)
	}
}

func TestUploader_FailedCandidateWithoutServiceEvidenceCanBeReselected(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)
	failedProviderID := onChainID(t, "303")
	failedBinding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          fixture.upload.BucketID,
		ProviderID:        failedProviderID,
		CopyIndex:         2,
		CreatedByUploadID: fixture.upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding failed candidate: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, fixture.upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: failedBinding.ID,
		CopyIndex:        2,
		TransferMethod:   model.StorageCopyTransferMethodPeerPull,
		ProviderID:       failedProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings failed candidate: %v", err)
	}

	prepareStage := "prepare_upload"
	completedAt := time.Now()
	completedRepair := &model.Task{
		Type: model.TaskTypeUpload, Stage: &prepareStage, RefType: "object", RefID: fixture.objID, RefVersionID: fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:prepare_upload:%d:repair", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID},
		Status:         model.TaskStatusCompleted, MaxRetries: 5, ScheduledAt: completedAt, CompletedAt: &completedAt,
	}
	if err := env.repos.Tasks.Create(ctx, completedRepair); err != nil {
		t.Fatalf("Create completed repair task: %v", err)
	}

	baseUploads := env.repos.Uploads
	env.repos.Uploads = &appendFailureUploadRepo{
		StorageUploadRepository: baseUploads,
		err:                     errors.New("append failure unavailable"),
	}

	failedCtx := newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t))
	failedCtx.skipCreateSubmission = true
	failedCtx.createErr = fmt.Errorf("create rejected: %w", pdp.ErrTxRejected)
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, failedCtx.providerID) {
			return failedCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	stage := "ensure_dataset"
	ensureTask := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          fixture.objID,
		RefVersionID:   fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:2", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 2, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     1,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, ensureTask); err != nil {
		t.Fatalf("Create ensure task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, ensureTask.ID, 5*time.Second)

	binding, err := baseUploads.GetDataSetBindingByCopyIndex(ctx, fixture.upload.BucketID, 2)
	if err != nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: %v", err)
	}
	if binding != nil {
		t.Fatalf("failed dataset candidate binding = %#v, want evidence-free candidate discarded", binding)
	}
	repairTasks, total, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", "", 10, 0)
	if err != nil {
		t.Fatalf("List repair tasks: %v", err)
	}
	if total != 1 || len(repairTasks) != 1 || repairTasks[0].Status != model.TaskStatusQueued || !strings.Contains(repairTasks[0].IdempotencyKey, ":repair") {
		t.Fatalf("repair tasks = %#v, want completed coordinator reactivated for the authorized slot", repairTasks)
	}
}

func TestUploader_EvidenceFreeDataSetCandidateIsNotSharedAcrossUploads(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	bucket, _, firstVersionID := seedCachedObject(t, env)
	firstVersion, err := env.repos.Objects.GetVersionByID(ctx, firstVersionID)
	if err != nil || firstVersion == nil {
		t.Fatalf("GetVersionByID(first): version=%#v err=%v", firstVersion, err)
	}
	firstUpload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: firstVersionID,
		ContentSize:     firstVersion.Size,
		Checksum:        firstVersion.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt(first): %v", err)
	}
	candidate, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "303"),
		CopyIndex:         0,
		CreatedByUploadID: firstUpload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding(candidate): %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, firstUpload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: candidate.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       candidate.ProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(first): %v", err)
	}

	secondVersionID := model.NewVersionID()
	cacheKey := ".versions/" + secondVersionID
	info, err := env.cache.Put(ctx, bucket.Name, cacheKey, strings.NewReader("second object"))
	if err != nil {
		t.Fatalf("cache second object: %v", err)
	}
	secondVersion := &model.ObjectVersion{
		VersionID: secondVersionID,
		BucketID:  bucket.ID,
		Key:       "second.txt",
		Size:      int64(len("second object")),
		ETag:      info.ETag,
		Checksum:  info.Checksum,
		CacheKey:  cacheKey,
	}
	secondObjectID, err := env.repos.Objects.CreateVersionAndSetCurrent(ctx, secondVersion)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(second): %v", err)
	}
	prepareTask := seedStagedUploadTask(t, env, secondObjectID, secondVersionID, 5)
	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(context.Context, *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		return nil, errors.New("occupied slot must wait for its creating upload")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, prepareTask.ID, model.TaskStatusWaiting, 5*time.Second)
	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls while candidate owns slot = %d, want 0", got)
	}
	secondUpload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(ctx, secondVersionID)
	if err != nil || secondUpload == nil {
		t.Fatalf("FindLatestUploadBySourceVersion(second): upload=%#v err=%v", secondUpload, err)
	}
	secondCopies, err := env.repos.Uploads.ListCopies(ctx, secondUpload.ID)
	if err != nil || len(secondCopies) != 0 {
		t.Fatalf("second upload copies before candidate resolves = %#v err=%v, want none", secondCopies, err)
	}
	gotTask, err := env.repos.Tasks.GetByID(ctx, prepareTask.ID)
	if err != nil || gotTask == nil || gotTask.RetryCount != 0 {
		t.Fatalf("second prepare task = %#v err=%v, want dependency wait without retry", gotTask, err)
	}

	if err := env.repos.Uploads.MarkDataSetFailed(ctx, candidate.ID, "creation rejected"); err != nil {
		t.Fatalf("MarkDataSetFailed(candidate): %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyFailed(ctx, firstUpload.ID, 0, "creation rejected"); err != nil {
		t.Fatalf("MarkUploadCopyFailed(first): %v", err)
	}
	discarded, err := env.repos.Uploads.DiscardFailedDataSetCandidate(ctx, firstUpload.ID, 0, candidate.ID)
	if err != nil || !discarded {
		t.Fatalf("DiscardFailedDataSetCandidate: discarded=%t err=%v, want safe discard", discarded, err)
	}

	replacement := newFakeUploadContext(sdktypes.NewBigInt(404), sdktypes.NewBigInt(4004), sdktypes.NewBigInt(5004), testCID(t))
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		if opts.Copies != 1 {
			t.Fatalf("CreateContexts copies = %d, want 1", opts.Copies)
		}
		return []synapse.UploadContext{replacement}, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, replacement.providerID) {
			return replacement, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", prepareTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule second prepare: %v", err)
	}
	runWorkerUntilTask(t, env, uploader, prepareTask.ID, 5*time.Second)
	if got := createContextsCalls.Load(); got != 1 {
		t.Fatalf("CreateContexts calls after safe discard = %d, want 1", got)
	}
	secondCopies, err = env.repos.Uploads.ListCopies(ctx, secondUpload.ID)
	if err != nil || len(secondCopies) != 1 || secondCopies[0].ProviderID == nil || secondCopies[0].ProviderID.String() != "404" {
		t.Fatalf("second upload copies after reselection = %#v err=%v, want provider 404", secondCopies, err)
	}
}

type appendFailureUploadRepo struct {
	repository.StorageUploadRepository
	err error
}

func (r *appendFailureUploadRepo) AppendUploadFailure(context.Context, repository.AppendUploadFailureInput) error {
	return r.err
}

func TestUploader_MissingVersion(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, _ := seedCachedObject(t, env)

	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   "01J000000000000000MISSING1",
		IdempotencyKey: fmt.Sprintf("upload:%d:missing", objID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("creating task: %v", err)
	}

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		t.Error("upload should not be called for missing version")
		return nil, errors.New("should not be called")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "object not found") {
		t.Errorf("expected object not found error, got %v", got.LastError)
	}
}

func TestUploader_NilStorageClient(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	uploader := worker.NewUploader(env.repos, env.cache, nil, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "storage client not configured") {
		t.Errorf("expected storage client error, got %v", got.LastError)
	}
}

func TestUploader_StagedPrimaryStoreCacheMissMarksCacheLocationAbsent(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	bucket, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
	ctx := context.Background()
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:              binding.ID,
		UploadID:        upload.ID,
		DataSetID:       onChainID(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "11001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	stage := "ingress_store"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ingress_store:%d", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create primary store task: %v", err)
	}
	primaryCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	dataSetID := sdktypes.NewBigInt(1001)
	primaryCtx.boundDataSet = &dataSetID
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)) {
			return primaryCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusScheduled {
		t.Errorf("expected task scheduled for retry, got %s", got.Status)
	}
	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if obj.State != model.ObjectStateUploading {
		t.Errorf("expected object state uploading after cache miss retry, got %s", obj.State)
	}
	if obj.InCache {
		t.Error("expected current object cache location to be false after staged cache miss")
	}
	version, _ = env.repos.Objects.GetVersionByID(ctx, versionID)
	if version.InCache {
		t.Error("expected version cache location to be false after staged cache miss")
	}
}

func TestUploader_IngressProviderFailureClassification(t *testing.T) {
	type ingressFixture struct {
		env       *testWorkerEnv
		bucket    *model.Bucket
		objectID  int64
		versionID string
		upload    *model.StorageUpload
		task      *model.Task
		bindings  []*model.StorageDataSet
		contexts  []*fakeUploadContext
	}
	seed := func(t *testing.T, copies, readyCopies int) ingressFixture {
		t.Helper()
		env := newTestWorkerEnv(t)
		bucket, objID, versionID := seedCachedObject(t, env)
		ctx := context.Background()
		if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("mark uploading: %v", err)
		}
		version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || version == nil {
			t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
		}
		upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
			BucketID: bucket.ID, SourceVersionID: versionID, ContentSize: version.Size, Checksum: version.Checksum, RequestedCopies: copies,
		})
		if err != nil {
			t.Fatalf("StartObjectUploadAttempt: %v", err)
		}
		fixture := ingressFixture{env: env, bucket: bucket, objectID: objID, versionID: versionID, upload: upload}
		for copyIndex := range copies {
			providerID := uint64(101 + copyIndex*101)
			dataSetID := uint64(1001 + copyIndex*1001)
			binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: onChainID(t, strconv.FormatUint(providerID, 10)), CopyIndex: copyIndex, CreatedByUploadID: upload.ID,
			})
			if err != nil {
				t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
			}
			if copyIndex < readyCopies {
				if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
					ID: binding.ID, UploadID: upload.ID, DataSetID: onChainID(t, strconv.FormatUint(dataSetID, 10)),
				}); err != nil {
					t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
				}
			}
			method := model.StorageCopyTransferMethodPeerPull
			if copyIndex == 0 {
				method = model.StorageCopyTransferMethodIngress
			}
			if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
				StorageDataSetID: binding.ID, CopyIndex: copyIndex, TransferMethod: method, ProviderID: onChainID(t, strconv.FormatUint(providerID, 10)),
			}}); err != nil {
				t.Fatalf("CreateUploadCopiesForBindings(%d): %v", copyIndex, err)
			}
			storageCtx := newFakeUploadContext(sdktypes.NewBigInt(providerID), sdktypes.NewBigInt(dataSetID), sdktypes.NewBigInt(uint64(2001+copyIndex)), testCID(t))
			boundID := sdktypes.NewBigInt(dataSetID)
			storageCtx.boundDataSet = &boundID
			fixture.bindings = append(fixture.bindings, binding)
			fixture.contexts = append(fixture.contexts, storageCtx)
		}
		stage := "ingress_store"
		fixture.task = &model.Task{
			Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: objID, RefVersionID: versionID,
			IdempotencyKey: fmt.Sprintf("upload:%s:ingress_store:%d", versionID, upload.ID),
			Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)},
			Status:         model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
		}
		if err := env.repos.Tasks.Create(ctx, fixture.task); err != nil {
			t.Fatalf("create ingress store task: %v", err)
		}
		env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
			for _, storageCtx := range fixture.contexts {
				if createContextDataSetIDEqual(opts, storageCtx.dataSetID) || createContextProviderIDEqual(opts, storageCtx.providerID) {
					return storageCtx, nil
				}
			}
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
		return fixture
	}

	t.Run("temporary outage reassigns ingress without replacing provider", func(t *testing.T) {
		fixture := seed(t, 2, 2)
		fixture.contexts[0].storeErr = &synapse.ProviderUnavailableError{Cause: errors.New("provider unavailable")}
		var createContextsCalls atomic.Int32
		fixture.env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
			createContextsCalls.Add(1)
			return nil, errors.New("existing slots must not be replaced")
		}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
		runWorkerUntilTask(t, fixture.env, uploader, fixture.task.ID, 5*time.Second)

		gotTask, _ := fixture.env.repos.Tasks.GetByID(context.Background(), fixture.task.ID)
		if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
			t.Fatalf("ingress task = %#v, want completed without retry", gotTask)
		}
		binding, _ := fixture.env.repos.Uploads.GetDataSetBindingByID(context.Background(), fixture.bindings[0].ID)
		if binding.Status != model.StorageDataSetStatusUnavailable {
			t.Fatalf("failed ingress binding status = %s, want unavailable", binding.Status)
		}
		copies, err := fixture.env.repos.Uploads.ListCopies(context.Background(), fixture.upload.ID)
		if err != nil || len(copies) != 2 || copies[0].TransferMethod != model.StorageCopyTransferMethodPeerPull || copies[1].TransferMethod != model.StorageCopyTransferMethodIngress {
			t.Fatalf("reassigned copies = %#v err=%v", copies, err)
		}
		repairTasks, total, err := fixture.env.repos.Tasks.List(context.Background(), string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
		if err != nil || total != 1 || taskPayloadInt64ForTest(repairTasks[0].Payload, "storage_upload_copy_id") != copies[0].ID {
			t.Fatalf("repair tasks = %#v total=%d err=%v, want original ingress copy", repairTasks, total, err)
		}
		ensureTasks, total, err := fixture.env.repos.Tasks.List(context.Background(), string(model.TaskTypeUpload), "ensure_dataset", "", 10, 0)
		if err != nil || total != 1 || taskPayloadInt64ForTest(ensureTasks[0].Payload, "copy_index") != 1 {
			t.Fatalf("ensure tasks = %#v total=%d err=%v, want reassigned slot 1", ensureTasks, total, err)
		}
		if got := createContextsCalls.Load(); got != 0 {
			t.Fatalf("CreateContexts calls = %d, want no topology change", got)
		}
	})

	t.Run("committed ingress recovers after live provider evidence", func(t *testing.T) {
		fixture := seed(t, 1, 1)
		ctx := context.Background()
		if err := fixture.env.repos.Objects.UpdateVersionState(ctx, fixture.versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
			t.Fatalf("mark committing: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID: fixture.upload.ID, CopyIndex: 0, PieceCID: testCID(t).String(), PieceID: onChainIDPtr(t, "2001"), RetrievalURL: "https://primary.example/piece",
		}); err != nil {
			t.Fatalf("MarkUploadCopyCommitted: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.bindings[0].ID, "crashed after commit"); err != nil {
			t.Fatalf("MarkDataSetUnavailable: %v", err)
		}
		stage := "ingress_commit"
		if _, err := fixture.env.db.NewUpdate().Model((*model.Task)(nil)).
			Set("stage = ?", stage).
			Set("payload = ?", map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)}).
			Where("id = ?", fixture.task.ID).
			Exec(ctx); err != nil {
			t.Fatalf("prepare committed ingress task: %v", err)
		}
		providerAvailable := atomic.Bool{}
		fixture.env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
			if !createContextDataSetIDEqual(opts, fixture.contexts[0].dataSetID) {
				return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
			}
			if !providerAvailable.Load() {
				return nil, &synapse.ProviderUnavailableError{Cause: context.DeadlineExceeded}
			}
			return fixture.contexts[0], nil
		}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
		runWorkerUntilTaskStatus(t, fixture.env, uploader, fixture.task.ID, model.TaskStatusWaiting, 5*time.Second)
		gotTask, err := fixture.env.repos.Tasks.GetByID(ctx, fixture.task.ID)
		if err != nil || gotTask.RetryCount != 0 {
			t.Fatalf("committed ingress while unavailable = %#v err=%v, want wait without retry", gotTask, err)
		}
		providerAvailable.Store(true)
		if _, err := fixture.env.db.NewUpdate().Model((*model.Task)(nil)).
			Set("scheduled_at = ?", time.Now().Add(-time.Second)).
			Where("id = ?", fixture.task.ID).
			Exec(ctx); err != nil {
			t.Fatalf("reschedule committed ingress: %v", err)
		}
		gotTask = runWorkerUntilTask(t, fixture.env, uploader, fixture.task.ID, 5*time.Second)
		if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
			t.Fatalf("recovered committed ingress task = %#v, want completed without retry", gotTask)
		}
		version, err := fixture.env.repos.Objects.GetVersionByID(ctx, fixture.versionID)
		if err != nil || version == nil || version.State != model.ObjectStateStored {
			t.Fatalf("recovered committed version = %#v err=%v, want stored", version, err)
		}
		binding, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.bindings[0].ID)
		if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusReady {
			t.Fatalf("recovered committed binding = %#v err=%v, want ready", binding, err)
		}
		if fixture.contexts[0].storeCalls.Load() != 0 || fixture.contexts[0].commitCalls.Load() != 0 {
			t.Fatalf("committed recovery repeated storage operation: store=%d commit=%d", fixture.contexts[0].storeCalls.Load(), fixture.contexts[0].commitCalls.Load())
		}
	})

	t.Run("submitted ingress waits without handoff", func(t *testing.T) {
		fixture := seed(t, 2, 2)
		ctx := context.Background()
		if err := fixture.env.repos.Objects.UpdateVersionState(ctx, fixture.versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
			t.Fatalf("mark committing: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			UploadID: fixture.upload.ID, CopyIndex: 0, PieceCID: testCID(t).String(), RetrievalURL: "https://primary.example/piece",
		}); err != nil {
			t.Fatalf("MarkUploadCopyPieceReady: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
			UploadID: fixture.upload.ID, CopyIndex: 0, CommitExtraDataHex: "01", CommitTransactionID: fakeSubmittedCommitTxHash,
		}); err != nil {
			t.Fatalf("MarkUploadCopyCommitting: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.bindings[0].ID, "provider timeout after submit"); err != nil {
			t.Fatalf("MarkDataSetUnavailable: %v", err)
		}
		stage := "ingress_commit"
		if _, err := fixture.env.db.NewUpdate().Model((*model.Task)(nil)).
			Set("stage = ?", stage).
			Set("payload = ?", map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)}).
			Where("id = ?", fixture.task.ID).
			Exec(ctx); err != nil {
			t.Fatalf("prepare submitted commit task: %v", err)
		}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
		runWorkerUntilTaskStatus(t, fixture.env, uploader, fixture.task.ID, model.TaskStatusWaiting, 5*time.Second)

		gotTask, err := fixture.env.repos.Tasks.GetByID(ctx, fixture.task.ID)
		if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
			t.Fatalf("submitted ingress task = %#v err=%v, want dependency wait without retry", gotTask, err)
		}
		copies, err := fixture.env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
		if err != nil || len(copies) != 2 || copies[0].TransferMethod != model.StorageCopyTransferMethodIngress || copies[1].TransferMethod != model.StorageCopyTransferMethodPeerPull {
			t.Fatalf("copies with submitted ingress = %#v err=%v, want unchanged roles", copies, err)
		}
	})

	t.Run("alternate failure preserves earlier submitted commit", func(t *testing.T) {
		fixture := seed(t, 2, 2)
		ctx := context.Background()
		if err := fixture.env.repos.Objects.UpdateVersionState(ctx, fixture.versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
			t.Fatalf("mark committing: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			UploadID: fixture.upload.ID, CopyIndex: 0, PieceCID: testCID(t).String(), RetrievalURL: "https://primary.example/piece",
		}); err != nil {
			t.Fatalf("MarkUploadCopyPieceReady: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
			UploadID: fixture.upload.ID, CopyIndex: 0, CommitExtraDataHex: "01", CommitTransactionID: fakeSubmittedCommitTxHash,
		}); err != nil {
			t.Fatalf("MarkUploadCopyCommitting: %v", err)
		}
		if err := fixture.env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.bindings[0].ID, "legacy handoff state"); err != nil {
			t.Fatalf("MarkDataSetUnavailable: %v", err)
		}
		if _, err := fixture.env.db.NewUpdate().Model((*model.StorageUploadCopy)(nil)).
			Set("transfer_method = CASE WHEN copy_index = 0 THEN ? ELSE ? END", model.StorageCopyTransferMethodPeerPull, model.StorageCopyTransferMethodIngress).
			Where("upload_id = ?", fixture.upload.ID).
			Exec(ctx); err != nil {
			t.Fatalf("seed legacy handoff roles: %v", err)
		}
		if _, err := fixture.env.db.NewUpdate().Model((*model.Task)(nil)).
			Set("payload = ?", map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodIngress)}).
			Set("max_retries = 1").
			Where("id = ?", fixture.task.ID).
			Exec(ctx); err != nil {
			t.Fatalf("prepare alternate ingress task: %v", err)
		}
		fixture.contexts[1].storeErr = errors.New("unexpected alternate provider response")
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
		gotTask := runWorkerUntilTask(t, fixture.env, uploader, fixture.task.ID, 5*time.Second)
		if gotTask.Status != model.TaskStatusExhausted {
			t.Fatalf("alternate ingress task = %#v, want exhausted", gotTask)
		}
		version, err := fixture.env.repos.Objects.GetVersionByID(ctx, fixture.versionID)
		if err != nil || version == nil || version.State != model.ObjectStateCommitting || version.FailedAtState != nil {
			t.Fatalf("version with submitted commit = %#v err=%v, want committing", version, err)
		}
		upload, err := fixture.env.repos.Uploads.GetByID(ctx, fixture.upload.ID)
		if err != nil || upload == nil || upload.Status == model.StorageUploadStatusFailed {
			t.Fatalf("upload with submitted commit = %#v err=%v, must not fail", upload, err)
		}
		copies, err := fixture.env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
		if err != nil || len(copies) != 2 || !copyCommitSubmittedForTest(&copies[0]) || copies[1].Status != model.StorageUploadCopyStatusFailed {
			t.Fatalf("copies after alternate exhaustion = %#v err=%v, want submitted source and failed alternate", copies, err)
		}
	})

	t.Run("handoff rollback keeps ingress retryable", func(t *testing.T) {
		fixture := seed(t, 2, 2)
		ctx := context.Background()
		if err := fixture.env.repos.Objects.UpdateVersionState(ctx, fixture.versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
			t.Fatalf("mark committing: %v", err)
		}
		var changed atomic.Bool
		fixture.env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
			if createContextDataSetIDEqual(opts, fixture.contexts[0].dataSetID) {
				if changed.CompareAndSwap(false, true) {
					if err := fixture.env.repos.Objects.UpdateVersionState(ctx, fixture.versionID, model.ObjectStateCommitting, model.ObjectStateUploading); err != nil {
						t.Fatalf("concurrent state change: %v", err)
					}
				}
				return nil, &synapse.ProviderUnavailableError{Cause: context.DeadlineExceeded}
			}
			for _, storageCtx := range fixture.contexts {
				if createContextDataSetIDEqual(opts, storageCtx.dataSetID) || createContextProviderIDEqual(opts, storageCtx.providerID) {
					return storageCtx, nil
				}
			}
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
		runWorkerUntilTaskRetryCount(t, fixture.env, uploader, fixture.task.ID, 1, 5*time.Second)

		copies, err := fixture.env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
		if err != nil || len(copies) != 2 || copies[0].TransferMethod != model.StorageCopyTransferMethodIngress || copies[1].TransferMethod != model.StorageCopyTransferMethodPeerPull {
			t.Fatalf("copies after rolled-back handoff = %#v err=%v, want original roles", copies, err)
		}
		ensureTasks, total, err := fixture.env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ensure_dataset", "", 10, 0)
		if err != nil || total != 0 {
			t.Fatalf("ensure tasks after rolled-back handoff = %#v total=%d err=%v, want none", ensureTasks, total, err)
		}
		staleStage := "ensure_dataset"
		staleTask := &model.Task{
			Type: model.TaskTypeUpload, Stage: &staleStage, RefType: "object", RefID: fixture.objectID, RefVersionID: fixture.versionID,
			IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:1", fixture.versionID, fixture.upload.ID),
			Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
			Status:         model.TaskStatusCompleted, MaxRetries: 5, ScheduledAt: time.Now(),
		}
		if err := fixture.env.repos.Tasks.Create(ctx, staleTask); err != nil {
			t.Fatalf("create stale completed ensure task: %v", err)
		}
		if _, err := fixture.env.db.NewUpdate().Model((*model.Task)(nil)).
			Set("scheduled_at = ?", time.Now().Add(-time.Second)).
			Where("id = ?", fixture.task.ID).
			Exec(ctx); err != nil {
			t.Fatalf("reschedule ingress retry: %v", err)
		}
		gotTask := runWorkerUntilTask(t, fixture.env, uploader, fixture.task.ID, 5*time.Second)
		if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 1 {
			t.Fatalf("retried ingress task = %#v, want completed after one bounded retry", gotTask)
		}
		copies, err = fixture.env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
		if err != nil || len(copies) != 2 || copies[0].TransferMethod != model.StorageCopyTransferMethodPeerPull || copies[1].TransferMethod != model.StorageCopyTransferMethodIngress {
			t.Fatalf("copies after retried handoff = %#v err=%v, want one alternate ingress", copies, err)
		}
		ensureTasks, total, err = fixture.env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ensure_dataset", "", 10, 0)
		if err != nil || total != 1 || taskPayloadInt64ForTest(ensureTasks[0].Payload, "copy_index") != 1 || ensureTasks[0].Payload["transfer_method"] != string(model.StorageCopyTransferMethodIngress) {
			t.Fatalf("ensure tasks after retried handoff = %#v total=%d err=%v, want alternate ingress", ensureTasks, total, err)
		}
	})

	t.Run("temporary outage without alternate hands off to one coordinator", func(t *testing.T) {
		fixture := seed(t, 1, 1)
		fixture.contexts[0].storeErr = &synapse.ProviderUnavailableError{Cause: errors.New("provider unavailable")}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
		gotTask := runWorkerUntilTask(t, fixture.env, uploader, fixture.task.ID, 5*time.Second)
		if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
			t.Fatalf("ingress task = %#v, want handoff without retry", gotTask)
		}
		copyRow, err := fixture.env.repos.Uploads.GetUploadCopy(context.Background(), fixture.upload.ID, 0)
		if err != nil || copyRow == nil || copyRow.Status != model.StorageUploadCopyStatusPending {
			t.Fatalf("original copy = %#v err=%v, want pending for in-place repair", copyRow, err)
		}
		repairTasks, total, err := fixture.env.repos.Tasks.List(context.Background(), string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
		if err != nil || total != 1 || taskPayloadInt64ForTest(repairTasks[0].Payload, "storage_upload_copy_id") != copyRow.ID {
			t.Fatalf("repair tasks = %#v total=%d err=%v, want the only remaining writer", repairTasks, total, err)
		}
	})

	t.Run("recovered ingress schedules remaining pending peer", func(t *testing.T) {
		fixture := seed(t, 2, 1)
		ctx := context.Background()
		fixture.contexts[0].storeErr = &synapse.ProviderUnavailableError{Cause: errors.New("provider unavailable")}
		var createContextsCalls atomic.Int32
		fixture.env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
			createContextsCalls.Add(1)
			return nil, errors.New("existing slots must not be replaced")
		}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
		gotIngress := runWorkerUntilTask(t, fixture.env, uploader, fixture.task.ID, 5*time.Second)
		if gotIngress.Status != model.TaskStatusCompleted || gotIngress.RetryCount != 0 {
			t.Fatalf("ingress task = %#v, want coordinator handoff without retry", gotIngress)
		}

		repairTasks, total, err := fixture.env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
		if err != nil || total != 1 {
			t.Fatalf("repair tasks = %#v total=%d err=%v, want one ingress coordinator", repairTasks, total, err)
		}
		peerBinding, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.bindings[1].ID)
		if err != nil || peerBinding == nil || peerBinding.Status != model.StorageDataSetStatusPending {
			t.Fatalf("peer binding before recovery = %#v err=%v, want pending", peerBinding, err)
		}

		fixture.contexts[0].storeErr = nil
		if _, err := fixture.env.db.NewUpdate().Model((*model.Task)(nil)).
			Set("scheduled_at = ?", time.Now().Add(-time.Second)).
			Where("id = ?", repairTasks[0].ID).
			Exec(ctx); err != nil {
			t.Fatalf("reschedule recovered ingress: %v", err)
		}
		runCtx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = uploader.Run(runCtx)
			close(done)
		}()
		defer func() {
			cancel()
			waitForSignal(t, done, time.Second, "uploader shutdown")
		}()
		waitForObjectState(t, fixture.env, fixture.versionID, model.ObjectStateStored, 5*time.Second)

		peerCopy, err := fixture.env.repos.Uploads.GetUploadCopy(ctx, fixture.upload.ID, 1)
		if err != nil || peerCopy == nil || peerCopy.Status != model.StorageUploadCopyStatusCommitted {
			t.Fatalf("peer copy after ingress recovery = %#v err=%v, want committed", peerCopy, err)
		}
		ensureTasks, total, err := fixture.env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ensure_dataset", "", 10, 0)
		if err != nil || total != 1 {
			t.Fatalf("peer ensure tasks = %#v total=%d err=%v, want one", ensureTasks, total, err)
		}
		if ensureTasks[0].RefType != "object" || ensureTasks[0].RefID != fixture.objectID || ensureTasks[0].RefVersionID != fixture.versionID || taskPayloadInt64ForTest(ensureTasks[0].Payload, "copy_index") != 1 {
			t.Fatalf("peer ensure task = %#v, want exact source object and pending slot", ensureTasks[0])
		}
		if got := createContextsCalls.Load(); got != 0 {
			t.Fatalf("CreateContexts calls = %d, want no topology change", got)
		}
	})

	t.Run("ended service waits for operator replacement", func(t *testing.T) {
		fixture := seed(t, 1, 1)
		fixture.contexts[0].storeErr = &storage.DataSetPDPPaymentTerminatedError{DataSetID: sdktypes.NewBigInt(1001), PDPEndEpoch: 3778900}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
		runWorkerUntilTaskStatus(t, fixture.env, uploader, fixture.task.ID, model.TaskStatusWaiting, 5*time.Second)
		gotTask, _ := fixture.env.repos.Tasks.GetByID(context.Background(), fixture.task.ID)
		if gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
			t.Fatalf("ended-service task = %#v, want dependency wait without retry", gotTask)
		}
		binding, _ := fixture.env.repos.Uploads.GetDataSetBindingByID(context.Background(), fixture.bindings[0].ID)
		if binding.Status != model.StorageDataSetStatusDraining {
			t.Fatalf("ended service binding status = %s, want draining", binding.Status)
		}
	})

	t.Run("concurrent lifecycle change converges without retry", func(t *testing.T) {
		fixture := seed(t, 1, 1)
		var changed atomic.Bool
		fixture.env.storage.CreateContextFunc = func(_ context.Context, _ *storage.CreateContextOptions) (synapse.UploadContext, error) {
			if changed.CompareAndSwap(false, true) {
				if err := fixture.env.repos.Uploads.MarkDataSetDraining(context.Background(), fixture.bindings[0].ID, "service ended concurrently"); err != nil {
					t.Fatalf("MarkDataSetDraining: %v", err)
				}
			}
			return nil, &synapse.ProviderUnavailableError{Cause: context.DeadlineExceeded}
		}
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
		runWorkerUntilTaskStatus(t, fixture.env, uploader, fixture.task.ID, model.TaskStatusWaiting, 5*time.Second)

		gotTask, err := fixture.env.repos.Tasks.GetByID(context.Background(), fixture.task.ID)
		if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
			t.Fatalf("task after lifecycle conflict = %#v err=%v, want dependency wait without retry", gotTask, err)
		}
		binding, err := fixture.env.repos.Uploads.GetDataSetBindingByID(context.Background(), fixture.bindings[0].ID)
		if err != nil || binding == nil || binding.Status != model.StorageDataSetStatusDraining {
			t.Fatalf("binding after lifecycle conflict = %#v err=%v, want concurrent draining state", binding, err)
		}
	})

	t.Run("unknown error uses bounded retry without lifecycle change", func(t *testing.T) {
		fixture := seed(t, 1, 1)
		fixture.contexts[0].storeErr = errors.New("unexpected provider response")
		uploader := worker.NewUploader(fixture.env.repos, fixture.env.cache, fixture.env.storage, nil, fixture.env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
		runWorkerUntilTaskRetryCount(t, fixture.env, uploader, fixture.task.ID, 1, 5*time.Second)
		gotTask, _ := fixture.env.repos.Tasks.GetByID(context.Background(), fixture.task.ID)
		if gotTask.Status != model.TaskStatusScheduled || gotTask.RetryCount != 1 {
			t.Fatalf("unknown-error task = %#v, want bounded retry", gotTask)
		}
		binding, _ := fixture.env.repos.Uploads.GetDataSetBindingByID(context.Background(), fixture.bindings[0].ID)
		if binding.Status != model.StorageDataSetStatusReady {
			t.Fatalf("unknown-error binding status = %s, want ready", binding.Status)
		}
	})
}

func TestUploader_UnestablishedCandidateOutageWaitsWithoutReplicaRepair(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	candidate := newFakeUploadContext(
		sdktypes.NewBigInt(101),
		sdktypes.NewBigInt(1001),
		sdktypes.NewBigInt(2001),
		testCID(t),
	)
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 1 {
			t.Fatalf("CreateContexts copies = %d, want one authorized slot", opts.Copies)
		}
		return []synapse.UploadContext{candidate}, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if !createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) || opts.DataSetID != nil {
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
		return nil, &synapse.ProviderUnavailableError{Cause: errors.New("candidate provider unavailable")}
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("candidate task = %#v err=%v, want dependency wait without retry", gotTask, err)
	}
	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(ctx, versionID)
	if err != nil || upload == nil {
		t.Fatalf("FindLatestUploadBySourceVersion: upload=%#v err=%v", upload, err)
	}
	bindings, err := env.repos.Uploads.ListDataSetBindings(ctx, upload.BucketID)
	if err != nil || len(bindings) != 1 || bindings[0].Status != model.StorageDataSetStatusPending || bindings[0].DataSetID != nil {
		t.Fatalf("candidate bindings = %#v err=%v, want one unestablished pending slot", bindings, err)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil || len(copies) != 1 || copies[0].Status != model.StorageUploadCopyStatusPending || copies[0].StorageDataSetID == nil || *copies[0].StorageDataSetID != bindings[0].ID {
		t.Fatalf("candidate copies = %#v err=%v, want one pending assigned copy", copies, err)
	}
	repairTasks, total, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
	if err != nil || total != 0 || len(repairTasks) != 0 {
		t.Fatalf("repair tasks = %#v total=%d err=%v, want none for unestablished candidate", repairTasks, total, err)
	}
}

func TestUploader_SPUploadFailure_Retry(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return nil, errors.New("SP unavailable")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got == nil {
		t.Fatal("expected task after retry")
	}
	if got.Status != model.TaskStatusScheduled {
		t.Errorf("expected task scheduled for retry, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
}

func TestUploader_SPUploadFailure_MaxRetries(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)

	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	if _, err := env.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("retry_count = ?", 4).
		Where("id = ?", task.ID).
		Exec(context.Background()); err != nil {
		t.Fatalf("set retry count: %v", err)
	}
	task.RetryCount = 4

	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return nil, errors.New("SP permanent failure")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusExhausted {
		t.Errorf("expected task exhausted, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if obj.State != model.ObjectStateUploading {
		t.Errorf("expected object state uploading after prepare exhaustion, got %s", obj.State)
	}
}

func TestUploader_AllAssignedProvidersUnavailableWaitsWithoutRetry(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	seedUpload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: "01J000000000000000SEEDSLOT", ContentSize: 1, Checksum: "seed-slots", RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt seed: %v", err)
	}
	bindings := make([]*model.StorageDataSet, 0, 3)
	for copyIndex, ids := range [][2]string{{"101", "1001"}, {"202", "2002"}, {"303", "3003"}} {
		binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: onChainID(t, ids[0]), CopyIndex: copyIndex, CreatedByUploadID: seedUpload.ID,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
		}
		if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: seedUpload.ID, DataSetID: onChainID(t, ids[1])}); err != nil {
			t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
		}
		if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, binding.ID, "temporary outage"); err != nil {
			t.Fatalf("MarkDataSetUnavailable(%d): %v", copyIndex, err)
		}
		bindings = append(bindings, binding)
	}
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	var createContextsCalls atomic.Int32
	var providerRecovered atomic.Bool
	recoveredContext := newFakeUploadContext(
		sdktypes.NewBigInt(101),
		sdktypes.NewBigInt(1001),
		sdktypes.NewBigInt(2001),
		testCID(t),
	)
	recoveredDataSetID := sdktypes.NewBigInt(1001)
	recoveredContext.boundDataSet = &recoveredDataSetID
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		return nil, errors.New("assigned slots must not select replacement providers")
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if providerRecovered.Load() && createContextDataSetIDEqual(opts, recoveredDataSetID) {
			return recoveredContext, nil
		}
		return nil, &synapse.ProviderUnavailableError{Cause: errors.New("provider still unavailable")}
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 3, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("waiting task = %#v err=%v", gotTask, err)
	}
	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls = %d, want no topology change", got)
	}
	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(ctx, versionID)
	if err != nil || upload == nil {
		t.Fatalf("FindLatestUploadBySourceVersion: upload=%#v err=%v", upload, err)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil || len(copies) != 3 {
		t.Fatalf("assigned copies = %#v err=%v, want all three slots", copies, err)
	}
	for _, copyRow := range copies {
		if copyRow.Status != model.StorageUploadCopyStatusPending || copyRow.StorageDataSetID == nil {
			t.Fatalf("assigned copy = %#v, want pending original slot", copyRow)
		}
	}
	exhausted, err := env.repos.Tasks.ListExhausted(ctx, 10)
	if err != nil || len(exhausted) != 0 {
		t.Fatalf("exhausted tasks = %#v err=%v, want none", exhausted, err)
	}
	repairTasks, repairTotal, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
	if err != nil || repairTotal != 3 {
		t.Fatalf("repair tasks = %#v total=%d err=%v, want one per assigned slot", repairTasks, repairTotal, err)
	}
	var recoveredTask *model.Task
	for i := range repairTasks {
		if taskPayloadInt64ForTest(repairTasks[i].Payload, "storage_data_set_id") == bindings[0].ID {
			recoveredTask = &repairTasks[i]
		}
		if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
			Set("scheduled_at = ?", time.Now().Add(time.Hour)).
			Where("id = ?", repairTasks[i].ID).
			Exec(ctx); err != nil {
			t.Fatalf("defer repair task %d: %v", repairTasks[i].ID, err)
		}
	}
	if recoveredTask == nil {
		t.Fatal("repair task for recovered slot not found")
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(time.Hour)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("defer original prepare task: %v", err)
	}
	providerRecovered.Store(true)
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", recoveredTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule recovered slot: %v", err)
	}
	recoveryUploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 3, 1, 10*time.Millisecond, slog.Default())
	gotRepair := runWorkerUntilTask(t, env, recoveryUploader, recoveredTask.ID, 5*time.Second)
	if gotRepair.Status != model.TaskStatusCompleted || gotRepair.RetryCount != 0 {
		t.Fatalf("recovered repair task = %#v, want completed without retry", gotRepair)
	}
	if got := recoveredContext.storeCalls.Load(); got != 1 {
		t.Fatalf("recovered provider Store calls = %d, want retained-cache repair", got)
	}
	recoveredVersion, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || recoveredVersion == nil || recoveredVersion.State != model.ObjectStateReplicating {
		t.Fatalf("version after one recovered slot = %#v err=%v, want readable replicating", recoveredVersion, err)
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("resume original prepare task: %v", err)
	}
	resumeUploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 3, 1, 10*time.Millisecond, slog.Default())
	gotPrepare := runWorkerUntilTask(t, env, resumeUploader, task.ID, 5*time.Second)
	if gotPrepare.Status != model.TaskStatusCompleted || gotPrepare.RetryCount != 0 {
		t.Fatalf("resumed prepare task = %#v, want completed without retry", gotPrepare)
	}
	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls after recovery = %d, want no topology change", got)
	}
}

func TestUploader_PrepareSkipsUnavailableAssignedContextAndReassignsIngress(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	seedUpload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: "01J000000000000000SEEDCTX1", ContentSize: 1, Checksum: "seed-contexts", RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt seed: %v", err)
	}
	bindings := make([]*model.StorageDataSet, 0, 2)
	for copyIndex, ids := range [][2]string{{"101", "1001"}, {"202", "2002"}} {
		binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: onChainID(t, ids[0]), CopyIndex: copyIndex, CreatedByUploadID: seedUpload.ID,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
		}
		if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: seedUpload.ID, DataSetID: onChainID(t, ids[1])}); err != nil {
			t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
		}
		bindings = append(bindings, binding)
	}
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	healthyCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), testCID(t))
	dataSetID := sdktypes.NewBigInt(2002)
	healthyCtx.boundDataSet = &dataSetID
	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		return nil, errors.New("assigned slots must not select replacement providers")
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		switch {
		case createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)):
			return nil, &synapse.ProviderUnavailableError{Cause: errors.New("provider 101 unavailable")}
		case createContextDataSetIDEqual(opts, dataSetID):
			return healthyCtx, nil
		default:
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
	}
	env.storage.PrepareUploadFunc = func(_ context.Context, _ uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error) {
		if len(contexts) != 1 || !contexts[0].ProviderID().Equal(sdktypes.NewBigInt(202)) {
			t.Fatalf("funding contexts = %#v, want only provider 202", contexts)
		}
		return &storage.MultiContextCosts{Ready: true}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if got := createContextsCalls.Load(); got != 0 {
		t.Fatalf("CreateContexts calls = %d, want no topology change", got)
	}
	failedBinding, err := env.repos.Uploads.GetDataSetBindingByID(ctx, bindings[0].ID)
	if err != nil || failedBinding == nil || failedBinding.Status != model.StorageDataSetStatusUnavailable {
		t.Fatalf("provider 101 binding = %#v err=%v, want unavailable", failedBinding, err)
	}
	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(ctx, versionID)
	if err != nil || upload == nil {
		t.Fatalf("FindLatestUploadBySourceVersion: upload=%#v err=%v", upload, err)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil || len(copies) != 2 || copies[0].TransferMethod != model.StorageCopyTransferMethodPeerPull || copies[1].TransferMethod != model.StorageCopyTransferMethodIngress {
		t.Fatalf("copies after context outage = %#v err=%v", copies, err)
	}
	repairTasks, repairTotal, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "repair_replica", "", 10, 0)
	if err != nil || repairTotal != 1 || taskPayloadInt64ForTest(repairTasks[0].Payload, "storage_upload_copy_id") != copies[0].ID {
		t.Fatalf("repair tasks = %#v total=%d err=%v", repairTasks, repairTotal, err)
	}
	ensureTasks, ensureTotal, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ensure_dataset", "", 10, 0)
	if err != nil || ensureTotal != 1 || taskPayloadInt64ForTest(ensureTasks[0].Payload, "copy_index") != 1 {
		t.Fatalf("ensure tasks = %#v total=%d err=%v, want ingress slot 1", ensureTasks, ensureTotal, err)
	}
}

func TestUploader_NewSlotProviderExhaustionWaitsWithoutRetry(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return nil, &synapse.NoProviderCandidatesError{Cause: errors.New("no remaining providers")}
	}
	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 3, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)
	gotTask, err := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if err != nil || gotTask.RetryCount != 0 || gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("provider exhaustion task = %#v err=%v, want dependency wait", gotTask, err)
	}
	exhausted, err := env.repos.Tasks.ListExhausted(context.Background(), 10)
	if err != nil || len(exhausted) != 0 {
		t.Fatalf("exhausted tasks = %#v err=%v, want none", exhausted, err)
	}
}

func TestUploader_NewSlotProviderExhaustionUsesExistingReadySlots(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	seedUpload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: "01J000000000000000SEEDGAP1", ContentSize: 1, Checksum: "seed-gap", RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt seed: %v", err)
	}
	readyContexts := make(map[string]*fakeUploadContext, 2)
	for copyIndex, ids := range [][2]string{{"101", "1001"}, {"202", "2002"}} {
		binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: onChainID(t, ids[0]), CopyIndex: copyIndex, CreatedByUploadID: seedUpload.ID,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
		}
		if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, UploadID: seedUpload.ID, DataSetID: onChainID(t, ids[1]),
		}); err != nil {
			t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
		}
		providerID, _ := strconv.ParseUint(ids[0], 10, 64)
		dataSetID, _ := strconv.ParseUint(ids[1], 10, 64)
		storageCtx := newFakeUploadContext(sdktypes.NewBigInt(providerID), sdktypes.NewBigInt(dataSetID), sdktypes.NewBigInt(uint64(3001+copyIndex)), testCID(t))
		boundID := sdktypes.NewBigInt(dataSetID)
		storageCtx.boundDataSet = &boundID
		readyContexts[ids[1]] = storageCtx
	}
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	var createContextsCalls atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalls.Add(1)
		if opts.Copies != 1 {
			t.Fatalf("CreateContexts copies = %d, want one missing slot", opts.Copies)
		}
		return nil, &synapse.NoProviderCandidatesError{Cause: errors.New("no remaining providers")}
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if opts.DataSetID != nil {
			if storageCtx := readyContexts[opts.DataSetID.String()]; storageCtx != nil {
				return storageCtx, nil
			}
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	env.storage.PrepareUploadFunc = func(_ context.Context, _ uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error) {
		if len(contexts) != 2 {
			t.Fatalf("PrepareUpload contexts = %d, want two existing ready slots", len(contexts))
		}
		return &storage.MultiContextCosts{Ready: true}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 3, 1, 10*time.Millisecond, slog.Default())
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
		t.Fatalf("prepare task = %#v, want existing slots to continue without retry", gotTask)
	}
	if got := createContextsCalls.Load(); got != 1 {
		t.Fatalf("CreateContexts calls = %d, want one bounded attempt for the missing slot", got)
	}
	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(ctx, versionID)
	if err != nil || upload == nil {
		t.Fatalf("FindLatestUploadBySourceVersion: upload=%#v err=%v", upload, err)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("partial upload copies = %#v err=%v, want existing two slots", copies, err)
	}
	ensureTasks, total, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ensure_dataset", string(model.TaskStatusQueued), 10, 0)
	if err != nil || total != 1 || taskPayloadInt64ForTest(ensureTasks[0].Payload, "copy_index") != 0 {
		t.Fatalf("ensure tasks = %#v total=%d err=%v, want ingress on existing slot 0", ensureTasks, total, err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(workerCtx)
		close(done)
	}()
	defer func() {
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for missing-slot repair dependency")
		case <-ticker.C:
			version, versionErr := env.repos.Objects.GetVersionByID(ctx, versionID)
			repairTasks, repairTotal, listErr := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", string(model.TaskStatusWaiting), 10, 0)
			if versionErr == nil && version != nil && version.State == model.ObjectStateReplicating && listErr == nil && repairTotal == 1 {
				if repairTasks[0].RetryCount != 0 || repairTasks[0].WaitReason == nil || *repairTasks[0].WaitReason != model.TaskWaitReasonDependency {
					t.Fatalf("missing-slot repair task = %#v, want dependency wait without retry", repairTasks[0])
				}
				return
			}
		}
	}
}

func TestUploader_FundingSkipsUnavailableCandidateAndUsesReadySlot(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	ctx := context.Background()
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%#v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: versionID, ContentSize: version.Size, Checksum: version.Checksum, RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	candidate, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding candidate: %v", err)
	}
	ready, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding ready: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: ready.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	readyCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3002), testCID(t))
	readyDataSetID := sdktypes.NewBigInt(2002)
	readyCtx.boundDataSet = &readyDataSetID
	candidateCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(3001), testCID(t))
	var candidateRecovered atomic.Bool
	var candidateCreateCalls atomic.Int32
	candidateCtx.createCalls = &candidateCreateCalls
	env.storage.CreateContextsFunc = func(context.Context, *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return nil, errors.New("all requested slots are already assigned")
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		switch {
		case createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)):
			if candidateRecovered.Load() {
				return candidateCtx, nil
			}
			return nil, &synapse.ProviderUnavailableError{Cause: errors.New("candidate provider unavailable")}
		case createContextDataSetIDEqual(opts, readyDataSetID):
			return readyCtx, nil
		default:
			return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
		}
	}
	var prepareCalls atomic.Int32
	env.storage.PrepareUploadFunc = func(_ context.Context, _ uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error) {
		call := prepareCalls.Add(1)
		wantProvider := sdktypes.NewBigInt(202)
		if call > 1 {
			wantProvider = sdktypes.NewBigInt(101)
		}
		if len(contexts) != 1 || !contexts[0].ProviderID().Equal(wantProvider) {
			t.Fatalf("funding contexts on call %d = %#v, want only provider %s", call, contexts, wantProvider.String())
		}
		return &storage.MultiContextCosts{Ready: true}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 2, 1, 10*time.Millisecond, slog.Default())
	gotTask := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)
	if gotTask.Status != model.TaskStatusCompleted || gotTask.RetryCount != 0 {
		t.Fatalf("prepare task = %#v, want ready slot to continue without retry", gotTask)
	}
	retainedCandidate, err := env.repos.Uploads.GetDataSetBindingByID(ctx, candidate.ID)
	if err != nil || retainedCandidate == nil || retainedCandidate.Status != model.StorageDataSetStatusPending {
		t.Fatalf("candidate binding = %#v err=%v, want pending without lifecycle change", retainedCandidate, err)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil || len(copies) != 2 || copies[0].TransferMethod != model.StorageCopyTransferMethodPeerPull || copies[1].TransferMethod != model.StorageCopyTransferMethodIngress {
		t.Fatalf("reassigned ingress copies = %#v err=%v, want ready slot 1 as ingress", copies, err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(workerCtx)
		close(done)
	}()
	defer func() {
		cancel()
		waitForSignal(t, done, time.Second, "uploader shutdown")
	}()
	var repairTask *model.Task
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for repairTask == nil {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for deferred candidate repair")
		case <-ticker.C:
			version, versionErr := env.repos.Objects.GetVersionByID(ctx, versionID)
			tasks, total, listErr := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", string(model.TaskStatusWaiting), 10, 0)
			if versionErr == nil && version != nil && version.State == model.ObjectStateReplicating && listErr == nil && total == 1 {
				repairTask = &tasks[0]
			}
		}
	}
	if candidateCreateCalls.Load() != 0 {
		t.Fatalf("CreateDataSet calls before candidate recovery = %d, want 0", candidateCreateCalls.Load())
	}
	candidateRecovered.Store(true)
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", repairTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule candidate repair: %v", err)
	}
	waitForObjectState(t, env, versionID, model.ObjectStateStored, 5*time.Second)
	if candidateCreateCalls.Load() != 1 {
		t.Fatalf("CreateDataSet calls after candidate recovery = %d, want 1", candidateCreateCalls.Load())
	}
	if prepareCalls.Load() != 2 {
		t.Fatalf("PrepareUpload calls = %d, want initial ready slot and delayed candidate preflight", prepareCalls.Load())
	}
}

func TestUploader_EvictTaskIdempotency(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	pieceCID := testCID(t)
	ingress := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), pieceCID)
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 1 {
			t.Fatalf("CreateContexts copies = %d, want 1", opts.Copies)
		}
		return []synapse.UploadContext{ingress}, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, ingress.providerID) {
			return ingress, nil
		}
		if createContextDataSetIDEqual(opts, ingress.dataSetID) {
			return ingress, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	// Pre-create conflicting evict_cache task to trigger idempotency collision
	evictionStage := cacheeviction.StageAfterUpload
	conflict := &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &evictionStage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("evict_cache:%s", versionID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), conflict); err != nil {
		t.Fatalf("creating conflict task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = uploader.Run(ctx)
		close(done)
	}()
	waitForObjectState(t, env, versionID, model.ObjectStateStored, 5*time.Second)
	cancel()
	waitForSignal(t, done, time.Second, "uploader shutdown")

	// ErrAlreadyExists is treated as idempotent success — task completes
	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (evict task already exists = idempotent), got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
	if obj.State != model.ObjectStateStored {
		t.Errorf("expected object in stored state, got %s", obj.State)
	}
}

func TestUploader_UploadFundingWaitsWithoutRetry(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	var createContextsCalled atomic.Int32
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalled.Add(1)
		return newFakeUploadContexts(t, opts.Copies, 0), nil
	}
	env.storage.PrepareUploadFunc = func(_ context.Context, dataSize uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error) {
		if dataSize == 0 {
			t.Fatal("PrepareUpload data size must be positive")
		}
		if len(contexts) != config.DefaultFilecoinCopies {
			t.Fatalf("PrepareUpload contexts = %d, want %d", len(contexts), config.DefaultFilecoinCopies)
		}
		return &storage.MultiContextCosts{
			DepositNeeded:        big.NewInt(123),
			NeedsFWSSMaxApproval: true,
			Ready:                false,
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusWaiting {
		t.Errorf("expected task waiting for funding, got %s", got.Status)
	}
	if got.RetryCount != 0 {
		t.Errorf("expected retry_count=0, got %d", got.RetryCount)
	}
	if got.WaitReason == nil || *got.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("wait_reason = %v, want dependency", got.WaitReason)
	}
	if got.StatusMessage == nil || !strings.Contains(*got.StatusMessage, "deposit 123") || !strings.Contains(*got.StatusMessage, "approve FWSS") {
		message := ""
		if got.StatusMessage != nil {
			message = *got.StatusMessage
		}
		t.Fatalf("status_message = %q, want deposit and approval guidance", message)
	}
	if createContextsCalled.Load() != 1 {
		t.Fatalf("CreateContexts calls = %d, want 1", createContextsCalled.Load())
	}
	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(context.Background(), versionID)
	if err != nil || upload == nil {
		t.Fatalf("expected upload attempt before funding wait, upload=%v err=%v", upload, err)
	}
	copies, err := env.repos.Uploads.ListCopies(context.Background(), upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != config.DefaultFilecoinCopies {
		t.Fatalf("copy rows before funding ready = %d, want one pending row per assigned slot", len(copies))
	}
	for _, copyRow := range copies {
		if copyRow.Status != model.StorageUploadCopyStatusPending || copyRow.StorageDataSetID == nil {
			t.Fatalf("copy before funding = %#v, want pending assigned copy", copyRow)
		}
	}
}

func TestUploader_UploadFundingRetryReusesBindings(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)
	ctx := context.Background()

	var (
		createContextsCalled atomic.Int32
		prepareCalled        atomic.Int32
		uploadContexts       []synapse.UploadContext
	)
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalled.Add(1)
		uploadContexts = newFakeUploadContexts(t, config.DefaultFilecoinCopies, 0)
		return uploadContexts, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		for _, uploadCtx := range uploadContexts {
			fakeCtx := uploadCtx.(*fakeUploadContext)
			if createContextProviderIDEqual(opts, fakeCtx.providerID) || createContextDataSetIDEqual(opts, fakeCtx.dataSetID) {
				return fakeCtx, nil
			}
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	env.storage.PrepareUploadFunc = func(_ context.Context, _ uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error) {
		if len(contexts) != config.DefaultFilecoinCopies {
			t.Fatalf("PrepareUpload contexts = %d, want %d", len(contexts), config.DefaultFilecoinCopies)
		}
		if prepareCalled.Add(1) == 1 {
			return &storage.MultiContextCosts{DepositNeeded: big.NewInt(10), Ready: false}, nil
		}
		return &storage.MultiContextCosts{DepositNeeded: big.NewInt(0), Ready: true}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, uploader, task.ID, model.TaskStatusWaiting, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.RetryCount != 0 {
		t.Fatalf("retry_count after funding wait = %d, want 0", got.RetryCount)
	}
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule funding wait: %v", err)
	}
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ = env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("expected retried task completed, got %s", got.Status)
	}
	if createContextsCalled.Load() != 1 {
		t.Fatalf("CreateContexts calls = %d, want 1", createContextsCalled.Load())
	}
	if prepareCalled.Load() != 2 {
		t.Fatalf("PrepareUpload calls = %d, want 2", prepareCalled.Load())
	}
	bindings, err := env.repos.Uploads.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("ListDataSetBindings: %v", err)
	}
	if len(bindings) != config.DefaultFilecoinCopies {
		t.Fatalf("bindings = %d, want %d", len(bindings), config.DefaultFilecoinCopies)
	}
	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(ctx, versionID)
	if err != nil || upload == nil {
		t.Fatalf("expected upload attempt, upload=%v err=%v", upload, err)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != config.DefaultFilecoinCopies {
		t.Fatalf("copy rows = %d, want %d", len(copies), config.DefaultFilecoinCopies)
	}
}

func TestUploader_PrepareFundingDoesNotQueryWallet(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	var walletCalled atomic.Bool
	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			walletCalled.Store(true)
			return nil, errors.New("wallet should not be queried during upload funding preparation")
		},
	}
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return newFakeUploadContexts(t, opts.Copies, 0), nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, cache.EvictionPolicyAfterUpload, config.DefaultFilecoinCopies, 1, 50*time.Millisecond, slog.Default())
	got := runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}
	if walletCalled.Load() {
		t.Fatal("wallet should not be queried before upload funding preparation")
	}
	if upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(context.Background(), versionID); err != nil || upload == nil {
		t.Fatalf("expected upload attempt, upload=%v err=%v", upload, err)
	}
}
