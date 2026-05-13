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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

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
	if taskType == model.TaskTypeUpload {
		task.Payload = map[string]interface{}{"stage": ""}
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
func runWorkerUntilTask(t *testing.T, env *testWorkerEnv, w worker.Worker, taskID int64, timeout time.Duration) {
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
			t.Fatalf("timed out waiting for task %d to be processed", taskID)
		case <-ticker.C:
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err != nil {
				continue
			}
			if task != nil && task.Status != model.TaskStatusQueued && task.Status != model.TaskStatusScheduled && task.Status != model.TaskStatusWaiting && task.Status != model.TaskStatusRunning {
				cancel()
				<-done
				return
			}
		}
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
	providerID    sdktypes.BigInt
	dataSetID     sdktypes.BigInt
	boundDataSet  *sdktypes.BigInt
	pieceID       sdktypes.BigInt
	pieceCID      cid.Cid
	clientDataID  sdktypes.BigInt
	pullEntered   chan struct{}
	releasePull   chan struct{}
	pullOnce      sync.Once
	pullErr       error
	createCalls   *atomic.Int32
	waitCalls     *atomic.Int32
	createErr     error
	waitErr       error
	storeErr      error
	storeProgress []int64
	commitErr     error
	serviceURL    string
	presignCalls  atomic.Int32
	commitCalls   atomic.Int32
	commitMu      sync.Mutex
	commitExtras  [][]byte
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
	if f.boundDataSet == nil {
		return nil
	}
	id := f.boundDataSet.Copy()
	return &id
}

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
	if opts != nil && opts.OnSubmitted != nil {
		opts.OnSubmitted(submission)
	}
	if f.createErr != nil {
		return nil, f.createErr
	}
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
	return &storage.CreateDataSetResult{
		TransactionID:   submission.TransactionID,
		DataSetID:       f.dataSetID.Copy(),
		ClientDataSetID: submission.ClientDataSetID.Copy(),
	}, nil
}

func (f *fakeUploadContext) Store(_ context.Context, r io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
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
	pieceID, _ := f.pieceID.Uint64()
	if req.OnSubmitted != nil {
		req.OnSubmitted(fmt.Sprintf("0xcommit%x", pieceID))
	}
	if f.commitErr != nil {
		return nil, f.commitErr
	}
	return &storage.CommitResult{
		TransactionID: fmt.Sprintf("0xcommit%x", pieceID),
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, pollInterval, slog.Default())
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 2, 20*time.Millisecond, slog.Default())
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, pollInterval, slog.Default())
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default(), worker.WithEventPublisher(publisher))
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default())
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
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 2 {
			t.Fatalf("CreateContexts copies = %d, want 2", opts.Copies)
		}
		return []synapse.UploadContext{
			newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), testCID(t)),
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 100*time.Millisecond, slog.Default())
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
	if len(uploads) != 1 || uploads[0].Status != model.StorageUploadStatusRunning || uploads[0].RequestedCopies != 2 {
		t.Fatalf("uploads after prepare = %#v, want one running upload with two requested copies", uploads)
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
		if opts.Copies != 3 {
			t.Fatalf("CreateContexts copies = %d, want 3", opts.Copies)
		}
		return []synapse.UploadContext{
			newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), testCID(t)),
			newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t)),
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 100*time.Millisecond, slog.Default(), worker.WithTargetCopies(3))
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	var uploads []model.StorageUpload
	if err := env.db.NewSelect().Model(&uploads).Where("source_version_id = ?", versionID).Scan(ctx); err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].RequestedCopies != 3 {
		t.Fatalf("uploads after prepare = %#v, want one upload with three requested copies", uploads)
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, uploads[0].ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 3 {
		t.Fatalf("copy rows = %d, want 3", len(copies))
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 100*time.Millisecond, slog.Default(), worker.WithTargetCopies(2))
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
		if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID:        binding.ID,
			UploadID:  upload.ID,
			DataSetID: onChainID(t, fmt.Sprintf("%d", 1000+copyIndex)),
		}); err != nil {
			t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
		}
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
	env.storage.CreateContextFunc = func(ctx context.Context, _ *storage.CreateContextOptions) (synapse.UploadContext, error) {
		<-ctx.Done()
		return nil, ctx.Err()
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
	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 100*time.Millisecond, slog.Default(), worker.WithTargetCopies(2))
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 100*time.Millisecond, slog.Default(), worker.WithTargetCopies(99))
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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

func TestUploader_EnsureDatasetSubmittedCreateErrorResumesWait(t *testing.T) {
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
	providerCtx.createErr = errors.New("create status poll timeout")
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return providerCtx, nil
		}
		if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)) {
			return providerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

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

func TestUploader_EnsureDatasetCreatingWaitErrorKeepsSubmission(t *testing.T) {
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
	providerCtx.waitErr = errors.New("create status poll timeout")
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(101)) {
			return providerCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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
}

func TestUploader_EnsureDatasetTerminalPrimaryFailureMarksUploadFailed(t *testing.T) {
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	gotTask, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if gotTask.Status != model.TaskStatusExhausted {
		t.Fatalf("task status = %s, want exhausted", gotTask.Status)
	}
	gotVersion, _ := env.repos.Objects.GetVersionByID(ctx, versionID)
	if gotVersion.State != model.ObjectStateFailed || gotVersion.StorageUploadID != nil {
		t.Fatalf("version after terminal dataset failure = state:%s upload:%v, want failed without upload id", gotVersion.State, gotVersion.StorageUploadID)
	}
	gotUpload, err := env.repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || gotUpload == nil {
		t.Fatalf("GetByID(upload): upload=%v err=%v", gotUpload, err)
	}
	if gotUpload.Status != model.StorageUploadStatusFailed {
		t.Fatalf("upload status = %s, want failed", gotUpload.Status)
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
		MaxRetries:     2,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create primary commit task: %v", err)
	}
	primaryCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), pieceCID)
	primaryCtx.commitErr = errors.New("commit status poll timeout")
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)) {
			return primaryCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

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
		if r.URL.Path != "/pdp/data-sets/1001/pieces/added/0xcommit7d1" {
			t.Fatalf("commit status path = %q, want /pdp/data-sets/1001/pieces/added/0xcommit7d1", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"txHash":"0xcommit7d1","txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"piecesAdded":true,"confirmedPieceIds":[2001]}`)
	}))
	defer statusServer.Close()
	primaryCtx.serviceURL = statusServer.URL
	if _, err := env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("reschedule primary commit retry: %v", err)
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

func TestUploader_PeerFailureKeepsObjectReadableAndQueuesRepair(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	_ = seedStagedUploadTask(t, env, objID, versionID, 1)

	pieceCID := testCID(t)
	ingress := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), pieceCID)
	peer := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3001), pieceCID)
	peer.pullErr = errors.New("provider pull failed")
	contextsByProvider := map[string]*fakeUploadContext{
		ingress.providerID.String(): ingress,
		peer.providerID.String():    peer,
	}
	contextsByDataSet := map[string]*fakeUploadContext{
		ingress.dataSetID.String(): ingress,
		peer.dataSetID.String():    peer,
	}
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies == 2 {
			return []synapse.UploadContext{ingress, peer}, nil
		}
		return nil, fmt.Errorf("replacement selection unavailable")
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default())
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
			t.Fatal("timed out waiting for peer failure to queue repair")
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
			if err != nil || len(exhaustedTasks) == 0 {
				continue
			}
			repairTasks, _, err := env.repos.Tasks.List(context.Background(), string(model.TaskTypeUpload), "prepare_upload", "", 10, 0)
			if err != nil {
				continue
			}
			for i := range repairTasks {
				if taskPayloadInt64ForTest(repairTasks[i].Payload, "upload_id") == upload.ID {
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
	if len(copies) != 2 || copies[0].Status != model.StorageUploadCopyStatusCommitted || copies[1].Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("copies after peer failure = %#v, want committed ingress and failed peer before repair runs", copies)
	}
	if repairTask.RefID != objID || repairTask.RefVersionID != versionID {
		t.Fatalf("repair task = %#v, want upload repair task for object %d/%s", repairTask, objID, versionID)
	}
	evict, err := env.repos.Tasks.ClaimReady(context.Background(), model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady(evict): %v", err)
	}
	if evict != nil {
		t.Fatalf("unexpected evict task for partial upload: %#v", evict)
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default())
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

func TestUploader_EnsureDatasetUnavailablePeerQueuesReplacement(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	fixture := seedReadableUploadWithPendingPeer(t, env)

	if err := env.repos.Uploads.MarkDataSetUnavailable(ctx, fixture.peer.ID, "provider dataset retired"); err != nil {
		t.Fatalf("MarkDataSetUnavailable peer: %v", err)
	}

	replacement := newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t))
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 1 {
			t.Fatalf("CreateContexts copies = %d, want 1", opts.Copies)
		}
		return []synapse.UploadContext{replacement}, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		return nil, fmt.Errorf("unexpected CreateContext opts for unavailable peer: %#v", opts)
	}

	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          fixture.objID,
		RefVersionID:   fixture.versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ensure_dataset:%d:1", fixture.versionID, fixture.upload.ID),
		Payload:        map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": 1, "transfer_method": string(model.StorageCopyTransferMethodPeerPull)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create ensure task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default())
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
	if len(copies) != 3 || copies[1].Status != model.StorageUploadCopyStatusFailed || copies[2].CopyIndex != 2 {
		t.Fatalf("copies after unavailable peer ensure = %#v, want failed original and replacement peer copy index 2", copies)
	}
}

func TestUploader_RepairPrepareCreatesReplacementForPeerDeficit(t *testing.T) {
	cases := []struct {
		name string
		mark func(context.Context, *testing.T, *testWorkerEnv, readableUploadWithPendingPeerFixture)
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
			name: "unavailable dataset",
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

			replacement := newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t))
			env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
				if opts.Copies != 1 {
					t.Fatalf("CreateContexts copies = %d, want 1", opts.Copies)
				}
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

			uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default())
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
			if len(copies) != 3 || copies[2].CopyIndex != 2 || copies[2].TransferMethod != model.StorageCopyTransferMethodPeerPull {
				t.Fatalf("copies after repair prepare = %#v, want replacement peer copy index 2", copies)
			}
		})
	}
}

func TestUploader_FailedNewPeerDataSetDoesNotRemainInBucketPool(t *testing.T) {
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
	replacement := newFakeUploadContext(sdktypes.NewBigInt(404), sdktypes.NewBigInt(4004), sdktypes.NewBigInt(5001), testCID(t))
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, failedCtx.providerID) {
			return failedCtx, nil
		}
		if createContextProviderIDEqual(opts, replacement.providerID) {
			return replacement, nil
		}
		if createContextDataSetIDEqual(opts, replacement.dataSetID) {
			return replacement, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		if opts.Copies != 1 {
			t.Fatalf("CreateContexts copies = %d, want 1", opts.Copies)
		}
		return []synapse.UploadContext{replacement}, nil
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, ensureTask.ID, 5*time.Second)

	repairTasks, _, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", "", 10, 0)
	if err != nil {
		t.Fatalf("list repair tasks: %v", err)
	}
	var repairTask *model.Task
	for i := range repairTasks {
		if repairTasks[i].RefVersionID == fixture.versionID && strings.Contains(repairTasks[i].IdempotencyKey, ":repair") {
			repairTask = &repairTasks[i]
			break
		}
	}
	if repairTask == nil {
		t.Fatalf("repair tasks = %#v, want one repair prepare task", repairTasks)
	}
	runWorkerUntilTask(t, env, uploader, repairTask.ID, 5*time.Second)

	bindings, err := env.repos.Uploads.ListDataSetBindings(ctx, fixture.upload.BucketID)
	if err != nil {
		t.Fatalf("ListDataSetBindings: %v", err)
	}
	if len(bindings) != 3 {
		t.Fatalf("dataset bindings = %#v, want three reusable bindings", bindings)
	}
	for _, binding := range bindings {
		if binding.ProviderID.String() == failedProviderID.String() || binding.Status == model.StorageDataSetStatusFailed {
			t.Fatalf("dataset bindings = %#v, want failed candidate removed from bucket pool", bindings)
		}
	}
	replacementBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, fixture.upload.BucketID, 2)
	if err != nil || replacementBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex replacement: binding=%v err=%v", replacementBinding, err)
	}
	if replacementBinding.ProviderID.String() != "404" {
		t.Fatalf("replacement provider = %s, want 404", replacementBinding.ProviderID.String())
	}

	copies, err := env.repos.Uploads.ListCopies(ctx, fixture.upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 3 || copies[2].CopyIndex != 2 || copies[2].ProviderID == nil || copies[2].ProviderID.String() != "404" {
		t.Fatalf("copies after repair = %#v, want replacement copy index 2 from provider 404", copies)
	}
	provenance, err := env.repos.Uploads.GetUploadProvenance(ctx, fixture.upload.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance: %v", err)
	}
	if len(provenance.Failures) != 1 || provenance.Failures[0].ProviderID == nil || provenance.Failures[0].ProviderID.String() != failedProviderID.String() {
		t.Fatalf("provenance failures = %#v, want failed provider attempt retained", provenance.Failures)
	}
}

func TestUploader_FailedNewPeerDataSetDiscardDoesNotDependOnFailureLog(t *testing.T) {
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

	baseUploads := env.repos.Uploads
	env.repos.Uploads = &appendFailureUploadRepo{
		StorageUploadRepository: baseUploads,
		err:                     errors.New("append failure unavailable"),
	}

	failedCtx := newFakeUploadContext(sdktypes.NewBigInt(303), sdktypes.NewBigInt(3003), sdktypes.NewBigInt(4001), testCID(t))
	failedCtx.waitErr = fmt.Errorf("wait rejected: %w", pdp.ErrTxRejected)
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, ensureTask.ID, 5*time.Second)

	binding, err := baseUploads.GetDataSetBindingByCopyIndex(ctx, fixture.upload.BucketID, 2)
	if err != nil {
		t.Fatalf("GetDataSetBindingByCopyIndex: %v", err)
	}
	if binding != nil {
		t.Fatalf("failed dataset candidate binding = %#v, want discarded even when AppendUploadFailure fails", binding)
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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

	uploader := worker.NewUploader(env.repos, env.cache, nil, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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

func TestUploader_StagedIngressStoreDataSetFailureReplans(t *testing.T) {
	writeBlockedErr := &storage.DataSetPDPPaymentTerminatedError{
		DataSetID:   sdktypes.NewBigInt(1001),
		PDPEndEpoch: 3778900,
	}
	cases := []struct {
		name              string
		storeErr          error
		wantBindingStatus model.StorageDataSetStatus
	}{
		{
			name:              "terminated dataset",
			storeErr:          errors.New("dataset terminated by provider"),
			wantBindingStatus: model.StorageDataSetStatusUnavailable,
		},
		{
			name:              "write-blocked dataset",
			storeErr:          writeBlockedErr,
			wantBindingStatus: model.StorageDataSetStatusDraining,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
				MaxRetries:     1,
				ScheduledAt:    time.Now(),
			}
			if err := env.repos.Tasks.Create(ctx, task); err != nil {
				t.Fatalf("create primary store task: %v", err)
			}
			primaryCtx := newFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
			primaryCtx.storeErr = tc.storeErr
			dataSetID := sdktypes.NewBigInt(1001)
			primaryCtx.boundDataSet = &dataSetID
			env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
				if createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)) {
					return primaryCtx, nil
				}
				return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
			}

			uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
			runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

			gotTask, _ := env.repos.Tasks.GetByID(ctx, task.ID)
			if gotTask.Status != model.TaskStatusCompleted {
				t.Fatalf("task status = %s, want completed", gotTask.Status)
			}
			gotVersion, _ := env.repos.Objects.GetVersionByID(ctx, versionID)
			if gotVersion.State != model.ObjectStateUploading || gotVersion.StorageUploadID != nil {
				t.Fatalf("version after dataset failure = state:%s upload:%v, want uploading without upload id", gotVersion.State, gotVersion.StorageUploadID)
			}
			gotUpload, err := env.repos.Uploads.GetByID(ctx, upload.ID)
			if err != nil || gotUpload == nil {
				t.Fatalf("GetByID(upload): upload=%v err=%v", gotUpload, err)
			}
			if gotUpload.Status != model.StorageUploadStatusFailed {
				t.Fatalf("upload status = %s, want failed", gotUpload.Status)
			}
			gotBinding, err := env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
			if err != nil || gotBinding == nil {
				t.Fatalf("GetDataSetBindingByCopyIndex: binding=%v err=%v", gotBinding, err)
			}
			if gotBinding.Status != tc.wantBindingStatus {
				t.Fatalf("binding status = %s, want %s", gotBinding.Status, tc.wantBindingStatus)
			}
			copyRow, err := env.repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
			if err != nil || copyRow == nil {
				t.Fatalf("GetUploadCopy: copy=%v err=%v", copyRow, err)
			}
			if copyRow.Status != model.StorageUploadCopyStatusFailed {
				t.Fatalf("ingress copy status = %s, want failed", copyRow.Status)
			}
			prepareTasks, prepareTotal, err := env.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", "", 10, 0)
			if err != nil {
				t.Fatalf("List prepare upload tasks: %v", err)
			}
			if prepareTotal != 1 || len(prepareTasks) != 1 {
				t.Fatalf("prepare_upload tasks total=%d tasks=%#v, want one replacement task", prepareTotal, prepareTasks)
			}
			if prepareTasks[0].RefID != objID || prepareTasks[0].RefVersionID != versionID {
				t.Fatalf("prepare_upload task = %#v, want replacement for object version", prepareTasks[0])
			}
			provenance, err := env.repos.Uploads.GetUploadProvenance(ctx, upload.ID)
			if err != nil {
				t.Fatalf("GetUploadProvenance: %v", err)
			}
			if provenance == nil || len(provenance.Failures) != 1 {
				t.Fatalf("provenance failures = %#v, want one ingress failure", provenance)
			}
			failure := provenance.Failures[0]
			if failure.ProviderID == nil || failure.ProviderID.String() != "101" || failure.TransferMethod != string(model.StorageCopyTransferMethodIngress) || failure.Stage == nil || *failure.Stage != "ingress store" || failure.ErrorMessage == nil || *failure.ErrorMessage != tc.storeErr.Error() {
				t.Fatalf("failure = %#v, want provider 101 ingress store dataset failure", failure)
			}
		})
	}
}

func TestUploader_SPUploadFailure_Retry(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return nil, errors.New("SP unavailable")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
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
	conflict := &model.Task{
		Type:           model.TaskTypeEvictCache,
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

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 10*time.Millisecond, slog.Default(), worker.WithTargetCopies(1))
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

func TestUploader_ZeroAvailableFunds_RequeuesTask(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				PaymentAccount: &synapse.PaymentAccountInfo{
					Funds:          big.NewInt(0),
					AvailableFunds: big.NewInt(0),
					LockupCurrent:  big.NewInt(0),
				},
			}, nil
		},
	}

	var uploadCalled atomic.Bool
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		uploadCalled.Store(true)
		return nil, errors.New("upload should not be called when available funds are zero")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	if uploadCalled.Load() {
		t.Fatal("upload should not be called when available funds are zero")
	}

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusScheduled {
		t.Errorf("expected task scheduled for retry, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "USDFC available funds = 0") {
		le := ""
		if got.LastError != nil {
			le = *got.LastError
		}
		t.Errorf("expected balance error message, got: %s", le)
	}

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
	if obj.State != model.ObjectStateUploading {
		t.Errorf("expected object to remain uploading for retry, got %s", obj.State)
	}
}

func TestUploader_StagedUploadZeroAvailableFundsRequeuesTask(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedStagedUploadTask(t, env, objID, versionID, 5)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				PaymentAccount: &synapse.PaymentAccountInfo{
					Funds:          big.NewInt(0),
					AvailableFunds: big.NewInt(0),
					LockupCurrent:  big.NewInt(0),
				},
			}, nil
		},
	}

	var createContextsCalled atomic.Bool
	env.storage.CreateContextsFunc = func(_ context.Context, _ *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		createContextsCalled.Store(true)
		return nil, errors.New("CreateContexts should not be called when available funds are zero")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	if createContextsCalled.Load() {
		t.Fatal("CreateContexts should not be called when available funds are zero")
	}

	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(context.Background(), versionID)
	if err != nil {
		t.Fatalf("FindLatestUploadBySourceVersion: %v", err)
	}
	if upload != nil {
		t.Fatalf("expected no upload attempt when available funds are zero, got %d", upload.ID)
	}

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusScheduled {
		t.Errorf("expected task scheduled for retry, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "USDFC available funds = 0") {
		le := ""
		if got.LastError != nil {
			le = *got.LastError
		}
		t.Errorf("expected balance error message, got: %s", le)
	}
}

func TestUploader_LockedAvailableFunds_RequeuesTask(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				PaymentAccount: &synapse.PaymentAccountInfo{
					Funds:          big.NewInt(100),
					AvailableFunds: big.NewInt(0),
					LockupCurrent:  big.NewInt(100),
				},
			}, nil
		},
	}

	var uploadCalled atomic.Bool
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		uploadCalled.Store(true)
		return nil, errors.New("upload should not be called when funds are locked")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	if uploadCalled.Load() {
		t.Fatal("upload should not be called when available funds are zero")
	}

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusScheduled {
		t.Errorf("expected task scheduled for retry, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
}

func TestUploader_AvailableFundsFallback_RequeuesTask(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				PaymentAccount: &synapse.PaymentAccountInfo{
					Funds:         big.NewInt(100),
					LockupCurrent: big.NewInt(100),
				},
			}, nil
		},
	}

	var uploadCalled atomic.Bool
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		uploadCalled.Store(true)
		return nil, errors.New("upload should not be called when fallback available funds are zero")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	if uploadCalled.Load() {
		t.Fatal("upload should not be called when fallback available funds are zero")
	}

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusScheduled {
		t.Errorf("expected task scheduled for retry, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
}

func TestUploader_NegativeAvailableFunds_RequeuesTask(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				PaymentAccount: &synapse.PaymentAccountInfo{
					Funds:          big.NewInt(100),
					AvailableFunds: big.NewInt(-1),
					LockupCurrent:  big.NewInt(101),
				},
			}, nil
		},
	}

	var uploadCalled atomic.Bool
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		uploadCalled.Store(true)
		return nil, errors.New("upload should not be called when available funds are negative")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	if uploadCalled.Load() {
		t.Fatal("upload should not be called when available funds are negative")
	}

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusScheduled {
		t.Errorf("expected task scheduled for retry, got %s", got.Status)
	}
}

func TestUploader_ZeroAvailableFunds_ExhaustedRetryRemainsRecoverable(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 1, 0)

	var walletCalls atomic.Int32
	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			if walletCalls.Add(1) == 1 {
				return &synapse.WalletInfo{
					PaymentAccount: &synapse.PaymentAccountInfo{
						Funds:          big.NewInt(0),
						AvailableFunds: big.NewInt(0),
						LockupCurrent:  big.NewInt(0),
					},
				}, nil
			}
			return &synapse.WalletInfo{
				PaymentAccount: &synapse.PaymentAccountInfo{
					Funds:          big.NewInt(100),
					AvailableFunds: big.NewInt(100),
					LockupCurrent:  big.NewInt(0),
				},
			}, nil
		},
	}

	var uploadContexts []synapse.UploadContext
	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		uploadContexts = newFakeUploadContexts(t, opts.Copies, 0)
		return uploadContexts, nil
	}
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		for _, uploadCtx := range uploadContexts {
			fakeCtx := uploadCtx.(*fakeUploadContext)
			if createContextProviderIDEqual(opts, fakeCtx.providerID) {
				return fakeCtx, nil
			}
			if createContextDataSetIDEqual(opts, fakeCtx.dataSetID) {
				return fakeCtx, nil
			}
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusExhausted {
		t.Fatalf("expected task exhausted, got %s", got.Status)
	}
	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
	if obj.State != model.ObjectStateUploading {
		t.Fatalf("expected object to remain uploading for manual retry, got %s", obj.State)
	}

	if err := env.repos.Tasks.RetryExhausted(context.Background(), task.ID); err != nil {
		t.Fatalf("retrying exhausted task: %v", err)
	}
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ = env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("expected retried task completed, got %s", got.Status)
	}
	upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(context.Background(), versionID)
	if err != nil || upload == nil {
		t.Fatalf("expected retried prepare to create upload, upload=%v err=%v", upload, err)
	}
}

func TestUploader_WalletError_ProceedsWithUpload(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return nil, errors.New("RPC timeout")
		},
	}

	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return newFakeUploadContexts(t, opts.Copies, 0), nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (wallet error should not block upload), got %s", got.Status)
	}
	if upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(context.Background(), versionID); err != nil || upload == nil {
		t.Fatalf("expected upload attempt after wallet error, upload=%v err=%v", upload, err)
	}
}

func TestUploader_WalletNilInfo_NoPanic(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return nil, nil // (nil, nil) — edge case
		},
	}

	env.storage.CreateContextsFunc = func(_ context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
		return newFakeUploadContexts(t, opts.Copies, 0), nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (nil info should not panic), got %s", got.Status)
	}
	if upload, err := env.repos.Uploads.FindLatestUploadBySourceVersion(context.Background(), versionID); err != nil || upload == nil {
		t.Fatalf("expected upload attempt after nil wallet info, upload=%v err=%v", upload, err)
	}
}
