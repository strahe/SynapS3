package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
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

func validUploadCopy(retrievalURL string) storage.CopyResult {
	return storage.CopyResult{
		ProviderID:   sdktypes.ProviderID(101),
		DataSetID:    sdktypes.DataSetID(123),
		PieceID:      sdktypes.PieceID(2001),
		Role:         storage.CopyRolePrimary,
		RetrievalURL: retrievalURL,
	}
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

// seedTask creates a pending task of the given type.
func seedTask(t *testing.T, env *testWorkerEnv, taskType model.TaskType, refID int64, versionID string, maxRetries, retryCount int) *model.Task {
	t.Helper()
	ctx := context.Background()
	task := &model.Task{
		Type:           taskType,
		RefType:        "object",
		RefID:          refID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("%s:%s", taskType, versionID),
		Status:         model.TaskStatusPending,
		RetryCount:     retryCount,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("creating task: %v", err)
	}
	return task
}

// runWorkerUntilTask runs a worker and waits until the given task
// leaves pending/running status, or times out.
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
			if task != nil && task.Status != model.TaskStatusPending && task.Status != model.TaskStatusRunning {
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
			if task != nil && task.RetryCount >= retryCount {
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

func uploadResultForCall(t *testing.T, call int32) *storage.UploadResult {
	t.Helper()
	id := uint64(1000 + call)
	return &storage.UploadResult{
		PieceCID: testCID(t),
		Size:     11,
		Complete: true,
		Copies: []storage.CopyResult{{
			ProviderID:   sdktypes.ProviderID(id),
			DataSetID:    sdktypes.DataSetID(id),
			PieceID:      sdktypes.PieceID(id),
			Role:         storage.CopyRolePrimary,
			RetrievalURL: fmt.Sprintf("https://provider.example/pieces/%d", call),
		}},
	}
}

func TestUploader_WaitsPollIntervalBeforeInitialClaim(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	uploadStarted := make(chan struct{})
	var closeStarted sync.Once
	var uploadCalls atomic.Int32
	pollInterval := 100 * time.Millisecond

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		call := uploadCalls.Add(1)
		closeStarted.Do(func() { close(uploadStarted) })
		return uploadResultForCall(t, call), nil
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
	firstTask := seedTask(t, env, model.TaskTypeUpload, firstObjID, firstVersionID, 5, 0)

	firstUploadEntered := make(chan struct{})
	releaseFirstUpload := make(chan struct{})
	var releaseOnce sync.Once
	var uploadCalls atomic.Int32

	env.storage.UploadFunc = func(ctx context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		call := uploadCalls.Add(1)
		if call == 1 {
			close(firstUploadEntered)
			select {
			case <-releaseFirstUpload:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return uploadResultForCall(t, call), nil
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
	secondTask := seedTask(t, env, model.TaskTypeUpload, secondObjID, secondVersionID, 5, 0)

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
	_ = seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	uploadEntered := make(chan struct{})
	releaseUpload := make(chan struct{})
	var releaseOnce sync.Once
	var uploadCalls atomic.Int32
	pollInterval := 20 * time.Millisecond

	env.storage.UploadFunc = func(ctx context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		call := uploadCalls.Add(1)
		close(uploadEntered)
		select {
		case <-releaseUpload:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return uploadResultForCall(t, call), nil
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

func TestUploader_HappyPath(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies: []storage.CopyResult{
				{
					Role:       storage.CopyRoleSecondary,
					ProviderID: sdktypes.ProviderID(202),
					DataSetID:  sdktypes.DataSetID(777),
					PieceID:    sdktypes.PieceID(3001),
				},
				{
					ProviderID:   sdktypes.ProviderID(101),
					Role:         storage.CopyRolePrimary,
					DataSetID:    sdktypes.DataSetID(123),
					PieceID:      sdktypes.PieceID(2001),
					RetrievalURL: "https://provider.example/pieces/1",
				},
			},
			FailedAttempts: []storage.FailedAttempt{{
				ProviderID: sdktypes.ProviderID(303),
				Role:       storage.CopyRoleSecondary,
				Stage:      storage.CopyStagePull,
				Err:        errors.New("pull timeout"),
				Explicit:   true,
			}},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default(), worker.WithEvictMaxRetries(7))
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()

	got, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	obj, err := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if obj.State != model.ObjectStateStored {
		t.Errorf("expected object state stored, got %s", obj.State)
	}
	if obj.PieceCID == nil || *obj.PieceCID != pieceCID.String() {
		t.Errorf("expected PieceCID %s, got %v", pieceCID.String(), obj.PieceCID)
	}
	if !obj.InFilecoin {
		t.Error("expected object filecoin location to be true after upload")
	}
	if obj.StorageUploadID == nil {
		t.Fatal("storage_upload_id is nil, want accepted upload")
	}
	copies, err := env.repos.Uploads.ListCopies(ctx, *obj.StorageUploadID)
	if err != nil {
		t.Fatalf("list upload copies: %v", err)
	}
	if len(copies) != 2 {
		t.Fatalf("copy count = %d, want 2", len(copies))
	}
	var primary *model.StorageUploadCopy
	var secondary *model.StorageUploadCopy
	for i := range copies {
		if copies[i].Role == string(storage.CopyRolePrimary) {
			primary = &copies[i]
		}
		if copies[i].Role == string(storage.CopyRoleSecondary) {
			secondary = &copies[i]
		}
	}
	if primary == nil || primary.ProviderID == nil || *primary.ProviderID != "101" || primary.DataSetID == nil || *primary.DataSetID != "123" || primary.PieceID == nil || *primary.PieceID != "2001" || primary.RetrievalURL == nil || *primary.RetrievalURL != "https://provider.example/pieces/1" {
		t.Fatalf("primary copy = %#v, want persisted provider/data set/piece/retrieval URL", primary)
	}
	if secondary == nil || secondary.ProviderID == nil || *secondary.ProviderID != "202" || secondary.DataSetID == nil || *secondary.DataSetID != "777" || secondary.PieceID == nil || *secondary.PieceID != "3001" {
		t.Fatalf("secondary copy = %#v, want independent provider/data set/piece", secondary)
	}
	var failures []model.StorageUploadFailure
	if err := env.db.NewSelect().Model(&failures).Where("upload_id = ?", *obj.StorageUploadID).Scan(ctx); err != nil {
		t.Fatalf("list upload failures: %v", err)
	}
	if len(failures) != 1 || failures[0].ProviderID == nil || *failures[0].ProviderID != "303" || failures[0].Stage == nil || *failures[0].Stage != string(storage.CopyStagePull) || failures[0].ErrorMessage == nil || *failures[0].ErrorMessage != "pull timeout" || !failures[0].Explicit {
		t.Fatalf("upload failures = %#v, want persisted failed attempt", failures)
	}
	// With autoEvict=true, an evict_cache task should be created
	evictTask, err := env.repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict_cache: %v", err)
	}
	if evictTask == nil {
		t.Fatal("expected evict_cache task to exist")
	}
	if evictTask.RefID != objID || evictTask.RefVersionID != versionID {
		t.Errorf("evict task refs mismatch: got refID=%d version=%s, want %d/%s",
			evictTask.RefID, evictTask.RefVersionID, objID, versionID)
	}
	if evictTask.MaxRetries != 7 {
		t.Fatalf("evict task MaxRetries = %d, want 7", evictTask.MaxRetries)
	}
}

func TestUploader_StoresVersionsFollowingActiveUpload(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, leaderVersionID := seedCachedObject(t, env)
	ctx := context.Background()

	leader, err := env.repos.Objects.GetVersionByID(ctx, leaderVersionID)
	if err != nil || leader == nil {
		t.Fatalf("leader version: version=%v err=%v", leader, err)
	}

	followerVersionID := model.NewVersionID()
	follower := &model.ObjectVersion{
		VersionID:   followerVersionID,
		BucketID:    bucket.ID,
		Key:         leader.Key,
		Size:        leader.Size,
		ETag:        leader.ETag,
		Checksum:    leader.Checksum,
		ContentType: leader.ContentType,
		CacheKey:    ".versions/" + followerVersionID,
		State:       model.ObjectStateUploading,
	}
	followerObjID, err := env.repos.Objects.CreateVersionAndSetCurrent(ctx, follower)
	if err != nil {
		t.Fatalf("creating follower version: %v", err)
	}
	if followerObjID != objID {
		t.Fatalf("follower object id = %d, want %d", followerObjID, objID)
	}

	task := seedTask(t, env, model.TaskTypeUpload, objID, leaderVersionID, 5, 0)
	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     leader.Size,
			Complete: true,
			Copies:   []storage.CopyResult{validUploadCopy("https://provider.example/pieces/shared")},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	for _, versionID := range []string{leaderVersionID, followerVersionID} {
		got, err := env.repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || got == nil {
			t.Fatalf("version %s: got=%v err=%v", versionID, got, err)
		}
		if got.State != model.ObjectStateStored {
			t.Fatalf("version %s state = %s, want stored", versionID, got.State)
		}
		if got.PieceCID == nil || *got.PieceCID != pieceCID.String() {
			t.Fatalf("version %s piece = %v, want %s", versionID, got.PieceCID, pieceCID.String())
		}
		if !got.InFilecoin {
			t.Fatalf("version %s in_filecoin = false, want true", versionID)
		}
	}

	current, err := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil {
		t.Fatalf("get current object: %v", err)
	}
	if current.VersionID != followerVersionID || current.State != model.ObjectStateStored {
		t.Fatalf("current object = version:%s state:%s, want %s stored", current.VersionID, current.State, followerVersionID)
	}
	if !current.InFilecoin {
		t.Fatal("current object in_filecoin = false, want true")
	}

	evictTasks, _, err := env.repos.Tasks.List(ctx, string(model.TaskTypeEvictCache), "", 10, 0)
	if err != nil {
		t.Fatalf("list evict tasks: %v", err)
	}
	if len(evictTasks) != 2 {
		t.Fatalf("evict task count = %d, want 2", len(evictTasks))
	}
	seen := map[string]bool{}
	for _, task := range evictTasks {
		seen[task.RefVersionID] = true
	}
	for _, versionID := range []string{leaderVersionID, followerVersionID} {
		if !seen[versionID] {
			t.Fatalf("missing evict task for version %s", versionID)
		}
	}
}

func TestUploader_TerminalUploadFailureFailsVersionsFollowingActiveUpload(t *testing.T) {
	env := newTestWorkerEnv(t)
	bucket, objID, leaderVersionID := seedCachedObject(t, env)
	ctx := context.Background()

	leader, err := env.repos.Objects.GetVersionByID(ctx, leaderVersionID)
	if err != nil || leader == nil {
		t.Fatalf("leader version: version=%v err=%v", leader, err)
	}

	followerVersionID := model.NewVersionID()
	follower := &model.ObjectVersion{
		VersionID:   followerVersionID,
		BucketID:    bucket.ID,
		Key:         leader.Key,
		Size:        leader.Size,
		ETag:        leader.ETag,
		Checksum:    leader.Checksum,
		ContentType: leader.ContentType,
		CacheKey:    ".versions/" + followerVersionID,
		State:       model.ObjectStateUploading,
	}
	if _, err := env.repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("creating follower version: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeUpload, objID, leaderVersionID, 1, 0)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return nil, errors.New("SP permanent failure")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if gotTask.Status != model.TaskStatusDeadLetter {
		t.Fatalf("task status = %s, want dead_letter", gotTask.Status)
	}

	for _, versionID := range []string{leaderVersionID, followerVersionID} {
		got, err := env.repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || got == nil {
			t.Fatalf("version %s: got=%v err=%v", versionID, got, err)
		}
		if got.State != model.ObjectStateFailed {
			t.Fatalf("version %s state = %s, want failed", versionID, got.State)
		}
		if got.FailedAtState == nil || *got.FailedAtState != model.ObjectStateUploading {
			t.Fatalf("version %s FailedAtState = %v, want uploading", versionID, got.FailedAtState)
		}
	}

	current, err := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil {
		t.Fatalf("get current object: %v", err)
	}
	if current.VersionID != followerVersionID || current.State != model.ObjectStateFailed {
		t.Fatalf("current object = version:%s state:%s, want %s failed", current.VersionID, current.State, followerVersionID)
	}
}

func TestUploader_PartialSuccessDoesNotMarkObjectStored(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 1, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID:        pieceCID,
			Size:            11,
			RequestedCopies: 2,
			Complete:        false,
			Copies: []storage.CopyResult{
				{RetrievalURL: "https://provider.example/pieces/1"},
			},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if gotTask.Status != model.TaskStatusDeadLetter {
		t.Fatalf("expected task dead-lettered, got %s", gotTask.Status)
	}

	obj, err := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if obj.State != model.ObjectStateFailed {
		t.Fatalf("expected object failed, got %s", obj.State)
	}

	evictTask, err := env.repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict_cache: %v", err)
	}
	if evictTask != nil {
		t.Fatal("did not expect evict_cache task for partial upload")
	}
}

func TestUploader_CompleteResultWithoutRetrievalURLDoesNotBindObject(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 1, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID:        pieceCID,
			Size:            11,
			RequestedCopies: 1,
			Complete:        true,
			Copies: []storage.CopyResult{{
				ProviderID: sdktypes.ProviderID(101),
				DataSetID:  sdktypes.DataSetID(123),
				PieceID:    sdktypes.PieceID(2001),
				Role:       storage.CopyRolePrimary,
			}},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	obj, err := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil || obj == nil {
		t.Fatalf("get object: obj=%v err=%v", obj, err)
	}
	if obj.State != model.ObjectStateFailed {
		t.Fatalf("object state = %s, want failed", obj.State)
	}
	if obj.StorageUploadID != nil || obj.InFilecoin {
		t.Fatalf("object storage = upload:%v in_filecoin:%v, want unbound", obj.StorageUploadID, obj.InFilecoin)
	}
	var uploads []model.StorageUpload
	if err := env.db.NewSelect().Model(&uploads).Where("source_version_id = ?", versionID).Scan(ctx); err != nil {
		t.Fatalf("list storage uploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].Status != model.StorageUploadStatusRejected {
		t.Fatalf("upload attempts = %#v, want one rejected attempt", uploads)
	}
}

func TestUploader_CompleteResultWithZeroCopyIdentifiersDoesNotBindObject(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 1, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID:        pieceCID,
			Size:            11,
			RequestedCopies: 1,
			Complete:        true,
			Copies: []storage.CopyResult{{
				Role:         storage.CopyRolePrimary,
				RetrievalURL: "https://provider.example/pieces/zero-id",
			}},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	obj, err := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil || obj == nil {
		t.Fatalf("get object: obj=%v err=%v", obj, err)
	}
	if obj.State != model.ObjectStateFailed {
		t.Fatalf("object state = %s, want failed", obj.State)
	}
	if obj.StorageUploadID != nil || obj.InFilecoin {
		t.Fatalf("object storage = upload:%v in_filecoin:%v, want unbound", obj.StorageUploadID, obj.InFilecoin)
	}
	var copies []model.StorageUploadCopy
	if err := env.db.NewSelect().Model(&copies).Where("upload_id IN (SELECT id FROM storage_uploads WHERE source_version_id = ?)", versionID).Scan(ctx); err != nil {
		t.Fatalf("list storage upload copies: %v", err)
	}
	if len(copies) != 1 || copies[0].ProviderID != nil || copies[0].DataSetID != nil || copies[0].PieceID != nil {
		t.Fatalf("copy identifiers = %#v, want zero IDs stored as missing", copies)
	}
	var uploads []model.StorageUpload
	if err := env.db.NewSelect().Model(&uploads).Where("source_version_id = ?", versionID).Scan(ctx); err != nil {
		t.Fatalf("list storage uploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].Status != model.StorageUploadStatusRejected {
		t.Fatalf("upload attempts = %#v, want one rejected attempt", uploads)
	}
}

func TestUploader_AcceptFailureRetryDoesNotUploadAgain(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	uploads := &acceptFailingUploadRepo{StorageUploadRepository: env.repos.Uploads}
	uploads.failAccept.Store(true)
	env.repos.Uploads = uploads

	pieceCID := testCID(t)
	var uploadCalls int32
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		atomic.AddInt32(&uploadCalls, 1)
		return &storage.UploadResult{
			PieceCID:        pieceCID,
			Size:            11,
			RequestedCopies: 1,
			Complete:        true,
			Copies: []storage.CopyResult{{
				ProviderID:   sdktypes.ProviderID(101),
				DataSetID:    sdktypes.DataSetID(123),
				PieceID:      sdktypes.PieceID(2001),
				Role:         storage.CopyRolePrimary,
				RetrievalURL: "https://provider.example/pieces/1",
			}},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, false, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	if got := atomic.LoadInt32(&uploadCalls); got != 1 {
		t.Fatalf("upload calls after accept failure = %d, want 1", got)
	}
	gotTask, err := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("get task after accept failure: %v", err)
	}
	if gotTask.Status != model.TaskStatusPending {
		t.Fatalf("task status after accept failure = %s, want pending", gotTask.Status)
	}
	attempt, err := env.repos.Uploads.FindAcceptableUploadAttempt(context.Background(), task.ID, versionID)
	if err != nil || attempt == nil {
		t.Fatalf("acceptable upload after accept failure = %v err=%v", attempt, err)
	}
	if attempt.AcceptError == nil || *attempt.AcceptError == "" {
		t.Fatalf("accept_error = %v, want recorded failure", attempt.AcceptError)
	}
	if _, err := env.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("id = ?", task.ID).
		Exec(context.Background()); err != nil {
		t.Fatalf("reschedule task retry: %v", err)
	}

	uploads.failAccept.Store(false)
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	if got := atomic.LoadInt32(&uploadCalls); got != 1 {
		t.Fatalf("upload calls after accept retry = %d, want 1", got)
	}
	gotTask, err = env.repos.Tasks.GetByID(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("get task after accept retry: %v", err)
	}
	if gotTask.Status != model.TaskStatusCompleted {
		t.Fatalf("task status after accept retry = %s, want completed", gotTask.Status)
	}
	obj, err := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
	if err != nil || obj == nil {
		t.Fatalf("get object after accept retry: obj=%v err=%v", obj, err)
	}
	if obj.State != model.ObjectStateStored || obj.StorageUploadID == nil {
		t.Fatalf("object after accept retry = state:%s upload:%v, want stored with upload", obj.State, obj.StorageUploadID)
	}
}

type acceptFailingUploadRepo struct {
	repository.StorageUploadRepository
	failAccept atomic.Bool
}

func (r *acceptFailingUploadRepo) AcceptCompleteUploadForContent(ctx context.Context, input repository.AcceptCompleteUploadInput) ([]repository.ObjectVersionRef, error) {
	if r.failAccept.Load() {
		return nil, errors.New("accept storage upload")
	}
	return r.StorageUploadRepository.AcceptCompleteUploadForContent(ctx, input)
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
		Status:         model.TaskStatusPending,
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

func TestUploader_CacheReadFailure(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, errors.New("disk I/O error")
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
	// RetryCount at max-1 so this attempt triggers dead-letter terminal path.
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 4)

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		t.Error("upload should not be called after cache read failure")
		return nil, errors.New("should not be called")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusDeadLetter {
		t.Errorf("expected task dead_letter, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if obj.State != model.ObjectStateFailed {
		t.Errorf("expected object state failed, got %s", obj.State)
	}
}

func TestUploader_CacheMissMarksCacheLocationAbsent(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		t.Error("upload should not be called after cache miss")
		return nil, errors.New("should not be called")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTaskRetryCount(t, env, uploader, task.ID, 1, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
	if obj.State != model.ObjectStateUploading {
		t.Errorf("expected object state uploading after cache miss retry, got %s", obj.State)
	}
	if obj.InCache {
		t.Error("expected current object cache location to be false after cache miss")
	}
	version, _ := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if version.InCache {
		t.Error("expected version cache location to be false after cache miss")
	}
}

func TestUploader_SPUploadFailure_Retry(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return nil, errors.New("SP unavailable")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	// After Fail+Requeue, task goes back to pending with future ScheduledAt.
	// Use a short Run since runWorkerUntilTask won't see it leave pending.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = uploader.Run(ctx)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
}

func TestUploader_SPUploadFailure_MaxRetries(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)

	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 4)

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return nil, errors.New("SP permanent failure")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusDeadLetter {
		t.Errorf("expected task dead_letter, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if obj.State != model.ObjectStateFailed {
		t.Errorf("expected object state failed, got %s", obj.State)
	}
}

func TestUploader_EvictTaskIdempotency(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies:   []storage.CopyResult{validUploadCopy("https://provider.example/pieces/1")},
		}, nil
	}

	// Pre-create conflicting evict_cache task to trigger idempotency collision
	conflict := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("evict_cache:%s", versionID),
		Status:         model.TaskStatusPending,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), conflict); err != nil {
		t.Fatalf("creating conflict task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

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
				USDFCAccount: &synapse.TokenAccountInfo{
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
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
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

func TestUploader_LockedAvailableFunds_RequeuesTask(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				USDFCAccount: &synapse.TokenAccountInfo{
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
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
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
				USDFCAccount: &synapse.TokenAccountInfo{
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
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
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
				USDFCAccount: &synapse.TokenAccountInfo{
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
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
	}
}

func TestUploader_ZeroAvailableFunds_DeadLetterRetryRemainsRecoverable(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, versionID := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, versionID, 1, 0)

	var walletCalls atomic.Int32
	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			if walletCalls.Add(1) == 1 {
				return &synapse.WalletInfo{
					USDFCAccount: &synapse.TokenAccountInfo{
						Funds:          big.NewInt(0),
						AvailableFunds: big.NewInt(0),
						LockupCurrent:  big.NewInt(0),
					},
				}, nil
			}
			return &synapse.WalletInfo{
				USDFCAccount: &synapse.TokenAccountInfo{
					Funds:          big.NewInt(100),
					AvailableFunds: big.NewInt(100),
					LockupCurrent:  big.NewInt(0),
				},
			}, nil
		},
	}

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID:        pieceCID,
			Complete:        true,
			RequestedCopies: 1,
			Copies: []storage.CopyResult{
				validUploadCopy("https://sp.example/piece"),
			},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusDeadLetter {
		t.Fatalf("expected task dead_letter, got %s", got.Status)
	}
	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
	if obj.State != model.ObjectStateUploading {
		t.Fatalf("expected object to remain uploading for manual retry, got %s", obj.State)
	}

	if err := env.repos.Tasks.RetryDeadLetter(context.Background(), task.ID); err != nil {
		t.Fatalf("retrying dead-letter task: %v", err)
	}
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ = env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("expected retried task completed, got %s", got.Status)
	}
	obj, _ = env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
	if obj.State != model.ObjectStateStored {
		t.Fatalf("expected retried object stored, got %s", obj.State)
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

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies:   []storage.CopyResult{validUploadCopy("https://provider.example/pieces/1")},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (wallet error should not block upload), got %s", got.Status)
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

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies:   []storage.CopyResult{validUploadCopy("https://provider.example/pieces/1")},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (nil info should not panic), got %s", got.Status)
	}
}
