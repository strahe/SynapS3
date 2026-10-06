package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

// sendOutcome is what the provider does with one add-pieces submission.
type sendOutcome int

const (
	// sendAccept lands every piece at consecutive IDs and reports it.
	sendAccept sendOutcome = iota
	// sendRefuse answers 4xx before sending any transaction.
	sendRefuse
	// sendLost loses the reply; nothing landed.
	sendLost
	// sendLandedLost lands every piece but loses the reply.
	sendLandedLost
	// sendSpentElsewhere reports a transaction, but the chain spent the
	// request's nonce on another data set.
	sendSpentElsewhere
	// sendAcceptUnlanded reports a transaction that never lands.
	sendAcceptUnlanded
)

// registrationProvider is a provider that registers whole requests. Each send
// follows the next scripted outcome, and accepts once the script runs out.
type registrationProvider struct {
	t      *testing.T
	target *testutil.MockStorageTarget
	nonces *testutil.MockCommitNonces
	mu     sync.Mutex
	script []sendOutcome
	sends  [][]cid.Cid
	extras [][]byte
	nextID uint64
	landed map[string][]sdktypes.BigInt
	// spendFirstNonce makes the chain spend the first signed nonce on another
	// data set before it is ever sent.
	spendFirstNonce bool
	// maxSigned refuses to sign requests of more pieces, as the SDK does with
	// one that does not fit an add-pieces message.
	maxSigned int
	// statusDown makes the provider's status endpoint fail.
	statusDown bool
	signed     int
	dataSet    sdktypes.BigInt
}

func newRegistrationProvider(t *testing.T, providerID, dataSetID, clientID sdktypes.BigInt, nonces *testutil.MockCommitNonces) *registrationProvider {
	p := &registrationProvider{
		t: t, nonces: nonces, nextID: 100,
		landed: make(map[string][]sdktypes.BigInt), dataSet: dataSetID,
	}
	p.target = testutil.NewMockDataSetTarget(providerID, dataSetID, nil)
	p.target.ClientDataSetIDValue = clientID
	var nonce uint64 = 40
	p.target.PresignForCommitFunc = func(_ context.Context, pieces []storage.PieceInput) ([]byte, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.maxSigned > 0 && len(pieces) > p.maxSigned {
			return nil, fmt.Errorf("%w: %d pieces do not fit one request", storage.ErrInvalidArgument, len(pieces))
		}
		nonce++
		extra := testutil.CommitExtraData(nonce)
		p.signed++
		if p.spendFirstNonce && p.signed == 1 {
			p.nonces.ConsumeRequest(extra, sdktypes.NewBigInt(9999), sdktypes.NewBigInt(1), pieceCIDsOf(pieces))
		}
		return extra, nil
	}
	p.target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		pieces := pieceCIDsOf(request.Pieces)
		p.sends = append(p.sends, pieces)
		p.extras = append(p.extras, append([]byte(nil), request.ExtraData...))
		outcome := sendAccept
		if len(p.script) > 0 {
			outcome, p.script = p.script[0], p.script[1:]
		}
		switch outcome {
		case sendRefuse:
			return nil, &pdp.HTTPError{StatusCode: 400, Body: "try again"}
		case sendLost:
			return nil, &pdp.HTTPError{StatusCode: 502, Body: "bad gateway"}
		case sendSpentElsewhere:
			p.nonces.ConsumeRequest(request.ExtraData, sdktypes.NewBigInt(9999), sdktypes.NewBigInt(1), pieces)
		case sendAcceptUnlanded:
		default:
			p.nonces.ConsumeRequest(request.ExtraData, p.dataSet, sdktypes.NewBigInt(p.nextID), pieces)
		}
		tx := fmt.Sprintf("0xtx-%d", len(p.sends))
		ids := make([]sdktypes.BigInt, len(pieces))
		for i := range ids {
			ids[i] = sdktypes.NewBigInt(p.nextID + uint64(i))
		}
		p.nextID += uint64(len(pieces))
		p.landed[tx] = ids
		if outcome == sendLandedLost {
			return nil, &pdp.HTTPError{StatusCode: 502, Body: "bad gateway"}
		}
		ref, _ := p.target.DataSetRef()
		submission := storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: tx, StatusURL: "https://provider.example/status/" + tx,
			DataSet: &ref, PieceCIDs: pieces,
		}
		request.OnSubmitted(submission)
		return &submission, nil
	}
	p.target.GetCommitStatusFunc = func(_ context.Context, statusURL string) (*storage.CommitStatus, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.statusDown {
			return nil, &pdp.HTTPError{StatusCode: 503, Body: "unavailable"}
		}
		tx := statusURL[len("https://provider.example/status/"):]
		ref, _ := p.target.DataSetRef()
		return &storage.CommitStatus{
			Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed, TransactionID: tx,
			DataSet: &ref, PieceIDs: p.landed[tx],
		}, nil
	}
	return p
}

func pieceCIDsOf(pieces []storage.PieceInput) []cid.Cid {
	out := make([]cid.Cid, len(pieces))
	for i, piece := range pieces {
		out[i] = piece.PieceCID
	}
	return out
}

func (p *registrationProvider) sent() ([][]cid.Cid, [][]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]cid.Cid(nil), p.sends...), append([][]byte(nil), p.extras...)
}

// sameSends reports whether every send carried the first one's pieces, in
// order, under the first one's signature.
func sameSends(sends [][]cid.Cid, extras [][]byte) bool {
	for i := range sends {
		if len(sends[i]) != len(sends[0]) || !bytes.Equal(extras[i], extras[0]) {
			return false
		}
		for j := range sends[i] {
			if !sends[i][j].Equals(sends[0][j]) {
				return false
			}
		}
	}
	return true
}

// wakeCommitTasks makes sleeping registration tasks runnable, except one
// backing off after a refusal, so a test does not wait out the poll.
func wakeCommitTasks(t *testing.T, runtime handlerTestRuntime) {
	t.Helper()
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE type = ? AND status = 'pending' AND (wait_reason IS NULL OR wait_reason <> ?)`,
		time.Now().Add(-time.Second), model.TaskTypeStorageCommit, storagecommit.ProviderRejectedWaitReason).Exec(t.Context()); err != nil {
		t.Fatalf("wake registration tasks: %v", err)
	}
}

// waitForCommitTask waits for the task, waking registration tasks whenever
// they sleep.
func waitForCommitTask(t *testing.T, runtime handlerTestRuntime, id int64, predicate func(*model.Task) bool) *model.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		task, err := runtime.repos.Tasks.GetByID(t.Context(), id)
		if err != nil {
			t.Fatalf("load task %d: %v", id, err)
		}
		if predicate(task) {
			return task
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %d did not reach the expected state: %#v", id, task)
		}
		wakeCommitTasks(t, runtime)
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForCommitted waits until every copy is committed, waking registration
// tasks meanwhile.
func waitForCommitted(t *testing.T, runtime handlerTestRuntime, copies []*model.StorageCopy) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		committed := 0
		for _, copyRow := range copies {
			current, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil {
				t.Fatalf("load copy %d: %v", copyRow.ID, err)
			}
			if current.Status == model.StorageCopyStatusCommitted {
				committed++
			}
		}
		if committed == len(copies) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d copies committed", committed, len(copies))
		}
		wakeCommitTasks(t, runtime)
		time.Sleep(20 * time.Millisecond)
	}
}

type registrationFixture struct {
	runtime  handlerTestRuntime
	provider *registrationProvider
	dataSet  *model.StorageDataSet
	copies   []*model.StorageCopy
}

// newRegistrationFixture seeds a data set and n contents whose single copies
// finished transferring to it.
func newRegistrationFixture(t *testing.T, n int, parked synapse.ParkedPieceChecker, maxWait time.Duration, configure ...func(*handlerRuntimeOptions)) registrationFixture {
	t.Helper()
	ctx := t.Context()
	storageClient := &testutil.MockStorageClient{}
	nonces := &testutil.MockCommitNonces{}
	options := handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone, commitNonces: nonces, parkedPieces: parked,
		commitMaxWait: maxWait,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	}
	for _, configure := range configure {
		configure(&options)
	}
	runtime := newHandlerTestRuntime(t, options)
	sequence := storedObjectSequence.Add(1)
	bucket := &model.Bucket{Name: fmt.Sprintf("registration-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	providerID := testOnChainID(t, 5000+sequence)
	dataSetID := testOnChainID(t, 6000+sequence)
	clientID := testOnChainID(t, 7000+sequence)
	var copies []*model.StorageCopy
	var binding *model.StorageDataSet
	for i := range n {
		content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
			BucketID: bucket.ID, ContentSize: 128,
			Checksum: testutil.StorageChecksum(fmt.Sprintf("registration-%d-%d", sequence, i)), RequestedCopies: 1,
		})
		if err != nil {
			t.Fatalf("EnsureContent: %v", err)
		}
		version := &model.ObjectVersion{
			VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: fmt.Sprintf("object-%d", i),
			ContentID: &content.ID, Size: 128, ETag: fmt.Sprintf("etag-%d", i), ContentType: "application/octet-stream",
		}
		if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("create version: %v", err)
		}
		if binding == nil {
			binding, err = runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: content.ID,
			})
			if err != nil {
				t.Fatalf("EnsureDataSetBinding: %v", err)
			}
			if err := runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
			}); err != nil {
				t.Fatalf("MarkDataSetReady: %v", err)
			}
			binding.DataSetID, binding.ClientDataSetID = &dataSetID, &clientID
		}
		if err := runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: providerID,
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings: %v", err)
		}
		copyRow, err := runtime.repos.Contents.GetUploadCopy(ctx, content.ID, 0)
		if err != nil || copyRow == nil {
			t.Fatalf("GetUploadCopy = %#v, %v", copyRow, err)
		}
		pieceCID := testPieceCID(t, fmt.Sprintf("registration-piece-%d-%d", sequence, i))
		if err := runtime.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageCopyID: copyRow.ID, ContentID: content.ID, CopyIndex: 0, PieceCID: pieceCID.String(), RequireEligibleCopy: true,
		}); err != nil {
			t.Fatalf("MarkUploadCopyPieceReady: %v", err)
		}
		copies = append(copies, copyRow)
	}
	provider := newRegistrationProvider(t, providerID.SDK(), dataSetID.SDK(), clientID.SDK(), nonces)
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return provider.target, nil
	}
	return registrationFixture{runtime: runtime, provider: provider, dataSet: binding, copies: copies}
}

// collect puts every seeded copy into one collecting request driven by a new
// task.
func (f registrationFixture) collect(t *testing.T) (string, int64) {
	return f.collectAt(t, fmt.Sprintf("registration-%d", f.dataSet.ID), time.Time{}, f.copies...)
}

func (f registrationFixture) collectAt(t *testing.T, requestID string, now time.Time, copies ...*model.StorageCopy) (string, int64) {
	t.Helper()
	taskRow, _, err := f.runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageCommit, IdempotencyKey: storagepipeline.CommitKey(requestID),
		Input:       storagepipeline.CommitRequestInput{RequestID: requestID},
		SubjectType: model.TaskSubjectStorageCommitRequest, SubjectKey: requestID,
	})
	if err != nil {
		t.Fatalf("enqueue commit task: %v", err)
	}
	ids := make([]int64, len(copies))
	for i, copyRow := range copies {
		ids[i] = copyRow.ID
	}
	if err := f.runtime.repos.Contents.CreateCollectingCommitRequest(t.Context(), repository.CreateCommitRequestInput{
		RequestID: requestID, TaskID: taskRow.ID, StorageDataSetID: f.dataSet.ID, CopyIDs: ids, Now: now,
	}); err != nil {
		t.Fatalf("CreateCollectingCommitRequest: %v", err)
	}
	return requestID, taskRow.ID
}

func (f registrationFixture) request(t *testing.T, requestID string) *storagecommit.Request {
	t.Helper()
	request, err := f.runtime.repos.Contents.GetCommitRequest(t.Context(), requestID)
	if err != nil {
		t.Fatalf("GetCommitRequest(%s): %v", requestID, err)
	}
	return request
}

func (f registrationFixture) pieceCID(t *testing.T, copyRow *model.StorageCopy) string {
	t.Helper()
	content, err := f.runtime.repos.Contents.GetByID(t.Context(), copyRow.ContentID)
	if err != nil || content.PieceCID == nil {
		t.Fatalf("content %d = %#v, %v", copyRow.ContentID, content, err)
	}
	return *content.PieceCID
}

// makeResendDue ages the request's latest send past any resend delay.
func (f registrationFixture) makeResendDue(t *testing.T, requestID string) {
	t.Helper()
	if _, err := f.runtime.db.NewRaw(`UPDATE storage_commit_requests SET last_sent_at = ? WHERE request_id = ?`,
		time.Now().Add(-time.Hour), requestID).Exec(t.Context()); err != nil {
		t.Fatalf("age latest send: %v", err)
	}
}

// retryRefusedNow makes a refused request's backoff due and wakes its task.
func (f registrationFixture) retryRefusedNow(t *testing.T, requestID string, taskID int64) {
	t.Helper()
	if _, err := f.runtime.db.NewRaw(`UPDATE storage_commit_requests SET retry_at = ? WHERE request_id = ?`, time.Now().Add(-time.Second), requestID).Exec(t.Context()); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	if _, err := f.runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskID).Exec(t.Context()); err != nil {
		t.Fatalf("wake refused task: %v", err)
	}
}

// finishTransfer records the member's piece as transferred again.
func (f registrationFixture) finishTransfer(t *testing.T, copyRow *model.StorageCopy) {
	t.Helper()
	if err := f.runtime.repos.Contents.MarkUploadCopyPieceReady(t.Context(), repository.MarkUploadCopyPieceReadyInput{
		StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex,
		PieceCID: f.pieceCID(t, copyRow), RequireEligibleCopy: true,
	}); err != nil {
		t.Fatalf("finish transfer of copy %d: %v", copyRow.ID, err)
	}
}

func TestCollectingCommitKeepsItsWindowAcrossJoinsAndRecovery(t *testing.T) {
	f := newRegistrationFixture(t, 3, nil, 30*time.Minute)
	readyAt := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	requestID, taskID := f.collectAt(t, "collecting", readyAt, f.copies[0])
	cancel, done := runHandlerEngine(t, f.runtime)
	defer func() { stopHandlerEngine(t, cancel, done) }()
	collecting := func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "collecting"
	}
	first := waitForCommitTask(t, f.runtime, taskID, collecting)
	if first.RetryCount != 0 || first.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("collecting task = %#v, want recovery without retries", first)
	}
	if delay := first.AvailableAt.Sub(first.UpdatedAt); delay < 29*time.Second || delay > 31*time.Second {
		t.Fatalf("collection check delay = %s, want 30s", delay)
	}
	for i, copyRow := range f.copies[1:] {
		joined, _, err := f.runtime.repos.Contents.JoinCollectingCommitRequest(t.Context(), repository.JoinCommitRequestInput{
			CopyID: copyRow.ID, StorageDataSetID: f.dataSet.ID, Now: readyAt.Add(time.Duration(i+1) * time.Minute),
		})
		if err != nil || joined != requestID {
			t.Fatalf("join member = %q, %v, want %s", joined, err, requestID)
		}
	}
	stopHandlerEngine(t, cancel, done)
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, f.runtime.repos, f.runtime.registry, slog.Default())
	if err != nil {
		t.Fatalf("restart engine: %v", err)
	}
	f.runtime.engine = engine
	wakeCommitTasks(t, f.runtime)
	cancel, done = runHandlerEngine(t, f.runtime)
	recovered := waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool {
		return collecting(task) && task.ClaimGeneration > first.ClaimGeneration
	})
	if recovered.RetryCount != 0 {
		t.Fatalf("recovered retry count = %d, want 0", recovered.RetryCount)
	}
	members, err := f.runtime.repos.Contents.ListCommitRequestMembers(t.Context(), requestID)
	if err != nil || len(members) != 3 {
		t.Fatalf("recovered members = %#v, %v, want three", members, err)
	}
	if members[0].CommitReadyAt == nil || !members[0].CommitReadyAt.Equal(readyAt) {
		t.Fatalf("oldest ready at = %v, want %s", members[0].CommitReadyAt, readyAt)
	}
	if sends, _ := f.provider.sent(); len(sends) != 0 || f.request(t, requestID).Status != storagecommit.RequestStatusCollecting {
		t.Fatalf("submissions = %v, want the request still collecting", sends)
	}
}

func TestCachePressureSealsAndRecoversWriteCapacity(t *testing.T) {
	for _, policy := range []cache.EvictionPolicy{cache.EvictionPolicyLRU, cache.EvictionPolicyAfterUpload} {
		t.Run(string(policy), func(t *testing.T) {
			fs, err := cache.NewFilesystem(t.TempDir(), 384)
			if err != nil {
				t.Fatal(err)
			}
			f := newRegistrationFixture(t, 2, nil, 30*time.Minute, func(options *handlerRuntimeOptions) {
				options.cache, options.policy, options.maxBytes, options.maxWriteBytes = fs, policy, 384, 256
				options.highPercent, options.lowPercent, options.commitSealOnCachePressure = 90, 50, true
				options.register = func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
					if err := h.RegisterCore(registry); err != nil {
						return err
					}
					return h.RegisterStorage(registry)
				}
			})
			bucket, err := f.runtime.repos.Buckets.GetByID(t.Context(), f.copies[0].BucketID)
			if err != nil {
				t.Fatal(err)
			}
			for _, copyRow := range f.copies {
				if _, err := fs.Put(t.Context(), bucket.Name, model.ContentCacheKey(copyRow.ContentID), bytes.NewReader(make([]byte, 128)), 128); err != nil {
					t.Fatal(err)
				}
				if err := f.runtime.repos.Objects.RecordContentCacheCommit(t.Context(), copyRow.ContentID, time.Now().Add(-time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			requestID, taskID := f.collect(t)
			if _, err := fs.Put(t.Context(), bucket.Name, "next", bytes.NewReader(make([]byte, 256)), 256); !errors.Is(err, cache.ErrCacheFull) {
				t.Fatalf("admission = %v", err)
			}
			planner, _, err := f.runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity"})
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, f.runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			if f.request(t, requestID).Status != storagecommit.RequestStatusConfirmed {
				t.Fatal("partial batch was not confirmed")
			}
			deadline := time.Now().Add(5 * time.Second)
			for fs.UsedBytes() > 128 && time.Now().Before(deadline) {
				wakeTask(t, f.runtime, planner.ID)
				time.Sleep(20 * time.Millisecond)
			}
			if fs.UsedBytes() > 128 {
				t.Fatalf("confirmed data wasn't safely cleaned up, cache = %d", fs.UsedBytes())
			}
			if _, err := fs.Put(t.Context(), bucket.Name, "next", bytes.NewReader(make([]byte, 256)), 256); err != nil {
				t.Fatalf("retry write: %v", err)
			}
		})
	}
}

func TestSafeCleanupKeepsCollectionWindowAndManualSealIgnoresPolicy(t *testing.T) {
	for _, policy := range []cache.EvictionPolicy{cache.EvictionPolicyLRU, cache.EvictionPolicyNone} {
		t.Run(string(policy), func(t *testing.T) {
			fs, err := cache.NewFilesystem(t.TempDir(), 384)
			if err != nil {
				t.Fatal(err)
			}
			f := newRegistrationFixture(t, 1, nil, 30*time.Minute, func(options *handlerRuntimeOptions) {
				options.cache, options.policy, options.maxBytes, options.maxWriteBytes = fs, policy, 384, 128
				options.highPercent, options.lowPercent, options.commitSealOnCachePressure = 90, 50, true
				options.register = func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
					if err := h.RegisterCore(registry); err != nil {
						return err
					}
					return h.RegisterStorage(registry)
				}
			})
			copyRow := f.copies[0]
			bucket, err := f.runtime.repos.Buckets.GetByID(t.Context(), copyRow.BucketID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fs.Put(t.Context(), bucket.Name, model.ContentCacheKey(copyRow.ContentID), bytes.NewReader(make([]byte, 128)), 128); err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.repos.Objects.RecordContentCacheCommit(t.Context(), copyRow.ContentID, time.Now()); err != nil {
				t.Fatal(err)
			}
			safe := seedStoredCacheObject(t, f.runtime, 256, time.Now().Add(-time.Hour))
			safeBucket, err := f.runtime.repos.Buckets.GetByID(t.Context(), safe.BucketID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fs.Put(t.Context(), safeBucket.Name, safe.CacheKey(), bytes.NewReader(make([]byte, 256)), 256); err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.repos.Objects.RecordContentCacheCommit(t.Context(), *safe.ContentID, time.Now().Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			requestID, taskID := f.collect(t)
			planner, _, err := f.runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity"})
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, f.runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForTask(t, f.runtime.repos, planner.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusPending && task.ClaimGeneration > 0 })
			waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.WaitReason != nil && *task.WaitReason == "collecting" })
			if f.request(t, requestID).Status != storagecommit.RequestStatusCollecting {
				t.Fatal("safe cleanup unexpectedly ended collection")
			}
			if _, err := f.runtime.repos.Contents.RequestCommitSeal(t.Context(), requestID); err != nil {
				t.Fatal(err)
			}
			waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			if f.request(t, requestID).SealRequestedAt != nil {
				t.Fatal("confirmed request retained manual intent")
			}
		})
	}
}

func (f registrationFixture) holdSubmissionSlots(t *testing.T) int64 {
	t.Helper()
	ctx := t.Context()
	readyAt := time.Now().Add(-31 * time.Minute)
	var heldTaskID int64
	for i, copyRow := range f.copies[:4] {
		requestID, taskID := f.collectAt(t, fmt.Sprintf("held-%d", i), readyAt, copyRow)
		claimed, err := f.runtime.repos.Tasks.ClaimNext(ctx, time.Minute)
		if err != nil || claimed == nil || claimed.ID != taskID {
			t.Fatalf("claim held task = %#v, %v, want %d", claimed, err, taskID)
		}
		if _, err := f.runtime.repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
			RequestID: requestID, TaskID: taskID, ExtraDataHex: fmt.Sprintf("%x", testutil.CommitExtraData(uint64(i+1))),
			Members: []repository.SealMember{{CopyID: copyRow.ID, ContentID: copyRow.ContentID, PieceCID: f.pieceCID(t, copyRow)}},
		}); err != nil {
			t.Fatalf("seal held request: %v", err)
		}
		if err := f.runtime.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: requestID, TaskID: taskID}); err != nil {
			t.Fatalf("begin held submission: %v", err)
		}
		if err := f.runtime.repos.Contents.RecordCommitSubmission(ctx, repository.CommitSubmissionInput{
			CommitSendInput: repository.CommitSendInput{RequestID: requestID, TaskID: taskID, Sends: 1},
			TransactionID:   fmt.Sprintf("0xheld-%d", i), StatusURL: fmt.Sprintf("https://provider.example/status/held-%d", i),
		}); err != nil {
			t.Fatalf("record held submission: %v", err)
		}
		// A stopped submitted request still owns its in-flight slot.
		failure := "handler_unavailable"
		if err := f.runtime.repos.Tasks.Settle(ctx, taskID, claimed.ClaimGeneration, repository.TaskTransition{
			Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &failure,
		}); err != nil {
			t.Fatalf("stop held task: %v", err)
		}
		if i == 0 {
			heldTaskID = taskID
		}
	}
	return heldTaskID
}

func TestCachePressureCollectsWhileSubmissionSlotsAreFull(t *testing.T) {
	for _, limit := range []int{32, 3} {
		t.Run(fmt.Sprintf("max-pieces-%d", limit), func(t *testing.T) {
			ctx := t.Context()
			fs, err := cache.NewFilesystem(t.TempDir(), 384)
			if err != nil {
				t.Fatal(err)
			}
			f := newRegistrationFixture(t, 8, nil, 30*time.Minute, func(options *handlerRuntimeOptions) {
				options.cache, options.policy, options.maxBytes = fs, cache.EvictionPolicyLRU, 384
				options.highPercent, options.lowPercent, options.commitSealOnCachePressure = 90, 50, true
				options.commitMaxPieces = limit
				options.register = func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
					if err := h.RegisterCore(registry); err != nil {
						return err
					}
					return h.RegisterStorage(registry)
				}
			})
			heldTaskID := f.holdSubmissionSlots(t)
			readyID, readyTaskID := f.collectAt(t, "earlier-ready", time.Now().Add(-time.Second), f.copies[4])
			if _, err := f.runtime.repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
				RequestID: readyID, TaskID: readyTaskID, ExtraDataHex: fmt.Sprintf("%x", testutil.CommitExtraData(5)),
				Members: []repository.SealMember{{CopyID: f.copies[4].ID, ContentID: f.copies[4].ContentID, PieceCID: f.pieceCID(t, f.copies[4])}},
			}); err != nil {
				t.Fatal(err)
			}
			bucket, err := f.runtime.repos.Buckets.GetByID(ctx, f.copies[5].BucketID)
			if err != nil {
				t.Fatal(err)
			}
			for _, copyRow := range f.copies[5:] {
				if _, err := fs.Put(ctx, bucket.Name, model.ContentCacheKey(copyRow.ContentID), bytes.NewReader(make([]byte, 128)), 128); err != nil {
					t.Fatal(err)
				}
				if err := f.runtime.repos.Objects.RecordContentCacheCommit(ctx, copyRow.ContentID, time.Now().Add(-time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			requestID, taskID := f.collectAt(t, "pressure-collecting", time.Now(), f.copies[5])
			if _, _, err := f.runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{
				Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey,
				Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity",
			}); err != nil {
				t.Fatal(err)
			}
			cancel, done := runHandlerEngine(t, f.runtime)
			defer stopHandlerEngine(t, cancel, done)
			blocked := waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == storagecommit.CommitQueueWaitReason
			})
			original, err := f.runtime.repos.Contents.GetUploadCopy(ctx, f.copies[5].ContentID, 0)
			if err != nil || original == nil || original.CommitReadyAt == nil {
				t.Fatalf("load original member: %#v, %v", original, err)
			}
			for _, copyRow := range f.copies[6:] {
				f.queueTransferredCopy(t, copyRow)
				joined, err := f.runtime.repos.Contents.GetUploadCopy(ctx, copyRow.ContentID, 0)
				if err != nil || joined == nil || joined.CommitRequestID == nil || *joined.CommitRequestID != requestID {
					t.Fatalf("member joined a different batch: copy = %#v, error = %v", joined, err)
				}
				after, err := f.runtime.repos.Tasks.GetByID(ctx, taskID)
				if err != nil || !after.AvailableAt.Equal(blocked.AvailableAt) || !after.UpdatedAt.Equal(blocked.UpdatedAt) {
					t.Fatalf("member woke blocked collection: before = %#v, after = %#v, error = %v", blocked, after, err)
				}
				if f.request(t, requestID).Status != storagecommit.RequestStatusCollecting {
					t.Fatal("pressure split the collection while submission slots were full")
				}
			}
			firstMember, err := f.runtime.repos.Contents.GetUploadCopy(ctx, f.copies[5].ContentID, 0)
			if err != nil || firstMember == nil || firstMember.CommitReadyAt == nil || !firstMember.CommitReadyAt.Equal(*original.CommitReadyAt) {
				t.Fatalf("member arrivals reset the collection window: before = %#v, after = %#v, error = %v", original, firstMember, err)
			}
			if sends, _ := f.provider.sent(); len(sends) != 0 {
				t.Fatalf("sent with all submission slots occupied: %v", sends)
			}
			if _, err := f.runtime.repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{
				RequestID: "held-0", TaskID: heldTaskID, FirstPieceID: testOnChainID(t, 50),
				ConfirmedTransactionID: "0xheld-0", RetrievalURLs: []string{"https://provider.example/retrieve/held-0"},
			}); err != nil {
				t.Fatal(err)
			}
			// Advance only confirmation polling; collection must wake through the queue.
			for _, id := range []int64{readyTaskID, taskID} {
				waiting := waitForTask(t, f.runtime.repos, id, func(task *model.Task) bool {
					return task.Status == model.TaskStatusCompleted || task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
				})
				if waiting.Status != model.TaskStatusCompleted {
					wakeTask(t, f.runtime, id)
				}
				waitForTask(t, f.runtime.repos, id, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			}
			completed := waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			sends, _ := f.provider.sent()
			if len(sends) != 2 || len(sends[0]) != 1 || sends[0][0].String() != f.pieceCID(t, f.copies[4]) || len(sends[1]) != 3 {
				t.Fatalf("submissions = %v, want earlier ready followed by the three-member batch", sends)
			}
			for i, piece := range sends[1] {
				if piece.String() != f.pieceCID(t, f.copies[i+5]) {
					t.Fatalf("sent member %d = %s", i, piece)
				}
			}
			final := f.request(t, requestID)
			if completed.RetryCount != 0 || final.Status != storagecommit.RequestStatusConfirmed {
				t.Fatalf("completed batch = %#v, task = %#v", final, completed)
			}
		})
	}
}

func (f registrationFixture) queueTransferredCopy(t *testing.T, copyRow *model.StorageCopy) {
	t.Helper()
	ctx := t.Context()
	var transferTaskID int64
	if err := f.runtime.repos.WithTx(ctx, func(repos *repository.Repositories) error {
		generation, err := repos.Contents.NextCopyWorkGeneration(ctx, copyRow.ID)
		if err != nil {
			return err
		}
		task, _, err := f.runtime.service.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
			Type: model.TaskTypeStorageTransferPlan, IdempotencyKey: storagepipeline.TransferPlanKey(copyRow.ID, generation),
			Input:       storagepipeline.CopyGenerationInput{CopyID: copyRow.ID, Generation: generation},
			SubjectType: model.TaskSubjectStorageCopy, SubjectKey: fmt.Sprint(copyRow.ID),
		})
		if err != nil {
			return err
		}
		transferTaskID = task.ID
		return repos.Contents.BindCopyTask(ctx, copyRow.ID, generation, task.ID)
	}); err != nil {
		t.Fatal(err)
	}
	waitForTask(t, f.runtime.repos, transferTaskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
}

func TestCollectingCommitWaitsForSubmissionCapacity(t *testing.T) {
	for _, trigger := range []string{"manual", "full", "timeout", "draining", "zero wait"} {
		t.Run(trigger, func(t *testing.T) {
			maxWait := 30 * time.Minute
			if trigger == "zero wait" {
				maxWait = 0
			}
			f := newRegistrationFixture(t, 6, nil, maxWait, func(options *handlerRuntimeOptions) { options.commitMaxPieces = 2 })
			ctx := t.Context()
			heldTaskID := f.holdSubmissionSlots(t)
			readyAt := time.Now()
			if trigger == "timeout" {
				readyAt = readyAt.Add(-time.Hour)
			}
			copies := f.copies[4:5]
			if trigger == "full" {
				copies = f.copies[4:]
			}
			requestID, taskID := f.collectAt(t, "waiting-to-send", readyAt, copies...)
			if trigger == "manual" {
				if _, err := f.runtime.repos.Contents.RequestCommitSeal(ctx, requestID); err != nil {
					t.Fatal(err)
				}
			}
			if trigger == "draining" {
				if err := f.runtime.repos.Contents.MarkDataSetDraining(ctx, f.dataSet.ID, "replacement in progress"); err != nil {
					t.Fatal(err)
				}
			}
			cancel, done := runHandlerEngine(t, f.runtime)
			defer stopHandlerEngine(t, cancel, done)
			waiting := waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == storagecommit.CommitQueueWaitReason
			})
			if request := f.request(t, requestID); request.Status != storagecommit.RequestStatusCollecting || request.ExtraDataHex != nil || waiting.RetryCount != 0 {
				t.Fatalf("blocked request/task = %#v / %#v", request, waiting)
			}
			if trigger != "full" {
				f.queueTransferredCopy(t, f.copies[5])
			}
			if _, err := f.runtime.repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{
				RequestID: "held-0", TaskID: heldTaskID, FirstPieceID: testOnChainID(t, 50),
				ConfirmedTransactionID: "0xheld-0", RetrievalURLs: []string{"https://provider.example/retrieve/held-0"},
			}); err != nil {
				t.Fatal(err)
			}
			waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
			})
			sends, _ := f.provider.sent()
			if len(sends) != 1 || len(sends[0]) != 2 {
				t.Fatalf("submissions = %v, want both collected members together", sends)
			}
		})
	}
}

func TestCollectingCommitJoinAtTransactionLimitWakes(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 30*time.Minute, func(options *handlerRuntimeOptions) { options.commitMaxPieces = 2 })
	_, taskID := f.collectAt(t, "collecting-to-limit", time.Now(), f.copies[0])
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "collecting"
	})
	f.queueTransferredCopy(t, f.copies[1])
	waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
	})
	if sends, _ := f.provider.sent(); len(sends) != 1 || len(sends[0]) != 2 {
		t.Fatalf("submissions = %v, want both members without waiting for the next poll", sends)
	}
}

func TestCollectingCommitFormsBatchesWhenSlotsOpen(t *testing.T) {
	f := newRegistrationFixture(t, 69, nil, 30*time.Minute)
	ctx := t.Context()
	f.provider.script = []sendOutcome{sendAcceptUnlanded, sendAcceptUnlanded}
	f.holdSubmissionSlots(t)
	requestID, taskID := f.collectAt(t, "large-collection", time.Now(), f.copies[4])
	if _, err := f.runtime.repos.Contents.RequestCommitSeal(ctx, requestID); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)
	blocked := waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == storagecommit.CommitQueueWaitReason
	})
	for _, copyRow := range f.copies[5:] {
		f.queueTransferredCopy(t, copyRow)
	}
	members, err := f.runtime.repos.Contents.ListCommitRequestMembers(ctx, requestID)
	if err != nil || len(members) != 65 {
		t.Fatalf("collection = %d members, %v, want 65", len(members), err)
	}
	after, err := f.runtime.repos.Tasks.GetByID(ctx, taskID)
	if err != nil || !after.AvailableAt.Equal(blocked.AvailableAt) || !after.UpdatedAt.Equal(blocked.UpdatedAt) {
		t.Fatalf("joins woke queue-blocked collection: %#v, %v", after, err)
	}
	f.provider.mu.Lock()
	signatures := f.provider.signed
	f.provider.mu.Unlock()
	if signatures != 0 {
		t.Fatalf("signatures while slots are full = %d, want none", signatures)
	}
	for batch := range 2 {
		heldID := fmt.Sprintf("held-%d", batch)
		if _, err := f.runtime.repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{
			RequestID: heldID, TaskID: *f.request(t, heldID).TaskID, FirstPieceID: testOnChainID(t, int64(50+batch)),
			ConfirmedTransactionID: "0x" + heldID, RetrievalURLs: []string{"https://provider.example/retrieve/" + heldID},
		}); err != nil {
			t.Fatal(err)
		}
		waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
			return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
		})
		sends, _ := f.provider.sent()
		if len(sends) != batch+1 || len(sends[batch]) != 32 {
			t.Fatalf("submissions = %v, want %d batches of 32", sends, batch+1)
		}
		for i, piece := range sends[batch] {
			if piece.String() != f.pieceCID(t, f.copies[4+batch*32+i]) {
				t.Fatalf("batch %d member %d is out of preparation order", batch, i)
			}
		}
		copyRow, err := f.runtime.repos.Contents.GetUploadCopyByID(ctx, f.copies[4+(batch+1)*32].ID)
		if err != nil || copyRow.CommitRequestID == nil {
			t.Fatalf("spilled membership = %#v, %v", copyRow, err)
		}
		requestID = *copyRow.CommitRequestID
		request := f.request(t, requestID)
		taskID = *request.TaskID
		waitReason := storagecommit.CommitQueueWaitReason
		if batch == 1 {
			waitReason = "collecting"
		}
		waitForTask(t, f.runtime.repos, taskID, func(task *model.Task) bool {
			return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == waitReason
		})
		remaining, err := f.runtime.repos.Contents.ListCommitRequestMembers(ctx, requestID)
		if err != nil || len(remaining) != 65-(batch+1)*32 || request.SealRequestedAt != nil || remaining[0].CommitReadyAt == nil {
			t.Fatalf("spilled collection = %#v, members = %d, %v", request, len(remaining), err)
		}
		original := members[(batch+1)*32]
		if !remaining[0].CommitReadyAt.Equal(*original.CommitReadyAt) {
			t.Fatal("spill reset the preparation time")
		}
	}
}

func TestCommitRequestRegistersEveryMemberInOneSubmission(t *testing.T) {
	f := newRegistrationFixture(t, 3, nil, 30*time.Minute)
	readyAt := time.Now().Add(-31 * time.Minute)
	requestID, taskID := f.collectAt(t, "expired", readyAt, f.copies[0])
	for i, copyRow := range f.copies[1:] {
		joined, _, err := f.runtime.repos.Contents.JoinCollectingCommitRequest(t.Context(), repository.JoinCommitRequestInput{
			CopyID: copyRow.ID, StorageDataSetID: f.dataSet.ID, Now: readyAt.Add(time.Duration(i+1) * time.Second),
		})
		if err != nil || joined != requestID {
			t.Fatalf("join member = %q, %v, want %s", joined, err, requestID)
		}
	}
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if sends, _ := f.provider.sent(); len(sends) != 1 || len(sends[0]) != 3 {
		t.Fatalf("submissions = %v, want one carrying all three pieces", sends)
	}
	for i, copyRow := range f.copies {
		committed := waitForCopy(t, f.runtime, copyRow.ID, func(c *model.StorageCopy) bool { return c.Status == model.StorageCopyStatusCommitted })
		if committed.PieceID == nil || committed.PieceID.String() != fmt.Sprint(100+i) {
			t.Fatalf("member %d piece = %v, want %d", i, committed.PieceID, 100+i)
		}
		content, err := f.runtime.repos.Contents.GetByID(t.Context(), copyRow.ContentID)
		if err != nil || content.AcceptedAt == nil {
			t.Fatalf("member %d content = %#v, %v, want accepted", i, content, err)
		}
	}
	if request := f.request(t, requestID); request.Status != storagecommit.RequestStatusConfirmed || request.ConfirmedTransactionID == nil {
		t.Fatalf("request = %#v", request)
	}
}

func TestRefusedCommitRequestBacksOffAndSendsTheSameRequest(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 0)
	f.provider.script = []sendOutcome{sendRefuse}
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waiting := waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == storagecommit.ProviderRejectedWaitReason
	})
	if waiting.ResumeMode != model.TaskResumeModeExecute {
		t.Fatalf("refused task = %#v, want an execute wait", waiting)
	}
	if request := f.request(t, requestID); request.Status != storagecommit.RequestStatusReady || request.Refusals != 1 || request.RetryAt == nil {
		t.Fatalf("refused request = %#v", request)
	}
	f.retryRefusedNow(t, requestID, taskID)
	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if sends, extras := f.provider.sent(); len(sends) != 2 || len(sends[0]) != 2 || !sameSends(sends, extras) {
		t.Fatalf("submissions = %v, want the same two-piece request twice", sends)
	}
}

func TestDroppedMemberIsTransferredAgainBeforeTheRequestIsResent(t *testing.T) {
	parked := &droppedPieces{missing: map[string]bool{}}
	f := newRegistrationFixture(t, 2, parked, 0)
	f.provider.script = []sendOutcome{sendRefuse}
	parked.drop(f.pieceCID(t, f.copies[0]))
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == storagecommit.ProviderRejectedWaitReason
	})
	// The member keeps its place in the signed request while its piece is
	// transferred again; the other member waits with it.
	dropped := waitForCopy(t, f.runtime, f.copies[0].ID, func(c *model.StorageCopy) bool { return c.ActiveTaskID != nil })
	if dropped.Status != model.StorageCopyStatusPending || dropped.CommitRequestID == nil || *dropped.CommitRequestID != requestID ||
		dropped.CommitPosition == nil || *dropped.CommitPosition != 0 {
		t.Fatalf("dropped member = %#v, want pending at its position", dropped)
	}
	transfer, err := f.runtime.repos.Tasks.GetByID(t.Context(), *dropped.ActiveTaskID)
	if err != nil || transfer.Type != model.TaskTypeStorageTransferPlan {
		t.Fatalf("dropped member task = %#v, %v, want a transfer plan", transfer, err)
	}
	if kept := waitForCopy(t, f.runtime, f.copies[1].ID, func(*model.StorageCopy) bool { return true }); kept.Status != model.StorageCopyStatusCommitting {
		t.Fatalf("other member = %#v, want still registering", kept)
	}

	// Once the piece is back, the whole request is sent again unchanged.
	parked.restore(f.pieceCID(t, f.copies[0]))
	f.finishTransfer(t, f.copies[0])
	f.retryRefusedNow(t, requestID, taskID)
	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	waitForCommitted(t, f.runtime, f.copies)
	if sends, extras := f.provider.sent(); len(sends) != 2 || len(sends[0]) != 2 || !sameSends(sends, extras) {
		t.Fatalf("submissions = %v, want the same two-piece request twice", sends)
	}
}

func TestSubmittedRequestWaitsForADroppedMemberBeforeSendingAgain(t *testing.T) {
	parked := &droppedPieces{missing: map[string]bool{}}
	f := newRegistrationFixture(t, 2, parked, 0)
	f.provider.script = []sendOutcome{sendLost, sendRefuse}
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(*model.Task) bool {
		return f.request(t, requestID).Status == storagecommit.RequestStatusSubmitted
	})
	// The resend is refused because the provider dropped a piece; the member
	// is transferred again under the same request.
	parked.drop(f.pieceCID(t, f.copies[0]))
	f.makeResendDue(t, requestID)
	waitForCommitTask(t, f.runtime, taskID, func(*model.Task) bool {
		current, err := f.runtime.repos.Contents.GetUploadCopyByID(t.Context(), f.copies[0].ID)
		return err == nil && current.Status == model.StorageCopyStatusPending
	})

	// Due again, the request still waits for that member.
	f.makeResendDue(t, requestID)
	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "commit_members"
	})
	if sends, _ := f.provider.sent(); len(sends) != 2 {
		t.Fatalf("submissions while a member is transferred = %d, want 2", len(sends))
	}

	parked.restore(f.pieceCID(t, f.copies[0]))
	f.finishTransfer(t, f.copies[0])
	f.makeResendDue(t, requestID)
	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	waitForCommitted(t, f.runtime, f.copies)
	if sends, extras := f.provider.sent(); len(sends) != 3 || len(sends[0]) != 2 || !sameSends(sends, extras) {
		t.Fatalf("submissions = %v, want the same two-piece request three times", sends)
	}
}

func TestLostSendIsResentUnchangedOnceItsDelayPasses(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 0)
	f.provider.script = []sendOutcome{sendLost}
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool {
		return f.request(t, requestID).Status == storagecommit.RequestStatusSubmitted &&
			task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover
	})
	// Recovery reads the chain on every wake but sends nothing before the
	// resend delay.
	for range 10 {
		wakeCommitTasks(t, f.runtime)
		time.Sleep(20 * time.Millisecond)
	}
	if sends, _ := f.provider.sent(); len(sends) != 1 {
		t.Fatalf("submissions before the resend delay = %d, want 1", len(sends))
	}
	f.makeResendDue(t, requestID)
	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	waitForCommitted(t, f.runtime, f.copies)
	if sends, extras := f.provider.sent(); len(sends) != 2 || !sameSends(sends, extras) {
		t.Fatalf("submissions = %v, want the same request twice", sends)
	}
}

func TestLostSendThatLandedIsConfirmedWithoutSendingAgain(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 0)
	f.provider.script = []sendOutcome{sendLandedLost}
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	waitForCommitted(t, f.runtime, f.copies)
	if sends, _ := f.provider.sent(); len(sends) != 1 {
		t.Fatalf("submissions = %d, want 1", len(sends))
	}
	// The chain proved the pieces, but no reply named the transaction that
	// carried them.
	if request := f.request(t, requestID); request.Status != storagecommit.RequestStatusConfirmed || request.ConfirmedTransactionID != nil {
		t.Fatalf("request = %#v, want confirmed by its nonce", request)
	}
}

func TestUnanswerableSubmissionIsSentAgainOnceFlagged(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 0)
	f.provider.script = []sendOutcome{sendAcceptUnlanded}
	f.provider.statusDown = true
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(*model.Task) bool { return f.request(t, requestID).TransactionID != nil })
	// The provider accepted the request but its transaction never landed, and
	// its status endpoint stays down past the attention threshold.
	legacyStart := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	if err := f.runtime.db.RunInTx(t.Context(), nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.NewRaw(`UPDATE storage_commit_requests SET first_sent_at = ?, submitted_at = ?, last_sent_at = ? WHERE request_id = ?`,
			legacyStart, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour), requestID).Exec(ctx); err != nil {
			return err
		}
		_, err := tx.NewRaw(`UPDATE tasks SET work_started_at = NULL WHERE id = ?`, taskID).Exec(ctx)
		return err
	}); err != nil {
		t.Fatalf("age submission: %v", err)
	}
	completed := waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if completed.WorkStartedAt == nil || !completed.WorkStartedAt.Equal(legacyStart) {
		t.Fatalf("registration recovery start = %v, want %v", completed.WorkStartedAt, legacyStart)
	}
	waitForCommitted(t, f.runtime, f.copies)
	if sends, extras := f.provider.sent(); len(sends) != 2 || !sameSends(sends, extras) {
		t.Fatalf("submissions = %v, want the same request sent again", sends)
	}
}

func TestReadyRequestWhoseNonceWasSpentIsSignedAgain(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 0)
	f.provider.spendFirstNonce = true
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	waitForCommitted(t, f.runtime, f.copies)
	if request := f.request(t, requestID); request.Status != storagecommit.RequestStatusAbandoned {
		t.Fatalf("first request = %#v, want given up", request)
	}
	sends, extras := f.provider.sent()
	if len(sends) != 1 || len(sends[0]) != 2 {
		t.Fatalf("submissions = %v, want one under the new signature", sends)
	}
	nonce, err := storagecommit.ExtraDataNonce(extras[0])
	if err != nil || nonce.String() != "42" {
		t.Fatalf("sent nonce = %v, %v, want the second signature", nonce, err)
	}
	for _, copyRow := range f.copies {
		committed := waitForCopy(t, f.runtime, copyRow.ID, func(*model.StorageCopy) bool { return true })
		if committed.CommitRequestID == nil || *committed.CommitRequestID == requestID {
			t.Fatalf("member %d request = %v, want the new request", copyRow.ID, committed.CommitRequestID)
		}
	}
}

func TestSubmittedRequestWhoseNonceWasSpentStopsForReview(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 0)
	f.provider.script = []sendOutcome{sendSpentElsewhere}
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	failed := waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != string(storagecommit.AttentionSubmissionMismatch) || !f.runtime.service.Retryable(failed) {
		t.Fatalf("stopped task = %#v, want a retryable submission_mismatch", failed)
	}
	request := f.request(t, requestID)
	if request.Status != storagecommit.RequestStatusSubmitted || request.AttentionCode == nil ||
		*request.AttentionCode != string(storagecommit.AttentionSubmissionMismatch) {
		t.Fatalf("request = %#v, want flagged submission_mismatch", request)
	}
	if err := f.runtime.repos.Tasks.AcknowledgeFailed(t.Context(), taskID, time.Hour); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("dismissing the stopped registration = %v, want conflict", err)
	}
	if sends, _ := f.provider.sent(); len(sends) != 1 {
		t.Fatalf("submissions = %d, want 1", len(sends))
	}
}

func TestRequestForADataSetThatRefusesWritesIsGivenUp(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil, 0)
	f.provider.target.CheckWritableFunc = func(context.Context) error { return storage.ErrDataSetUnavailable }
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if request := f.request(t, requestID); request.Status != storagecommit.RequestStatusAbandoned {
		t.Fatalf("request = %#v, want given up", request)
	}
	for _, copyRow := range f.copies {
		if failed := waitForCopy(t, f.runtime, copyRow.ID, func(c *model.StorageCopy) bool { return c.Status == model.StorageCopyStatusFailed }); failed.CommitRequestID != nil {
			t.Fatalf("member = %#v, want failed outside the request", failed)
		}
	}
	if sends, _ := f.provider.sent(); len(sends) != 0 {
		t.Fatalf("submissions = %d, want none", len(sends))
	}
}

func TestUnknownAttentionCodeStopsRecovery(t *testing.T) {
	f := newRegistrationFixture(t, 1, nil, 0)
	f.provider.script = []sendOutcome{sendLost}
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(*model.Task) bool {
		return f.request(t, requestID).Status == storagecommit.RequestStatusSubmitted
	})
	// A later version flagged the request with a code this one does not know.
	if _, err := f.runtime.db.NewRaw(`UPDATE storage_commit_requests SET attention_code = 'future_review', attention_at = ? WHERE request_id = ?`,
		time.Now(), requestID).Exec(t.Context()); err != nil {
		t.Fatalf("flag request: %v", err)
	}
	f.makeResendDue(t, requestID)
	failed := waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "commit_attention_unknown" || f.runtime.service.Retryable(failed) {
		t.Fatalf("stopped task = %#v, want a non-retryable unknown attention stop", failed)
	}
	if sends, _ := f.provider.sent(); len(sends) != 1 {
		t.Fatalf("submissions = %d, want only the lost one", len(sends))
	}
}

func TestRequestTooLargeToSignIsSplitInHalves(t *testing.T) {
	f := newRegistrationFixture(t, 4, nil, 30*time.Minute)
	f.provider.maxSigned = 2
	requestID, taskID := f.collect(t)
	if _, err := f.runtime.repos.Contents.RequestCommitSeal(t.Context(), requestID); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	spilled, err := f.runtime.repos.Contents.ListCommitBatches(t.Context(), repository.CommitBatchFilter{Status: storagecommit.RequestStatusCollecting, Limit: 20})
	if err != nil || len(spilled) != 1 || spilled[0].SealRequestedAt != nil {
		t.Fatalf("spilled requests = %#v, %v", spilled, err)
	}
	if _, err := f.runtime.repos.Contents.RequestCommitSeal(t.Context(), spilled[0].RequestID); err != nil {
		t.Fatal(err)
	}
	waitForCommitted(t, f.runtime, f.copies)
	if request := f.request(t, requestID); request.PieceCount != 2 || request.Status != storagecommit.RequestStatusConfirmed {
		t.Fatalf("first request = %#v, want the half that fits", request)
	}
	if sends, _ := f.provider.sent(); len(sends) != 2 || len(sends[0]) != 2 || len(sends[1]) != 2 {
		t.Fatalf("submissions = %v, want two requests of two pieces", sends)
	}
}

// pullRegistration is a peer copy whose first Pull succeeds and every later one
// fails for good. Its provider answers sends from script, and has dropped the
// pulled piece once it refuses one.
type pullRegistration struct {
	nonce    uint64
	runtime  handlerTestRuntime
	pipeline seededCopyPipeline
	mu       sync.Mutex
	script   []sendOutcome
	sends    int
	pulls    int
}

func newPullRegistration(t *testing.T, script ...sendOutcome) *pullRegistration {
	t.Helper()
	r := &pullRegistration{script: script}
	parked := &droppedPieces{missing: map[string]bool{}}
	target := &testutil.MockStorageTarget{
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.nonce++
			return testutil.CommitExtraData(r.nonce), nil
		},
		SubmitPullFunc: func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.pulls++
			if r.pulls == 1 {
				return pullStatusResult(request, storage.PullStatusComplete), nil
			}
			return pullStatusResult(request, storage.PullStatusFailed), nil
		},
		SubmitCommitFunc: func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.sends++
			parked.drop(request.Pieces[0].PieceCID.String())
			if r.sends > len(r.script) || r.script[r.sends-1] == sendRefuse {
				return nil, &pdp.HTTPError{StatusCode: 400, Body: "piece not found"}
			}
			return nil, &pdp.HTTPError{StatusCode: 502, Body: "bad gateway"}
		},
	}
	storageClient := &testutil.MockStorageClient{}
	r.runtime = newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone,
		commitNonces: &testutil.MockCommitNonces{}, parkedPieces: parked,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	r.pipeline = seedCopyPipeline(t, r.runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: r.pipeline.upload.BucketID, Key: "pull-registration.bin",
		ContentID: &r.pipeline.upload.ID, Size: r.pipeline.upload.ContentSize, ETag: "pull-registration",
		ContentType: "application/octet-stream",
	}
	if _, err := r.runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	target.ProviderIDValue = r.pipeline.targetSet.ProviderID.SDK()
	dataSetID := r.pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &dataSetID
	target.ClientDataSetIDValue = r.pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	bindCopyTask(t, r.runtime, r.pipeline.target, model.TaskTypeStoragePull)
	return r
}

func (r *pullRegistration) sent() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sends
}

// copyRow waits for the peer copy to satisfy predicate, waking registration
// tasks meanwhile.
func (r *pullRegistration) copyRow(t *testing.T, predicate func(*model.StorageCopy) bool) *model.StorageCopy {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		copyRow, err := r.runtime.repos.Contents.GetUploadCopyByID(t.Context(), r.pipeline.target.ID)
		if err != nil {
			t.Fatalf("load copy: %v", err)
		}
		if predicate(copyRow) {
			return copyRow
		}
		if time.Now().After(deadline) {
			t.Fatalf("copy did not reach the expected state: %#v", copyRow)
		}
		wakeCommitTasks(t, r.runtime)
		time.Sleep(20 * time.Millisecond)
	}
}

func TestUnacceptedRequestFailsAMemberThatCannotBeTransferredAgain(t *testing.T) {
	r := newPullRegistration(t, sendRefuse)
	cancel, done := runHandlerEngine(t, r.runtime)
	defer stopHandlerEngine(t, cancel, done)

	// The provider refused the request and had dropped the piece; pulling it
	// again fails for good. The request was never accepted, so it is given up
	// and the copy fails as any other copy would.
	failed := r.copyRow(t, func(c *model.StorageCopy) bool { return c.Status == model.StorageCopyStatusFailed })
	if failed.CommitRequestID != nil || failed.ActiveTaskID != nil || failed.LastError == nil {
		t.Fatalf("failed copy = %#v, want released with its transfer error", failed)
	}
	var requests []storagecommit.Request
	if err := r.runtime.db.NewSelect().Model(&requests).Where("storage_data_set_id = ?", r.pipeline.targetSet.ID).Scan(t.Context()); err != nil {
		t.Fatalf("load requests: %v", err)
	}
	if len(requests) != 1 || requests[0].Status != storagecommit.RequestStatusAbandoned {
		t.Fatalf("requests = %#v, want the refused request given up", requests)
	}
	if r.sent() != 1 {
		t.Fatalf("submissions = %d, want 1", r.sent())
	}
	// The source still holds a readable copy, so the content itself is fine.
	if content, err := r.runtime.repos.Contents.GetByID(t.Context(), r.pipeline.upload.ID); err != nil || content.ErrorMessage != nil {
		t.Fatalf("content = %#v, %v, want it not flagged", content, err)
	}
}

func TestSubmittedRequestTransfersAGivenUpMemberAgainLater(t *testing.T) {
	r := newPullRegistration(t, sendLost, sendRefuse)
	cancel, done := runHandlerEngine(t, r.runtime)
	defer stopHandlerEngine(t, cancel, done)

	member := r.copyRow(t, func(c *model.StorageCopy) bool {
		return c.CommitRequestID != nil && c.Status == model.StorageCopyStatusCommitting
	})
	requestID := *member.CommitRequestID
	waitForCommitTask(t, r.runtime, requestTaskID(t, r.runtime, requestID), func(*model.Task) bool { return r.sent() == 1 })
	if _, err := r.runtime.db.NewRaw(`UPDATE storage_commit_requests SET last_sent_at = ? WHERE request_id = ?`,
		time.Now().Add(-time.Hour), requestID).Exec(t.Context()); err != nil {
		t.Fatalf("age latest send: %v", err)
	}

	// The resend is refused for a dropped piece that cannot be pulled again.
	// The first send may still land, so the copy stays in its request and is
	// transferred again later.
	retried := r.copyRow(t, func(c *model.StorageCopy) bool {
		if c.Status != model.StorageCopyStatusPending || c.ActiveTaskID == nil || c.LastError == nil {
			return false
		}
		task, err := r.runtime.repos.Tasks.GetByID(t.Context(), *c.ActiveTaskID)
		return err == nil && task.Type == model.TaskTypeStorageTransferPlan && task.AvailableAt.After(time.Now().Add(time.Minute))
	})
	if retried.CommitRequestID == nil || *retried.CommitRequestID != requestID || retried.CommitPosition == nil {
		t.Fatalf("copy = %#v, want it kept at its position", retried)
	}
	var request storagecommit.Request
	if err := r.runtime.db.NewSelect().Model(&request).Where("request_id = ?", requestID).Scan(t.Context()); err != nil ||
		request.Status != storagecommit.RequestStatusSubmitted {
		t.Fatalf("request = %#v, %v, want still submitted", request, err)
	}
	if r.sent() != 2 {
		t.Fatalf("submissions = %d, want 2", r.sent())
	}
}

func requestTaskID(t *testing.T, runtime handlerTestRuntime, requestID string) int64 {
	t.Helper()
	request, err := runtime.repos.Contents.GetCommitRequest(t.Context(), requestID)
	if err != nil || request.TaskID == nil {
		t.Fatalf("request %s = %#v, %v", requestID, request, err)
	}
	return *request.TaskID
}

type droppedPieces struct {
	mu      sync.Mutex
	missing map[string]bool
}

func (d *droppedPieces) drop(pieceCID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.missing[pieceCID] = true
}

func (d *droppedPieces) restore(pieceCID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.missing, pieceCID)
}

func (d *droppedPieces) FindParkedPiece(_ context.Context, _ string, pieceCID cid.Cid) (synapse.ParkedPieceState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.missing[pieceCID.String()] {
		return synapse.ParkedPieceMissing, nil
	}
	return synapse.ParkedPieceReady, nil
}
