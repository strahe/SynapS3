package worker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

// registrationProvider is a provider that registers whole requests: a send it
// accepts lands every piece at consecutive IDs.
type registrationProvider struct {
	t       *testing.T
	target  *testutil.MockStorageTarget
	nonces  *testutil.MockCommitNonces
	mu      sync.Mutex
	sends   [][]cid.Cid
	refuse  int
	nextID  uint64
	landed  map[string][]sdktypes.BigInt
	dataSet sdktypes.BigInt
}

func newRegistrationProvider(t *testing.T, providerID, dataSetID, clientID sdktypes.BigInt) *registrationProvider {
	p := &registrationProvider{
		t: t, nonces: &testutil.MockCommitNonces{}, nextID: 100,
		landed: make(map[string][]sdktypes.BigInt), dataSet: dataSetID,
	}
	p.target = testutil.NewMockDataSetTarget(providerID, dataSetID, nil)
	p.target.ClientDataSetIDValue = clientID
	var nonce uint64 = 40
	p.target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		nonce++
		return testutil.CommitExtraData(nonce), nil
	}
	p.target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		pieces := make([]cid.Cid, len(request.Pieces))
		for i, piece := range request.Pieces {
			pieces[i] = piece.PieceCID
		}
		p.sends = append(p.sends, pieces)
		if p.refuse > 0 {
			p.refuse--
			return nil, &pdp.HTTPError{StatusCode: 400, Body: "try again"}
		}
		tx := fmt.Sprintf("0xtx-%d", len(p.sends))
		ids := make([]sdktypes.BigInt, len(pieces))
		for i := range ids {
			ids[i] = sdktypes.NewBigInt(p.nextID + uint64(i))
		}
		p.nonces.ConsumeRequest(request.ExtraData, p.dataSet, sdktypes.NewBigInt(p.nextID), pieces)
		p.nextID += uint64(len(pieces))
		p.landed[tx] = ids
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
		tx := statusURL[len("https://provider.example/status/"):]
		ref, _ := p.target.DataSetRef()
		return &storage.CommitStatus{
			Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed, TransactionID: tx,
			DataSet: &ref, PieceIDs: p.landed[tx],
		}, nil
	}
	return p
}

// waitForCommitTask waits for the task, waking it whenever it sleeps so a
// test does not wait out the confirmation poll.
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
		if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ? AND status = 'pending' AND (wait_reason IS NULL OR wait_reason <> ?)`,
			time.Now().Add(-time.Second), id, storagecommit.ProviderRejectedWaitReason).Exec(t.Context()); err != nil {
			t.Fatalf("wake task %d: %v", id, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *registrationProvider) sent() [][]cid.Cid {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]cid.Cid(nil), p.sends...)
}

type registrationFixture struct {
	runtime  handlerTestRuntime
	provider *registrationProvider
	dataSet  *model.StorageDataSet
	copies   []*model.StorageCopy
}

// newRegistrationFixture seeds a data set and n contents whose single copies
// finished transferring to it.
func newRegistrationFixture(t *testing.T, n int, parked synapse.ParkedPieceChecker) registrationFixture {
	t.Helper()
	ctx := t.Context()
	storageClient := &testutil.MockStorageClient{}
	nonces := &testutil.MockCommitNonces{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: storageClient, policy: cache.EvictionPolicyNone, commitNonces: nonces, parkedPieces: parked,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
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
	provider := newRegistrationProvider(t, providerID.SDK(), dataSetID.SDK(), clientID.SDK())
	provider.nonces = nonces
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return provider.target, nil
	}
	return registrationFixture{runtime: runtime, provider: provider, dataSet: binding, copies: copies}
}

// collect puts every seeded copy into one collecting request driven by a new
// task.
func (f registrationFixture) collect(t *testing.T) (string, int64) {
	t.Helper()
	requestID := fmt.Sprintf("registration-%d", f.dataSet.ID)
	taskRow, _, err := f.runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageCommit, IdempotencyKey: storagepipeline.CommitKey(requestID),
		Input:       storagepipeline.CommitRequestInput{RequestID: requestID},
		SubjectType: model.TaskSubjectStorageCommitRequest, SubjectKey: requestID,
	})
	if err != nil {
		t.Fatalf("enqueue commit task: %v", err)
	}
	ids := make([]int64, len(f.copies))
	for i, copyRow := range f.copies {
		ids[i] = copyRow.ID
	}
	if err := f.runtime.repos.Contents.CreateCollectingCommitRequest(t.Context(), repository.CreateCommitRequestInput{
		RequestID: requestID, TaskID: taskRow.ID, StorageDataSetID: f.dataSet.ID, CopyIDs: ids,
	}); err != nil {
		t.Fatalf("CreateCollectingCommitRequest: %v", err)
	}
	return requestID, taskRow.ID
}

func TestCommitRequestRegistersEveryMemberInOneSubmission(t *testing.T) {
	f := newRegistrationFixture(t, 3, nil)
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	sends := f.provider.sent()
	if len(sends) != 1 || len(sends[0]) != 3 {
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
	request, err := f.runtime.repos.Contents.GetCommitRequest(t.Context(), requestID)
	if err != nil || request.Status != storagecommit.RequestStatusConfirmed || request.ConfirmedTransactionID == nil {
		t.Fatalf("request = %#v, %v", request, err)
	}
}

func TestRefusedCommitRequestBacksOffAndSendsTheSameRequest(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil)
	f.provider.refuse = 1
	requestID, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)

	waiting := waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == storagecommit.ProviderRejectedWaitReason
	})
	if waiting.ResumeMode != model.TaskResumeModeExecute {
		t.Fatalf("refused task = %#v, want an execute wait", waiting)
	}
	request, err := f.runtime.repos.Contents.GetCommitRequest(t.Context(), requestID)
	if err != nil || request.Status != storagecommit.RequestStatusReady || request.Refusals != 1 || request.RetryAt == nil {
		t.Fatalf("refused request = %#v, %v", request, err)
	}
	if _, err := f.runtime.db.NewRaw(`UPDATE storage_commit_requests SET retry_at = ? WHERE request_id = ?`, time.Now().Add(-time.Second), requestID).Exec(t.Context()); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	if _, err := f.runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskID).Exec(t.Context()); err != nil {
		t.Fatalf("wake refused task: %v", err)
	}
	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	sends := f.provider.sent()
	if len(sends) != 2 || len(sends[0]) != 2 || len(sends[1]) != 2 || !sends[0][0].Equals(sends[1][0]) || !sends[0][1].Equals(sends[1][1]) {
		t.Fatalf("submissions = %v, want the same two-piece request twice", sends)
	}
}

func TestDroppedMemberIsTransferredAgainBeforeTheRequestIsResent(t *testing.T) {
	parked := &droppedPieces{missing: map[string]bool{}}
	f := newRegistrationFixture(t, 2, parked)
	f.provider.refuse = 1
	first, err := f.runtime.repos.Contents.GetByID(t.Context(), f.copies[0].ContentID)
	if err != nil || first.PieceCID == nil {
		t.Fatalf("first content = %#v, %v", first, err)
	}
	parked.missing[*first.PieceCID] = true
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
}

type droppedPieces struct {
	mu      sync.Mutex
	missing map[string]bool
}

func (d *droppedPieces) FindParkedPiece(_ context.Context, _ string, pieceCID cid.Cid) (synapse.ParkedPieceState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.missing[pieceCID.String()] {
		return synapse.ParkedPieceMissing, nil
	}
	return synapse.ParkedPieceReady, nil
}
