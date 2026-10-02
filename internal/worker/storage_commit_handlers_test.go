package worker_test

import (
	"bytes"
	"context"
	"errors"
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
	signed          int
	dataSet         sdktypes.BigInt
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
	provider := newRegistrationProvider(t, providerID.SDK(), dataSetID.SDK(), clientID.SDK(), nonces)
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

func TestCommitRequestRegistersEveryMemberInOneSubmission(t *testing.T) {
	f := newRegistrationFixture(t, 3, nil)
	requestID, taskID := f.collect(t)
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
	f := newRegistrationFixture(t, 2, nil)
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
	f := newRegistrationFixture(t, 2, parked)
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
	f := newRegistrationFixture(t, 2, parked)
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
	f := newRegistrationFixture(t, 2, nil)
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
	f := newRegistrationFixture(t, 2, nil)
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

func TestReadyRequestWhoseNonceWasSpentIsSignedAgain(t *testing.T) {
	f := newRegistrationFixture(t, 2, nil)
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
	f := newRegistrationFixture(t, 2, nil)
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
	f := newRegistrationFixture(t, 2, nil)
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
	f := newRegistrationFixture(t, 1, nil)
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
