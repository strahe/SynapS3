package repository_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/uptrace/bun"
)

type commitFixture struct {
	db        *bun.DB
	repos     *repository.Repositories
	bucket    *model.Bucket
	dataSetID int64
}

func newCommitFixture(t *testing.T, db *bun.DB) commitFixture {
	t.Helper()
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-requests")
	anchor := seedContent(t, repos, bucket.ID, "commit-anchor", 10)
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "701"), CopyIndex: 0, CreatedByContentID: anchor,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: onChainID(t, "801")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	return commitFixture{db: db, repos: repos, bucket: bucket, dataSetID: binding.ID}
}

// commitLedgerCases run against every supported database.
var commitLedgerCases = []struct {
	name string
	run  func(*testing.T, commitFixture)
}{
	{"CommitRequestCollectsUpToItsLimitAndSealsTheSignedSet", testCommitRequestCollectsUpToItsLimitAndSealsTheSignedSet},
	{"CommitSubmissionTakesTheOldestEligibleRequestWithinCapacity", testCommitSubmissionTakesTheOldestEligibleRequestWithinCapacity},
	{"CommitRefusalReturnsRequestToReadyUntilItsRetryIsDue", testCommitRefusalReturnsRequestToReadyUntilItsRetryIsDue},
	{"CommitConfirmationCommitsMembersByPositionAndReplays", testCommitConfirmationCommitsMembersByPositionAndReplays},
	{"SignedSingleMemberIsDecidedByItsRequest", func(t *testing.T, f commitFixture) { testSignedCommitMembersAreDecidedByTheirRequest(t, f, 1) }},
	{"SignedBatchMembersAreDecidedByTheirRequest", func(t *testing.T, f commitFixture) { testSignedCommitMembersAreDecidedByTheirRequest(t, f, 2) }},
	{"RetainedCommitTaskIsKeptWhileItsRequestIsOpen", testRetainedCommitTaskIsKeptWhileItsRequestIsOpen},
	{"StoppedRequestDoesNotHoldTheQueue", testStoppedRequestDoesNotHoldTheQueue},
	{"LateTransferOfACommittedMemberCompletes", testLateTransferOfACommittedMemberCompletes},
}

func TestCommitRequestLedger(t *testing.T) {
	for _, c := range commitLedgerCases {
		t.Run(c.name, func(t *testing.T) { c.run(t, newCommitFixture(t, testDB(t))) })
	}
}

// transferredCopy creates a content whose ingress copy finished transferring.
func (f commitFixture) transferredCopy(t *testing.T, name string) *model.StorageCopy {
	t.Helper()
	contentID := seedContent(t, f.repos, f.bucket.ID, name, 10)
	version := newObjectVersion(f.bucket.ID, name, model.NewVersionID(), 10)
	version.ContentID = &contentID
	if _, err := createVersion(t, f.repos, version); err != nil {
		t.Fatalf("create version %s: %v", name, err)
	}
	if err := f.repos.Contents.CreateUploadCopiesForBindings(t.Context(), contentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: f.dataSetID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "701"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(%s): %v", name, err)
	}
	copyRow, err := f.repos.Contents.GetUploadCopy(t.Context(), contentID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy(%s) = %#v, %v", name, copyRow, err)
	}
	if err := f.repos.Contents.MarkUploadCopyPieceReady(t.Context(), repository.MarkUploadCopyPieceReadyInput{
		StorageCopyID: copyRow.ID, ContentID: contentID, CopyIndex: 0, PieceCID: "piece-" + name, RequireEligibleCopy: true,
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady(%s): %v", name, err)
	}
	return f.copy(t, copyRow.ID)
}

func (f commitFixture) copy(t *testing.T, id int64) *model.StorageCopy {
	t.Helper()
	copyRow, err := f.repos.Contents.GetUploadCopyByID(t.Context(), id)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopyByID(%d) = %#v, %v", id, copyRow, err)
	}
	return copyRow
}

func (f commitFixture) request(t *testing.T, id string) *storagecommit.Request {
	t.Helper()
	request, err := f.repos.Contents.GetCommitRequest(t.Context(), id)
	if err != nil {
		t.Fatalf("GetCommitRequest(%s): %v", id, err)
	}
	return request
}

// commitTask is the task row a request with the given ID is driven by.
func commitTask(requestID string) *model.Task {
	return &model.Task{
		Type: model.TaskTypeStorageCommit, IdempotencyKey: requestID, InputVersion: 1,
		Input: fmt.Appendf(nil, `{"request_id":%q}`, requestID), InputHash: requestID,
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	}
}

func (f commitFixture) task(t *testing.T, key string) int64 {
	t.Helper()
	row, _, err := f.repos.Tasks.Enqueue(t.Context(), commitTask(key))
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", key, err)
	}
	return row.ID
}

// collecting starts a collecting request holding the copies.
func (f commitFixture) collecting(t *testing.T, id string, copies ...*model.StorageCopy) int64 {
	return f.collectingAt(t, id, time.Time{}, copies...)
}

func (f commitFixture) collectingAt(t *testing.T, id string, now time.Time, copies ...*model.StorageCopy) int64 {
	t.Helper()
	taskID := f.task(t, id)
	ids := make([]int64, len(copies))
	for i, copyRow := range copies {
		ids[i] = copyRow.ID
	}
	if err := f.repos.Contents.CreateCollectingCommitRequest(t.Context(), repository.CreateCommitRequestInput{
		RequestID: id, TaskID: taskID, StorageDataSetID: f.dataSetID, CopyIDs: ids, Now: now,
	}); err != nil {
		t.Fatalf("CreateCollectingCommitRequest(%s): %v", id, err)
	}
	return taskID
}

func (f commitFixture) seal(t *testing.T, id string, taskID int64, copies ...*model.StorageCopy) []int64 {
	t.Helper()
	members := make([]repository.SealMember, len(copies))
	for i, copyRow := range copies {
		members[i] = repository.SealMember{CopyID: copyRow.ID, ContentID: copyRow.ContentID, PieceCID: fmt.Sprintf("piece-%d", copyRow.ContentID)}
	}
	spilled, err := f.repos.Contents.SealCommitRequest(t.Context(), repository.SealCommitRequestInput{
		RequestID: id, TaskID: taskID, Members: members, ExtraDataHex: "abcd",
	})
	if err != nil {
		t.Fatalf("SealCommitRequest(%s): %v", id, err)
	}
	return spilled
}

// sealedRequest is a ready request holding one fresh transferred copy.
func (f commitFixture) sealedRequest(t *testing.T, id string) (int64, *model.StorageCopy) {
	t.Helper()
	copyRow := f.transferredCopy(t, id)
	taskID := f.collecting(t, id, copyRow)
	f.seal(t, id, taskID, copyRow)
	return taskID, copyRow
}

func testCommitRequestCollectsUpToItsLimitAndSealsTheSignedSet(t *testing.T, f commitFixture) {
	ctx := t.Context()
	first, second, third := f.transferredCopy(t, "first"), f.transferredCopy(t, "second"), f.transferredCopy(t, "third")
	readyAt := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	taskID := f.collectingAt(t, "collect", readyAt, first)
	requestID, members, err := f.repos.Contents.JoinCollectingCommitRequest(ctx, repository.JoinCommitRequestInput{
		CopyID: second.ID, StorageDataSetID: f.dataSetID, MaxPieces: 2, Now: readyAt.Add(time.Minute),
	})
	if err != nil || requestID != "collect" || members != 2 {
		t.Fatalf("join second = %q, %d, %v", requestID, members, err)
	}
	joined, err := f.repos.Contents.ListCommitRequestMembers(ctx, "collect")
	if err != nil || len(joined) != 2 {
		t.Fatalf("members = %#v, %v, want two", joined, err)
	}
	if joined[0].ID != first.ID || joined[0].CommitReadyAt == nil || !joined[0].CommitReadyAt.Equal(readyAt) {
		t.Fatalf("oldest member = %#v, want first ready at %s", joined[0], readyAt)
	}
	if _, _, err := f.repos.Contents.JoinCollectingCommitRequest(ctx, repository.JoinCommitRequestInput{
		CopyID: third.ID, StorageDataSetID: f.dataSetID, MaxPieces: 2,
	}); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("join a full request = %v, want not found", err)
	}

	// The request was signed for the first copy only; the second joined after
	// its members were read and waits for the next request.
	spilled := f.seal(t, "collect", taskID, first)
	if len(spilled) != 1 || spilled[0] != second.ID {
		t.Fatalf("spilled = %v, want the late copy", spilled)
	}
	request := f.request(t, "collect")
	if request.Status != storagecommit.RequestStatusReady || request.PieceCount != 1 || request.SealedAt == nil {
		t.Fatalf("sealed request = %#v", request)
	}
	if sealed := f.copy(t, first.ID); sealed.Status != model.StorageCopyStatusCommitting || sealed.CommitPosition == nil || *sealed.CommitPosition != 0 {
		t.Fatalf("signed member = %#v, want committing at position 0", sealed)
	}
	if late := f.copy(t, second.ID); late.Status != model.StorageCopyStatusPieceReady || late.CommitRequestID != nil {
		t.Fatalf("late copy = %#v, want piece_ready outside any request", late)
	}
	f.collectingAt(t, "spill", readyAt.Add(time.Hour), second)
	if copyRow := f.copy(t, second.ID); copyRow.CommitReadyAt == nil || !copyRow.CommitReadyAt.Equal(readyAt.Add(time.Minute)) {
		t.Fatalf("spilled member ready at = %v, want %s", copyRow.CommitReadyAt, readyAt.Add(time.Minute))
	}
	pieces, err := f.repos.Contents.ListCommitRequestPieces(ctx, "collect")
	if err != nil || len(pieces) != 1 || pieces[0].ContentID != first.ContentID {
		t.Fatalf("pieces = %#v, %v", pieces, err)
	}
	// A signature names its members exactly: a member that left refuses it.
	other := f.collecting(t, "other", third)
	if _, err := f.repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
		RequestID: "other", TaskID: other, ExtraDataHex: "abcd",
		Members: []repository.SealMember{{CopyID: second.ID, ContentID: second.ContentID, PieceCID: "piece"}},
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("seal naming a copy outside the request = %v, want conflict", err)
	}
}

func testCommitSubmissionTakesTheOldestEligibleRequestWithinCapacity(t *testing.T, f commitFixture) {
	ctx := t.Context()
	tasks := make([]int64, storagecommit.MaxSubmittedRequestsPerDataSet+1)
	for i := range tasks {
		tasks[i], _ = f.sealedRequest(t, fmt.Sprintf("request-%d", i))
	}
	begin := func(i int) error {
		return f.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{
			RequestID: fmt.Sprintf("request-%d", i), TaskID: tasks[i],
		})
	}
	if err := begin(1); !errors.Is(err, storagecommit.ErrNotEligible) {
		t.Fatalf("a younger request went first: %v", err)
	}
	later := time.Now().Add(time.Hour)
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", later).
		Where("id IN (?)", bun.List(tasks)).Exec(ctx); err != nil {
		t.Fatalf("park tasks: %v", err)
	}
	if err := begin(0); err != nil {
		t.Fatalf("begin request 0: %v", err)
	}
	// The data set still has room, so the next request in line is woken; the
	// one behind it keeps waiting.
	for i, wantWoken := range map[int]bool{1: true, 2: false} {
		task, err := f.repos.Tasks.GetByID(ctx, tasks[i])
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		// Stored timestamps may be coarser than Go's clock, so the parked
		// time is compared with a margin rather than exactly.
		if woken := task.AvailableAt.Before(later.Add(-time.Minute)); woken != wantWoken {
			t.Fatalf("request-%d woken = %v, want %v", i, woken, wantWoken)
		}
	}
	for i := 1; i < storagecommit.MaxSubmittedRequestsPerDataSet; i++ {
		if err := begin(i); err != nil {
			t.Fatalf("begin request %d: %v", i, err)
		}
	}
	if err := begin(storagecommit.MaxSubmittedRequestsPerDataSet); !errors.Is(err, storagecommit.ErrNotEligible) {
		t.Fatalf("a request beyond capacity was sent: %v", err)
	}
	submitted := f.request(t, "request-0")
	if submitted.Status != storagecommit.RequestStatusSubmitted || submitted.Sends != 1 || submitted.FirstSentAt == nil {
		t.Fatalf("submitted request = %#v", submitted)
	}
	// Another task cannot send a request it does not drive.
	if err := f.repos.Contents.RecordCommitResend(ctx, repository.CommitSendInput{RequestID: "request-0", TaskID: tasks[1], Sends: 2}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("resend by another task = %v, want conflict", err)
	}
}

func testCommitRefusalReturnsRequestToReadyUntilItsRetryIsDue(t *testing.T, f commitFixture) {
	ctx := t.Context()
	taskID, _ := f.sealedRequest(t, "refused")
	if err := f.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: "refused", TaskID: taskID}); err != nil {
		t.Fatalf("BeginCommitSubmission: %v", err)
	}
	if err := f.repos.Contents.ReturnCommitRequestToReady(ctx, repository.ReturnCommitRequestInput{
		RequestID: "refused", TaskID: taskID, Refused: true, SubmitError: "provider returned HTTP 400", RetryAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("ReturnCommitRequestToReady: %v", err)
	}
	request := f.request(t, "refused")
	if request.Status != storagecommit.RequestStatusReady || request.Refusals != 1 || request.Sends != 0 ||
		request.FirstSentAt == nil || request.SubmitError == nil {
		t.Fatalf("refused request = %#v", request)
	}
	if err := f.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: "refused", TaskID: taskID}); !errors.Is(err, storagecommit.ErrNotEligible) {
		t.Fatalf("send before the retry is due = %v, want not eligible", err)
	}
	// A request whose send may have landed never goes back to ready.
	other, _ := f.sealedRequest(t, "resent")
	if err := f.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: "resent", TaskID: other}); err != nil {
		t.Fatalf("BeginCommitSubmission(resent): %v", err)
	}
	if err := f.repos.Contents.RecordCommitResend(ctx, repository.CommitSendInput{RequestID: "resent", TaskID: other, Sends: 2}); err != nil {
		t.Fatalf("RecordCommitResend: %v", err)
	}
	if err := f.repos.Contents.ReturnCommitRequestToReady(ctx, repository.ReturnCommitRequestInput{
		RequestID: "resent", TaskID: other, Refused: true, SubmitError: "refused", RetryAt: time.Now(),
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("refusing a resend = %v, want conflict", err)
	}
}

func testCommitConfirmationCommitsMembersByPositionAndReplays(t *testing.T, f commitFixture) {
	ctx := t.Context()
	first, second := f.transferredCopy(t, "first"), f.transferredCopy(t, "second")
	taskID := f.collecting(t, "batch", first, second)
	f.seal(t, "batch", taskID, first, second)
	if err := f.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: "batch", TaskID: taskID}); err != nil {
		t.Fatalf("BeginCommitSubmission: %v", err)
	}
	if err := f.repos.Contents.RecordCommitSubmission(ctx, repository.CommitSubmissionInput{
		CommitSendInput: repository.CommitSendInput{RequestID: "batch", TaskID: taskID, Sends: 1},
		TransactionID:   "0xtx", StatusURL: "https://provider.example/status/0xtx",
	}); err != nil {
		t.Fatalf("RecordCommitSubmission: %v", err)
	}
	confirm := func(first string) ([]model.StorageCopy, error) {
		return f.repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{
			RequestID: "batch", TaskID: taskID, ConfirmedTransactionID: "0xtx", FirstPieceID: onChainID(t, first),
			RetrievalURLs: []string{"https://provider.example/piece/first", "https://provider.example/piece/second"},
		})
	}
	members, err := confirm("40")
	if err != nil || len(members) != 2 {
		t.Fatalf("ConfirmCommitRequest = %#v, %v", members, err)
	}
	for _, want := range []struct {
		copy  *model.StorageCopy
		piece string
		url   string
	}{{first, "40", "https://provider.example/piece/first"}, {second, "41", "https://provider.example/piece/second"}} {
		committed := f.copy(t, want.copy.ID)
		if committed.Status != model.StorageCopyStatusCommitted || committed.PieceID == nil || committed.PieceID.String() != want.piece ||
			committed.RetrievalURL == nil || *committed.RetrievalURL != want.url {
			t.Fatalf("committed member = %#v, want piece %s", committed, want.piece)
		}
	}
	request := f.request(t, "batch")
	if request.Status != storagecommit.RequestStatusConfirmed || request.TaskID != nil || request.FirstPieceID == nil {
		t.Fatalf("confirmed request = %#v", request)
	}
	if _, err := confirm("40"); err != nil {
		t.Fatalf("replayed confirmation: %v", err)
	}
	if _, err := confirm("41"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("confirmation with other piece IDs = %v, want conflict", err)
	}
}

func testSignedCommitMembersAreDecidedByTheirRequest(t *testing.T, f commitFixture, count int) {
	ctx := t.Context()
	copyRow := f.transferredCopy(t, "pinned")
	copies := []*model.StorageCopy{copyRow}
	if count == 2 {
		copies = append(copies, f.transferredCopy(t, "pinned-other"))
	}
	taskID := f.collecting(t, "pinned", copies...)
	f.seal(t, "pinned", taskID, copies...)
	fail := func() error {
		return f.repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{
			StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex, LastError: "stop",
		})
	}
	if err := fail(); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("failing a signed member = %v, want conflict", err)
	}
	if _, err := f.repos.Contents.AbandonCommitRequest(ctx, repository.AbandonCommitRequestInput{RequestID: "pinned", Reason: "stop"}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("abandoning a sealed request without its task = %v, want conflict", err)
	}
	if backlog, err := f.repos.Contents.CountCommitBacklog(ctx, f.dataSetID); err != nil || backlog != count {
		t.Fatalf("backlog = %d, %v, want the unsent members", backlog, err)
	}
	members, err := f.repos.Contents.AbandonCommitRequest(ctx, repository.AbandonCommitRequestInput{
		RequestID: "pinned", TaskID: taskID, Reason: "the storage service ended",
	})
	if err != nil || len(members) != count {
		t.Fatalf("AbandonCommitRequest = %#v, %v", members, err)
	}
	released := f.copy(t, copyRow.ID)
	if released.Status != model.StorageCopyStatusPieceReady || released.CommitRequestID != nil || released.CommitPosition != nil {
		t.Fatalf("released member = %#v", released)
	}
	if err := fail(); err != nil {
		t.Fatalf("failing a released member: %v", err)
	}
	request := f.request(t, "pinned")
	if request.Status != storagecommit.RequestStatusAbandoned || request.TaskID != nil {
		t.Fatalf("abandoned request = %#v", request)
	}
}

func testRetainedCommitTaskIsKeptWhileItsRequestIsOpen(t *testing.T, f commitFixture) {
	ctx := t.Context()
	taskID, _ := f.sealedRequest(t, "retained")
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCompleted).Set("finished_at = ?", time.Now()).
		Set("retention_until = ?", time.Now().Add(-time.Hour)).
		Where("id = ?", taskID).Exec(ctx); err != nil {
		t.Fatalf("expire task: %v", err)
	}
	if deleted, err := f.repos.Tasks.DeleteRetained(ctx, time.Now(), 10); err != nil || deleted != 0 {
		t.Fatalf("DeleteRetained = %d, %v, want the request's task kept", deleted, err)
	}
}

func testStoppedRequestDoesNotHoldTheQueue(t *testing.T, f commitFixture) {
	ctx := t.Context()
	stopped, _ := f.sealedRequest(t, "stopped")
	next, _ := f.sealedRequest(t, "next")
	// The older request's task failed before sending; it waits for an operator
	// while the requests behind it still leave.
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).
		Set("failure_reason = ?", "handler_panic").Set("last_error = ?", "handler panicked").
		Where("id = ?", stopped).Exec(ctx); err != nil {
		t.Fatalf("stop task: %v", err)
	}
	state, err := f.repos.Contents.CommitQueueState(ctx, f.dataSetID, time.Now())
	if err != nil || state.ReadyHead != "next" {
		t.Fatalf("queue = %#v, %v, want the live request next", state, err)
	}
	if err := f.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: "next", TaskID: next}); err != nil {
		t.Fatalf("BeginCommitSubmission(next): %v", err)
	}
	if request := f.request(t, "stopped"); request.Status != storagecommit.RequestStatusReady || request.TaskID == nil || *request.TaskID != stopped {
		t.Fatalf("stopped request = %#v, want it kept for its task", request)
	}
}

func testLateTransferOfACommittedMemberCompletes(t *testing.T, f commitFixture) {
	ctx := t.Context()
	first, second := f.transferredCopy(t, "first"), f.transferredCopy(t, "second")
	taskID := f.collecting(t, "late", first, second)
	f.seal(t, "late", taskID, first, second)
	if err := f.repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: "late", TaskID: taskID}); err != nil {
		t.Fatalf("BeginCommitSubmission: %v", err)
	}
	// The provider dropped the first piece, so it is transferred again; an
	// earlier send lands meanwhile and commits it anyway.
	if err := f.repos.Contents.ReturnCommitMembersToTransfer(ctx, "late", taskID, []int64{first.ID}, time.Now()); err != nil {
		t.Fatalf("ReturnCommitMembersToTransfer: %v", err)
	}
	firstURL := "https://provider.example/piece/first"
	if _, err := f.repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{
		RequestID: "late", TaskID: taskID, FirstPieceID: onChainID(t, "50"),
		RetrievalURLs: []string{firstURL, "https://provider.example/piece/second"},
	}); err != nil {
		t.Fatalf("ConfirmCommitRequest: %v", err)
	}
	finish := func(pieceCID string) error {
		return f.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageCopyID: first.ID, ContentID: first.ContentID, CopyIndex: first.CopyIndex,
			PieceCID: pieceCID, RetrievalURL: firstURL, RequireEligibleCopy: true,
		})
	}
	content, err := f.repos.Contents.GetByID(ctx, first.ContentID)
	if err != nil || content.PieceCID == nil {
		t.Fatalf("content = %#v, %v", content, err)
	}
	if err := finish(*content.PieceCID); err != nil {
		t.Fatalf("late transfer with the same evidence = %v, want it to complete", err)
	}
	if committed := f.copy(t, first.ID); committed.Status != model.StorageCopyStatusCommitted || committed.PieceID == nil || committed.PieceID.String() != "50" {
		t.Fatalf("member after its late transfer = %#v, want it still committed", committed)
	}
	if err := finish("piece-other"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("late transfer of other bytes = %v, want conflict", err)
	}
}
