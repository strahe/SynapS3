package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storageremoval"
	"github.com/strahe/synaps3/internal/testutil"
)

type removalFixture struct {
	commitFixture
	identity storagepipeline.ExecutionIdentity
	rows     []model.StorageCleanupCopy
	owners   []int64
}

func newRemovalFixture(t *testing.T, members int) removalFixture {
	t.Helper()
	f := removalFixture{commitFixture: newCommitFixture(t, testDB(t)), identity: storagepipeline.NewExecutionIdentity(testutil.DefaultContextIdentity, testutil.DefaultPDPVerifierAddress)}
	for i := range members {
		contentID := seedContent(t, f.repos, f.bucket.ID, fmt.Sprintf("removal-%d", i), 10)
		task, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeStorageCleanup, InputVersion: 1, Input: []byte(fmt.Sprintf(`{"content_id":%d,"generation":1}`, contentID)), SubjectType: model.TaskSubjectStorageContent, SubjectKey: strconv.FormatInt(contentID, 10), Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now()}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("cleanup_generation = 1").Set("cleanup_task_id = ?", task.ID).Where("id = ?", contentID).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		row := model.StorageCleanupCopy{ContentID: contentID, BucketID: f.bucket.ID, StorageDataSetID: f.dataSetID, ProviderID: onChainID(t, "701"), CopyIndex: 0, PieceID: onChainID(t, strconv.Itoa(100+i)), PieceCID: fmt.Sprintf("piece-%d", i), Checksum: testutil.StorageChecksum(fmt.Sprintf("removal-%d", i)), Status: model.StorageCleanupCopyStatusPending}
		if _, err := f.db.NewInsert().Model(&row).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		f.rows = append(f.rows, row)
		f.owners = append(f.owners, task.ID)
	}
	return f
}

func (f removalFixture) enqueue(ctx context.Context, repos *repository.Repositories, input storageremoval.Input) (int64, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return 0, err
	}
	row, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: model.TaskTypeStorageRemoval, InputVersion: 1, Input: raw, SubjectType: model.TaskSubjectStorageRemovalRequest, SubjectKey: input.RequestID, Generation: new(int64), Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now()}))
	if err != nil {
		return 0, err
	}
	return row.ID, nil
}

func (f removalFixture) join(t *testing.T, index int) *storageremoval.Request {
	t.Helper()
	row := f.rows[index]
	request, err := f.repos.StorageRemoval.JoinOrCreateCollecting(t.Context(), repository.JoinRemovalInput{ContentID: row.ContentID, Generation: 1, ContentTaskID: f.owners[index], StorageDataSetID: f.dataSetID, CopyIDs: []int64{row.ID}, Identity: f.identity, Enqueue: f.enqueue})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestRemovalCollectingMembershipIsConcurrentAndBounded(t *testing.T) {
	f := newRemovalFixture(t, 3)
	var wg sync.WaitGroup
	requests := make(chan string, 3)
	errs := make(chan error, 3)
	for i, row := range f.rows {
		wg.Go(func() {
			request, err := f.repos.StorageRemoval.JoinOrCreateCollecting(t.Context(), repository.JoinRemovalInput{ContentID: row.ContentID, Generation: 1, ContentTaskID: f.owners[i], StorageDataSetID: f.dataSetID, CopyIDs: []int64{row.ID}, Identity: f.identity, Enqueue: f.enqueue})
			if err != nil {
				errs <- err
				return
			}
			requests <- request.RequestID
		})
	}
	wg.Wait()
	close(requests)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var id string
	for got := range requests {
		if id != "" && got != id {
			t.Fatalf("collecting requests = %q and %q", id, got)
		}
		id = got
	}
	request, err := f.repos.StorageRemoval.GetRemovalRequest(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("wait_reason = ?", "removal_pending").Set("available_at = ?", time.Now().Add(time.Hour)).Where("id = ?", f.owners[2]).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: id, TaskID: *request.TaskID, MaxPieces: 2}); err != nil {
		t.Fatal(err)
	}
	rows, err := f.repos.StorageRemoval.ListRemovalMembers(t.Context(), id)
	if err != nil || len(rows) != 2 {
		t.Fatalf("sealed members = %v, %v", rows, err)
	}
	woken, err := f.repos.Tasks.GetByID(t.Context(), f.owners[2])
	if err != nil || woken.AvailableAt.After(time.Now()) {
		t.Fatalf("overflow content task was not woken: %#v, %v", woken, err)
	}
	next := f.join(t, 2)
	if next.RequestID == id {
		t.Fatal("sealed request accepted another member")
	}
}

func TestRemovalSendLedgerPreservesEvidenceAndAuthorization(t *testing.T) {
	f := newRemovalFixture(t, 3)
	request := f.join(t, 0)
	f.join(t, 1)
	f.join(t, 2)
	id, taskID := request.RequestID, *request.TaskID
	if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: id, TaskID: taskID, MaxPieces: 3}); err != nil {
		t.Fatal(err)
	}
	admit := repository.AdmitRemovalSendInput{RequestID: id, TaskID: taskID, SendNo: 1, CopyIDs: []int64{f.rows[0].ID, f.rows[1].ID}, AttemptedAt: time.Now(), QueueLength: 1999, SnapshotBlock: 10}
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); !errors.Is(err, repository.ErrNoRemovalRoom) {
		t.Fatalf("capacity = %v", err)
	}
	admit.QueueLength = 0
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
		t.Fatal(err)
	}
	evidence := repository.RemovalSendEvidenceInput{RequestID: id, TaskID: taskID, SendNo: 1, Message: "provider refused"}
	if err := f.repos.StorageRemoval.RecordSendRefused(t.Context(), evidence); err != nil {
		t.Fatal(err)
	}
	admit.SendNo = 2
	admit.CopyIDs = []int64{f.rows[2].ID}
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("authorization expanded: %v", err)
	}
	admit.CopyIDs = []int64{f.rows[1].ID}
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
		t.Fatal(err)
	}
	evidence.SendNo = 2
	evidence.TxHash = "hash-2"
	if err := f.repos.StorageRemoval.RecordSendSubmission(t.Context(), evidence); err != nil {
		t.Fatal(err)
	}
	evidence.TxHash = "replacement"
	if err := f.repos.StorageRemoval.RecordSendSubmission(t.Context(), evidence); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("hash replaced: %v", err)
	}
	status := int64(0)
	evidence.ReceiptStatus = &status
	evidence.ConfirmedTxHash = "hash-2"
	if err := f.repos.StorageRemoval.RecordSendReceipt(t.Context(), evidence); err != nil {
		t.Fatal(err)
	}
	rows, err := f.repos.StorageRemoval.ListRemovalSends(t.Context(), id)
	if err != nil || len(rows) != 2 || rows[0].RefusedAt == nil || len(rows[0].Pieces) != 2 || rows[1].TxHash == nil || *rows[1].TxHash != "hash-2" || rows[1].ReceiptStatus == nil || *rows[1].ReceiptStatus != 0 || len(rows[1].Pieces) != 1 {
		t.Fatalf("send history = %#v, %v", rows, err)
	}
}

func TestRemovalCapacityCountsBatchesQueuedAfterSnapshot(t *testing.T) {
	f := newRemovalFixture(t, 2)
	first := f.join(t, 0)
	if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: first.RequestID, TaskID: *first.TaskID, MaxPieces: 1}); err != nil {
		t.Fatal(err)
	}
	second := f.join(t, 1)
	if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: second.RequestID, TaskID: *second.TaskID, MaxPieces: 1}); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), repository.AdmitRemovalSendInput{RequestID: first.RequestID, TaskID: *first.TaskID, SendNo: 1, CopyIDs: []int64{f.rows[0].ID}, SnapshotBlock: 100}); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.MarkRemovalQueued(t.Context(), repository.RemovalQueuedInput{RequestID: first.RequestID, TaskID: *first.TaskID, QueuedBlock: 101}); err != nil {
		t.Fatal(err)
	}
	admit := repository.AdmitRemovalSendInput{RequestID: second.RequestID, TaskID: *second.TaskID, SendNo: 1, CopyIDs: []int64{f.rows[1].ID}, SnapshotBlock: 100, QueueLength: 1999}
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); !errors.Is(err, repository.ErrNoRemovalRoom) {
		t.Fatalf("newly queued reservation lost: %v", err)
	}
	admit.SnapshotBlock = 101
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
		t.Fatal(err)
	}
}

func TestRemovalCapacityRetainsUnresolvedSendMembers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reserved int
	}{
		{name: "unresolved", reserved: 3},
		{name: "refused", reserved: 1},
		{name: "unsent", reserved: 1},
		{name: "reverted", reserved: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRemovalFixture(t, 4)
			first := f.join(t, 0)
			f.join(t, 1)
			f.join(t, 2)
			if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: first.RequestID, TaskID: *first.TaskID, MaxPieces: 3}); err != nil {
				t.Fatal(err)
			}
			second := f.join(t, 3)
			if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: second.RequestID, TaskID: *second.TaskID, MaxPieces: 1}); err != nil {
				t.Fatal(err)
			}
			admit := repository.AdmitRemovalSendInput{RequestID: first.RequestID, TaskID: *first.TaskID, SendNo: 1, CopyIDs: []int64{f.rows[0].ID, f.rows[1].ID, f.rows[2].ID}, SnapshotBlock: 100}
			if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
				t.Fatal(err)
			}
			evidence := repository.RemovalSendEvidenceInput{RequestID: first.RequestID, TaskID: *first.TaskID, SendNo: 1}
			var err error
			switch tc.name {
			case "unresolved":
			case "refused":
				err = f.repos.StorageRemoval.RecordSendRefused(t.Context(), evidence)
			case "unsent":
				err = f.repos.StorageRemoval.RecordSendUnsent(t.Context(), evidence)
			case "reverted":
				status := int64(0)
				evidence.ReceiptStatus, evidence.ConfirmedTxHash = &status, "removal-1"
				err = f.repos.StorageRemoval.RecordSendReceipt(t.Context(), evidence)
			}
			if err != nil {
				t.Fatal(err)
			}
			admit.SendNo, admit.CopyIDs = 2, []int64{f.rows[2].ID}
			admit.SnapshotBlock, admit.QueueLength = 101, 1999
			if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
				t.Fatalf("overlapping resend reserves itself twice: %v", err)
			}
			admit = repository.AdmitRemovalSendInput{RequestID: second.RequestID, TaskID: *second.TaskID, SendNo: 1, CopyIDs: []int64{f.rows[3].ID}, SnapshotBlock: 100, QueueLength: 2000 - tc.reserved}
			if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); !errors.Is(err, repository.ErrNoRemovalRoom) {
				t.Fatalf("admission exceeds queue capacity: %v", err)
			}
			admit.QueueLength--
			if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
				t.Fatalf("distinct in-flight members fit exactly: %v", err)
			}
		})
	}
}

func TestRemovalRefusalCannotReleaseUnresolvedAuthorization(t *testing.T) {
	f := newRemovalFixture(t, 2)
	request := f.join(t, 0)
	f.join(t, 1)
	id, taskID := request.RequestID, *request.TaskID
	if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: id, TaskID: taskID, MaxPieces: 2}); err != nil {
		t.Fatal(err)
	}
	admit := repository.AdmitRemovalSendInput{RequestID: id, TaskID: taskID, SendNo: 1, CopyIDs: []int64{f.rows[0].ID, f.rows[1].ID}, AttemptedAt: time.Now()}
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
		t.Fatal(err)
	}
	admit.SendNo = 2
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), admit); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.RecordSendRefused(t.Context(), repository.RemovalSendEvidenceInput{RequestID: id, TaskID: taskID, SendNo: 2, Message: "Piece not found"}); err != nil {
		t.Fatal(err)
	}
	member := repository.RemovalMembersInput{RequestID: id, TaskID: taskID, CopyIDs: []int64{f.rows[0].ID}}
	if err := f.repos.StorageRemoval.DetachMembers(t.Context(), member); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("detach unresolved member = %v, want ErrConflict", err)
	}
	if err := f.repos.StorageRemoval.MarkMemberFailed(t.Context(), member); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("fail unresolved member = %v, want ErrConflict", err)
	}
	got, err := f.repos.StorageRemoval.GetRemovalRequest(t.Context(), id)
	if err != nil || got.Status != storageremoval.RequestStatusSubmitted || got.TaskID == nil || *got.TaskID != taskID {
		t.Fatalf("request lost unresolved ownership: %#v, %v", got, err)
	}
	members, err := f.repos.StorageRemoval.ListRemovalMembers(t.Context(), id)
	if err != nil || len(members) != 2 {
		t.Fatalf("unresolved membership = %#v, %v", members, err)
	}
	sends, err := f.repos.StorageRemoval.ListRemovalSends(t.Context(), id)
	if err != nil || len(sends) != 2 || !sends[0].PossiblyAccepted() || sends[1].RefusedAt == nil || len(sends[0].Pieces) != 2 || len(sends[1].Pieces) != 2 {
		t.Fatalf("unresolved authorization lost: %#v, %v", sends, err)
	}
}

func TestRemovalConcurrentAdmissionReservesQueueCapacity(t *testing.T) {
	f := newRemovalFixture(t, 2)
	requests := make([]*storageremoval.Request, 2)
	for i := range requests {
		requests[i] = f.join(t, i)
		if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: requests[i].RequestID, TaskID: *requests[i].TaskID, MaxPieces: 1}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	results := make(chan error, len(requests))
	var wg sync.WaitGroup
	for i, request := range requests {
		wg.Go(func() {
			<-start
			results <- f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), repository.AdmitRemovalSendInput{RequestID: request.RequestID, TaskID: *request.TaskID, SendNo: 1, CopyIDs: []int64{f.rows[i].ID}, QueueLength: 1999, SnapshotBlock: 100})
		})
	}
	close(start)
	wg.Wait()
	close(results)
	admitted, waiting := 0, 0
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, repository.ErrNoRemovalRoom):
			waiting++
		default:
			t.Fatalf("concurrent admission = %v", err)
		}
	}
	if admitted != 1 || waiting != 1 {
		t.Fatalf("admitted = %d, waiting = %d", admitted, waiting)
	}
	count, err := f.db.NewSelect().Model((*storageremoval.Send)(nil)).Count(t.Context())
	if err != nil || count != 1 {
		t.Fatalf("admission ledger count = %d, %v", count, err)
	}
}

func TestRemovalLegacyAdoptionSeparatesOldHashAndUnknownRetry(t *testing.T) {
	f := newRemovalFixture(t, 1)
	originalTaskID := f.owners[0]
	at := time.Now().UTC().Truncate(time.Microsecond)
	checkpoint, _ := json.Marshal(map[string]any{"copy_id": f.rows[0].ID, "attempted_at": at, "identity": f.identity.Identity, "pdp_verifier": f.identity.PDPVerifier})
	runtime, _ := json.Marshal(model.TaskRuntime{OperationKey: fmt.Sprintf("cleanup:%d", f.rows[0].ID), OperationStartedAt: &at, LastAdmittedAttempt: 1})
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(checkpoint)).Set("runtime_json = ?", json.RawMessage(runtime)).Where("id = ?", f.owners[0]).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Where("id = ?", originalTaskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Tasks.SupersedeTerminal(t.Context(), originalTaskID); err != nil {
		t.Fatal(err)
	}
	successor, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeStorageCleanup, InputVersion: 1, Input: []byte(fmt.Sprintf(`{"content_id":%d,"generation":1}`, f.rows[0].ContentID)), Checkpoint: checkpoint, RetryOfTaskID: &originalTaskID, SubjectType: model.TaskSubjectStorageContent, SubjectKey: strconv.FormatInt(f.rows[0].ContentID, 10), Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageCleanup.TransferTaskOwner(t.Context(), f.rows[0].ContentID, 1, originalTaskID, successor.ID); err != nil {
		t.Fatal(err)
	}
	f.owners[0] = successor.ID
	if _, err := f.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("delete_tx_hash = ?", "old-hash").Where("id = ?", f.rows[0].ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	request, err := f.repos.StorageRemoval.AdoptLegacyCleanupCopy(t.Context(), repository.AdoptLegacyCleanupCopyInput{ContentID: f.rows[0].ContentID, Generation: 1, ContentTaskID: f.owners[0], CopyID: f.rows[0].ID, AttemptedAt: at, RetryOfTxHash: "old-hash", Identity: f.identity, Enqueue: f.enqueue})
	if err != nil {
		t.Fatal(err)
	}
	sends, err := f.repos.StorageRemoval.ListRemovalSends(t.Context(), request.RequestID)
	if err != nil || len(sends) != 2 {
		t.Fatalf("legacy sends = %#v, %v", sends, err)
	}
	if sends[0].TaskID != nil || sends[0].AttemptedAt != nil || sends[0].TxHash == nil || *sends[0].TxHash != "old-hash" || sends[1].TaskID == nil || *sends[1].TaskID != originalTaskID || sends[1].TxHash != nil || sends[1].AttemptedAt == nil || !sends[1].AttemptedAt.Equal(at) {
		t.Fatalf("mixed legacy evidence = %#v", sends)
	}
	row := new(model.StorageCleanupCopy)
	if err := f.db.NewSelect().Model(row).Where("id = ?", f.rows[0].ID).Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if row.DeleteTxHash == nil || *row.DeleteTxHash != "old-hash" {
		t.Fatalf("legacy hash lost: %#v", row)
	}
}

func TestRemovalOwnerCannotBeAcknowledged(t *testing.T) {
	f := newRemovalFixture(t, 1)
	request := f.join(t, 0)
	taskID := *request.TaskID
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now().Add(-time.Hour)).Where("id = ?", taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Tasks.AcknowledgeFailed(t.Context(), taskID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("single acknowledge = %v", err)
	}
	if count, err := f.repos.Tasks.AcknowledgeFailedForSubject(t.Context(), model.TaskSubjectStorageRemovalRequest, request.RequestID); err != nil || count != 0 {
		t.Fatalf("subject acknowledge = %d, %v", count, err)
	}
	if count, err := f.repos.Tasks.AcknowledgeFailedMatching(t.Context(), repository.TaskAcknowledgeFilter{FailedBefore: time.Now(), Type: model.TaskTypeStorageRemoval}); err != nil || count != 0 {
		t.Fatalf("bulk acknowledge = %d, %v", count, err)
	}
}

func TestRemovalRetryTransfersOwnershipAndKeepsSendEvidence(t *testing.T) {
	f := newRemovalFixture(t, 1)
	request := f.join(t, 0)
	id, originalTaskID := request.RequestID, *request.TaskID
	if err := f.repos.StorageRemoval.SealRemovalRequest(t.Context(), repository.SealRemovalInput{RequestID: id, TaskID: originalTaskID, MaxPieces: 1}); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.AdmitRemovalSend(t.Context(), repository.AdmitRemovalSendInput{RequestID: id, TaskID: originalTaskID, SendNo: 1, CopyIDs: []int64{f.rows[0].ID}, AttemptedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.RecordSendSubmission(t.Context(), repository.RemovalSendEvidenceInput{RequestID: id, TaskID: originalTaskID, SendNo: 1, TxHash: "preserved-hash"}); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.MarkRemovalReverted(t.Context(), repository.RemovalRequestInput{RequestID: id, TaskID: originalTaskID, Message: "Removal transaction failed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Where("id = ?", originalTaskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Tasks.SupersedeTerminal(t.Context(), originalTaskID); err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(storageremoval.Input{ExecutionIdentity: f.identity, RequestID: id})
	if err != nil {
		t.Fatal(err)
	}
	successor, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeStorageRemoval, InputVersion: 1, Input: input, RetryOfTaskID: &originalTaskID, SubjectType: model.TaskSubjectStorageRemovalRequest, SubjectKey: id, Generation: new(int64), Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.TransferRemovalTaskOwner(t.Context(), id, originalTaskID, successor.ID); err != nil {
		t.Fatal(err)
	}
	got, err := f.repos.StorageRemoval.GetRemovalRequest(t.Context(), id)
	if err != nil || got.TaskID == nil || *got.TaskID != successor.ID || got.Status != storageremoval.RequestStatusSubmitted || got.AttentionCode != nil || got.AttentionAt != nil {
		t.Fatalf("retry ownership = %#v, %v", got, err)
	}
	sends, err := f.repos.StorageRemoval.ListRemovalSends(t.Context(), id)
	if err != nil || len(sends) != 1 || sends[0].TaskID == nil || *sends[0].TaskID != originalTaskID || sends[0].TxHash == nil || *sends[0].TxHash != "preserved-hash" {
		t.Fatalf("retry altered send provenance: %#v, %v", sends, err)
	}
	if err := f.repos.StorageRemoval.SetRemovalAttention(t.Context(), repository.RemovalAttentionInput{RequestID: id, TaskID: originalTaskID, Code: storageremoval.AttentionProviderUnreachable}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("superseded owner wrote attention: %v", err)
	}
	if _, err := f.db.NewUpdate().Model((*storageremoval.Request)(nil)).Set("attention_code = ?", "future_stop_code").Set("attention_at = ?", time.Now()).Where("request_id = ?", id).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Where("id = ?", successor.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Tasks.SupersedeTerminal(t.Context(), successor.ID); err != nil {
		t.Fatal(err)
	}
	next, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeStorageRemoval, InputVersion: 1, Input: input, RetryOfTaskID: &successor.ID, SubjectType: model.TaskSubjectStorageRemovalRequest, SubjectKey: id, Generation: new(int64), Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.StorageRemoval.TransferRemovalTaskOwner(t.Context(), id, successor.ID, next.ID); err != nil {
		t.Fatal(err)
	}
	got, err = f.repos.StorageRemoval.GetRemovalRequest(t.Context(), id)
	if err != nil || got.TaskID == nil || *got.TaskID != next.ID || got.AttentionCode == nil || *got.AttentionCode != "future_stop_code" || got.AttentionAt == nil {
		t.Fatalf("retry cleared an unsupported stop condition: %#v, %v", got, err)
	}
}
