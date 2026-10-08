package repository_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepull"
)

func TestPullTaskRecoveryProtection(t *testing.T) {
	testPullTaskRecoveryProtection(t, newCommitFixture(t, testDB(t)))
}

func testPullTaskRecoveryProtection(t *testing.T, f commitFixture) {
	ctx := t.Context()
	copyRow := f.transferredCopy(t, "protected-pull")
	if _, err := f.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("status = ?", model.StorageCopyStatusPending).
		Set("transfer_method = ?", model.StorageCopyTransferMethodPeerPull).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	generation, err := f.repos.Contents.NextCopyWorkGeneration(ctx, copyRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	row := commitTask("protected-pull")
	row.Type = model.TaskTypeStoragePull
	row, _, err = f.repos.Tasks.Enqueue(ctx, row)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Contents.BindCopyTask(ctx, copyRow.ID, generation, row.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.repos.Tasks.ClaimNext(ctx, time.Minute)
	if err != nil || claimed == nil || claimed.ID != row.ID {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	sourceProvider, sourceSet, sourcePiece := onChainID(t, "901"), onChainID(t, "902"), onChainID(t, "0")
	checkpoint := []byte(`{"attempt_id":"protected-pull"}`)
	if err := f.repos.WithTx(ctx, func(tx *repository.Repositories) error {
		if err := tx.Contents.ReservePullRequest(ctx, repository.ReservePullRequestInput{
			CopyID: copyRow.ID, Generation: generation, TaskID: row.ID, AttemptID: "protected-pull",
			SourceProviderID: &sourceProvider, SourceDataSetID: &sourceSet, SourcePieceID: &sourcePiece,
			SourcePieceCID: "piece-protected-pull", SourceRetrievalURL: "https://source.example/piece", ExtraDataHex: "abcd",
		}); err != nil {
			return err
		}
		return tx.Tasks.WriteCheckpoint(ctx, row.ID, claimed.ClaimGeneration, checkpoint)
	}); err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(2 * time.Minute).UTC().Truncate(time.Microsecond)
	if err := f.repos.Tasks.Settle(ctx, row.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: due, WaitReason: new(storagepull.WaitQueueFull),
	}); err != nil {
		t.Fatal(err)
	}
	if woken, err := f.repos.Tasks.WakePendingOfTypes(ctx, []int64{row.ID}, []model.TaskType{model.TaskTypeStoragePull}, []string{storagepull.WaitQueueFull}); err != nil || woken != 0 {
		t.Fatalf("early wake = %d, %v", woken, err)
	}
	waiting, err := f.repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || !waiting.AvailableAt.Equal(due) {
		t.Fatalf("queue-full deadline changed: %#v, %v", waiting, err)
	}
	if err := f.repos.Tasks.RequestCancellation(ctx, row.ID, "cancel pull"); err != nil {
		t.Fatal(err)
	}
	claimed, err = f.repos.Tasks.ClaimNext(ctx, time.Minute)
	if err != nil || claimed == nil || claimed.ID != row.ID || !claimed.CancellationRequested() {
		t.Fatalf("cancel did not wake recovery: %#v, %v", claimed, err)
	}
	if err := f.repos.Tasks.Settle(ctx, row.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: new(storagepull.FailureRecoveryBlocked),
	}); err != nil {
		t.Fatal(err)
	}
	version := new(model.ObjectVersion)
	if err := f.db.NewSelect().Model(version).Where("content_id = ?", copyRow.ContentID).Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: f.bucket.ID, Key: version.Key, VersionID: version.VersionID,
	}); !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
		t.Fatalf("delete unresolved pull's last reference = %v", err)
	}
	row = repositorySuccessor(t, f.repos, row, true)
	retried, err := f.repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || !retried.CancellationRequested() || retried.CancellationReason == nil || *retried.CancellationReason != "cancel pull" ||
		retried.ResumeMode != model.TaskResumeModeRecover || !bytes.Equal(retried.Checkpoint, claimed.Checkpoint) {
		t.Fatalf("dependency retry lost cancellation or evidence: %#v, %v", retried, err)
	}
	claimed, err = f.repos.Tasks.ClaimNext(ctx, time.Minute)
	if err != nil || claimed == nil || claimed.ID != row.ID || !claimed.CancellationRequested() {
		t.Fatalf("retry did not resume cancellation recovery: %#v, %v", claimed, err)
	}
	if err := f.repos.Tasks.Settle(ctx, row.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: new(storagepull.FailureCancelOutcomeUnknown),
	}); err != nil {
		t.Fatal(err)
	}
	row = repositorySuccessor(t, f.repos, row, false)
	retried, err = f.repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || retried.Status != model.TaskStatusPending || retried.ResumeMode != model.TaskResumeModeRecover ||
		retried.CancellationRequestedAt != nil || retried.CancellationReason != nil || !bytes.Equal(retried.Checkpoint, claimed.Checkpoint) {
		t.Fatalf("manual retry lost evidence or cancellation persists: %#v, %v", retried, err)
	}
	if _, err := f.repos.Contents.GetUnresolvedPullAttempt(ctx, copyRow.ContentID, copyRow.StorageDataSetID); err != nil {
		t.Fatalf("protected retry lost its attempt: %v", err)
	}
	if _, err := f.repos.Contents.NextCopyWorkGeneration(ctx, copyRow.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("new generation replaced protected owner: %v", err)
	}
	normal := commitTask("ordinary-wake")
	normal.AvailableAt = due
	normal, _, err = f.repos.Tasks.Enqueue(ctx, normal)
	if err != nil {
		t.Fatal(err)
	}
	if woken, err := f.repos.Tasks.WakePending(ctx, []int64{normal.ID}); err != nil || woken != 1 {
		t.Fatalf("ordinary wake = %d, %v", woken, err)
	}
}

func TestPullAuthorizationLedger(t *testing.T) {
	testPullAuthorizationLedger(t, newCommitFixture(t, testDB(t)))
}

func testPullAuthorizationLedger(t *testing.T, f commitFixture) {
	ctx := t.Context()
	copyRow := f.transferredCopy(t, "pull-ledger")
	if _, err := f.db.NewUpdate().Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusPending).
		Set("transfer_method = ?", model.StorageCopyTransferMethodPeerPull).
		Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	generation, err := f.repos.Contents.NextCopyWorkGeneration(ctx, copyRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	row := commitTask("pull-ledger")
	row.Type = model.TaskTypeStoragePull
	row, _, err = f.repos.Tasks.Enqueue(ctx, row)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Contents.BindCopyTask(ctx, copyRow.ID, generation, row.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.repos.Tasks.ClaimNext(ctx, time.Minute)
	if err != nil || claimed == nil || claimed.ID != row.ID {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	sourceProvider, sourceSet, sourcePiece := onChainID(t, "901"), onChainID(t, "902"), onChainID(t, "0")
	input := repository.ReservePullRequestInput{
		CopyID: copyRow.ID, Generation: generation, TaskID: row.ID, AttemptID: "pull-attempt",
		SourceProviderID: &sourceProvider, SourceDataSetID: &sourceSet, SourcePieceID: &sourcePiece,
		SourcePieceCID: "piece-pull-ledger", SourceRetrievalURL: "https://source.example/piece", ExtraDataHex: "abcd",
	}
	write := func(tx *repository.Repositories) error {
		if err := tx.Contents.ReservePullRequest(ctx, input); err != nil {
			return err
		}
		return tx.Tasks.WriteCheckpoint(ctx, row.ID, claimed.ClaimGeneration, []byte(`{"attempt_id":"pull-attempt"}`))
	}
	rollback := errors.New("rollback")
	if err := f.repos.WithTx(ctx, func(tx *repository.Repositories) error {
		if err := write(tx); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("rolled back reservation = %v", err)
	}
	if _, err := f.repos.Contents.GetUnresolvedPullAttempt(ctx, copyRow.ContentID, copyRow.StorageDataSetID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("attempt after rollback = %v", err)
	}
	if task, err := f.repos.Tasks.GetByID(ctx, row.ID); err != nil || len(task.Checkpoint) != 0 || task.ResumeMode != model.TaskResumeModeExecute {
		t.Fatalf("checkpoint after rollback = %#v, %v", task, err)
	}
	if err := f.repos.WithTx(ctx, func(tx *repository.Repositories) error { return write(tx) }); err != nil {
		t.Fatal(err)
	}
	original, err := f.repos.Contents.GetPullAttempt(ctx, input.AttemptID, copyRow.ContentID, copyRow.StorageDataSetID)
	if err != nil || original.ExtraDataHex != input.ExtraDataHex {
		t.Fatalf("reserved attempt = %#v, %v", original, err)
	}
	if err := f.repos.Contents.ReservePullRequest(ctx, input); err != nil {
		t.Fatalf("identical reservation: %v", err)
	}
	otherID := onChainID(t, "999")
	for _, change := range []struct {
		name  string
		apply func(*repository.ReservePullRequestInput)
	}{
		{"attempt", func(i *repository.ReservePullRequestInput) { i.AttemptID = "another-attempt" }},
		{"provider", func(i *repository.ReservePullRequestInput) { i.SourceProviderID = &otherID }},
		{"data set", func(i *repository.ReservePullRequestInput) { i.SourceDataSetID = &otherID }},
		{"piece id", func(i *repository.ReservePullRequestInput) { i.SourcePieceID = &otherID }},
		{"CID", func(i *repository.ReservePullRequestInput) { i.SourcePieceCID = "other-piece" }},
		{"URL", func(i *repository.ReservePullRequestInput) { i.SourceRetrievalURL += "/changed" }},
		{"authorization", func(i *repository.ReservePullRequestInput) { i.ExtraDataHex = "ef" }},
		{"generation", func(i *repository.ReservePullRequestInput) { i.Generation++ }},
		{"task", func(i *repository.ReservePullRequestInput) { i.TaskID++ }},
	} {
		t.Run(change.name, func(t *testing.T) {
			changed := input
			change.apply(&changed)
			if err := f.repos.Contents.ReservePullRequest(ctx, changed); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("conflicting reservation = %v", err)
			}
		})
	}
	if _, err := f.repos.Contents.GetPullAttempt(ctx, input.AttemptID, copyRow.ContentID+1, copyRow.StorageDataSetID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("foreign content lookup = %v", err)
	}
	if _, err := f.repos.Contents.GetPullAttempt(ctx, input.AttemptID, copyRow.ContentID, copyRow.StorageDataSetID+1); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("foreign data set lookup = %v", err)
	}
	if err := f.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex,
		PieceCID: input.SourcePieceCID, PullAttemptID: input.AttemptID, RequireEligibleCopy: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Contents.ReservePullRequest(ctx, input); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("resolved reservation = %v", err)
	}
	if err := f.repos.Contents.CompleteCopyTask(ctx, copyRow.ID, generation, row.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Tasks.Settle(ctx, row.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover,
	}); err != nil {
		t.Fatal(err)
	}
	if stored, err := f.repos.Tasks.GetByID(ctx, row.ID); err != nil || stored == nil {
		t.Fatalf("completed task history missing: %#v, %v", stored, err)
	}
	history, err := f.repos.Contents.GetPullAttempt(ctx, input.AttemptID, copyRow.ContentID, copyRow.StorageDataSetID)
	if err != nil || history.Status != storagepull.AttemptStatusAttempted || history.ResolvedAt == nil ||
		history.ExtraDataHex != original.ExtraDataHex || !history.AttemptedAt.Equal(original.AttemptedAt) {
		t.Fatalf("authorization history after task completion = %#v, %v", history, err)
	}
}
