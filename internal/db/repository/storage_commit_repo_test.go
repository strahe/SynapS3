package repository_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/uptrace/bun"
)

func TestStorageCommitReservationsEnforceCapacityAndFIFO(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-capacity-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copies := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 5)
	base := time.Now().Add(-time.Minute)

	for i := range 4 {
		result, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
			Copy: commitCopyIdentity(copies[i]), AttemptID: fmt.Sprintf("attempt-%d", i),
			Now: base.Add(time.Duration(i) * time.Second),
		})
		if err != nil || result.State != storagecommit.ReservationAcquired {
			t.Fatalf("reserve copy %d = %#v err=%v, want acquired", i, result, err)
		}
	}
	active, err := repos.Uploads.CountActiveCommitAttemptsForDataSet(t.Context(), dataSet.ID)
	if err != nil || active != storagecommit.MaxActiveAttemptsPerDataSet {
		t.Fatalf("active reservations = %d err=%v, want %d", active, err, storagecommit.MaxActiveAttemptsPerDataSet)
	}
	fifth, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: commitCopyIdentity(copies[4]), AttemptID: "attempt-4", Now: base.Add(4 * time.Second),
	})
	if err != nil || fifth.State != storagecommit.ReservationWaiting || fifth.Copy.CommitAttemptID != nil {
		t.Fatalf("fifth reservation = %#v err=%v, want waiting without attempt", fifth, err)
	}

	if err := repos.Uploads.ReleaseCommitAttempt(t.Context(), storagecommit.ReleaseInput{
		Copy: commitCopyIdentity(copies[0]), AttemptID: "attempt-0", ClearReadyAt: true,
	}); err != nil {
		t.Fatalf("release first reservation: %v", err)
	}
	fifth, err = repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: commitCopyIdentity(copies[4]), AttemptID: "attempt-4", Now: base.Add(10 * time.Second),
	})
	if err != nil || fifth.State != storagecommit.ReservationAcquired {
		t.Fatalf("fifth reservation after release = %#v err=%v, want acquired", fifth, err)
	}
}

func TestStorageCommitReservationAllowsBoundDrainingGeneration(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-draining-generation-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copyRow := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 1)[0]
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("is_current = ?", false).
		Set("status = ?", model.StorageDataSetStatusDraining).
		Where("id = ?", dataSet.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("drain data set: %v", err)
	}

	result, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: commitCopyIdentity(copyRow), AttemptID: "draining-attempt",
	})
	if err != nil || result.State != storagecommit.ReservationAcquired {
		t.Fatalf("draining reservation = %#v err=%v, want acquired", result, err)
	}
}

func TestSQLiteConcurrentStorageCommitReservationsRespectCapacity(t *testing.T) {
	assertConcurrentStorageCommitReservationsRespectCapacity(t, concurrentTestDB(t))
}

func assertConcurrentStorageCommitReservationsRespectCapacity(t *testing.T, db *bun.DB) {
	t.Helper()
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "concurrent-commit-capacity-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copies := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 8)

	start := make(chan struct{})
	results := make(chan storagecommit.ReserveResult, len(copies))
	errorsOut := make(chan error, len(copies))
	var workers sync.WaitGroup
	for i := range copies {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			result, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
				Copy: commitCopyIdentity(copies[index]), AttemptID: fmt.Sprintf("concurrent-attempt-%d", index),
			})
			if err != nil {
				errorsOut <- err
				return
			}
			results <- result
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsOut)
	for err := range errorsOut {
		t.Errorf("concurrent reservation: %v", err)
	}
	if t.Failed() {
		return
	}
	acquired := 0
	waiting := 0
	for result := range results {
		switch result.State {
		case storagecommit.ReservationAcquired:
			acquired++
		case storagecommit.ReservationWaiting:
			waiting++
		default:
			t.Fatalf("unexpected concurrent reservation state %q", result.State)
		}
	}
	if acquired != storagecommit.MaxActiveAttemptsPerDataSet || waiting != len(copies)-storagecommit.MaxActiveAttemptsPerDataSet {
		t.Fatalf("concurrent reservations acquired=%d waiting=%d, want %d/%d",
			acquired, waiting, storagecommit.MaxActiveAttemptsPerDataSet, len(copies)-storagecommit.MaxActiveAttemptsPerDataSet)
	}
	count, err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Where("storage_data_set_id = ?", dataSet.ID).
		Where("commit_attempt_id IS NOT NULL").
		Count(t.Context())
	if err != nil || count != storagecommit.MaxActiveAttemptsPerDataSet {
		t.Fatalf("persisted active reservations = %d err=%v, want %d", count, err, storagecommit.MaxActiveAttemptsPerDataSet)
	}
}

func TestStorageCommitAttemptFenceBlocksAutomaticFailure(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-fence-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copyRow := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 1)[0]
	identity := commitCopyIdentity(copyRow)

	reservation, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "attempt-fenced",
	})
	if err != nil || reservation.State != storagecommit.ReservationAcquired {
		t.Fatalf("reserve: %#v err=%v", reservation, err)
	}
	attempt, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "attempt-fenced", ExtraDataHex: "abcd",
	})
	if err != nil || !attempt.Entered {
		t.Fatalf("mark attempted: %#v err=%v", attempt, err)
	}
	err = repos.Uploads.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
		StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex, LastError: "injected",
	})
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkUploadCopyFailed error = %v, want conflict", err)
	}
	if err := repos.Uploads.ReleaseCommitAttempt(t.Context(), storagecommit.ReleaseInput{
		Copy: identity, AttemptID: "attempt-fenced",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("automatic attempted release error = %v, want conflict", err)
	}
}

func TestStorageCommitAttentionRequiresAcknowledgedFencedRelease(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-attention-release-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copyRow := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 1)[0]
	identity := commitCopyIdentity(copyRow)

	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "attention-attempt",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "attention-attempt", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	if err := repos.Uploads.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
		Copy: identity, AttemptID: "attention-attempt", Code: storagecommit.AttentionAttemptOnlyAmbiguous,
	}); err != nil {
		t.Fatalf("mark attention: %v", err)
	}
	stage := "peer_commit"
	waitReason := model.TaskWaitReasonExternalConfirmation
	legacyTask := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: 1,
		RefVersionID: "legacy-confirmation", IdempotencyKey: "legacy-confirmation-task",
		Payload: map[string]interface{}{
			"upload_id": copyRow.UploadID, "copy_index": copyRow.CopyIndex,
		},
		Status: model.TaskStatusWaiting, WaitReason: &waitReason,
		ScheduledAt: time.Now().Add(24 * time.Hour),
	}
	if _, err := db.NewInsert().Model(legacyTask).Exec(t.Context()); err != nil {
		t.Fatalf("insert legacy confirmation task: %v", err)
	}
	records, err := repos.Uploads.ListCommitAttention(t.Context(), 10)
	if err != nil || len(records) != 1 || records[0].CopyID != copyRow.ID ||
		records[0].Code != storagecommit.AttentionAttemptOnlyAmbiguous {
		t.Fatalf("attention records = %#v err=%v", records, err)
	}
	if err := repos.Uploads.ReleaseCommitAttention(t.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyRow.ID, ExpectedAttemptID: "attention-attempt",
	}); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("unacknowledged release error = %v, want invalid input", err)
	}
	if err := repos.Uploads.ReleaseCommitAttention(t.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyRow.ID, ExpectedAttemptID: "stale-attempt", AcknowledgePossibleDuplicate: true,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale attempt release error = %v, want conflict", err)
	}
	if err := repos.Uploads.ReleaseCommitAttention(t.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyRow.ID, ExpectedAttemptID: "attention-attempt", AcknowledgePossibleDuplicate: true,
	}); err != nil {
		t.Fatalf("acknowledged release: %v", err)
	}
	persisted, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil {
		t.Fatalf("GetUploadCopyByID: %v", err)
	}
	if persisted.Status != model.StorageUploadCopyStatusPieceReady || persisted.CommitReadyAt != nil ||
		persisted.CommitAttemptID != nil || persisted.CommitAttemptedAt != nil ||
		persisted.CommitSubmissionJSON != nil || persisted.CommitAttentionAt != nil ||
		persisted.CommitAttentionCode != nil {
		t.Fatalf("released copy = %#v", persisted)
	}
	persistedTask := new(model.Task)
	if err := db.NewSelect().Model(persistedTask).Where("id = ?", legacyTask.ID).Scan(t.Context()); err != nil {
		t.Fatalf("reload legacy confirmation task: %v", err)
	}
	if persistedTask.Status != model.TaskStatusScheduled || persistedTask.WaitReason != nil || persistedTask.ScheduledAt.After(time.Now().Add(time.Minute)) {
		t.Fatalf("legacy confirmation task was not woken: %#v", persistedTask)
	}
}

func seedCommitDataSet(t *testing.T, db *bun.DB, bucketID int64) *model.StorageDataSet {
	t.Helper()
	providerID := onChainID(t, "101")
	dataSetID := onChainID(t, "1001")
	row := &model.StorageDataSet{
		BucketID: bucketID, ProviderID: providerID, CopyIndex: 0, Generation: 1,
		IsCurrent: true, DataSetID: &dataSetID, Status: model.StorageDataSetStatusReady,
	}
	if _, err := db.NewInsert().Model(row).Exec(t.Context()); err != nil {
		t.Fatalf("insert data set: %v", err)
	}
	return row
}

func seedCommitCopies(t *testing.T, db *bun.DB, bucketID, dataSetID int64, count int) []model.StorageUploadCopy {
	t.Helper()
	providerID := onChainID(t, "101")
	copies := make([]model.StorageUploadCopy, 0, count)
	for i := range count {
		upload := &model.StorageUpload{
			BucketID: bucketID, ContentSize: 1, Checksum: fmt.Sprintf("checksum-%d", i),
			Status: model.StorageUploadStatusRunning, RequestedCopies: 1,
		}
		if _, err := db.NewInsert().Model(upload).Exec(t.Context()); err != nil {
			t.Fatalf("insert upload %d: %v", i, err)
		}
		copyRow := model.StorageUploadCopy{
			UploadID: upload.ID, CopyIndex: 0, ProviderID: &providerID,
			TransferMethod: model.StorageCopyTransferMethodPeerPull,
			Status:         model.StorageUploadCopyStatusPieceReady, StorageDataSetID: &dataSetID,
		}
		if _, err := db.NewInsert().Model(&copyRow).Exec(t.Context()); err != nil {
			t.Fatalf("insert copy %d: %v", i, err)
		}
		copies = append(copies, copyRow)
	}
	return copies
}

func commitCopyIdentity(copyRow model.StorageUploadCopy) storagecommit.CopyIdentity {
	return storagecommit.CopyIdentity{
		StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
		CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
	}
}

func seedRepositoryCommitAttempt(
	t *testing.T,
	repos *repository.Repositories,
	copyRow model.StorageUploadCopy,
	attemptID string,
	extraDataHex string,
	transactionID string,
) {
	t.Helper()
	identity := commitCopyIdentity(copyRow)
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: attemptID,
	}); err != nil {
		t.Fatalf("ReserveCommitAttempt: %v", err)
	}
	if _, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: attemptID, ExtraDataHex: extraDataHex,
	}); err != nil {
		t.Fatalf("MarkCommitAttempted: %v", err)
	}
	if transactionID != "" {
		if err := repos.Uploads.RecordCommitTransaction(t.Context(), storagecommit.EvidenceInput{
			Copy: identity, AttemptID: attemptID, TransactionID: transactionID,
		}); err != nil {
			t.Fatalf("RecordCommitTransaction: %v", err)
		}
	}
}
