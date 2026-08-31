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
	"github.com/strahe/synaps3/internal/storagereplacement"
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
	if fifth.Copy.CommitReadyAt == nil {
		t.Fatal("fifth reservation has no FIFO timestamp")
	}
	readyAt := *fifth.Copy.CommitReadyAt
	updatedAt := fifth.Copy.UpdatedAt
	fifth, err = repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: commitCopyIdentity(copies[4]), AttemptID: "attempt-4", Now: base.Add(time.Hour),
	})
	if err != nil || fifth.State != storagecommit.ReservationWaiting || fifth.Copy.CommitAttemptID != nil ||
		fifth.Copy.CommitReadyAt == nil ||
		!fifth.Copy.CommitReadyAt.Equal(readyAt) || !fifth.Copy.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("repeated fifth reservation = %#v err=%v, want unchanged FIFO row", fifth, err)
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

func TestStorageCommitReservationReleaseRejectsCommittedCopy(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-release-committed-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copyRow := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 1)[0]
	pieceID := onChainID(t, "2001")
	if err := repos.Uploads.MarkUploadCopyCommitted(t.Context(), repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copyRow.ID,
		UploadID:            copyRow.UploadID,
		CopyIndex:           copyRow.CopyIndex,
		PieceCID:            "bafkqaaa",
		PieceID:             &pieceID,
		RetrievalURL:        "https://provider.example/piece",
		CommitExtraDataHex:  "abcd",
		CommitTransactionID: "0xcommitted",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}

	err := repos.Uploads.ReleaseCommitReservation(t.Context(), storagecommit.ReservationReleaseInput{
		Copy: commitCopyIdentity(copyRow), ClearReadyAt: true, ClearExtraData: true,
	})
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("release committed reservation error = %v, want conflict", err)
	}
	persisted, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil || persisted == nil || persisted.Status != model.StorageUploadCopyStatusCommitted ||
		persisted.CommitExtraDataHex == nil || *persisted.CommitExtraDataHex != "abcd" ||
		persisted.CommitTransactionID == nil || *persisted.CommitTransactionID != "0xcommitted" {
		t.Fatalf("committed evidence after reservation release = %#v err=%v", persisted, err)
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
	readyEvidence := repository.MarkUploadCopyPieceReadyInput{
		StorageUploadCopyID: copyRow.ID,
		UploadID:            copyRow.UploadID,
		CopyIndex:           copyRow.CopyIndex,
		PieceCID:            "bafkqaaa",
		RetrievalURL:        "https://provider.example/piece",
		CommitExtraDataHex:  "abcd",
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(t.Context(), readyEvidence); err != nil {
		t.Fatalf("seed piece evidence: %v", err)
	}

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
	if err := repos.Uploads.MarkUploadCopyPieceReady(t.Context(), readyEvidence); err != nil {
		t.Fatalf("idempotent late MarkUploadCopyPieceReady: %v", err)
	}
	conflictingEvidence := readyEvidence
	conflictingEvidence.RetrievalURL = "https://provider.example/different"
	if err := repos.Uploads.MarkUploadCopyPieceReady(t.Context(), conflictingEvidence); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("conflicting late MarkUploadCopyPieceReady error = %v, want conflict", err)
	}
	persisted, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil || persisted.Status != model.StorageUploadCopyStatusCommitting ||
		persisted.CommitAttemptID == nil || *persisted.CommitAttemptID != "attempt-fenced" ||
		persisted.CommitAttemptedAt == nil {
		t.Fatalf("copy after late piece-ready = %#v err=%v, want unchanged attempted fence", persisted, err)
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
	if err := repos.Uploads.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
		Copy: identity, AttemptID: "attention-attempt", Code: storagecommit.AttentionDataSetUnavailable,
	}); err != nil {
		t.Fatalf("repeat attention: %v", err)
	}
	stage := "peer_commit"
	waitReason := model.TaskWaitReasonExternalConfirmation
	legacyTask := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: 1,
		RefVersionID: "legacy-confirmation", IdempotencyKey: "legacy-confirmation-task",
		Payload: map[string]any{
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
	if _, err := db.NewUpdate().Model((*model.StorageUploadCopy)(nil)).
		Set("commit_attention_code = ?", "future_attention_code").
		Where("id = ?", copyRow.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("set future attention code: %v", err)
	}
	records, err = repos.Uploads.ListCommitAttention(t.Context(), 10)
	if err != nil || len(records) != 1 || records[0].Code != storagecommit.AttentionCode("future_attention_code") {
		t.Fatalf("future attention records = %#v err=%v", records, err)
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

func TestStorageCommitAttentionReleaseSucceedsWithoutRecoverableWork(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-attention-orphan-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copyRow := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 1)[0]
	identity := commitCopyIdentity(copyRow)
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "orphan-attention",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "orphan-attention", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	if err := repos.Uploads.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
		Copy: identity, AttemptID: "orphan-attention", Code: storagecommit.AttentionAttemptOnlyAmbiguous,
	}); err != nil {
		t.Fatalf("mark attention: %v", err)
	}

	if err := repos.Uploads.ReleaseCommitAttention(t.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyRow.ID, ExpectedAttemptID: "orphan-attention", AcknowledgePossibleDuplicate: true,
	}); err != nil {
		t.Fatalf("orphan release: %v", err)
	}
	persisted, loadErr := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
	if loadErr != nil || persisted.CommitAttemptID != nil || persisted.CommitAttentionAt != nil ||
		persisted.Status != model.StorageUploadCopyStatusPieceReady {
		t.Fatalf("orphan release did not clear the fence: copy=%#v err=%v", persisted, loadErr)
	}
	if err := repos.Uploads.ReleaseCommitAttention(t.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyRow.ID, ExpectedAttemptID: "orphan-attention", AcknowledgePossibleDuplicate: true,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("replayed orphan release error = %v, want conflict", err)
	}
}

func TestStorageCommitAttentionReleaseWaitsForReplacementClaim(t *testing.T) {
	for _, tc := range []struct {
		name       string
		owner      storagereplacement.Status
		wantStatus storagereplacement.ItemStatus
	}{
		{name: "failed", owner: storagereplacement.StatusFailed, wantStatus: storagereplacement.ItemStatusFailed},
		{name: "superseded", owner: storagereplacement.StatusSuperseded, wantStatus: storagereplacement.ItemStatusCancelled},
		{name: "active", owner: storagereplacement.StatusMigrating, wantStatus: storagereplacement.ItemStatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			fixture := seedCommitAttentionReplacement(t, db, "commit-release-claimed-"+tc.name, tc.owner, true)
			input := storagecommit.ManualReleaseInput{
				CopyID: fixture.copyRow.ID, ExpectedAttemptID: fixture.attemptID,
				AcknowledgePossibleDuplicate: true,
			}

			if err := fixture.repos.Uploads.ReleaseCommitAttention(t.Context(), input); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("claimed release error = %v, want conflict", err)
			}
			persistedCopy, err := fixture.repos.Uploads.GetUploadCopyByID(t.Context(), fixture.copyRow.ID)
			if err != nil || persistedCopy.CommitAttemptID == nil || *persistedCopy.CommitAttemptID != fixture.attemptID ||
				persistedCopy.CommitAttemptedAt == nil || persistedCopy.CommitAttentionAt == nil {
				t.Fatalf("copy after claimed release = %#v err=%v, want intact attention fence", persistedCopy, err)
			}
			persistedItem := new(storagereplacement.Item)
			if err := db.NewSelect().Model(persistedItem).Where("id = ?", fixture.item.ID).Scan(t.Context()); err != nil {
				t.Fatalf("load claimed item: %v", err)
			}
			if persistedItem.Status != storagereplacement.ItemStatusRunning || persistedItem.ClaimedAt == nil ||
				!persistedItem.ClaimedAt.Equal(fixture.token.ClaimedAt) || persistedItem.LeaseUntil == nil {
				t.Fatalf("item after claimed release = %#v, want unchanged claim", persistedItem)
			}

			if err := fixture.repos.Replacements.ReleaseReplacementItemClaim(t.Context(), fixture.token); err != nil {
				t.Fatalf("release replacement claim: %v", err)
			}
			if err := fixture.repos.Uploads.ReleaseCommitAttention(t.Context(), input); err != nil {
				t.Fatalf("release after claim yielded: %v", err)
			}
			persistedItem = new(storagereplacement.Item)
			if err := db.NewSelect().Model(persistedItem).Where("id = ?", fixture.item.ID).Scan(t.Context()); err != nil {
				t.Fatalf("load settled item: %v", err)
			}
			if persistedItem.Status != tc.wantStatus || persistedItem.ClaimedAt != nil || persistedItem.LeaseUntil != nil {
				t.Fatalf("settled item = %#v, want status %s without claim", persistedItem, tc.wantStatus)
			}
		})
	}
}

func TestStorageCommitSettlementUsesConcreteDrainingGenerationOrigin(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-concrete-generation-bucket")
	oldDataSet := seedCommitDataSet(t, db, bucket.ID)
	copyRow := seedCommitCopies(t, db, bucket.ID, oldDataSet.ID, 1)[0]
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("is_current = ?", false).
		Set("status = ?", model.StorageDataSetStatusDraining).
		Set("created_by_upload_id = ?", copyRow.UploadID).
		Where("id = ?", oldDataSet.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("drain old data set: %v", err)
	}
	newDataSetID := onChainID(t, "1002")
	newDataSet := &model.StorageDataSet{
		BucketID: bucket.ID, ProviderID: oldDataSet.ProviderID, CopyIndex: oldDataSet.CopyIndex,
		Generation: 2, IsCurrent: true, DataSetID: &newDataSetID, Status: model.StorageDataSetStatusReady,
	}
	if _, err := db.NewInsert().Model(newDataSet).Exec(t.Context()); err != nil {
		t.Fatalf("insert current generation: %v", err)
	}
	identity := commitCopyIdentity(copyRow)
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "draining-settlement",
	}); err != nil {
		t.Fatalf("reserve draining copy: %v", err)
	}
	if _, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "draining-settlement", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark draining copy attempted: %v", err)
	}
	pieceID := onChainID(t, "5001")
	if err := repos.Uploads.MarkUploadCopyCommitted(t.Context(), repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
		PieceCID: "bafkqaaa", PieceID: &pieceID, RetrievalURL: "https://old.example/piece",
		CommitAttemptID: "draining-settlement",
	}); err != nil {
		t.Fatalf("settle draining copy: %v", err)
	}
	persisted, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil || !persisted.IsNewDataSet {
		t.Fatalf("draining copy origin = %#v err=%v, want new-data-set provenance from concrete generation", persisted, err)
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

type commitAttentionReplacementFixture struct {
	repos     *repository.Repositories
	copyRow   model.StorageUploadCopy
	item      storagereplacement.Item
	token     storagereplacement.ClaimToken
	attemptID string
}

func seedCommitAttentionReplacement(
	t *testing.T,
	db *bun.DB,
	name string,
	ownerStatus storagereplacement.Status,
	claimed bool,
) commitAttentionReplacementFixture {
	t.Helper()
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, name)
	source := seedCommitDataSet(t, db, bucket.ID)
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("is_current = ?", false).
		Set("status = ?", model.StorageDataSetStatusDraining).
		Where("id = ?", source.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("drain source data set: %v", err)
	}
	targetProviderID := onChainID(t, "202")
	targetDataSetID := onChainID(t, "2002")
	target := &model.StorageDataSet{
		BucketID: bucket.ID, ProviderID: targetProviderID, CopyIndex: source.CopyIndex,
		Generation: 2, IsCurrent: true, DataSetID: &targetDataSetID, Status: model.StorageDataSetStatusReady,
	}
	if _, err := db.NewInsert().Model(target).Exec(t.Context()); err != nil {
		t.Fatalf("insert target data set: %v", err)
	}
	copyRow := seedCommitCopies(t, db, bucket.ID, target.ID, 1)[0]
	if _, err := db.NewUpdate().Model((*model.StorageUploadCopy)(nil)).
		Set("provider_id = ?", targetProviderID).
		Where("id = ?", copyRow.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("align target copy provider: %v", err)
	}
	copyRow.ProviderID = &targetProviderID
	now := time.Now().Add(-time.Minute)
	replacement := &storagereplacement.Replacement{
		BucketID: bucket.ID, CopyIndex: source.CopyIndex,
		SourceDataSetID: source.ID, TargetDataSetID: target.ID,
		SelectionMode:   storagereplacement.SelectionModeManual,
		ClientRequestID: name, Status: ownerStatus,
		ItemsTotal: 1, ConfirmedAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := db.NewInsert().Model(replacement).Exec(t.Context()); err != nil {
		t.Fatalf("insert replacement: %v", err)
	}
	maxRetries := 5
	item := storagereplacement.Item{
		ReplacementID: replacement.ID, UploadID: copyRow.UploadID, TargetCopyID: &copyRow.ID,
		Status: storagereplacement.ItemStatusPending, ScheduledAt: now,
		MaxRetries: &maxRetries, CreatedAt: now, UpdatedAt: now,
	}
	if claimed {
		claimedAt := time.Now().Add(-time.Second)
		leaseUntil := claimedAt.Add(time.Hour)
		item.Status = storagereplacement.ItemStatusRunning
		item.ClaimedAt = &claimedAt
		item.LeaseUntil = &leaseUntil
	}
	if _, err := db.NewInsert().Model(&item).Exec(t.Context()); err != nil {
		t.Fatalf("insert replacement item: %v", err)
	}
	// Claim tokens must carry the persisted timestamp. Timestamps round trip with
	// microsecond precision, so an in-memory time.Now() never matches the stored
	// value on a platform whose wall clock exposes nanoseconds.
	token := storagereplacement.ClaimToken{}
	if claimed {
		if err := db.NewSelect().Model(&item).Where("id = ?", item.ID).Scan(t.Context()); err != nil {
			t.Fatalf("reload claimed replacement item: %v", err)
		}
		token = storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	}
	attemptID := name + "-attempt"
	seedRepositoryCommitAttempt(t, repos, copyRow, attemptID, "abcd", "0x"+name)
	if err := repos.Uploads.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
		Copy: commitCopyIdentity(copyRow), AttemptID: attemptID,
		Code: storagecommit.AttentionAttemptOnlyAmbiguous,
	}); err != nil {
		t.Fatalf("mark commit attention: %v", err)
	}
	return commitAttentionReplacementFixture{
		repos: repos, copyRow: copyRow, item: item, token: token, attemptID: attemptID,
	}
}

func TestStorageCommitCapacityReportsSlotsHeldForAttention(t *testing.T) {
	cases := []struct {
		name              string
		attention         storagecommit.AttentionCode
		wantAttentionHeld int
	}{
		{
			name:              "attention only an operator can clear holds the slot",
			attention:         storagecommit.AttentionAttemptOnlyAmbiguous,
			wantAttentionHeld: 4,
		},
		{
			// data_set_unavailable is raised both as a terminal hold and as one
			// the advancer keeps observing. Classifying by code drops the terminal
			// ones, which is a silent under-count of the very case that strands a
			// data set, so every flagged attempt counts.
			name:              "data set unavailable attention holds the slot",
			attention:         storagecommit.AttentionDataSetUnavailable,
			wantAttentionHeld: 4,
		},
		{
			name:              "confirmation timeout attention holds the slot",
			attention:         storagecommit.AttentionConfirmationTimeout,
			wantAttentionHeld: 4,
		},
		{
			name:              "plain in-flight attempts are not flagged",
			wantAttentionHeld: 0,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			bucket := seedBucket(t, db, "commit-review-capacity-bucket")
			dataSet := seedCommitDataSet(t, db, bucket.ID)
			copies := seedCommitCopies(t, db, bucket.ID, dataSet.ID, storagecommit.MaxActiveAttemptsPerDataSet+1)

			for i := range storagecommit.MaxActiveAttemptsPerDataSet {
				attemptID := fmt.Sprintf("review-attempt-%d", i)
				seedRepositoryCommitAttempt(t, repos, copies[i], attemptID, "abcd", "")
				if testCase.attention == "" {
					continue
				}
				if err := repos.Uploads.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
					Copy: commitCopyIdentity(copies[i]), AttemptID: attemptID, Code: testCase.attention,
				}); err != nil {
					t.Fatalf("MarkCommitAttention(%d): %v", i, err)
				}
			}

			// Attention never frees the slot: those attempts may already have been
			// accepted by the provider.
			active, err := repos.Uploads.CountActiveCommitAttemptsForDataSet(t.Context(), dataSet.ID)
			if err != nil || active != storagecommit.MaxActiveAttemptsPerDataSet {
				t.Fatalf("active attempts = %d err=%v, want %d", active, err, storagecommit.MaxActiveAttemptsPerDataSet)
			}

			blocked, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
				Copy:      commitCopyIdentity(copies[storagecommit.MaxActiveAttemptsPerDataSet]),
				AttemptID: "review-blocked-attempt",
			})
			if err != nil || blocked.State != storagecommit.ReservationWaiting {
				t.Fatalf("blocked reservation = %#v err=%v, want waiting", blocked, err)
			}
			if blocked.AttentionHeld != testCase.wantAttentionHeld {
				t.Fatalf("blocked reservation attention held = %d, want %d", blocked.AttentionHeld, testCase.wantAttentionHeld)
			}
		})
	}
}

func TestStorageCommitQueuedReservationReportsNoAttentionHold(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "commit-review-queued-bucket")
	dataSet := seedCommitDataSet(t, db, bucket.ID)
	copies := seedCommitCopies(t, db, bucket.ID, dataSet.ID, 2)
	base := time.Now().Add(-time.Minute)

	// Give the head an earlier FIFO position without reserving it, so the second
	// copy waits on order rather than on capacity.
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: commitCopyIdentity(copies[0]), AttemptID: "queued-head", Now: base,
	}); err != nil {
		t.Fatalf("reserve head: %v", err)
	}
	if err := repos.Uploads.ReleaseCommitAttempt(t.Context(), storagecommit.ReleaseInput{
		Copy: commitCopyIdentity(copies[0]), AttemptID: "queued-head",
	}); err != nil {
		t.Fatalf("release head reservation: %v", err)
	}

	queued, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: commitCopyIdentity(copies[1]), AttemptID: "queued-follower", Now: base.Add(time.Second),
	})
	if err != nil || queued.State != storagecommit.ReservationWaiting || queued.AttentionHeld != 0 {
		t.Fatalf("queued reservation = %#v err=%v, want waiting behind the FIFO head with no attention hold", queued, err)
	}
}
