package repository_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestCopyRetryAdmissionAndHistory(t *testing.T) {
	copyRetryAdmissionAndHistory(t, testDB(t))
}

func TestCopyRetryFailedIngressWithSuccessor(t *testing.T) {
	copyRetryFailedIngressWithSuccessor(t, testDB(t))
}

func copyRetryFailedIngressWithSuccessor(t *testing.T, db *bun.DB) {
	t.Helper()
	for scenario, method := range []model.StorageCopyTransferMethod{model.StorageCopyTransferMethodCacheRestore, model.StorageCopyTransferMethodPeerPull} {
		t.Run(string(method), func(t *testing.T) {
			repos := repository.NewRepositories(db)
			ctx := t.Context()
			bucket := seedBucket(t, db, "ingress-successor-"+string(method))
			content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
				BucketID: bucket.ID, ContentSize: 10, Checksum: testutil.StorageChecksum(string(method)), RequestedCopies: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "retry.bin", ContentID: &content.ID, Size: 10, ETag: "retry", ContentType: "application/octet-stream"}
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
				t.Fatal(err)
			}
			if err := repos.Objects.SetVersionCachePresence(ctx, version.VersionID, true); err != nil {
				t.Fatal(err)
			}
			var bindings []repository.UploadCopyBindingInput
			for index := range 2 {
				provider := onChainID(t, fmt.Sprint(100+10*scenario+index))
				dataSet := onChainID(t, fmt.Sprint(200+10*scenario+index))
				binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: provider, CopyIndex: index})
				if err != nil {
					t.Fatal(err)
				}
				if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: dataSet}); err != nil {
					t.Fatal(err)
				}
				transfer := model.StorageCopyTransferMethodIngress
				if index == 1 {
					transfer = model.StorageCopyTransferMethodPeerPull
				}
				bindings = append(bindings, repository.UploadCopyBindingInput{StorageDataSetID: binding.ID, CopyIndex: index, ProviderID: provider, TransferMethod: transfer})
			}
			if err := repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, bindings); err != nil {
				t.Fatal(err)
			}
			copies, err := repos.Contents.ListCopies(ctx, content.ID)
			if err != nil || len(copies) != 2 {
				t.Fatalf("copies=%+v err=%v", copies, err)
			}
			failed := copies[0]
			if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
				Set("ingress_bytes_transferred = 5").Set("ingress_store_attempt = 1").Set("progress_updated_at = ?", time.Now()).
				Where("id = ?", failed.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{StorageCopyID: failed.ID, ContentID: content.ID, CopyIndex: 0, LastError: "ingress failed"}); err != nil {
				t.Fatal(err)
			}
			successor, err := repos.Contents.PromotePendingIngress(ctx, content.ID)
			if err != nil || successor == nil || successor.ID != copies[1].ID {
				t.Fatalf("successor=%+v err=%v", successor, err)
			}
			if method == model.StorageCopyTransferMethodPeerPull {
				testutil.CommitStorageCopy(t, db, repos, testutil.CommitCopyInput{StorageCopyID: successor.ID, PieceCID: "bafk2bzacecpiecerestore"})
			}
			states, err := repos.Contents.CopyRetryStates(ctx, []int64{failed.ID})
			if err != nil || !states[failed.ID].Available || states[failed.ID].NextMethod != method {
				t.Fatalf("states=%+v err=%v", states, err)
			}
			release := cacheaccess.NewGate().HoldRead(model.ContentCacheKey(content.ID))
			defer release()
			reopened, err := repos.Contents.RetryFailedCopy(ctx, failed.ID)
			if err != nil {
				t.Fatal(err)
			}
			if reopened.Status != model.StorageCopyStatusPending || reopened.TransferMethod != method || reopened.ActiveTaskID != nil || reopened.LastError != nil || reopened.IngressBytesTransferred != 0 || reopened.IngressStoreAttempt != 0 || reopened.ProgressUpdatedAt != nil {
				t.Fatalf("reopened=%+v", reopened)
			}
			ingress, err := repos.Contents.GetIngressCopy(ctx, content.ID)
			if err != nil || ingress == nil || ingress.ID != successor.ID {
				t.Fatalf("ingress=%+v err=%v", ingress, err)
			}
		})
	}
}

func copyRetryAdmissionAndHistory(t *testing.T, db *bun.DB) *model.StorageCopy {
	t.Helper()
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "copy-retry")
	content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{BucketID: bucket.ID, ContentSize: 10, Checksum: testutil.StorageChecksum("retry"), RequestedCopies: 2})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "retry.bin", ContentID: &content.ID, Size: 10, ETag: "retry", ContentType: "application/octet-stream"}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatal(err)
	}
	if err := repos.Objects.SetVersionCachePresence(ctx, version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	var bindings []repository.UploadCopyBindingInput
	for index := range 2 {
		provider := onChainID(t, fmt.Sprint(100+index))
		dataSet := onChainID(t, fmt.Sprint(200+index))
		client := onChainID(t, fmt.Sprint(300+index))
		row, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: provider, CopyIndex: index})
		if err != nil {
			t.Fatal(err)
		}
		if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: row.ID, DataSetID: dataSet, ClientDataSetID: &client}); err != nil {
			t.Fatal(err)
		}
		method := model.StorageCopyTransferMethodIngress
		if index == 1 {
			method = model.StorageCopyTransferMethodPeerPull
		}
		bindings = append(bindings, repository.UploadCopyBindingInput{StorageDataSetID: row.ID, CopyIndex: index, ProviderID: provider, TransferMethod: method})
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, bindings); err != nil {
		t.Fatal(err)
	}
	copies, err := repos.Contents.ListCopies(ctx, content.ID)
	if err != nil {
		t.Fatal(err)
	}
	copyRow := copies[1]
	if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{StorageCopyID: copyRow.ID, ContentID: content.ID, CopyIndex: 1, LastError: "failed"}); err != nil {
		t.Fatal(err)
	}
	states, err := repos.Contents.CopyRetryStates(ctx, []int64{copies[0].ID, copyRow.ID})
	if err != nil {
		t.Fatal(err)
	}
	if states[copies[0].ID].Available || !states[copyRow.ID].Available || states[copyRow.ID].NextMethod != model.StorageCopyTransferMethodCacheRestore {
		t.Fatalf("states=%+v", states)
	}
	if err := repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); err != nil {
		t.Fatal(err)
	}
	states, err = repos.Contents.CopyRetryStates(ctx, []int64{copyRow.ID})
	if err != nil || !states[copyRow.ID].Available || states[copyRow.ID].NextMethod != model.StorageCopyTransferMethodPeerPull {
		t.Fatalf("retry without a current source=%+v, error=%v", states, err)
	}
	attempt := &storagepull.Attempt{
		AttemptID: "unresolved", ContentID: content.ID, StorageDataSetID: copyRow.StorageDataSetID, Status: storagepull.AttemptStatusAttempted,
		SourceProviderID: onChainID(t, "100"), SourceDataSetID: onChainID(t, "200"), SourcePieceID: onChainID(t, "0"), SourcePieceCID: "piece", SourceRetrievalURL: "https://source.example", ExtraDataHex: "abcd", AttemptedAt: time.Now(),
	}
	if _, err := db.NewInsert().Model(attempt).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = repos.Contents.RetryFailedCopy(ctx, copyRow.ID)
	var blocked *repository.CopyRetryBlockedError
	if !errors.As(err, &blocked) || blocked.Block != storagepipeline.CopyRetryRecoveryRequiresAttention {
		t.Fatalf("unresolved retry=%v", err)
	}
	if _, err := db.NewUpdate().Model(attempt).Set("status = ?", storagepull.AttemptStatusAbandoned).Set("resolved_at = ?", time.Now()).WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	last, err := repos.Contents.GetLastAbandonedPullAttempt(ctx, content.ID, copyRow.StorageDataSetID)
	if err != nil || last.AttemptID != attempt.AttemptID {
		t.Fatalf("last=%+v err=%v", last, err)
	}
	rollback := errors.New("rollback")
	if err := repos.WithTx(ctx, func(tx *repository.Repositories) error {
		reopened, err := tx.Contents.RetryFailedCopy(ctx, copyRow.ID)
		if err != nil {
			return err
		}
		if reopened.Status != model.StorageCopyStatusPending || reopened.TransferMethod != model.StorageCopyTransferMethodPeerPull {
			t.Fatalf("retry without a source=%+v", reopened)
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if err := repos.Objects.SetVersionCachePresence(ctx, version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	reopened, err := repos.Contents.RetryFailedCopy(ctx, copyRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Status != model.StorageCopyStatusPending || reopened.TransferMethod != model.StorageCopyTransferMethodCacheRestore || reopened.LastError != nil || reopened.IngressStoreAttempt != 0 {
		t.Fatalf("reopened=%+v", reopened)
	}
	if _, err := repos.Contents.RetryFailedCopy(ctx, copyRow.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("duplicate retry=%v", err)
	}
	subjectType, subjectKey := "storage_copy", fmt.Sprint(copyRow.ID)
	finishedAt := time.Now()
	old := repositoryTestTask(&model.Task{Type: model.TaskTypeStoragePull, IdempotencyKey: "old-copy-pull", SubjectType: &subjectType, SubjectKey: &subjectKey, InputVersion: 1, Input: []byte(`{}`), InputHash: "old", Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now()})
	if _, _, err := repos.Tasks.Enqueue(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", finishedAt).Where("id = ?", old.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	count, err := repos.Tasks.AcknowledgeFailedForSubject(ctx, "storage_copy", fmt.Sprint(copyRow.ID))
	if err != nil || count != 1 {
		t.Fatalf("dismiss count=%d err=%v", count, err)
	}
	got, err := repos.Tasks.GetByID(ctx, old.ID)
	if err != nil || got.Status != model.TaskStatusFailed || got.AcknowledgedAt == nil {
		t.Fatalf("history=%+v err=%v", got, err)
	}
	return reopened
}

func TestOrdinaryCacheRestoreProtectsCache(t *testing.T) {
	ordinaryCacheRestoreProtectsCache(t, testDB(t))
}

func ordinaryCacheRestoreProtectsCache(t *testing.T, db *bun.DB) {
	t.Helper()
	copyRow := copyRetryAdmissionAndHistory(t, db)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket, err := repos.Buckets.GetByID(ctx, copyRow.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	one := 1
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{Name: bucket.Name, SetMinimumDurableCopies: true, MinimumDurableCopies: &one}); err != nil {
		t.Fatal(err)
	}
	source, err := repos.Contents.GetUploadCopy(ctx, copyRow.ContentID, 0)
	if err != nil {
		t.Fatal(err)
	}
	piece := onChainID(t, "0")
	testutil.CommitStorageCopy(t, db, repos, testutil.CommitCopyInput{StorageCopyID: source.ID, ContentID: copyRow.ContentID, CopyIndex: 0, PieceCID: "bafk2bzacecmigrationcache", PieceID: &piece, RetrievalURL: "https://source.example/piece"})
	if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("transfer_method = ?", model.StorageCopyTransferMethodPeerPull).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	work, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: model.TaskTypeStoragePull, IdempotencyKey: "restore-pull", InputVersion: 1, Input: []byte(fmt.Sprintf(`{"copy_id":%d,"generation":1}`, copyRow.ID)), SubjectType: new(model.TaskSubjectStorageCopy), SubjectKey: new(fmt.Sprint(copyRow.ID)), InputHash: "restore"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.BindCopyTask(ctx, copyRow.ID, 1, work.ID); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.SetCopyCacheRestore(ctx, copyRow.ID, 1, work.ID, "", ""); err != nil {
		t.Fatal(err)
	}
	reservation, err := repos.CacheEvictions.PrepareEviction(ctx, copyRow.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	evict := enqueueCacheEvictionTask(t, repos, copyRow.ContentID, reservation.Generation, "restore-evict")
	if err := repos.CacheEvictions.BindEvictionTask(ctx, copyRow.ContentID, reservation.Generation, evict.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.CacheEvictions.AuthorizeDeletion(ctx, copyRow.ContentID, reservation.Generation, evict.ID, nil); !errors.Is(err, cacheeviction.ErrNoLongerEligible) {
		t.Fatalf("eviction with pending restore=%v", err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("transfer_method = ?", model.StorageCopyTransferMethodPeerPull).Where("id = ?", copyRow.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.SetCopyCacheRestore(ctx, copyRow.ID, 1, work.ID, "", ""); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("restore during eviction=%v", err)
	}
}
