package repository_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestFailedIngressCanBeReplacedThenPulledFromCommittedSuccessor(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "failed-ingress-successor")
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 1,
		Checksum: testutil.StorageChecksum("failed-ingress-successor"), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var bindings [2]*model.StorageDataSet
	for index := range bindings {
		providerID := onChainID(t, fmt.Sprint(101+index))
		binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: index, CreatedByContentID: content.ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		dataSetID := onChainID(t, fmt.Sprint(201+index))
		clientID := onChainID(t, fmt.Sprint(301+index))
		if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
			ID: binding.ID, ContentID: content.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
		}); err != nil {
			t.Fatal(err)
		}
		bindings[index] = binding
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: bindings[0].ID, CopyIndex: 0, ProviderID: bindings[0].ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress},
		{StorageDataSetID: bindings[1].ID, CopyIndex: 1, ProviderID: bindings[1].ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull},
	}); err != nil {
		t.Fatal(err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("copies = %#v, err=%v", copies, err)
	}
	if err := repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
		StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0, LastError: "ingress failed",
	}); err != nil {
		t.Fatal(err)
	}
	promoted, err := repos.Contents.PromotePendingIngress(t.Context(), content.ID)
	if err != nil || promoted == nil || promoted.ID != copies[1].ID {
		t.Fatalf("promoted = %#v, err=%v", promoted, err)
	}
	ingress, err := repos.Contents.GetIngressCopy(t.Context(), content.ID)
	if err != nil || ingress == nil || ingress.ID != copies[1].ID {
		t.Fatalf("active ingress = %#v, err=%v", ingress, err)
	}
	pieceID := onChainID(t, "901")
	testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[1].ID, ContentID: content.ID, CopyIndex: 1,
		PieceCID: "bafk2bzacecpiecerestore", PieceID: &pieceID, RetrievalURL: "https://provider.example/piece",
	})
	reopened, err := repos.Contents.ReopenFailedIngressForPull(t.Context(), content.ID)
	if err != nil || len(reopened) != 1 || reopened[0].ID != copies[0].ID {
		t.Fatalf("reopened = %#v, err=%v", reopened, err)
	}
	old, err := repos.Contents.GetUploadCopyByID(t.Context(), copies[0].ID)
	if err != nil || old.Status != model.StorageCopyStatusPending || old.TransferMethod != model.StorageCopyTransferMethodPeerPull {
		t.Fatalf("old ingress = %#v, err=%v", old, err)
	}
	sources, err := repos.Contents.ListReadableCommittedCopies(t.Context(), content.ID)
	if err != nil || len(sources) != 1 || sources[0].CopyIndex != 1 {
		t.Fatalf("pull sources = %#v, err=%v", sources, err)
	}
}

func TestMigrationCacheRestoreIsExplicitAndBlocksEviction(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "migration-cache-restore")
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 10,
		Checksum: testutil.StorageChecksum("migration-cache-restore"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "restore.bin", ContentID: &content.ID,
		Size: 10, ETag: "restore", ContentType: "application/octet-stream",
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := repos.Objects.SetVersionCachePresence(t.Context(), version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "401"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceDataSetID, sourceClientID := onChainID(t, "501"), onChainID(t, "601")
	if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: sourceDataSetID, ClientDataSetID: &sourceClientID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: source.ID, CopyIndex: 0,
		ProviderID: source.ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress,
	}}); err != nil {
		t.Fatal(err)
	}
	sourceCopy, err := repos.Contents.GetUploadCopyForDataSet(t.Context(), content.ID, source.ID)
	if err != nil || sourceCopy == nil {
		t.Fatalf("source copy = %#v, err=%v", sourceCopy, err)
	}
	sourcePieceID := onChainID(t, "701")
	testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: sourceCopy.ID, ContentID: content.ID, CopyIndex: 0,
		PieceCID: "bafk2bzacecmigrationcache", PieceID: &sourcePieceID,
		RetrievalURL: "https://source.example/piece",
	})
	replacement, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "402"), ClientRequestID: "cache-restore",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: replacement.TargetDataSetID, CopyIndex: 0,
		ProviderID: onChainID(t, "402"), TransferMethod: model.StorageCopyTransferMethodPeerPull,
	}}); err != nil {
		t.Fatal(err)
	}
	copyRow, err := repos.Contents.GetUploadCopyForDataSet(t.Context(), content.ID, replacement.TargetDataSetID)
	if err != nil || copyRow == nil {
		t.Fatalf("target copy = %#v, err=%v", copyRow, err)
	}
	item := &storagereplacement.Item{
		ReplacementID: replacement.ID, ContentID: content.ID,
		TargetDataSetID: replacement.TargetDataSetID, Status: storagereplacement.ItemStatusPending,
	}
	if _, err := db.NewInsert().Model(item).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if candidates, err := repos.CacheEvictions.ListLRUCandidates(t.Context(), 10); err != nil || len(candidates) != 0 {
		t.Fatalf("LRU candidates during pending migration = %#v, err=%v", candidates, err)
	}
	evict := enqueueAndClaimTask(t, repos, "cache-restore-eviction", time.Minute)
	reservation, err := repos.CacheEvictions.PrepareEviction(t.Context(), content.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.CacheEvictions.BindEvictionTask(t.Context(), content.ID, reservation.Generation, evict.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.CacheEvictions.AuthorizeDeletion(t.Context(), content.ID, reservation.Generation, evict.ID, nil); !errors.Is(err, cacheeviction.ErrNoLongerEligible) {
		t.Fatalf("eviction during pending migration = %v, want ineligible", err)
	}
	taskRow, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: model.TaskTypeStorageTransferPlan, IdempotencyKey: "cache-restore-plan", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "cache-restore-plan", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil || !created {
		t.Fatalf("enqueue copy task = %#v, created=%v, err=%v", taskRow, created, err)
	}
	if err := repos.Contents.BindCopyTask(t.Context(), copyRow.ID, 1, taskRow.ID); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.SetCopyCacheRestore(t.Context(), copyRow.ID, 1, taskRow.ID, ""); err != nil {
		t.Fatal(err)
	}
	stored, err := repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil || stored.TransferMethod != model.StorageCopyTransferMethodCacheRestore {
		t.Fatalf("explicit restore method = %#v, err=%v", stored, err)
	}
	if _, err := repos.Contents.BeginIngressStoreProgress(t.Context(), repository.BeginIngressStoreProgressInput{
		CopyID: copyRow.ID, Generation: 1, TaskID: taskRow.ID, Attempt: 1,
	}); err != nil {
		t.Fatalf("begin restore progress: %v", err)
	}
	if _, err := repos.Contents.RecordIngressStoreProgress(t.Context(), repository.RecordIngressStoreProgressInput{
		CopyID: copyRow.ID, Generation: 1, TaskID: taskRow.ID, Attempt: 1, BytesUploaded: 5,
	}); err != nil {
		t.Fatalf("record restore progress: %v", err)
	}
	if _, err := db.NewUpdate().Model((*storagereplacement.Item)(nil)).
		Set("status = ?", storagereplacement.ItemStatusAttention).
		Where("id = ?", item.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.CacheEvictions.AuthorizeDeletion(t.Context(), content.ID, reservation.Generation, evict.ID, nil); !errors.Is(err, cacheeviction.ErrNoLongerEligible) {
		t.Fatalf("eviction while migration needs attention = %v, want ineligible", err)
	}
}
