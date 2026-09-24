package repository_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestUnfinishedStoreProtectsCacheAfterMinimumDurability(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "unfinished-store-cache")
	if _, err := db.NewUpdate().Model((*model.Bucket)(nil)).
		Set("default_copies = 2").Set("minimum_durable_copies = 1").Where("id = ?", bucket.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 128, Checksum: testutil.StorageChecksum("unfinished-store-cache"), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID,
		Key: "pinned.bin", ContentID: &content.ID, Size: 128, ETag: "pinned", ContentType: "application/octet-stream",
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := repos.Objects.SetVersionCachePresence(t.Context(), version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	bindings := make([]*model.StorageDataSet, 2)
	for i := range bindings {
		providerID := onChainID(t, fmt.Sprint(101+i))
		binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: i, CreatedByContentID: content.ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		dataSetID := onChainID(t, fmt.Sprint(301+i))
		clientID := onChainID(t, fmt.Sprint(501+i))
		if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
			ID: binding.ID, ContentID: content.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
		}); err != nil {
			t.Fatal(err)
		}
		bindings[i] = binding
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: bindings[0].ID, CopyIndex: 0, ProviderID: bindings[0].ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull},
		{StorageDataSetID: bindings[1].ID, CopyIndex: 1, ProviderID: bindings[1].ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress},
	}); err != nil {
		t.Fatal(err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("copies = %#v, err=%v", copies, err)
	}
	firstPieceID := onChainID(t, "71")
	testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0,
		PieceCID: "bafk2bzacecpinnedsource", PieceID: &firstPieceID, RetrievalURL: "https://source.example/piece",
	})
	if candidates, err := repos.CacheEvictions.ListLRUCandidates(t.Context(), 10); err != nil || len(candidates) != 0 {
		t.Fatalf("LRU candidates with unfinished Store = %#v, err=%v", candidates, err)
	}
	durabilityTask := enqueueAndClaimTask(t, repos, "unfinished-store-durability", time.Minute)
	durabilityGeneration, err := repos.CacheEvictions.NextDurabilityGeneration(t.Context(), bucket.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.CacheEvictions.BindDurabilityTask(t.Context(), bucket.ID, durabilityGeneration, durabilityTask.ID); err != nil {
		t.Fatal(err)
	}
	if candidate, err := repos.CacheEvictions.NextBucketDurabilityCandidate(t.Context(), bucket.ID, durabilityGeneration, durabilityTask.ID); err != nil || candidate != nil {
		t.Fatalf("capacity cleanup candidate with unfinished Store = %#v, err=%v", candidate, err)
	}
	evict := enqueueAndClaimTask(t, repos, "unfinished-store-eviction", time.Minute)
	reservation, err := repos.CacheEvictions.PrepareEviction(t.Context(), content.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.CacheEvictions.BindEvictionTask(t.Context(), content.ID, reservation.Generation, evict.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.CacheEvictions.AuthorizeDeletion(t.Context(), content.ID, reservation.Generation, evict.ID, nil); !errors.Is(err, cacheeviction.ErrNoLongerEligible) {
		t.Fatalf("final deletion authorization = %v, want ineligible", err)
	}
	secondPieceID := onChainID(t, "72")
	testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[1].ID, ContentID: content.ID, CopyIndex: 1,
		PieceCID: "bafk2bzacecpinnedsource", PieceID: &secondPieceID, RetrievalURL: "https://target.example/piece",
	})
	if _, err := repos.CacheEvictions.AuthorizeDeletion(t.Context(), content.ID, reservation.Generation, evict.ID, nil); err != nil {
		t.Fatalf("final deletion authorization after Store commit: %v", err)
	}
}
