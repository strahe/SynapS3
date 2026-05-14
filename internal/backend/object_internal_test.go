package backend

import (
	"context"
	"log/slog"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestCompleteFollowerIfStoredReuseWonRaceFinalizesReplicatingFollower(t *testing.T) {
	ctx := context.Background()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	bucket := &model.Bucket{Name: "replicating-finalize-race-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	sourceID := model.NewVersionID()
	source := &model.ObjectVersion{
		VersionID:   sourceID,
		BucketID:    bucket.ID,
		Key:         "file.txt",
		Size:        9,
		ETag:        "source-etag",
		Checksum:    "shared-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/" + sourceID,
		State:       model.ObjectStateCached,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
		t.Fatalf("create source version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, sourceID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("source uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, sourceID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("source committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: sourceID,
		ContentSize:     source.Size,
		Checksum:        source.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("secondary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("primary ready: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: secondary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002")}); err != nil {
		t.Fatalf("secondary ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("create copy rows: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 0, PieceCID: "bafk2bzacedummy", PieceID: onChainIDPtr(t, "1"), RetrievalURL: "https://provider.example/primary"}); err != nil {
		t.Fatalf("primary committed: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: source.Size,
		Checksum:    source.Checksum,
	}); err != nil {
		t.Fatalf("bind primary committed: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 1, PieceCID: "bafk2bzacedummy", PieceID: onChainIDPtr(t, "2"), RetrievalURL: "https://provider.example/secondary"}); err != nil {
		t.Fatalf("secondary committed: %v", err)
	}

	uploadID := upload.ID
	followerID := model.NewVersionID()
	follower := &model.ObjectVersion{
		VersionID:       followerID,
		BucketID:        bucket.ID,
		Key:             "file.txt",
		Size:            source.Size,
		ETag:            "follower-etag",
		Checksum:        source.Checksum,
		ContentType:     "text/plain",
		CacheKey:        ".versions/" + followerID,
		StorageUploadID: &uploadID,
		State:           model.ObjectStateReplicating,
	}
	followerObjectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower)
	if err != nil {
		t.Fatalf("create follower version: %v", err)
	}

	b := &SynapseBackend{repos: repos, logger: slog.Default()}
	b.completeFollowerIfStoredReuseWonRace(ctx, bucket.ID, bucket.Name, follower.Size, follower.Checksum, followerObjectID, followerID, model.ObjectStateReplicating)

	got, err := repos.Objects.GetVersionByID(ctx, followerID)
	if err != nil || got == nil {
		t.Fatalf("get follower version: version=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateStored {
		t.Fatalf("follower state = %s, want stored", got.State)
	}
}

func TestCompleteFollowerIfStoredReuseWonRaceFinalizesAfterBindingFollower(t *testing.T) {
	ctx := context.Background()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	bucket := &model.Bucket{Name: "active-follower-finalize-race-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	sourceID := model.NewVersionID()
	source := &model.ObjectVersion{
		VersionID:   sourceID,
		BucketID:    bucket.ID,
		Key:         "file.txt",
		Size:        9,
		ETag:        "source-etag",
		Checksum:    "shared-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/" + sourceID,
		State:       model.ObjectStateCached,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
		t.Fatalf("create source version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, sourceID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("source uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, sourceID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("source committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: sourceID,
		ContentSize:     source.Size,
		Checksum:        source.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("secondary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("primary ready: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: secondary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002")}); err != nil {
		t.Fatalf("secondary ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("create copy rows: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 0, PieceCID: "bafk2bzacedummy", PieceID: onChainIDPtr(t, "1"), RetrievalURL: "https://provider.example/primary"}); err != nil {
		t.Fatalf("primary committed: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: source.Size,
		Checksum:    source.Checksum,
	}); err != nil {
		t.Fatalf("bind primary committed: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 1, PieceCID: "bafk2bzacedummy", PieceID: onChainIDPtr(t, "2"), RetrievalURL: "https://provider.example/secondary"}); err != nil {
		t.Fatalf("secondary committed: %v", err)
	}

	followerID := model.NewVersionID()
	follower := &model.ObjectVersion{
		VersionID:   followerID,
		BucketID:    bucket.ID,
		Key:         "file.txt",
		Size:        source.Size,
		ETag:        "follower-etag",
		Checksum:    source.Checksum,
		ContentType: "text/plain",
		CacheKey:    ".versions/" + followerID,
		State:       model.ObjectStateUploading,
	}
	followerObjectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower)
	if err != nil {
		t.Fatalf("create follower version: %v", err)
	}

	b := &SynapseBackend{repos: repos, logger: slog.Default()}
	b.completeFollowerIfStoredReuseWonRace(ctx, bucket.ID, bucket.Name, follower.Size, follower.Checksum, followerObjectID, followerID, model.ObjectStateUploading)

	got, err := repos.Objects.GetVersionByID(ctx, followerID)
	if err != nil || got == nil {
		t.Fatalf("get follower version: version=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateStored {
		t.Fatalf("follower state = %s, want stored", got.State)
	}
}
