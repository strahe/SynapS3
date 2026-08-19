package worker

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestEvictorBucketDurabilityReconciliationProcessesOneHundredVersionsPerClaim(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, "durability-batch")
	minimum := 1
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:                    bucket.Name,
		SetMinimumDurableCopies: true,
		MinimumDurableCopies:    &minimum,
	}); err != nil {
		t.Fatalf("UpdateCopyPolicy: %v", err)
	}

	const versionCount = bucketDurabilityBatchSize + 1
	var sourceVersionID string
	for index := range versionCount {
		version := &model.ObjectVersion{
			VersionID:   model.NewVersionID(),
			BucketID:    bucket.ID,
			Key:         fmt.Sprintf("shared-%03d.bin", index),
			Size:        10,
			ETag:        "shared-etag",
			Checksum:    "shared-durability-checksum",
			ContentType: "application/octet-stream",
			CacheKey:    fmt.Sprintf(".versions/shared-%03d", index),
		}
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%d): %v", index, err)
		}
		if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("UpdateVersionState(%d): %v", index, err)
		}
		if index == 0 {
			sourceVersionID = version.VersionID
		}
	}

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: sourceVersionID,
		ContentSize:     10,
		Checksum:        "shared-durability-checksum",
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	providerID := onChainID(t, "101")
	dataSetID := onChainID(t, "1001")
	pieceID := onChainID(t, "2001")
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        providerID,
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  upload.ID,
		DataSetID: dataSetID,
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       providerID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacedurabilitybatch",
		PieceID:      &pieceID,
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	refs, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: 10,
		Checksum:    "shared-durability-checksum",
	})
	if err != nil || len(refs) != versionCount {
		t.Fatalf("BindReadableUploadForContent: refs=%d err=%v", len(refs), err)
	}
	if _, err := repos.CacheEvictions.EnsureBucketDurabilityReconciliation(ctx, bucket.ID, 4); err != nil {
		t.Fatalf("EnsureBucketDurabilityReconciliation: %v", err)
	}

	evictor := NewEvictor(
		repos,
		&testutil.MockCache{},
		cacheaccess.NewGate(),
		cacheaccess.NewTracker(0, repos.Objects),
		state.NewObjectStateMachine(),
		1,
		time.Millisecond,
		slog.Default(),
		WithCacheEvictionPolicy(cache.EvictionPolicyNone, 0, 90, 80, 4),
	)
	task, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil || task == nil {
		t.Fatalf("ClaimReady first batch: task=%#v err=%v", task, err)
	}
	evictor.processTask(ctx, task)
	assertObjectVersionStateCount(t, db, bucket.ID, model.ObjectStateStored, bucketDurabilityBatchSize)
	assertObjectVersionStateCount(t, db, bucket.ID, model.ObjectStateReplicating, 1)
	firstBatchTask, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || firstBatchTask == nil || firstBatchTask.Status != model.TaskStatusQueued {
		t.Fatalf("task after first batch = %#v err=%v, want queued", firstBatchTask, err)
	}

	task, err = repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil || task == nil {
		t.Fatalf("ClaimReady second batch: task=%#v err=%v", task, err)
	}
	evictor.processTask(ctx, task)
	assertObjectVersionStateCount(t, db, bucket.ID, model.ObjectStateStored, versionCount)
	assertObjectVersionStateCount(t, db, bucket.ID, model.ObjectStateReplicating, 0)
	completed, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || completed == nil || completed.Status != model.TaskStatusCompleted {
		t.Fatalf("task after second batch = %#v err=%v, want completed", completed, err)
	}
}

func assertObjectVersionStateCount(
	t *testing.T,
	db bun.IDB,
	bucketID int64,
	wantState model.ObjectState,
	wantCount int,
) {
	t.Helper()
	count, err := db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND state = ?", bucketID, wantState).
		Count(context.Background())
	if err != nil {
		t.Fatalf("count %s versions: %v", wantState, err)
	}
	if count != wantCount {
		t.Fatalf("%s version count = %d, want %d", wantState, count, wantCount)
	}
}
