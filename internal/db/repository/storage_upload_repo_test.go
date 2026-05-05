package repository_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"
)

func TestStorageUploadRepo_RecordCompleteResultAndAcceptsUploadingContent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "upload-provenance-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000010001", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	task := seedRunningUploadTask(t, repos, objectID, version.VersionID)

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceTaskID:    task.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        strPtr("bafk2bzaceprovenance"),
		RequestedCopies: 2,
		RawResultJSON:   []byte(`{"complete":true}`),
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), Role: "primary", RetrievalURL: strPtr("https://primary.example/piece"), IsNewDataSet: true},
			{ProviderID: onChainIDPtr(t, "202"), DataSetID: onChainIDPtr(t, "2002"), PieceID: onChainIDPtr(t, "3001"), Role: "secondary", RetrievalURL: strPtr("https://secondary.example/piece"), IsNewDataSet: true},
		},
	}); err != nil {
		t.Fatalf("RecordUploadResult: %v", err)
	}

	refs, err := repos.Uploads.AcceptCompleteUploadForContent(ctx, repository.AcceptCompleteUploadInput{
		UploadID:        upload.ID,
		TaskID:          task.ID,
		BucketID:        bucket.ID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		AutoEvict:       true,
		EvictMaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("AcceptCompleteUploadForContent: %v", err)
	}
	if len(refs) != 1 || refs[0].VersionID != version.VersionID {
		t.Fatalf("accepted refs = %#v, want source version", refs)
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID: got=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateStored {
		t.Fatalf("state = %s, want stored", got.State)
	}
	if got.StorageUploadID == nil || *got.StorageUploadID != upload.ID {
		t.Fatalf("storage_upload_id = %#v, want %d", got.StorageUploadID, upload.ID)
	}
	if got.PieceCID == nil || *got.PieceCID != "bafk2bzaceprovenance" {
		t.Fatalf("piece_cid = %#v, want derived piece cid", got.PieceCID)
	}
	if !got.InFilecoin {
		t.Fatal("in_filecoin = false, want derived true")
	}

	completed, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID(task): %v", err)
	}
	if completed.Status != model.TaskStatusCompleted {
		t.Fatalf("task status = %s, want completed", completed.Status)
	}
}

func TestStorageUploadRepo_OnChainIDsRoundTripLargeValuesAndZeroPieceID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "onchain-id-round-trip-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000000BIG01",
		ContentSize:     10,
		Checksum:        "checksum-onchain-id",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	providerID := onChainID(t, "18446744073709551616")
	dataSetID := onChainID(t, "18446744073709551617")
	clientDataSetID := onChainIDPtr(t, "0")
	pieceID := onChainIDPtr(t, "0")

	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        providerID,
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	gotPending, err := repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotPending == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex pending: binding=%v err=%v", gotPending, err)
	}
	if gotPending.DataSetID != nil || gotPending.ClientDataSetID != nil {
		t.Fatalf("pending binding ids = data:%v client:%v, want nil SQL NULLs", gotPending.DataSetID, gotPending.ClientDataSetID)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:              binding.ID,
		UploadID:        upload.ID,
		DataSetID:       dataSetID,
		ClientDataSetID: clientDataSetID,
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, Role: "primary", ProviderID: providerID},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-onchain-id",
		PieceID:      pieceID,
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}

	gotBinding, err := repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || gotBinding == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex ready: binding=%v err=%v", gotBinding, err)
	}
	if gotBinding.ProviderID.String() != providerID.String() || gotBinding.DataSetID == nil || gotBinding.DataSetID.String() != dataSetID.String() || gotBinding.ClientDataSetID == nil || gotBinding.ClientDataSetID.String() != "0" {
		t.Fatalf("ready binding = %#v, want large provider/data set and client 0", gotBinding)
	}
	copies, err := repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 1 || copies[0].ProviderID == nil || copies[0].ProviderID.String() != providerID.String() || copies[0].DataSetID == nil || copies[0].DataSetID.String() != dataSetID.String() || copies[0].PieceID == nil || copies[0].PieceID.String() != "0" {
		t.Fatalf("copy = %#v, want large IDs and piece ID 0", copies)
	}
}

func TestStorageUploadRepo_RequiredOnChainIDValidationUsesInvalidInput(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "onchain-id-validation-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000000BAD01",
		ContentSize:     10,
		Checksum:        "checksum-onchain-validation",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}

	if _, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        types.OnChainID{},
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	}); !errors.Is(err, repository.ErrInvalidInput) || errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("EnsureDataSetBinding zero provider error = %v, want ErrInvalidInput only", err)
	}

	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding valid: %v", err)
	}

	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, Role: "primary", ProviderID: types.OnChainID{}},
	}); !errors.Is(err, repository.ErrInvalidInput) || errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("CreateUploadCopiesForBindings zero provider error = %v, want ErrInvalidInput only", err)
	}

	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  upload.ID,
		DataSetID: types.OnChainID{},
	}); !errors.Is(err, repository.ErrInvalidInput) || errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("MarkDataSetReady zero data set error = %v, want ErrInvalidInput only", err)
	}
}

func TestStorageUploadRepo_DataSetBindingIsBucketProviderCopySlot(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "dataset-binding-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J00000000000000000020001",
		ContentSize:     10,
		Checksum:        "checksum-binding",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}

	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding primary: %v", err)
	}
	again, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding again: %v", err)
	}
	if again.ID != primary.ID {
		t.Fatalf("binding id = %d, want reused %d", again.ID, primary.ID)
	}
	if primary.Status != model.StorageDataSetStatusPending || primary.DataSetID != nil || primary.ClientDataSetID != nil {
		t.Fatalf("new binding = status:%s dataSet:%v client:%v, want pending without ids", primary.Status, primary.DataSetID, primary.ClientDataSetID)
	}

	if _, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "202"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	}); err == nil {
		t.Fatal("same bucket copy_index with different provider should be rejected")
	}
	if _, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         1,
		CreatedByUploadID: upload.ID,
	}); err == nil {
		t.Fatal("same bucket provider with different copy_index should be rejected")
	}
}

func TestStorageUploadRepo_RecordResultDoesNotOverwriteStagedCopies(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "staged-result-guard")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J00000000000000000020006",
		ContentSize:     10,
		Checksum:        "checksum-staged-result",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}

	err = repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        strPtr("bafk2bzacestagedguard"),
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), Role: "primary", RetrievalURL: strPtr("https://provider.example/piece")},
		},
	})
	if err == nil {
		t.Fatal("RecordUploadResult should reject uploads that already have staged copy rows")
	}

	copies, err := repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 1 || copies[0].Status != model.StorageUploadCopyStatusPending || copies[0].StorageDataSetID == nil || *copies[0].StorageDataSetID != binding.ID {
		t.Fatalf("copies after rejected legacy result = %#v, want original staged pending copy", copies)
	}
}

func TestStorageUploadRepo_BindPrimaryCommittedUploadForContentMovesFollowersToReplicating(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "primary-commit-bind-bucket")

	leader := newObjectVersion(bucket.ID, "leader.txt", "01J00000000000000000020002", 10)
	leader.Checksum = "same-primary-commit"
	leaderObjectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, leader)
	if err != nil {
		t.Fatalf("create leader: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, leader.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("leader uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, leader.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("leader committing: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, leader.VersionID, model.ObjectStateCommitting, "stale primary failure"); err != nil {
		t.Fatalf("leader stale failed: %v", err)
	}
	follower := newObjectVersion(bucket.ID, "follower.txt", "01J00000000000000000020003", 10)
	follower.Checksum = leader.Checksum
	follower.State = model.ObjectStateUploading
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("create follower: %v", err)
	}
	independent := newObjectVersion(bucket.ID, "independent.txt", "01J00000000000000000020004", 10)
	independent.Checksum = leader.Checksum
	independent.State = model.ObjectStateUploading
	independentObjectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, independent)
	if err != nil {
		t.Fatalf("create independent: %v", err)
	}
	if err := repos.Tasks.Create(ctx, &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          independentObjectID,
		RefVersionID:   independent.VersionID,
		IdempotencyKey: "upload:" + independent.VersionID,
		Status:         model.TaskStatusPending,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}); err != nil {
		t.Fatalf("create independent task: %v", err)
	}

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: leader.VersionID,
		ContentSize:     leader.Size,
		Checksum:        leader.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzaceprimarybind",
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := repos.Uploads.BindPrimaryCommittedUploadForContent(ctx, repository.BindPrimaryCommittedUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: leader.Size,
		Checksum:    leader.Checksum,
	}); err != nil {
		t.Fatalf("BindPrimaryCommittedUploadForContent: %v", err)
	}

	for _, versionID := range []string{leader.VersionID, follower.VersionID} {
		got, err := repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || got == nil {
			t.Fatalf("GetVersionByID(%s): got=%v err=%v", versionID, got, err)
		}
		if got.State != model.ObjectStateReplicating || got.StorageUploadID == nil || *got.StorageUploadID != upload.ID || !got.InFilecoin {
			t.Fatalf("version %s = state:%s upload:%v in_filecoin:%v, want replicating bound to %d", versionID, got.State, got.StorageUploadID, got.InFilecoin, upload.ID)
		}
		if versionID == leader.VersionID && (got.FailedAtState != nil || got.LastError != nil) {
			t.Fatalf("leader failure details = failed_at_state:%#v last_error:%#v, want nil", got.FailedAtState, got.LastError)
		}
	}
	gotIndependent, err := repos.Objects.GetVersionByID(ctx, independent.VersionID)
	if err != nil || gotIndependent == nil {
		t.Fatalf("GetVersionByID(independent): got=%v err=%v", gotIndependent, err)
	}
	if gotIndependent.State != model.ObjectStateUploading || gotIndependent.StorageUploadID != nil {
		t.Fatalf("independent = state:%s upload:%v, want untouched uploading", gotIndependent.State, gotIndependent.StorageUploadID)
	}
	if leaderObjectID == 0 {
		t.Fatal("leader object id should be set")
	}
}

func TestStorageUploadRepo_BindPrimaryCommittedUploadForVersionCompletesFollowerTask(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "primary-commit-version-bind-bucket")

	leader := newObjectVersion(bucket.ID, "leader.txt", "01J00000000000000000020102", 10)
	leader.Checksum = "same-primary-version-bind"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, leader); err != nil {
		t.Fatalf("create leader: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, leader.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("leader uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, leader.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("leader committing: %v", err)
	}
	follower := newObjectVersion(bucket.ID, "follower.txt", "01J00000000000000000020103", 10)
	follower.Checksum = leader.Checksum
	follower.State = model.ObjectStateUploading
	followerObjectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower)
	if err != nil {
		t.Fatalf("create follower: %v", err)
	}
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          followerObjectID,
		RefVersionID:   follower.VersionID,
		IdempotencyKey: "upload:" + follower.VersionID,
		Status:         model.TaskStatusPending,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create follower task: %v", err)
	}

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: leader.VersionID,
		ContentSize:     leader.Size,
		Checksum:        leader.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzaceversionbind",
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}

	refs, err := repos.Uploads.BindPrimaryCommittedUploadForVersion(ctx, repository.BindPrimaryCommittedUploadForVersionInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: follower.Size,
		Checksum:    follower.Checksum,
		VersionID:   follower.VersionID,
	})
	if err != nil {
		t.Fatalf("BindPrimaryCommittedUploadForVersion: %v", err)
	}
	if len(refs) != 1 || refs[0].VersionID != follower.VersionID {
		t.Fatalf("bound refs = %#v, want follower version", refs)
	}
	gotFollower, err := repos.Objects.GetVersionByID(ctx, follower.VersionID)
	if err != nil || gotFollower == nil {
		t.Fatalf("GetVersionByID(follower): got=%v err=%v", gotFollower, err)
	}
	if gotFollower.State != model.ObjectStateReplicating || gotFollower.StorageUploadID == nil || *gotFollower.StorageUploadID != upload.ID || !gotFollower.InFilecoin {
		t.Fatalf("follower = state:%s upload:%v in_filecoin:%v, want replicating bound to %d", gotFollower.State, gotFollower.StorageUploadID, gotFollower.InFilecoin, upload.ID)
	}
	gotTask, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || gotTask == nil {
		t.Fatalf("GetByID(follower task): task=%v err=%v", gotTask, err)
	}
	if gotTask.Status != model.TaskStatusCompleted {
		t.Fatalf("follower task status = %s, want completed", gotTask.Status)
	}
}

func TestStorageUploadRepo_FinalizeUploadIfAllCopiesCommittedMovesReplicatingToStored(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "finalize-upload-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000020005", 10)
	version.Checksum = "finalize-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("secondary binding: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, Role: "secondary", ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 0, PieceCID: "bafk2bzacefinalize", PieceID: onChainIDPtr(t, "301"), RetrievalURL: "https://primary.example/piece"}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted primary: %v", err)
	}
	if _, err := repos.Uploads.BindPrimaryCommittedUploadForContent(ctx, repository.BindPrimaryCommittedUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindPrimaryCommittedUploadForContent: %v", err)
	}

	done, refs, err := repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfAllCopiesCommitted partial: %v", err)
	}
	if done || len(refs) != 0 {
		t.Fatalf("partial finalize = done:%v refs:%v, want no-op", done, refs)
	}
	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID partial: got=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateReplicating {
		t.Fatalf("partial state = %s, want replicating", got.State)
	}

	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 1, PieceCID: "bafk2bzacefinalize", PieceID: onChainIDPtr(t, "302"), RetrievalURL: "https://secondary.example/piece"}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted secondary: %v", err)
	}
	done, refs, err = repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfAllCopiesCommitted complete: %v", err)
	}
	if !done || len(refs) != 1 || refs[0].VersionID != version.VersionID {
		t.Fatalf("complete finalize = done:%v refs:%v, want stored source version", done, refs)
	}
	got, err = repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID complete: got=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateStored {
		t.Fatalf("complete state = %s, want stored", got.State)
	}
}

func TestStorageUploadRepo_PartialResultDoesNotBindObjectVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "partial-upload-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000010002", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	task := seedRunningUploadTask(t, repos, objectID, version.VersionID)

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceTaskID:    task.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        false,
		PieceCID:        strPtr("bafk2bzacepartial"),
		RequestedCopies: 2,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), Role: "primary", RetrievalURL: strPtr("https://primary.example/piece"), IsNewDataSet: true},
		},
	}); err != nil {
		t.Fatalf("RecordUploadResult: %v", err)
	}
	if _, err := repos.Uploads.AcceptCompleteUploadForContent(ctx, repository.AcceptCompleteUploadInput{
		UploadID:    upload.ID,
		TaskID:      task.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err == nil {
		t.Fatal("AcceptCompleteUploadForContent should reject partial upload")
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID: got=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateUploading || got.StorageUploadID != nil || got.InFilecoin {
		t.Fatalf("version after partial = state:%s upload:%#v filecoin:%v", got.State, got.StorageUploadID, got.InFilecoin)
	}
}

func TestStorageUploadRepo_PrimaryCopyFailureMarksUploadFailed(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "primary-copy-failure-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J00000000000000000010030",
		ContentSize:     10,
		Checksum:        "checksum-primary-failure",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 0, "primary store: provider rejected piece"); err != nil {
		t.Fatalf("MarkUploadCopyFailed: %v", err)
	}
	got, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: got=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusFailed {
		t.Fatalf("upload status = %s, want failed", got.Status)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != "primary store: provider rejected piece" {
		t.Fatalf("upload error_message = %#v, want primary failure reason", got.ErrorMessage)
	}

	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-after-store-retry",
		RetrievalURL: "https://provider.example/retry",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady after failure: %v", err)
	}
	got, err = repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID after store retry: got=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusStoredOnPrimary {
		t.Fatalf("upload status after store retry = %s, want stored_on_primary", got.Status)
	}

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 0, "primary commit: provider rejected piece"); err != nil {
		t.Fatalf("MarkUploadCopyFailed after store retry: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-after-commit-retry",
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://provider.example/retry",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted after failure: %v", err)
	}
	got, err = repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID after commit retry: got=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusPrimaryCommitted {
		t.Fatalf("upload status after commit retry = %s, want primary_committed", got.Status)
	}
}

func TestStorageUploadRepo_CommittedCopyIgnoresStaleStatusUpdates(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "committed-copy-stale-status-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J00000000000000000010031",
		ContentSize:     10,
		Checksum:        "checksum-committed-stale-status",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding primary: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "202"),
		CopyIndex:         1,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding secondary: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady primary: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: secondary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002"), ClientDataSetID: onChainIDPtr(t, "9002")}); err != nil {
		t.Fatalf("MarkDataSetReady secondary: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, Role: "secondary", ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-committed-stale-status",
		PieceID:      onChainIDPtr(t, "3001"),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted primary: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    1,
		PieceCID:     "piece-committed-stale-status",
		PieceID:      onChainIDPtr(t, "3002"),
		RetrievalURL: "https://secondary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted secondary: %v", err)
	}

	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    1,
		PieceCID:     "piece-stale-ready",
		RetrievalURL: "https://secondary.example/stale",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady stale: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 1, "secondary pull: stale failure"); err != nil {
		t.Fatalf("MarkUploadCopyFailed stale: %v", err)
	}

	copyRow, err := repos.Uploads.GetUploadCopy(ctx, upload.ID, 1)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy: copy=%v err=%v", copyRow, err)
	}
	if copyRow.Status != model.StorageUploadCopyStatusCommitted {
		t.Fatalf("copy status = %s, want committed", copyRow.Status)
	}
}

func TestStorageUploadRepo_AcceptSupersededUploadPreservesAcceptError(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "superseded-accept-error-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000010020", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	_ = acceptTestStorageUploadForVersion(t, repos, bucket.ID, version, "bafk2bzacesupersededfirst")

	second, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("second StartObjectUploadAttempt: %v", err)
	}
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        second.ID,
		Complete:        true,
		PieceCID:        strPtr("bafk2bzacesupersededsecond"),
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001002"), PieceID: onChainIDPtr(t, "2002"), Role: "primary", RetrievalURL: strPtr("https://provider.example/second")},
		},
	}); err != nil {
		t.Fatalf("second RecordUploadResult: %v", err)
	}
	refs, err := repos.Uploads.AcceptCompleteUploadForContent(ctx, repository.AcceptCompleteUploadInput{
		UploadID:    second.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	})
	if err != nil {
		t.Fatalf("second AcceptCompleteUploadForContent: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("second accepted refs = %#v, want none", refs)
	}
	got, err := repos.Uploads.GetByID(ctx, second.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID(second): got=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusSuperseded {
		t.Fatalf("second status = %s, want superseded", got.Status)
	}
	if got.AcceptError == nil || !strings.Contains(*got.AcceptError, "source version already stored by another upload") {
		t.Fatalf("second accept_error = %#v, want superseded reason", got.AcceptError)
	}
}

func TestStorageUploadRepo_DataSetCrossBucketConflictPreservesCopyButRejects(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucketA := seedBucket(t, db, "dataset-owner-a")
	bucketB := seedBucket(t, db, "dataset-owner-b")

	first, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucketA.ID,
		SourceVersionID: "first",
		ContentSize:     1,
		Checksum:        "sum-a",
	})
	if err != nil {
		t.Fatalf("first StartObjectUploadAttempt: %v", err)
	}
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        first.ID,
		Complete:        true,
		PieceCID:        strPtr("bafk2bzacefirst"),
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), Role: "primary", RetrievalURL: strPtr("https://provider.example/first")},
		},
	}); err != nil {
		t.Fatalf("first RecordUploadResult: %v", err)
	}

	second, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucketB.ID,
		SourceVersionID: "second",
		ContentSize:     1,
		Checksum:        "sum-b",
	})
	if err != nil {
		t.Fatalf("second StartObjectUploadAttempt: %v", err)
	}
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        second.ID,
		Complete:        true,
		PieceCID:        strPtr("bafk2bzacesecond"),
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "9999"), Role: "primary", RetrievalURL: strPtr("https://provider.example/second")},
			{ProviderID: onChainIDPtr(t, "202"), DataSetID: onChainIDPtr(t, "2002"), PieceID: onChainIDPtr(t, "3001"), Role: "secondary", RetrievalURL: strPtr("https://provider.example/second-copy")},
		},
	}); err != nil {
		t.Fatalf("second RecordUploadResult: %v", err)
	}

	got, err := repos.Uploads.GetByID(ctx, second.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID(second): got=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusRejected {
		t.Fatalf("second status = %s, want rejected", got.Status)
	}
	copies, err := repos.Uploads.ListCopies(ctx, second.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 2 {
		t.Fatalf("copies len = %d, want 2", len(copies))
	}
	for _, copy := range copies {
		if copy.StorageDataSetID != nil {
			t.Fatalf("rejected copy storage_data_set_id = %#v, want nil", copy.StorageDataSetID)
		}
	}
	count, err := db.NewSelect().Model((*model.StorageDataSet)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("count storage data sets: %v", err)
	}
	if count != 1 {
		t.Fatalf("storage data set count = %d, want only original owner row", count)
	}
}

func TestStorageUploadRepo_RecordResultRejectsRacedCrossBucketDataSetConflict(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "storage-upload-cross-bucket-race.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening sqlite db: %v", err)
	}
	sqldb.SetMaxOpenConns(8)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, migrations.Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}
	repos := repository.NewRepositories(db)
	ownerBucket := seedBucket(t, db, "cross-bucket-race-owner")
	racingBucket := seedBucket(t, db, "cross-bucket-race-loser")

	ownerUpload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:    ownerBucket.ID,
		ContentSize: 1,
		Checksum:    "race-owner",
	})
	if err != nil {
		t.Fatalf("owner StartObjectUploadAttempt: %v", err)
	}
	racingUpload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:    racingBucket.ID,
		ContentSize: 1,
		Checksum:    "race-loser",
	})
	if err != nil {
		t.Fatalf("racing StartObjectUploadAttempt: %v", err)
	}

	hook := &storageDataSetRaceHook{
		bucketID:   ownerBucket.ID,
		uploadID:   ownerUpload.ID,
		providerID: "404",
		dataSetID:  "1404",
	}
	db.AddQueryHook(hook)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("opening bun conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	connRepos := repository.NewRepositories(conn)

	if err := connRepos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        racingUpload.ID,
		Complete:        true,
		PieceCID:        strPtr("piece-cross-race"),
		RequestedCopies: 2,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "303"), DataSetID: onChainIDPtr(t, "1303"), PieceID: onChainIDPtr(t, "1"), Role: "primary", RetrievalURL: strPtr("https://provider.example/safe")},
			{ProviderID: onChainIDPtr(t, hook.providerID), DataSetID: onChainIDPtr(t, hook.dataSetID), PieceID: onChainIDPtr(t, "2"), Role: "primary", RetrievalURL: strPtr("https://provider.example/cross-race")},
		},
	}); err != nil {
		t.Fatalf("RecordUploadResult: %v", err)
	}
	if hookErr := hook.err.Load(); hookErr != nil {
		t.Fatalf("race hook insert: %v", hookErr)
	}
	if !hook.triggered.Load() {
		t.Fatal("race hook did not run")
	}
	if hook.uniqueViolationSeen.Load() {
		t.Fatal("RecordUploadResult recovered from a unique violation, which aborts PostgreSQL transactions")
	}

	got, err := repos.Uploads.GetByID(ctx, racingUpload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID(racing upload): got=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusRejected {
		t.Fatalf("racing upload status = %s, want rejected", got.Status)
	}
	copies, err := repos.Uploads.ListCopies(ctx, racingUpload.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 2 {
		t.Fatalf("copies len = %d, want 2", len(copies))
	}
	for _, copy := range copies {
		if copy.StorageDataSetID != nil {
			t.Fatalf("raced rejected copy storage_data_set_id = %#v, want nil", copy.StorageDataSetID)
		}
	}
	count, err := db.NewSelect().
		Model((*model.StorageDataSet)(nil)).
		Where("bucket_id = ?", racingBucket.ID).
		Count(ctx)
	if err != nil {
		t.Fatalf("count racing bucket storage data sets: %v", err)
	}
	if count != 0 {
		t.Fatalf("racing bucket storage data set count = %d, want 0", count)
	}
}

func TestStorageUploadRepo_RecordResultReusesSameBucketDataSetAfterUniqueRace(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "storage-upload-race.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening sqlite db: %v", err)
	}
	sqldb.SetMaxOpenConns(8)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, migrations.Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "same-bucket-race")

	first, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:    bucket.ID,
		ContentSize: 1,
		Checksum:    "race-first",
	})
	if err != nil {
		t.Fatalf("first StartObjectUploadAttempt: %v", err)
	}
	second, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:    bucket.ID,
		ContentSize: 1,
		Checksum:    "race-second",
	})
	if err != nil {
		t.Fatalf("second StartObjectUploadAttempt: %v", err)
	}

	hook := &storageDataSetRaceHook{
		bucketID:   bucket.ID,
		uploadID:   first.ID,
		providerID: "505",
		dataSetID:  "1505",
	}
	db.AddQueryHook(hook)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("opening bun conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	connRepos := repository.NewRepositories(conn)

	retrievalURL := "https://provider.example/race"
	if err := connRepos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        second.ID,
		Complete:        true,
		PieceCID:        strPtr("piece-race"),
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, hook.providerID), DataSetID: onChainIDPtr(t, hook.dataSetID), PieceID: onChainIDPtr(t, "2"), Role: "primary", RetrievalURL: &retrievalURL},
		},
	}); err != nil {
		t.Fatalf("RecordUploadResult: %v", err)
	}
	if hookErr := hook.err.Load(); hookErr != nil {
		t.Fatalf("race hook insert: %v", hookErr)
	}
	if !hook.triggered.Load() {
		t.Fatal("race hook did not run")
	}
	if hook.uniqueViolationSeen.Load() {
		t.Fatal("RecordUploadResult recovered from a unique violation, which aborts PostgreSQL transactions")
	}

	copies, err := repos.Uploads.ListCopies(ctx, second.ID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 1 || copies[0].StorageDataSetID == nil {
		t.Fatalf("copies = %#v, want one copy linked to existing data set", copies)
	}
	dataSet := new(model.StorageDataSet)
	if err := db.NewSelect().
		Model(dataSet).
		Where("provider_id = ? AND data_set_id = ?", hook.providerID, hook.dataSetID).
		Scan(ctx); err != nil {
		t.Fatalf("selecting data set: %v", err)
	}
	if dataSet.CreatedByUploadID == nil || *dataSet.CreatedByUploadID != first.ID ||
		dataSet.LastUsedUploadID == nil || *dataSet.LastUsedUploadID != second.ID {
		t.Fatalf("data set seen uploads = created:%v last:%v, want created:%d last:%d", dataSet.CreatedByUploadID, dataSet.LastUsedUploadID, first.ID, second.ID)
	}
}

type storageDataSetRaceHook struct {
	bucketID            int64
	uploadID            int64
	providerID          string
	dataSetID           string
	triggered           atomic.Bool
	uniqueViolationSeen atomic.Bool
	err                 atomic.Value
}

func (h *storageDataSetRaceHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if !strings.Contains(event.Query, "INSERT INTO") || !strings.Contains(event.Query, "storage_data_sets") {
		return ctx
	}
	if !h.triggered.CompareAndSwap(false, true) {
		return ctx
	}
	providerID, err := types.ParseOnChainID("providerID", h.providerID)
	if err != nil {
		h.err.Store(err)
		return ctx
	}
	dataSetID, err := types.ParseOnChainID("dataSetID", h.dataSetID)
	if err != nil {
		h.err.Store(err)
		return ctx
	}
	dataSet := &model.StorageDataSet{
		BucketID:          h.bucketID,
		ProviderID:        providerID,
		CopyIndex:         0,
		DataSetID:         &dataSetID,
		Status:            model.StorageDataSetStatusReady,
		CreatedByUploadID: &h.uploadID,
		LastUsedUploadID:  &h.uploadID,
	}
	if _, err := event.DB.NewInsert().Model(dataSet).Exec(ctx); err != nil {
		h.err.Store(err)
	}
	return ctx
}

func (h *storageDataSetRaceHook) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if event.Err != nil && strings.Contains(event.Err.Error(), "UNIQUE constraint") {
		h.uniqueViolationSeen.Store(true)
	}
}

func seedRunningUploadTask(t *testing.T, repos *repository.Repositories, objectID int64, versionID string) *model.Task {
	t.Helper()
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: "upload:" + versionID,
		Status:         model.TaskStatusRunning,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	return task
}

func acceptTestStorageUploadForVersion(t *testing.T, repos *repository.Repositories, bucketID int64, version *model.ObjectVersion, pieceCID string) int64 {
	t.Helper()
	upload, err := repos.Uploads.StartObjectUploadAttempt(context.Background(), repository.StartObjectUploadAttemptInput{
		BucketID:        bucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	dataSetID := "1001" + strconv.FormatInt(upload.ID, 10)
	if err := repos.Uploads.RecordUploadResult(context.Background(), repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        strPtr(pieceCID),
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, dataSetID), PieceID: onChainIDPtr(t, "2001"), Role: "primary", RetrievalURL: strPtr("https://provider.example/" + version.VersionID)},
		},
	}); err != nil {
		t.Fatalf("RecordUploadResult: %v", err)
	}
	if _, err := repos.Uploads.AcceptCompleteUploadForContent(context.Background(), repository.AcceptCompleteUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("AcceptCompleteUploadForContent: %v", err)
	}
	return upload.ID
}

func strPtr(v string) *string {
	return &v
}
