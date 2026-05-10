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
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	task := seedRunningUploadTask(t, repos, objectID, version.VersionID)

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceTaskID:    task.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	seedCommittedUploadCopies(t, repos, bucket.ID, upload.ID, "bafk2bzaceprovenance", []storageUploadCopySeed{
		{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodIngress, RetrievalURL: strPtr("https://ingress.example/piece"), IsNewDataSet: true},
		{ProviderID: onChainIDPtr(t, "202"), DataSetID: onChainIDPtr(t, "2002"), PieceID: onChainIDPtr(t, "3001"), TransferMethod: model.StorageCopyTransferMethodPeerPull, RetrievalURL: strPtr("https://peer.example/piece"), IsNewDataSet: true},
	})
	refs := bindReadableUploadForContent(t, repos, upload.ID, bucket.ID, version.Size, version.Checksum)
	finalizeUploadForTest(t, repos, upload.ID)
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

	_ = task
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
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: providerID},
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

func TestStorageUploadRepo_PrimaryStoreProgressTracksAttemptsAndClamps(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "primary-store-progress-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000000PRG01",
		ContentSize:     10,
		Checksum:        "checksum-primary-progress",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	if upload.IngressBytesTransferred != 0 || upload.IngressStoreAttempt != 0 || upload.ProgressUpdatedAt != nil {
		t.Fatalf("new upload progress = bytes:%d attempt:%d updated:%v, want zero values", upload.IngressBytesTransferred, upload.IngressStoreAttempt, upload.ProgressUpdatedAt)
	}

	attemptOne, err := repos.Uploads.BeginIngressStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginIngressStoreProgress first: %v", err)
	}
	if attemptOne.IngressStoreAttempt != 1 || attemptOne.IngressBytesTransferred != 0 || attemptOne.ProgressUpdatedAt == nil {
		t.Fatalf("first attempt progress = bytes:%d attempt:%d updated:%v, want reset attempt 1", attemptOne.IngressBytesTransferred, attemptOne.IngressStoreAttempt, attemptOne.ProgressUpdatedAt)
	}

	if _, err := repos.Uploads.RecordIngressStoreProgress(ctx, repository.RecordIngressStoreProgressInput{
		UploadID:      upload.ID,
		Attempt:       attemptOne.IngressStoreAttempt,
		BytesUploaded: 7,
	}); err != nil {
		t.Fatalf("RecordIngressStoreProgress 7: %v", err)
	}
	if _, err := repos.Uploads.RecordIngressStoreProgress(ctx, repository.RecordIngressStoreProgressInput{
		UploadID:      upload.ID,
		Attempt:       attemptOne.IngressStoreAttempt,
		BytesUploaded: 4,
	}); err != nil {
		t.Fatalf("RecordIngressStoreProgress stale bytes: %v", err)
	}
	if _, err := repos.Uploads.RecordIngressStoreProgress(ctx, repository.RecordIngressStoreProgressInput{
		UploadID:      upload.ID,
		Attempt:       attemptOne.IngressStoreAttempt,
		BytesUploaded: 99,
	}); err != nil {
		t.Fatalf("RecordIngressStoreProgress clamp: %v", err)
	}
	got, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID after progress: got=%v err=%v", got, err)
	}
	if got.IngressBytesTransferred != 10 {
		t.Fatalf("primary bytes after attempt one = %d, want clamped content size", got.IngressBytesTransferred)
	}

	attemptTwo, err := repos.Uploads.BeginIngressStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginIngressStoreProgress second: %v", err)
	}
	if attemptTwo.IngressStoreAttempt != 2 || attemptTwo.IngressBytesTransferred != 0 {
		t.Fatalf("second attempt progress = bytes:%d attempt:%d, want reset attempt 2", attemptTwo.IngressBytesTransferred, attemptTwo.IngressStoreAttempt)
	}
	if _, err := repos.Uploads.RecordIngressStoreProgress(ctx, repository.RecordIngressStoreProgressInput{
		UploadID:      upload.ID,
		Attempt:       attemptOne.IngressStoreAttempt,
		BytesUploaded: 8,
	}); err != nil {
		t.Fatalf("RecordIngressStoreProgress old attempt: %v", err)
	}
	got, err = repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID after old attempt: got=%v err=%v", got, err)
	}
	if got.IngressStoreAttempt != 2 || got.IngressBytesTransferred != 0 {
		t.Fatalf("old attempt progress changed current attempt = bytes:%d attempt:%d, want reset attempt 2", got.IngressBytesTransferred, got.IngressStoreAttempt)
	}
}

func TestStorageUploadRepo_GetUploadProvenanceIncludesCopiesAndFailures(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "upload-provenance-detail-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000PROV001",
		ContentSize:     10,
		Checksum:        "checksum-provenance-detail",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	seedCommittedUploadCopies(t, repos, bucket.ID, upload.ID, "bafk2bzaceprovenancedetail", []storageUploadCopySeed{
		{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodIngress, RetrievalURL: strPtr("https://ingress.example/piece"), IsNewDataSet: true},
		{ProviderID: onChainIDPtr(t, "202"), DataSetID: onChainIDPtr(t, "2002"), PieceID: onChainIDPtr(t, "3001"), TransferMethod: model.StorageCopyTransferMethodPeerPull, RetrievalURL: strPtr("https://peer.example/piece")},
	})
	if err := repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
		UploadID:       upload.ID,
		ProviderID:     onChainIDPtr(t, "303"),
		TransferMethod: string(model.StorageCopyTransferMethodPeerPull),
		Stage:          "peer_pull",
		ErrorMessage:   "provider timed out",
		Explicit:       true,
	}); err != nil {
		t.Fatalf("AppendUploadFailure: %v", err)
	}

	got, err := repos.Uploads.GetUploadProvenance(ctx, upload.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance: %v", err)
	}
	if got == nil || got.Upload.ID != upload.ID {
		t.Fatalf("provenance upload = %#v, want upload %d", got, upload.ID)
	}
	if len(got.Copies) != 2 {
		t.Fatalf("copies len = %d, want 2", len(got.Copies))
	}
	if !got.Copies[0].IsNewDataSet {
		t.Fatalf("primary is_new_data_set = false, want true")
	}
	if got.Copies[1].IsNewDataSet {
		t.Fatalf("secondary is_new_data_set = true, want false")
	}
	if got.Copies[1].DataSetID == nil || got.Copies[1].DataSetID.String() != "2002" {
		t.Fatalf("secondary data_set_id = %#v, want 2002", got.Copies[1].DataSetID)
	}
	if len(got.Failures) != 1 {
		t.Fatalf("failures len = %d, want 1", len(got.Failures))
	}
	if got.Failures[0].ProviderID == nil || got.Failures[0].ProviderID.String() != "303" || got.Failures[0].Stage == nil || *got.Failures[0].Stage != "peer_pull" {
		t.Fatalf("failure = %#v, want provider 303 stage peer_pull", got.Failures[0])
	}
}

func TestStorageUploadRepo_StagedProvenanceInfersNewDataSetAndAppendsFailures(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "staged-provenance-detail-bucket")

	first, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000PROV101",
		ContentSize:     10,
		Checksum:        "checksum-staged-provenance-1",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt first: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: first.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: first.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, first.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings first: %v", err)
	}
	pendingFirstProvenance, err := repos.Uploads.GetUploadProvenance(ctx, first.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance pending first: %v", err)
	}
	if len(pendingFirstProvenance.Copies) != 1 || !pendingFirstProvenance.Copies[0].IsNewDataSet {
		t.Fatalf("pending first copies = %#v, want inferred new data set before commit", pendingFirstProvenance.Copies)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     first.ID,
		CopyIndex:    0,
		PieceCID:     "piece-staged-first",
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://provider.example/first",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted first: %v", err)
	}
	if err := repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
		UploadID:       first.ID,
		CopyIndex:      0,
		Stage:          "ingress_commit",
		ErrorMessage:   "temporary commit failure",
		ProviderID:     nil,
		TransferMethod: "",
		Explicit:       false,
	}); err != nil {
		t.Fatalf("AppendUploadFailure: %v", err)
	}

	firstProvenance, err := repos.Uploads.GetUploadProvenance(ctx, first.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance first: %v", err)
	}
	if len(firstProvenance.Copies) != 1 || !firstProvenance.Copies[0].IsNewDataSet {
		t.Fatalf("first copies = %#v, want inferred new data set", firstProvenance.Copies)
	}
	if len(firstProvenance.Failures) != 1 || firstProvenance.Failures[0].ProviderID == nil || firstProvenance.Failures[0].ProviderID.String() != "101" {
		t.Fatalf("first failures = %#v, want provider inferred from copy", firstProvenance.Failures)
	}

	second, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000PROV102",
		ContentSize:     10,
		Checksum:        "checksum-staged-provenance-2",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt second: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, second.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings second: %v", err)
	}
	pendingSecondProvenance, err := repos.Uploads.GetUploadProvenance(ctx, second.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance pending second: %v", err)
	}
	if len(pendingSecondProvenance.Copies) != 1 || pendingSecondProvenance.Copies[0].IsNewDataSet {
		t.Fatalf("pending second copies = %#v, want reused data set before commit", pendingSecondProvenance.Copies)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     second.ID,
		CopyIndex:    0,
		PieceCID:     "piece-staged-second",
		PieceID:      onChainIDPtr(t, "2002"),
		RetrievalURL: "https://provider.example/second",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted second: %v", err)
	}

	secondProvenance, err := repos.Uploads.GetUploadProvenance(ctx, second.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance second: %v", err)
	}
	if len(secondProvenance.Copies) != 1 || secondProvenance.Copies[0].IsNewDataSet {
		t.Fatalf("second copies = %#v, want reused data set", secondProvenance.Copies)
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
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: types.OnChainID{}},
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

func TestStorageUploadRepo_BindReadableUploadForContentMovesFollowersToReplicating(t *testing.T) {
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
		Status:         model.TaskStatusQueued,
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
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
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
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: leader.Size,
		Checksum:    leader.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
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

func TestStorageUploadRepo_BindReadableUploadForVersionCompletesFollowerTask(t *testing.T) {
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
		Status:         model.TaskStatusQueued,
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
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
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

	refs, err := repos.Uploads.BindReadableUploadForVersion(ctx, repository.BindReadableUploadForVersionInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: follower.Size,
		Checksum:    follower.Checksum,
		VersionID:   follower.VersionID,
	})
	if err != nil {
		t.Fatalf("BindReadableUploadForVersion: %v", err)
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

func TestStorageUploadRepo_FinalizeUploadIfTargetCopiesMetMovesReplicatingToStored(t *testing.T) {
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
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("primary ready: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: secondary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002"), ClientDataSetID: onChainIDPtr(t, "9002")}); err != nil {
		t.Fatalf("secondary ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 0, PieceCID: "bafk2bzacefinalize", PieceID: onChainIDPtr(t, "301"), RetrievalURL: "https://primary.example/piece"}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted primary: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}

	done, refs, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet partial: %v", err)
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

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 1, "peer pull: dataset unavailable"); err != nil {
		t.Fatalf("MarkUploadCopyFailed peer: %v", err)
	}
	replacement, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "303"), CopyIndex: 2, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("replacement binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: replacement.ID, UploadID: upload.ID, DataSetID: onChainID(t, "3003"), ClientDataSetID: onChainIDPtr(t, "9003")}); err != nil {
		t.Fatalf("replacement ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: replacement.ID, CopyIndex: 2, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "303")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings replacement: %v", err)
	}
	upload, err = repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil {
		t.Fatalf("GetByID after replacement: %v", err)
	}
	if upload.RequestedCopies != 2 {
		t.Fatalf("requested copies after replacement append = %d, want 2", upload.RequestedCopies)
	}
	done, refs, err = repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet after replacement append: %v", err)
	}
	if done || len(refs) != 0 {
		t.Fatalf("replacement append finalize = done:%v refs:%v, want no-op until target copies are met", done, refs)
	}

	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{UploadID: upload.ID, CopyIndex: 2, PieceCID: "bafk2bzacefinalize", PieceID: onChainIDPtr(t, "302"), RetrievalURL: "https://replacement.example/piece"}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted replacement: %v", err)
	}
	done, refs, err = repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet complete: %v", err)
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
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 0, "ingress store: provider rejected piece"); err != nil {
		t.Fatalf("MarkUploadCopyFailed: %v", err)
	}
	got, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: got=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusFailed {
		t.Fatalf("upload status = %s, want failed", got.Status)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != "ingress store: provider rejected piece" {
		t.Fatalf("upload error_message = %#v, want ingress failure reason", got.ErrorMessage)
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
	if got.Status != model.StorageUploadStatusIngressReady {
		t.Fatalf("upload status after store retry = %s, want ingress_ready", got.Status)
	}

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 0, "ingress commit: provider rejected piece"); err != nil {
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
	if got.Status != model.StorageUploadStatusReadable {
		t.Fatalf("upload status after commit retry = %s, want readable", got.Status)
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
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
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

func TestStorageUploadRepo_AppendUploadFailureRetriesRacedAttemptIndex(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "storage-upload-failure-race.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
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
	bucket := seedBucket(t, db, "failure-race-bucket")
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:    bucket.ID,
		ContentSize: 1,
		Checksum:    "failure-race",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}

	hook := &storageUploadFailureRaceHook{uploadID: upload.ID, collisionCount: 4}
	db.AddQueryHook(hook)

	if err := repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
		UploadID:       upload.ID,
		CopyIndex:      0,
		ProviderID:     onChainIDPtr(t, "101"),
		TransferMethod: string(model.StorageCopyTransferMethodIngress),
		Stage:          "ingress_store",
		ErrorMessage:   "provider store failed",
	}); err != nil {
		t.Fatalf("AppendUploadFailure: %v", err)
	}
	if hookErr := hook.err.Load(); hookErr != nil {
		t.Fatalf("race hook insert: %v", hookErr)
	}
	if !hook.triggered.Load() {
		t.Fatal("race hook did not run")
	}
	provenance, err := repos.Uploads.GetUploadProvenance(ctx, upload.ID)
	if err != nil {
		t.Fatalf("GetUploadProvenance: %v", err)
	}
	if len(provenance.Failures) != 5 {
		t.Fatalf("failures len = %d, want four raced failures and retried append", len(provenance.Failures))
	}
	for index, failure := range provenance.Failures {
		if failure.AttemptIndex != index {
			t.Fatalf("attempt index at row %d = %d, want %d", index, failure.AttemptIndex, index)
		}
	}
	lastFailure := provenance.Failures[len(provenance.Failures)-1]
	if lastFailure.ErrorMessage == nil || *lastFailure.ErrorMessage != "provider store failed" {
		t.Fatalf("retried failure = %#v, want original append data", lastFailure)
	}
}

type storageUploadFailureRaceHook struct {
	uploadID         int64
	collisionCount   int64
	inserted         atomic.Int64
	triggered        atomic.Bool
	insertInProgress atomic.Bool
	err              atomic.Value
}

func (h *storageUploadFailureRaceHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if !strings.Contains(event.Query, "INSERT INTO") || !strings.Contains(event.Query, "storage_upload_failures") {
		return ctx
	}
	if !h.insertInProgress.CompareAndSwap(false, true) {
		return ctx
	}
	defer h.insertInProgress.Store(false)
	attemptIndex := int(h.inserted.Load())
	if int64(attemptIndex) >= h.collisionCount {
		return ctx
	}
	h.inserted.Add(1)
	h.triggered.Store(true)
	providerID, err := types.ParseOnChainID("providerID", "101")
	if err != nil {
		h.err.Store(err)
		return ctx
	}
	failure := &model.StorageUploadFailure{
		UploadID:       h.uploadID,
		AttemptIndex:   attemptIndex,
		ProviderID:     &providerID,
		TransferMethod: string(model.StorageCopyTransferMethodIngress),
		Stage:          strPtr("raced_failure"),
		ErrorMessage:   strPtr("raced insert"),
	}
	if _, err := event.DB.NewInsert().Model(failure).Exec(ctx); err != nil {
		h.err.Store(err)
	}
	return ctx
}

func (h *storageUploadFailureRaceHook) AfterQuery(context.Context, *bun.QueryEvent) {}

func seedRunningUploadTask(t *testing.T, repos *repository.Repositories, objectID int64, versionID string) *model.Task {
	t.Helper()
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: "upload:" + versionID,
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(context.Background(), model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimReady returned nil")
	}
	return claimed
}

func acceptTestStorageUploadForVersion(t *testing.T, repos *repository.Repositories, bucketID int64, version *model.ObjectVersion, pieceCID string) int64 {
	t.Helper()
	upload, err := repos.Uploads.StartObjectUploadAttempt(context.Background(), repository.StartObjectUploadAttemptInput{
		BucketID:        bucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	dataSetID := "1001" + strconv.FormatInt(upload.ID, 10)
	seedCommittedUploadCopies(t, repos, bucketID, upload.ID, pieceCID, []storageUploadCopySeed{
		{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, dataSetID), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodIngress, RetrievalURL: strPtr("https://provider.example/" + version.VersionID), IsNewDataSet: true},
	})
	bindReadableUploadForContent(t, repos, upload.ID, bucketID, version.Size, version.Checksum)
	finalizeUploadForTest(t, repos, upload.ID)
	return upload.ID
}

func strPtr(v string) *string {
	return &v
}
