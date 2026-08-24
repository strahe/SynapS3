package repository_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
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

func TestStorageUploadRepo_StartObjectUploadAttemptRequiresRequestedCopies(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	tests := []struct {
		name            string
		requestedCopies int
	}{
		{name: "zero", requestedCopies: 0},
		{name: "negative", requestedCopies: -1},
		{name: "too-high", requestedCopies: 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := seedBucket(t, db, "upload-invalid-copies-"+tt.name)

			if _, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
				BucketID:        bucket.ID,
				SourceVersionID: "01J000000000000000DEF" + strings.ToUpper(tt.name),
				ContentSize:     10,
				Checksum:        "checksum-invalid-copies-" + tt.name,
				RequestedCopies: tt.requestedCopies,
			}); err == nil {
				t.Fatal("StartObjectUploadAttempt succeeded, want error")
			}
		})
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
		RequestedCopies: 3,
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
	gotPendingByID, err := repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || gotPendingByID == nil {
		t.Fatalf("GetDataSetBindingByID pending: binding=%v err=%v", gotPendingByID, err)
	}
	if gotPendingByID.ID != binding.ID || gotPendingByID.BucketID != bucket.ID || gotPendingByID.CopyIndex != 0 {
		t.Fatalf("binding by ID = %#v, want id/bucket/copy index", gotPendingByID)
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
	gotReadyByID, err := repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || gotReadyByID == nil {
		t.Fatalf("GetDataSetBindingByID ready: binding=%v err=%v", gotReadyByID, err)
	}
	if gotReadyByID.DataSetID == nil || gotReadyByID.DataSetID.String() != dataSetID.String() {
		t.Fatalf("ready binding by ID = %#v, want data set %s", gotReadyByID, dataSetID.String())
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
		RequestedCopies: 3,
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
		RequestedCopies: 3,
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

func TestStorageUploadRepo_ListBucketStorageHealthSummariesClassifiesRetainedVersionRisk(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-health-risk-bucket")
	otherBucket := seedBucket(t, db, "storage-health-other-bucket")
	checkedAt := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	staleBefore := checkedAt.Add(-time.Hour)

	redundantVersion := newObjectVersion(bucket.ID, "redundant.txt", "01J0000000000000000SHREDU", 4)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, redundantVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent redundant: %v", err)
	}
	redundantUpload := startCopyHealthUpload(t, repos, bucket.ID, redundantVersion.VersionID, redundantVersion.Size, redundantVersion.Checksum, 2)
	degraded := commitStorageHealthCopy(t, repos, bucket.ID, redundantUpload.ID, 0, "101", "2101", "3101", "https://provider.example/degraded")
	readable := commitStorageHealthCopy(t, repos, bucket.ID, redundantUpload.ID, 1, "202", "2202", "3202", "https://provider.example/readable")
	bindStorageHealthVersion(t, repos, bucket.ID, redundantUpload.ID, redundantVersion)

	unavailableVersion := newObjectVersion(bucket.ID, "unavailable.txt", "01J0000000000000000SHUNAV", 5)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, unavailableVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent unavailable: %v", err)
	}
	unavailableUpload := startCopyHealthUpload(t, repos, bucket.ID, unavailableVersion.VersionID, unavailableVersion.Size, unavailableVersion.Checksum, 1)
	unavailable := commitStorageHealthCopy(t, repos, bucket.ID, unavailableUpload.ID, 2, "303", "2303", "3303", "https://provider.example/unavailable")
	bindStorageHealthVersion(t, repos, bucket.ID, unavailableUpload.ID, unavailableVersion)
	if err := repos.Uploads.MarkDataSetUnavailable(ctx, unavailable.ID, "provider offline"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}

	unknownVersion := newObjectVersion(bucket.ID, "unknown.txt", "01J0000000000000000SHUNKN", 6)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, unknownVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent unknown: %v", err)
	}
	unknownUpload := startCopyHealthUpload(t, repos, bucket.ID, unknownVersion.VersionID, unknownVersion.Size, unknownVersion.Checksum, 1)
	commitStorageHealthCopy(t, repos, bucket.ID, unknownUpload.ID, 3, "404", "2404", "3404", "https://provider.example/unknown")
	bindStorageHealthVersion(t, repos, bucket.ID, unknownUpload.ID, unknownVersion)

	oldVersion := newObjectVersion(bucket.ID, "old.txt", "01J0000000000000000SHOLD1", 7)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent old: %v", err)
	}
	oldUpload := startCopyHealthUpload(t, repos, bucket.ID, oldVersion.VersionID, oldVersion.Size, oldVersion.Checksum, 1)
	oldUnavailable := commitStorageHealthCopy(t, repos, bucket.ID, oldUpload.ID, 4, "505", "2505", "3505", "https://provider.example/old")
	bindStorageHealthVersion(t, repos, bucket.ID, oldUpload.ID, oldVersion)
	replacementVersion := newObjectVersion(bucket.ID, "old.txt", "01J0000000000000000SHCURR", 8)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, replacementVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent replacement: %v", err)
	}

	deleteVersion := newObjectVersion(bucket.ID, "deleted.txt", "01J0000000000000000SHDEL1", 9)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, deleteVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent delete data: %v", err)
	}
	deleteUpload := startCopyHealthUpload(t, repos, bucket.ID, deleteVersion.VersionID, deleteVersion.Size, deleteVersion.Checksum, 1)
	deleteUnavailable := commitStorageHealthCopy(t, repos, bucket.ID, deleteUpload.ID, 5, "606", "2606", "3606", "https://provider.example/delete")
	bindStorageHealthVersion(t, repos, bucket.ID, deleteUpload.ID, deleteVersion)
	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "deleted.txt", "01J0000000000000000SHDELM"); err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	unreferencedUpload := startCopyHealthUpload(t, repos, bucket.ID, "01J0000000000000000SHUNRF", 1, "checksum-unreferenced", 1)
	unreferenced := commitStorageHealthCopy(t, repos, bucket.ID, unreferencedUpload.ID, 6, "707", "2707", "3707", "https://provider.example/unreferenced")

	otherVersion := newObjectVersion(otherBucket.ID, "other.txt", "01J0000000000000000SHOTHR", 9)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, otherVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent other: %v", err)
	}
	otherUpload := startCopyHealthUpload(t, repos, otherBucket.ID, otherVersion.VersionID, otherVersion.Size, otherVersion.Checksum, 1)
	otherUnavailable := commitStorageHealthCopy(t, repos, otherBucket.ID, otherUpload.ID, 0, "808", "2808", "3808", "https://provider.example/other")
	bindStorageHealthVersion(t, repos, otherBucket.ID, otherUpload.ID, otherVersion)

	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{
		{LocalDataSetID: degraded.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: 0, ProviderID: onChainID(t, "101"), Status: observability.StatusDegraded, ReasonCodes: []observability.ReasonCode{observability.ReasonChainDataSetUnmanaged}, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: readable.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: 1, ProviderID: onChainID(t, "202"), Status: observability.StatusAvailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: unavailable.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: 0, ProviderID: onChainID(t, "303"), Status: observability.StatusUnavailable, ReasonCodes: []observability.ReasonCode{observability.ReasonChainDataSetMissing}, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: oldUnavailable.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: 0, ProviderID: onChainID(t, "505"), Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: deleteUnavailable.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: 0, ProviderID: onChainID(t, "606"), Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: unreferenced.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: 0, ProviderID: onChainID(t, "707"), Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: otherUnavailable.ID, BucketID: otherBucket.ID, BucketName: otherBucket.Name, CopyIndex: 0, ProviderID: onChainID(t, "808"), Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
	}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	summaries, err := repos.Uploads.ListBucketStorageHealthSummaries(ctx, bucket.ID, staleBefore, 4)
	if err != nil {
		t.Fatalf("ListBucketStorageHealthSummaries: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("summaries len = %d, want one bucket summary: %#v", len(summaries), summaries)
	}
	got := summaries[0]
	if got.BucketID != bucket.ID ||
		got.AbnormalDataSets != 6 ||
		got.AffectedVersionsCapped != 4 ||
		!got.AffectedVersionsExceedsCap ||
		got.AffectedVersionsCap != 4 ||
		!got.LocalStatusNotReady ||
		!got.ObservationUnavailable ||
		!got.ObservationDegraded ||
		!got.ObservationMissing ||
		got.ObservationStale ||
		!hasStorageHealthReason(got.ReasonCodes, observability.ReasonChainDataSetUnmanaged) ||
		!hasStorageHealthReason(got.ReasonCodes, observability.ReasonChainDataSetMissing) ||
		!hasStorageHealthReason(got.ReasonCodes, observability.ReasonLocalStatusNotReady) ||
		hasStorageHealthReason(got.ReasonCodes, observability.ReasonChainDataSetInactive) ||
		got.LastCheckedAt == nil ||
		!got.LastCheckedAt.Equal(checkedAt) {
		t.Fatalf("summary = %#v, want retained version risk from affected abnormal data sets only", got)
	}

	allSummaries, err := repos.Uploads.ListBucketStorageHealthSummaries(ctx, 0, staleBefore, 200)
	if err != nil {
		t.Fatalf("ListBucketStorageHealthSummaries all: %v", err)
	}
	if len(allSummaries) != 2 {
		t.Fatalf("all summaries len = %d, want both buckets with abnormal data sets: %#v", len(allSummaries), allSummaries)
	}
}

func TestStorageUploadRepo_ListBucketStorageHealthSummariesReturnsHealthyBucketFreshness(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-health-healthy-bucket")
	checkedAt := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	staleBefore := checkedAt.Add(-time.Hour)

	version := newObjectVersion(bucket.ID, "healthy.txt", "01J0000000000000000SHGOOD", 4)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 1)
	ready := commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "2101", "3101", "https://provider.example/healthy")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{{
		LocalDataSetID: ready.ID,
		BucketID:       bucket.ID,
		BucketName:     bucket.Name,
		CopyIndex:      0,
		ProviderID:     onChainID(t, "101"),
		Status:         observability.StatusAvailable,
		LastCheckedAt:  checkedAt,
		Evidence:       map[string]any{},
	}}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	summaries, err := repos.Uploads.ListBucketStorageHealthSummaries(ctx, bucket.ID, staleBefore, 200)
	if err != nil {
		t.Fatalf("ListBucketStorageHealthSummaries: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("summaries len = %d, want one healthy bucket summary: %#v", len(summaries), summaries)
	}
	got := summaries[0]
	if got.BucketID != bucket.ID ||
		got.AbnormalDataSets != 0 ||
		got.AffectedVersionsCapped != 0 ||
		got.AffectedVersionsExceedsCap ||
		got.ObservationStale ||
		len(got.ReasonCodes) != 0 ||
		got.LastCheckedAt == nil ||
		!got.LastCheckedAt.Equal(checkedAt) {
		t.Fatalf("summary = %#v, want available bucket observation freshness without data risk", got)
	}
}

func TestStorageUploadRepo_ListBucketStorageHealthSummariesReportsNoAffectedStaleFreshness(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-health-stale-unaffected-bucket")
	checkedAt := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	staleBefore := checkedAt.Add(-time.Hour)
	staleCheckedAt := checkedAt.Add(-2 * time.Hour)

	upload := startCopyHealthUpload(t, repos, bucket.ID, "01J0000000000000000SHORPH", 1, "checksum-orphan", 1)
	stale := commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "2101", "3101", "https://provider.example/stale-unaffected")
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{{
		LocalDataSetID: stale.ID,
		BucketID:       bucket.ID,
		BucketName:     bucket.Name,
		CopyIndex:      0,
		ProviderID:     onChainID(t, "101"),
		Status:         observability.StatusAvailable,
		LastCheckedAt:  staleCheckedAt,
		Evidence:       map[string]any{},
	}}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	summaries, err := repos.Uploads.ListBucketStorageHealthSummaries(ctx, bucket.ID, staleBefore, 200)
	if err != nil {
		t.Fatalf("ListBucketStorageHealthSummaries: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("summaries len = %d, want one bucket summary: %#v", len(summaries), summaries)
	}
	got := summaries[0]
	if got.BucketID != bucket.ID ||
		got.AbnormalDataSets != 1 ||
		got.AffectedVersionsCapped != 0 ||
		got.AffectedVersionsExceedsCap ||
		!got.ObservationStale ||
		got.ObservationMissing ||
		got.ObservationUnavailable ||
		got.ObservationDegraded ||
		got.ObservationUnknown ||
		len(got.ReasonCodes) != 0 ||
		got.LastCheckedAt == nil ||
		!got.LastCheckedAt.Equal(staleCheckedAt) {
		t.Fatalf("summary = %#v, want stale bucket observation freshness without affected retained versions", got)
	}
}

func TestStorageUploadRepo_ListBucketStorageHealthSummariesTreatsStaleOnlyRiskAsUnknown(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-health-stale-bucket")
	checkedAt := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	staleBefore := checkedAt.Add(-time.Hour)

	version := newObjectVersion(bucket.ID, "stale.txt", "01J0000000000000000SHSTAL", 4)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 1)
	stale := commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "2101", "3101", "https://provider.example/stale")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)
	staleCheckedAt := checkedAt.Add(-2 * time.Hour)
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{{
		LocalDataSetID: stale.ID,
		BucketID:       bucket.ID,
		BucketName:     bucket.Name,
		CopyIndex:      0,
		ProviderID:     onChainID(t, "101"),
		Status:         observability.StatusAvailable,
		LastCheckedAt:  staleCheckedAt,
		Evidence:       map[string]any{},
	}}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	summaries, err := repos.Uploads.ListBucketStorageHealthSummaries(ctx, bucket.ID, staleBefore, 200)
	if err != nil {
		t.Fatalf("ListBucketStorageHealthSummaries: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("summaries len = %d, want one bucket summary: %#v", len(summaries), summaries)
	}
	got := summaries[0]
	if got.AffectedVersionsCapped != 1 ||
		got.AffectedVersionsCap != 200 ||
		got.AffectedVersionsExceedsCap ||
		!got.ObservationStale ||
		got.LastCheckedAt == nil ||
		!got.LastCheckedAt.Equal(staleCheckedAt) {
		t.Fatalf("summary = %#v, want stale-only risk classified as unknown", got)
	}
}

func TestStorageUploadRepo_ListBucketStorageHealthAffectedVersionsReportsRetainedVersionRisk(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-health-affected-bucket")
	otherBucket := seedBucket(t, db, "storage-health-affected-other-bucket")
	checkedAt := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	staleBefore := checkedAt.Add(-time.Hour)

	currentVersion := newObjectVersion(bucket.ID, "current.txt", "01J0000000000000000SHD001", 4)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent current: %v", err)
	}
	currentUpload := startCopyHealthUpload(t, repos, bucket.ID, currentVersion.VersionID, currentVersion.Size, currentVersion.Checksum, 3)
	currentRiskA := commitStorageHealthCopy(t, repos, bucket.ID, currentUpload.ID, 0, "101", "2101", "3101", "https://provider.example/current-risk-a")
	currentRiskB := commitStorageHealthCopy(t, repos, bucket.ID, currentUpload.ID, 1, "102", "2102", "3102", "https://provider.example/current-risk-b")
	currentReadable := commitStorageHealthCopy(t, repos, bucket.ID, currentUpload.ID, 2, "103", "2103", "3103", "https://provider.example/current-readable")
	bindStorageHealthVersion(t, repos, bucket.ID, currentUpload.ID, currentVersion)

	oldVersion := newObjectVersion(bucket.ID, "archive/old.txt", "01J0000000000000000SHD002", 5)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent old: %v", err)
	}
	oldUpload := startCopyHealthUpload(t, repos, bucket.ID, oldVersion.VersionID, oldVersion.Size, oldVersion.Checksum, 1)
	oldRisk := commitStorageHealthCopy(t, repos, bucket.ID, oldUpload.ID, 3, "104", "2104", "3104", "https://provider.example/old-risk")
	bindStorageHealthVersion(t, repos, bucket.ID, oldUpload.ID, oldVersion)
	replacementVersion := newObjectVersion(bucket.ID, "archive/old.txt", "01J0000000000000000SHD003", 6)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, replacementVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent replacement: %v", err)
	}

	trashVersion := newObjectVersion(bucket.ID, "trash.txt", "01J0000000000000000SHD004", 7)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, trashVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent trash: %v", err)
	}
	trashUpload := startCopyHealthUpload(t, repos, bucket.ID, trashVersion.VersionID, trashVersion.Size, trashVersion.Checksum, 1)
	trashRisk := commitStorageHealthCopy(t, repos, bucket.ID, trashUpload.ID, 4, "105", "2105", "3105", "https://provider.example/trash-risk")
	bindStorageHealthVersion(t, repos, bucket.ID, trashUpload.ID, trashVersion)
	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "trash.txt", "01J0000000000000000SHD005"); err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	missingPieceVersion := newObjectVersion(bucket.ID, "missing-piece.txt", "01J0000000000000000SHD008", 9)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, missingPieceVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent missing piece: %v", err)
	}
	missingPieceUpload := startCopyHealthUpload(t, repos, bucket.ID, missingPieceVersion.VersionID, missingPieceVersion.Size, missingPieceVersion.Checksum, 2)
	missingPieceRisk := commitStorageHealthCopy(t, repos, bucket.ID, missingPieceUpload.ID, 6, "107", "2107", "3107", "https://provider.example/missing-piece-risk")
	missingPieceReadable := commitStorageHealthCopy(t, repos, bucket.ID, missingPieceUpload.ID, 7, "108", "2108", "3108", "https://provider.example/missing-piece-readable")
	bindStorageHealthVersion(t, repos, bucket.ID, missingPieceUpload.ID, missingPieceVersion)
	mustExec(t, db, `UPDATE storage_uploads SET piece_cid = NULL WHERE id = ?`, missingPieceUpload.ID)

	unreferencedUpload := startCopyHealthUpload(t, repos, bucket.ID, "01J0000000000000000SHD006", 1, "checksum-unreferenced-risk", 1)
	unreferencedRisk := commitStorageHealthCopy(t, repos, bucket.ID, unreferencedUpload.ID, 5, "106", "2106", "3106", "https://provider.example/unreferenced-risk")

	otherVersion := newObjectVersion(otherBucket.ID, "other.txt", "01J0000000000000000SHD007", 8)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, otherVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent other: %v", err)
	}
	otherUpload := startCopyHealthUpload(t, repos, otherBucket.ID, otherVersion.VersionID, otherVersion.Size, otherVersion.Checksum, 1)
	otherRisk := commitStorageHealthCopy(t, repos, otherBucket.ID, otherUpload.ID, 0, "201", "2201", "3201", "https://provider.example/other-risk")
	bindStorageHealthVersion(t, repos, otherBucket.ID, otherUpload.ID, otherVersion)

	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{
		{LocalDataSetID: currentRiskA.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: currentRiskA.CopyIndex, ProviderID: currentRiskA.ProviderID, ChainDataSetID: currentRiskA.DataSetID, Status: observability.StatusDegraded, ReasonCodes: []observability.ReasonCode{observability.ReasonChainDataSetUnmanaged}, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: currentRiskB.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: currentRiskB.CopyIndex, ProviderID: currentRiskB.ProviderID, ChainDataSetID: currentRiskB.DataSetID, Status: observability.StatusUnavailable, ReasonCodes: []observability.ReasonCode{observability.ReasonChainDataSetMissing}, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: currentReadable.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: currentReadable.CopyIndex, ProviderID: currentReadable.ProviderID, ChainDataSetID: currentReadable.DataSetID, Status: observability.StatusAvailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: oldRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: oldRisk.CopyIndex, ProviderID: oldRisk.ProviderID, ChainDataSetID: oldRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: trashRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: trashRisk.CopyIndex, ProviderID: trashRisk.ProviderID, ChainDataSetID: trashRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: missingPieceRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: missingPieceRisk.CopyIndex, ProviderID: missingPieceRisk.ProviderID, ChainDataSetID: missingPieceRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: missingPieceReadable.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: missingPieceReadable.CopyIndex, ProviderID: missingPieceReadable.ProviderID, ChainDataSetID: missingPieceReadable.DataSetID, Status: observability.StatusAvailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: unreferencedRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: unreferencedRisk.CopyIndex, ProviderID: unreferencedRisk.ProviderID, ChainDataSetID: unreferencedRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: otherRisk.ID, BucketID: otherBucket.ID, BucketName: otherBucket.Name, CopyIndex: otherRisk.CopyIndex, ProviderID: otherRisk.ProviderID, ChainDataSetID: otherRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
	}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	page, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:    bucket.ID,
		StaleBefore: staleBefore,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListBucketStorageHealthAffectedVersions: %v", err)
	}
	if page.HasMore || page.NextKeyMarker != "" || page.NextVersionIDMarker != "" || !page.NextCreatedAtMarker.IsZero() {
		t.Fatalf("pagination = %#v, want single complete page", page)
	}
	if got, want := affectedVersionIDs(page.Versions), []string{oldVersion.VersionID, currentVersion.VersionID, missingPieceVersion.VersionID, trashVersion.VersionID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("version ids = %#v, want %#v", got, want)
	}
	current := affectedVersionByID(page.Versions, currentVersion.VersionID)
	if current == nil || !current.Version.IsCurrent || current.ReadableAlternativeCount != 1 || len(current.RiskDataSets) != 2 {
		t.Fatalf("current affected version = %#v, want current with two risk data sets and one alternative", current)
	}
	old := affectedVersionByID(page.Versions, oldVersion.VersionID)
	if old == nil || old.Version.IsCurrent || old.ReadableAlternativeCount != 0 || len(old.RiskDataSets) != 1 {
		t.Fatalf("old affected version = %#v, want retained old version without alternative", old)
	}
	trash := affectedVersionByID(page.Versions, trashVersion.VersionID)
	if trash == nil || trash.Version.IsCurrent || trash.ReadableAlternativeCount != 0 || len(trash.RiskDataSets) != 1 {
		t.Fatalf("trash affected version = %#v, want retained trashed data version without alternative", trash)
	}
	missingPiece := affectedVersionByID(page.Versions, missingPieceVersion.VersionID)
	if missingPiece == nil || missingPiece.ReadableAlternativeCount != 0 || len(missingPiece.RiskDataSets) != 1 {
		t.Fatalf("missing piece affected version = %#v, want no readable alternative without upload piece cid", missingPiece)
	}
}

func TestStorageUploadRepo_ListBucketStorageHealthAffectedVersionsFiltersAndPaginates(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-health-affected-filter-bucket")
	checkedAt := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	staleBefore := checkedAt.Add(-time.Hour)

	baseCreatedAt := time.Date(2026, 5, 23, 12, 30, 0, 123456000, time.UTC)
	firstOldVersion := newObjectVersion(bucket.ID, "docs/a.txt", "01J0000000000000000SHF000", 1)
	firstOldVersion.CreatedAt = baseCreatedAt.Add(-time.Minute)
	firstOldVersion.UpdatedAt = firstOldVersion.CreatedAt
	firstVersion := newObjectVersion(bucket.ID, "docs/a.txt", "01J0000000000000000SHF001", 1)
	firstVersion.CreatedAt = baseCreatedAt
	firstVersion.UpdatedAt = firstVersion.CreatedAt
	secondVersion := newObjectVersion(bucket.ID, "docs/b.txt", "01J0000000000000000SHF002", 1)
	secondVersion.CreatedAt = baseCreatedAt.Add(time.Minute)
	secondVersion.UpdatedAt = secondVersion.CreatedAt
	thirdVersion := newObjectVersion(bucket.ID, "logs/c.txt", "01J0000000000000000SHF003", 1)
	thirdVersion.CreatedAt = baseCreatedAt.Add(2 * time.Minute)
	thirdVersion.UpdatedAt = thirdVersion.CreatedAt
	for _, version := range []*model.ObjectVersion{firstOldVersion, firstVersion, secondVersion, thirdVersion} {
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent %s: %v", version.VersionID, err)
		}
	}
	firstOldUpload := startCopyHealthUpload(t, repos, bucket.ID, firstOldVersion.VersionID, firstOldVersion.Size, firstOldVersion.Checksum, 1)
	firstUpload := startCopyHealthUpload(t, repos, bucket.ID, firstVersion.VersionID, firstVersion.Size, firstVersion.Checksum, 1)
	secondUpload := startCopyHealthUpload(t, repos, bucket.ID, secondVersion.VersionID, secondVersion.Size, secondVersion.Checksum, 1)
	thirdUpload := startCopyHealthUpload(t, repos, bucket.ID, thirdVersion.VersionID, thirdVersion.Size, thirdVersion.Checksum, 1)
	firstOldRisk := commitStorageHealthCopy(t, repos, bucket.ID, firstOldUpload.ID, 3, "304", "3304", "4304", "https://provider.example/a-old")
	firstRisk := commitStorageHealthCopy(t, repos, bucket.ID, firstUpload.ID, 0, "301", "3301", "4301", "https://provider.example/a")
	secondRisk := commitStorageHealthCopy(t, repos, bucket.ID, secondUpload.ID, 1, "302", "3302", "4302", "https://provider.example/b")
	thirdRisk := commitStorageHealthCopy(t, repos, bucket.ID, thirdUpload.ID, 2, "303", "3303", "4303", "https://provider.example/c")
	bindStorageHealthVersion(t, repos, bucket.ID, firstOldUpload.ID, firstOldVersion)
	bindStorageHealthVersion(t, repos, bucket.ID, firstUpload.ID, firstVersion)
	bindStorageHealthVersion(t, repos, bucket.ID, secondUpload.ID, secondVersion)
	bindStorageHealthVersion(t, repos, bucket.ID, thirdUpload.ID, thirdVersion)

	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{
		{LocalDataSetID: firstOldRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: firstOldRisk.CopyIndex, ProviderID: firstOldRisk.ProviderID, ChainDataSetID: firstOldRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: firstRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: firstRisk.CopyIndex, ProviderID: firstRisk.ProviderID, ChainDataSetID: firstRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: secondRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: secondRisk.CopyIndex, ProviderID: secondRisk.ProviderID, ChainDataSetID: secondRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: thirdRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: thirdRisk.CopyIndex, ProviderID: thirdRisk.ProviderID, ChainDataSetID: thirdRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
	}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	prefixPage, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:    bucket.ID,
		Prefix:      "docs/",
		StaleBefore: staleBefore,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListBucketStorageHealthAffectedVersions prefix: %v", err)
	}
	if got, want := affectedVersionIDs(prefixPage.Versions), []string{firstVersion.VersionID, firstOldVersion.VersionID, secondVersion.VersionID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix version ids = %#v, want %#v", got, want)
	}

	keyPage, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:    bucket.ID,
		Key:         "docs/b.txt",
		StaleBefore: staleBefore,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListBucketStorageHealthAffectedVersions key: %v", err)
	}
	if got, want := affectedVersionIDs(keyPage.Versions), []string{secondVersion.VersionID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("key version ids = %#v, want %#v", got, want)
	}

	dataSetPage, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:       bucket.ID,
		LocalDataSetID: secondRisk.ID,
		StaleBefore:    staleBefore,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("ListBucketStorageHealthAffectedVersions dataset: %v", err)
	}
	if got, want := affectedVersionIDs(dataSetPage.Versions), []string{secondVersion.VersionID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dataset version ids = %#v, want %#v", got, want)
	}

	firstPage, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:    bucket.ID,
		StaleBefore: staleBefore,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("ListBucketStorageHealthAffectedVersions first page: %v", err)
	}
	if !firstPage.HasMore || firstPage.NextKeyMarker != "docs/a.txt" || firstPage.NextVersionIDMarker != firstVersion.VersionID || !firstPage.NextCreatedAtMarker.Equal(firstVersion.CreatedAt) {
		t.Fatalf("first page pagination = %#v, want marker for docs/a.txt", firstPage)
	}
	secondPage, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:        bucket.ID,
		KeyMarker:       firstPage.NextKeyMarker,
		VersionIDMarker: firstPage.NextVersionIDMarker,
		CreatedAtMarker: firstPage.NextCreatedAtMarker,
		StaleBefore:     staleBefore,
		Limit:           2,
	})
	if err != nil {
		t.Fatalf("ListBucketStorageHealthAffectedVersions second page: %v", err)
	}
	if got, want := affectedVersionIDs(secondPage.Versions), []string{firstOldVersion.VersionID, secondVersion.VersionID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second page version ids = %#v, want %#v", got, want)
	}

	seedPrefixRisk := func(key, versionID string, copyIndex int, providerID, dataSetID, pieceID string) *model.StorageDataSet {
		t.Helper()
		version := newObjectVersion(bucket.ID, key, versionID, 1)
		version.CreatedAt = baseCreatedAt.Add(time.Duration(copyIndex) * time.Minute)
		version.UpdatedAt = version.CreatedAt
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent %s: %v", versionID, err)
		}
		upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 1)
		risk := commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, copyIndex, providerID, dataSetID, pieceID, "https://provider.example/"+versionID)
		bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)
		return risk
	}
	wildPercentRisk := seedPrefixRisk("wild%/literal.txt", "01J0000000000000000SHF101", 10, "401", "5401", "6401")
	wildSiblingRisk := seedPrefixRisk("wildX/literal.txt", "01J0000000000000000SHF102", 11, "402", "5402", "6402")
	underScoreRisk := seedPrefixRisk("under_/literal.txt", "01J0000000000000000SHF103", 12, "403", "5403", "6403")
	underSiblingRisk := seedPrefixRisk("underX/literal.txt", "01J0000000000000000SHF104", 13, "404", "5404", "6404")
	backslashRisk := seedPrefixRisk(`back\slash/literal.txt`, "01J0000000000000000SHF105", 14, "405", "5405", "6405")
	unicodeRisk := seedPrefixRisk("¿/literal.txt", "01J0000000000000000SHF106", 15, "406", "5406", "6406")
	unicodeSiblingRisk := seedPrefixRisk("?/literal.txt", "01J0000000000000000SHF107", 16, "407", "5407", "6407")
	caseRisk := seedPrefixRisk("case/literal.txt", "01J0000000000000000SHF108", 17, "408", "5408", "6408")
	caseSiblingRisk := seedPrefixRisk("Case/literal.txt", "01J0000000000000000SHF109", 18, "409", "5409", "6409")
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{
		{LocalDataSetID: wildPercentRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: wildPercentRisk.CopyIndex, ProviderID: wildPercentRisk.ProviderID, ChainDataSetID: wildPercentRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: wildSiblingRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: wildSiblingRisk.CopyIndex, ProviderID: wildSiblingRisk.ProviderID, ChainDataSetID: wildSiblingRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: underScoreRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: underScoreRisk.CopyIndex, ProviderID: underScoreRisk.ProviderID, ChainDataSetID: underScoreRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: underSiblingRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: underSiblingRisk.CopyIndex, ProviderID: underSiblingRisk.ProviderID, ChainDataSetID: underSiblingRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: backslashRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: backslashRisk.CopyIndex, ProviderID: backslashRisk.ProviderID, ChainDataSetID: backslashRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: unicodeRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: unicodeRisk.CopyIndex, ProviderID: unicodeRisk.ProviderID, ChainDataSetID: unicodeRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: unicodeSiblingRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: unicodeSiblingRisk.CopyIndex, ProviderID: unicodeSiblingRisk.ProviderID, ChainDataSetID: unicodeSiblingRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: caseRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: caseRisk.CopyIndex, ProviderID: caseRisk.ProviderID, ChainDataSetID: caseRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{LocalDataSetID: caseSiblingRisk.ID, BucketID: bucket.ID, BucketName: bucket.Name, CopyIndex: caseSiblingRisk.CopyIndex, ProviderID: caseSiblingRisk.ProviderID, ChainDataSetID: caseSiblingRisk.DataSetID, Status: observability.StatusUnavailable, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
	}); err != nil {
		t.Fatalf("ReplaceDataSetStates special prefixes: %v", err)
	}
	for _, tc := range []struct {
		name   string
		prefix string
		want   []string
	}{
		{name: "percent", prefix: "wild%/", want: []string{"01J0000000000000000SHF101"}},
		{name: "underscore", prefix: "under_/", want: []string{"01J0000000000000000SHF103"}},
		{name: "backslash", prefix: `back\slash/`, want: []string{"01J0000000000000000SHF105"}},
		{name: "unicode", prefix: "¿/", want: []string{"01J0000000000000000SHF106"}},
		{name: "case-sensitive", prefix: "case/", want: []string{"01J0000000000000000SHF108"}},
	} {
		page, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
			BucketID:    bucket.ID,
			Prefix:      tc.prefix,
			StaleBefore: staleBefore,
			Limit:       10,
		})
		if err != nil {
			t.Fatalf("ListBucketStorageHealthAffectedVersions prefix %s: %v", tc.name, err)
		}
		if got := affectedVersionIDs(page.Versions); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("prefix %s version ids = %#v, want %#v", tc.name, got, tc.want)
		}
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
		RequestedCopies: 3,
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
		RequestedCopies: 3,
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
		RequestedCopies: 3,
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

func TestStorageUploadRepo_MarkUploadCopyCommittedValidatesReadableIdentity(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "committed-copy-validation-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000000BAD02",
		ContentSize:     10,
		Checksum:        "checksum-committed-validation",
		RequestedCopies: 3,
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

	valid := repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-committed-validation",
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://provider.example/piece",
	}
	tests := []struct {
		name  string
		input repository.MarkUploadCopyCommittedInput
	}{
		{name: "missing upload", input: repository.MarkUploadCopyCommittedInput{UploadID: 0, CopyIndex: valid.CopyIndex, PieceCID: valid.PieceCID, PieceID: valid.PieceID, RetrievalURL: valid.RetrievalURL}},
		{name: "negative copy index", input: repository.MarkUploadCopyCommittedInput{UploadID: valid.UploadID, CopyIndex: -1, PieceCID: valid.PieceCID, PieceID: valid.PieceID, RetrievalURL: valid.RetrievalURL}},
		{name: "missing piece cid", input: repository.MarkUploadCopyCommittedInput{UploadID: valid.UploadID, CopyIndex: valid.CopyIndex, PieceID: valid.PieceID, RetrievalURL: valid.RetrievalURL}},
		{name: "missing piece id", input: repository.MarkUploadCopyCommittedInput{UploadID: valid.UploadID, CopyIndex: valid.CopyIndex, PieceCID: valid.PieceCID, RetrievalURL: valid.RetrievalURL}},
		{name: "missing retrieval url", input: repository.MarkUploadCopyCommittedInput{UploadID: valid.UploadID, CopyIndex: valid.CopyIndex, PieceCID: valid.PieceCID, PieceID: valid.PieceID}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := repos.Uploads.MarkUploadCopyCommitted(ctx, tt.input)
			if !errors.Is(err, repository.ErrInvalidInput) || errors.Is(err, repository.ErrNotFound) {
				t.Fatalf("MarkUploadCopyCommitted error = %v, want ErrInvalidInput only", err)
			}
		})
	}
}

func TestStorageUploadRepo_MarkUploadCopyCommittedMissingCopyDoesNotMarkReadable(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "missing-committed-copy-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000000BAD03",
		ContentSize:     10,
		Checksum:        "checksum-missing-committed-copy",
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}

	err = repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-missing-copy",
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://provider.example/missing-copy",
	})
	if !errors.Is(err, repository.ErrNotFound) || errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("MarkUploadCopyCommitted missing copy error = %v, want ErrNotFound only", err)
	}

	got, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: upload=%v err=%v", got, err)
	}
	if got.Status != model.StorageUploadStatusRunning {
		t.Fatalf("upload status = %s, want running", got.Status)
	}
	if got.PieceCID != nil {
		t.Fatalf("upload piece cid = %q, want nil", *got.PieceCID)
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
		RequestedCopies: 3,
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
		RequestedCopies: 3,
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
		RequestedCopies: 3,
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
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
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
		RequestedCopies: 2,
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

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{UploadID: upload.ID, CopyIndex: 1, LastError: "peer pull: dataset unavailable"}); err != nil {
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
	stage := "peer_commit"
	claimedTask := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: "upload:finalize-preserves-claimed-task",
		Status:         model.TaskStatusQueued,
		ScheduledAt:    time.Now().Add(-time.Second),
	}
	if err := repos.Tasks.Create(ctx, claimedTask); err != nil {
		t.Fatalf("Create claimed upload task: %v", err)
	}
	claimedTask, err = repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady upload task: %v", err)
	}
	if claimedTask == nil {
		t.Fatal("ClaimReady upload task returned nil")
	}
	pendingTask := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: "upload:finalize-clears-pending-task",
		Status:         model.TaskStatusQueued,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, pendingTask); err != nil {
		t.Fatalf("Create pending upload task: %v", err)
	}
	maxEvictionRetries := 7
	mustExec(t, db, `CREATE TRIGGER reject_after_upload_eviction
		BEFORE INSERT ON tasks
		WHEN NEW.type = 'evict_cache'
		BEGIN
			SELECT RAISE(FAIL, 'injected eviction task failure');
		END`)
	_, _, err = repos.Uploads.FinalizeUploadIfTargetCopiesMet(
		ctx,
		repository.NewFinalizeUploadInput(
			upload.ID,
			true,
			maxEvictionRetries,
		),
	)
	if err == nil {
		t.Fatal("FinalizeUploadIfTargetCopiesMet with task insert failure returned nil error")
	}
	got, getErr := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if getErr != nil || got == nil {
		t.Fatalf("GetVersionByID after rolled-back finalize: got=%v err=%v", got, getErr)
	}
	if got.State != model.ObjectStateReplicating {
		t.Fatalf("state after rolled-back finalize = %s, want replicating", got.State)
	}
	uploadAfterRollback, getErr := repos.Uploads.GetByID(ctx, upload.ID)
	if getErr != nil || uploadAfterRollback == nil {
		t.Fatalf("GetByID after rolled-back finalize: upload=%v err=%v", uploadAfterRollback, getErr)
	}
	if uploadAfterRollback.Status != model.StorageUploadStatusReadable {
		t.Fatalf("upload status after rolled-back finalize = %s, want readable", uploadAfterRollback.Status)
	}
	pendingAfterRollback, getErr := repos.Tasks.GetByID(ctx, pendingTask.ID)
	if getErr != nil || pendingAfterRollback == nil {
		t.Fatalf("GetByID pending task after rollback: task=%v err=%v", pendingAfterRollback, getErr)
	}
	if pendingAfterRollback.Status != model.TaskStatusQueued {
		t.Fatalf("pending task status after rollback = %s, want queued", pendingAfterRollback.Status)
	}
	mustExec(t, db, `DROP TRIGGER reject_after_upload_eviction`)

	done, refs, err = repos.Uploads.FinalizeUploadIfTargetCopiesMet(
		ctx,
		repository.NewFinalizeUploadInput(
			upload.ID,
			true,
			maxEvictionRetries,
		),
	)
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
	preservedTask, err := repos.Tasks.GetByID(ctx, claimedTask.ID)
	if err != nil {
		t.Fatalf("GetByID claimed task: %v", err)
	}
	if preservedTask.Status != model.TaskStatusRunning {
		t.Fatalf("claimed upload task status = %s, want running", preservedTask.Status)
	}
	if err := repos.Tasks.Complete(ctx, claimedTask); err != nil {
		t.Fatalf("Complete claimed upload task after finalization: %v", err)
	}
	finalizedTask, err := repos.Tasks.GetByID(ctx, pendingTask.ID)
	if err != nil {
		t.Fatalf("GetByID pending task: %v", err)
	}
	if finalizedTask.Status != model.TaskStatusCompleted {
		t.Fatalf("pending upload task status = %s, want completed", finalizedTask.Status)
	}
	evictionTasks, total, err := repos.Tasks.List(
		ctx,
		string(model.TaskTypeEvictCache),
		cacheeviction.StageAfterUpload,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List after-upload eviction tasks: %v", err)
	}
	if total != 1 || len(evictionTasks) != 1 {
		t.Fatalf("after-upload eviction tasks total=%d tasks=%#v, want one", total, evictionTasks)
	}
	if evictionTasks[0].RefVersionID != version.VersionID ||
		evictionTasks[0].MaxRetries != maxEvictionRetries {
		t.Fatalf(
			"after-upload eviction task = version:%s retries:%d, want version:%s retries:%d",
			evictionTasks[0].RefVersionID,
			evictionTasks[0].MaxRetries,
			version.VersionID,
			maxEvictionRetries,
		)
	}
}

func TestStorageUploadRepo_MinimumDurabilityStoresBeforeTargetAndKeepsRepairWork(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "minimum-durability-finalize-bucket")
	minimum := 2
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:                    bucket.Name,
		SetMinimumDurableCopies: true,
		MinimumDurableCopies:    &minimum,
	}); err != nil {
		t.Fatalf("UpdateCopyPolicy: %v", err)
	}

	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000MIN02", 10)
	version.Checksum = "minimum-durability-checksum"
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 3)
	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "1001", "2001", "https://one.example/piece")
	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 1, "202", "2002", "2002", "https://two.example/piece")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)

	stage := "peer_pull"
	repairTask := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: "upload:minimum-durability-third-copy",
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 2},
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, repairTask); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}

	done, refs, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet minimum: %v", err)
	}
	if done || len(refs) != 1 || refs[0].VersionID != version.VersionID {
		t.Fatalf("minimum finalize = done:%v refs:%#v, want stored without upload completion", done, refs)
	}
	gotVersion, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || gotVersion == nil || gotVersion.State != model.ObjectStateStored {
		t.Fatalf("version after minimum = %#v err=%v, want stored", gotVersion, err)
	}
	gotUpload, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || gotUpload == nil || gotUpload.Status != model.StorageUploadStatusReadable {
		t.Fatalf("upload after minimum = %#v err=%v, want readable", gotUpload, err)
	}
	gotTask, err := repos.Tasks.GetByID(ctx, repairTask.ID)
	if err != nil || gotTask == nil || gotTask.Status != model.TaskStatusQueued {
		t.Fatalf("repair task after minimum = %#v err=%v, want queued", gotTask, err)
	}

	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 2, "303", "3003", "2003", "https://three.example/piece")
	done, refs, err = repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet target: %v", err)
	}
	if !done || len(refs) != 0 {
		t.Fatalf("target finalize = done:%v refs:%#v, want complete without another state transition", done, refs)
	}
	gotUpload, err = repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || gotUpload == nil || gotUpload.Status != model.StorageUploadStatusComplete {
		t.Fatalf("upload after target = %#v err=%v, want complete", gotUpload, err)
	}
	gotTask, err = repos.Tasks.GetByID(ctx, repairTask.ID)
	if err != nil || gotTask == nil || gotTask.Status != model.TaskStatusCompleted {
		t.Fatalf("repair task after target = %#v err=%v, want completed", gotTask, err)
	}
}

func TestStorageUploadRepo_MinimumDurabilityEnqueuesAfterUploadBeforeTarget(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "minimum-durability-after-upload-bucket")
	minimum := 2
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:                    bucket.Name,
		SetMinimumDurableCopies: true,
		MinimumDurableCopies:    &minimum,
	}); err != nil {
		t.Fatalf("UpdateCopyPolicy: %v", err)
	}

	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000MINAU", 10)
	version.Checksum = "minimum-durability-after-upload"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 3)
	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "1001", "2001", "https://one.example/piece")
	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 1, "202", "2002", "2002", "https://two.example/piece")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)

	maxEvictionRetries := 7
	done, refs, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(
		ctx,
		repository.NewFinalizeUploadInput(upload.ID, true, maxEvictionRetries),
	)
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet minimum: %v", err)
	}
	if done || len(refs) != 1 || refs[0].VersionID != version.VersionID {
		t.Fatalf("minimum finalize = done:%v refs:%#v, want stored without upload completion", done, refs)
	}
	gotUpload, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || gotUpload == nil || gotUpload.Status != model.StorageUploadStatusReadable {
		t.Fatalf("upload after minimum = %#v err=%v, want readable", gotUpload, err)
	}
	evictionTasks, total, err := repos.Tasks.List(
		ctx,
		string(model.TaskTypeEvictCache),
		cacheeviction.StageAfterUpload,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List after-upload eviction tasks: %v", err)
	}
	if total != 1 || len(evictionTasks) != 1 {
		t.Fatalf("after-upload eviction tasks after minimum total=%d tasks=%#v, want one", total, evictionTasks)
	}
	if evictionTasks[0].RefVersionID != version.VersionID || evictionTasks[0].MaxRetries != maxEvictionRetries {
		t.Fatalf(
			"after-upload eviction task = version:%s retries:%d, want version:%s retries:%d",
			evictionTasks[0].RefVersionID,
			evictionTasks[0].MaxRetries,
			version.VersionID,
			maxEvictionRetries,
		)
	}

	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 2, "303", "3003", "2003", "https://three.example/piece")
	done, refs, err = repos.Uploads.FinalizeUploadIfTargetCopiesMet(
		ctx,
		repository.NewFinalizeUploadInput(upload.ID, true, maxEvictionRetries),
	)
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet target: %v", err)
	}
	if !done || len(refs) != 0 {
		t.Fatalf("target finalize = done:%v refs:%#v, want complete without another state transition", done, refs)
	}
	evictionTasks, total, err = repos.Tasks.List(
		ctx,
		string(model.TaskTypeEvictCache),
		cacheeviction.StageAfterUpload,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List after-upload eviction tasks after target: %v", err)
	}
	if total != 1 || len(evictionTasks) != 1 || evictionTasks[0].RefVersionID != version.VersionID {
		t.Fatalf("after-upload eviction tasks after target total=%d tasks=%#v, want the same task", total, evictionTasks)
	}
}

func TestStorageUploadRepo_ListIncompleteReadableUploadsIncludesCommittedUnavailableSlot(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "minimum-durability-recovery-bucket")
	minimum := 1
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:                    bucket.Name,
		SetMinimumDurableCopies: true,
		MinimumDurableCopies:    &minimum,
	}); err != nil {
		t.Fatalf("UpdateCopyPolicy: %v", err)
	}

	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000MIN03", 10)
	version.Checksum = "minimum-durability-recovery-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 2)
	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "1001", "2001", "https://one.example/piece")
	unavailable := commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 1, "202", "2002", "2002", "https://two.example/piece")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)
	if err := repos.Uploads.MarkDataSetUnavailable(ctx, unavailable.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	if complete, _, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID}); err != nil || complete {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet = complete:%t err:%v, want durable before target", complete, err)
	}

	items, err := repos.Uploads.ListIncompleteReadableUploads(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ListIncompleteReadableUploads: %v", err)
	}
	if len(items) != 1 || items[0].Upload.ID != upload.ID || items[0].Version.VersionID != version.VersionID {
		t.Fatalf("incomplete readable uploads = %#v, want upload %d version %s", items, upload.ID, version.VersionID)
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
		RequestedCopies: 3,
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

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{UploadID: upload.ID, CopyIndex: 0, LastError: "ingress store: provider rejected piece"}); err != nil {
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

	if err := repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{UploadID: upload.ID, CopyIndex: 0, LastError: "ingress commit: provider rejected piece"}); err != nil {
		t.Fatalf("MarkUploadCopyFailed after store retry: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-after-store-retry",
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

func TestStorageUploadRepo_ResetRejectedUploadCopyCommitUsesTransactionCAS(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "rejected-commit-reset-bucket")

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J00000000000000000010031",
		ContentSize:     10,
		Checksum:        "checksum-rejected-commit-reset",
		RequestedCopies: 1,
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
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-rejected-commit-reset",
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID:            upload.ID,
		CopyIndex:           0,
		CommitExtraDataHex:  "01",
		CommitTransactionID: "0xrejected",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitting: %v", err)
	}

	err = repos.Uploads.ResetRejectedUploadCopyCommit(ctx, repository.ResetRejectedUploadCopyCommitInput{
		UploadID:            upload.ID,
		CopyIndex:           0,
		CommitTransactionID: "0xnewer",
		LastError:           "rejected",
	})
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale reset error = %v, want ErrConflict", err)
	}
	copyRow, err := repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow.Status != model.StorageUploadCopyStatusCommitting || copyRow.CommitTransactionID == nil || *copyRow.CommitTransactionID != "0xrejected" {
		t.Fatalf("copy after stale reset = %#v err=%v, want original submitted commit", copyRow, err)
	}

	if err := repos.Uploads.ResetRejectedUploadCopyCommit(ctx, repository.ResetRejectedUploadCopyCommitInput{
		UploadID:            upload.ID,
		CopyIndex:           0,
		CommitTransactionID: "0xrejected",
		LastError:           "commit transaction rejected",
	}); err != nil {
		t.Fatalf("ResetRejectedUploadCopyCommit: %v", err)
	}
	copyRow, err = repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || copyRow.Status != model.StorageUploadCopyStatusPieceReady || copyRow.CommitTransactionID != nil || copyRow.CommitExtraDataHex != nil {
		t.Fatalf("copy after rejected reset = %#v err=%v, want piece_ready without commit data", copyRow, err)
	}
	if copyRow.LastError == nil || *copyRow.LastError != "commit transaction rejected" {
		t.Fatalf("copy last error = %#v, want rejected reason", copyRow.LastError)
	}
}

func TestStorageUploadRepo_PeerPieceReadyPersistsPieceCIDWithoutIngressTransition(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "peer-piece-ready-bucket")
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000PEERPIECE",
		ContentSize:     10,
		Checksum:        "checksum-peer-piece-ready",
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "202"),
		CopyIndex:         1,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        1,
		TransferMethod:   model.StorageCopyTransferMethodPeerPull,
		ProviderID:       onChainID(t, "202"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	pieceCID := "bafkreifm6jgq3qxvcvul2woy6t3vht5m6wkh5jgsslnnq3qjm2f2x7x2hu"
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    1,
		PieceCID:     pieceCID,
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	got, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: upload=%#v err=%v", got, err)
	}
	if got.PieceCID == nil || *got.PieceCID != pieceCID {
		t.Fatalf("piece CID = %#v, want %q", got.PieceCID, pieceCID)
	}
	if got.Status != model.StorageUploadStatusRunning {
		t.Fatalf("upload status = %s, want running for peer transfer", got.Status)
	}
	if got.IngressBytesTransferred != 0 {
		t.Fatalf("ingress bytes = %d, want unchanged", got.IngressBytesTransferred)
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
		RequestedCopies: 3,
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
	if err := repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{UploadID: upload.ID, CopyIndex: 1, LastError: "secondary pull: stale failure"}); err != nil {
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
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "failure-race-bucket")
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		ContentSize:     1,
		Checksum:        "failure-race",
		RequestedCopies: 3,
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

func TestStorageUploadRepo_UnavailableDataSetRecoveryUsesIncompleteCopies(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "unavailable-recovery-bucket")

	newUpload := func(versionID string) *model.StorageUpload {
		version := newObjectVersion(bucket.ID, versionID+".txt", versionID, 10)
		version.Checksum = "shared-recovery-content"
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%s): %v", versionID, err)
		}
		upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
			BucketID: bucket.ID, SourceVersionID: versionID, ContentSize: 10, Checksum: "shared-recovery-content", RequestedCopies: 1,
		})
		if err != nil {
			t.Fatalf("StartObjectUploadAttempt(%s): %v", versionID, err)
		}
		return upload
	}
	firstUpload := newUpload("01J000000000000000REPAIR01")
	secondUpload := newUpload("01J000000000000000REPAIR02")
	orphanUpload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: "01J000000000000000ORPHAN01", ContentSize: 10, Checksum: "shared-recovery-content", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt(orphan): %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: firstUpload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: firstUpload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	for _, upload := range []*model.StorageUpload{firstUpload, secondUpload, orphanUpload} {
		if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "101"),
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings(%d): %v", upload.ID, err)
		}
	}
	if err := repos.Uploads.MarkDataSetUnavailable(ctx, binding.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	mustExec(t, db, `UPDATE storage_uploads SET status = ? WHERE id = ?`, model.StorageUploadStatusComplete, firstUpload.ID)
	mustExec(t, db, `UPDATE storage_uploads SET status = ? WHERE id = ?`, model.StorageUploadStatusFailed, secondUpload.ID)

	bindings, err := repos.Uploads.ListUnavailableDataSetsWithIncompleteCopies(ctx, 0, 10)
	if err != nil || len(bindings) != 1 || bindings[0].ID != binding.ID {
		t.Fatalf("ListUnavailableDataSetsWithIncompleteCopies = %#v err=%v", bindings, err)
	}
	firstCopy, err := repos.Uploads.NextIncompleteCopyForDataSet(ctx, binding.ID)
	if err != nil || firstCopy == nil || firstCopy.UploadID != firstUpload.ID {
		t.Fatalf("first incomplete copy = %#v err=%v", firstCopy, err)
	}
	if err := repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{UploadID: firstUpload.ID, CopyIndex: firstCopy.CopyIndex, LastError: "skip completed repair item"}); err != nil {
		t.Fatalf("MarkUploadCopyFailed: %v", err)
	}
	secondCopy, err := repos.Uploads.NextIncompleteCopyForDataSet(ctx, binding.ID)
	if err != nil || secondCopy == nil || secondCopy.UploadID != secondUpload.ID {
		t.Fatalf("second incomplete copy = %#v err=%v", secondCopy, err)
	}
	if err := repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{UploadID: secondUpload.ID, CopyIndex: secondCopy.CopyIndex, LastError: "skip second repair item"}); err != nil {
		t.Fatalf("MarkUploadCopyFailed second: %v", err)
	}
	next, err := repos.Uploads.NextIncompleteCopyForDataSet(ctx, binding.ID)
	if err != nil || next != nil {
		t.Fatalf("next incomplete copy after live references = %#v err=%v, want orphan ignored", next, err)
	}
	bindings, err = repos.Uploads.ListUnavailableDataSetsWithIncompleteCopies(ctx, 0, 10)
	if err != nil || len(bindings) != 0 {
		t.Fatalf("unavailable data sets after live references = %#v err=%v, want orphan ignored", bindings, err)
	}
	recovered, err := repos.Uploads.RecoverDataSet(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: secondUpload.ID, DataSetID: onChainID(t, "1001")})
	if err != nil || !recovered {
		t.Fatalf("RecoverDataSet unavailable: recovered=%t err=%v", recovered, err)
	}
	if err := repos.Uploads.MarkDataSetDraining(ctx, binding.ID, "service ended"); err != nil {
		t.Fatalf("MarkDataSetDraining: %v", err)
	}
	recovered, err = repos.Uploads.RecoverDataSet(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: secondUpload.ID, DataSetID: onChainID(t, "1001")})
	if err != nil || recovered {
		t.Fatalf("RecoverDataSet draining: recovered=%t err=%v", recovered, err)
	}
}

func TestStorageUploadRepo_DataSetOutageTransitionsRejectStaleState(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "outage-transition-cas-bucket")
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: "01J000000000000000CASSTATE1", ContentSize: 10, Checksum: "cas-state", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.MarkDataSetFailed(ctx, binding.ID, "stale creation failure"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkDataSetFailed after ready error = %v, want ErrConflict", err)
	}
	if err := repos.Uploads.MarkDataSetDraining(ctx, binding.ID, "service ended"); err != nil {
		t.Fatalf("MarkDataSetDraining: %v", err)
	}
	if err := repos.Uploads.MarkDataSetUnavailable(ctx, binding.ID, "stale outage"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkDataSetUnavailable after draining error = %v, want ErrConflict", err)
	}
	if err := repos.Uploads.MarkDataSetDraining(ctx, binding.ID+1000, "missing"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("MarkDataSetDraining missing error = %v, want ErrNotFound", err)
	}
}

func TestStorageUploadRepo_ReassignIngressUsesOnlyPendingReadyCopy(t *testing.T) {
	for _, tc := range []struct {
		name            string
		candidateStatus model.StorageUploadCopyStatus
		sourceSubmitted bool
		rejectPromotion bool
		wantReassigned  bool
		wantConflict    bool
		wantIngress     int
	}{
		{name: "pending", candidateStatus: model.StorageUploadCopyStatusPending, wantReassigned: true, wantIngress: 1},
		{name: "piece ready", candidateStatus: model.StorageUploadCopyStatusPieceReady, wantIngress: 0},
		{name: "submitted source", candidateStatus: model.StorageUploadCopyStatusPending, sourceSubmitted: true, wantConflict: true, wantIngress: 0},
		{name: "promotion conflict", candidateStatus: model.StorageUploadCopyStatusPending, rejectPromotion: true, wantConflict: true, wantIngress: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			bucket := seedBucket(t, db, "reassign-ingress-"+strings.ReplaceAll(tc.name, " ", "-"))
			upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
				BucketID: bucket.ID, SourceVersionID: "01J000000000000000INGRESS1", ContentSize: 10, Checksum: "reassign-ingress", RequestedCopies: 2,
			})
			if err != nil {
				t.Fatalf("StartObjectUploadAttempt: %v", err)
			}
			bindings := make([]*model.StorageDataSet, 0, 2)
			for copyIndex, ids := range [][2]string{{"101", "1001"}, {"202", "2002"}} {
				binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
					BucketID: bucket.ID, ProviderID: onChainID(t, ids[0]), CopyIndex: copyIndex, CreatedByUploadID: upload.ID,
				})
				if err != nil {
					t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
				}
				if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: upload.ID, DataSetID: onChainID(t, ids[1])}); err != nil {
					t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
				}
				bindings = append(bindings, binding)
			}
			if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
				{StorageDataSetID: bindings[0].ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
				{StorageDataSetID: bindings[1].ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
			}); err != nil {
				t.Fatalf("CreateUploadCopiesForBindings: %v", err)
			}
			if err := repos.Uploads.MarkDataSetUnavailable(ctx, bindings[0].ID, "temporary outage"); err != nil {
				t.Fatalf("MarkDataSetUnavailable: %v", err)
			}
			if tc.candidateStatus != model.StorageUploadCopyStatusPending {
				mustExec(t, db, `UPDATE storage_upload_copies SET status = ? WHERE upload_id = ? AND copy_index = 1`, tc.candidateStatus, upload.ID)
			}
			if tc.sourceSubmitted {
				mustExec(t, db, `UPDATE storage_upload_copies SET status = ?, commit_transaction_id = ? WHERE upload_id = ? AND copy_index = 0`, model.StorageUploadCopyStatusCommitting, "0xsubmitted", upload.ID)
			}
			if tc.rejectPromotion {
				mustExec(t, db, `CREATE TRIGGER reject_ingress_promotion
					BEFORE UPDATE OF transfer_method ON storage_upload_copies
					WHEN OLD.copy_index = 1 AND NEW.transfer_method = 'ingress'
					BEGIN
						SELECT RAISE(IGNORE);
					END`)
			}
			reassigned, err := repos.Uploads.ReassignIngressCopy(ctx, upload.ID, 0)
			if tc.wantConflict {
				if !errors.Is(err, repository.ErrConflict) {
					t.Fatalf("ReassignIngressCopy error = %v, want ErrConflict", err)
				}
			} else if err != nil {
				t.Fatalf("ReassignIngressCopy: %v", err)
			}
			if (reassigned != nil) != tc.wantReassigned {
				t.Fatalf("reassigned = %#v, want %t", reassigned, tc.wantReassigned)
			}
			if reassigned != nil && (reassigned.CopyIndex != 1 || reassigned.TransferMethod != model.StorageCopyTransferMethodIngress) {
				t.Fatalf("reassigned copy = %#v", reassigned)
			}
			copies, err := repos.Uploads.ListCopies(ctx, upload.ID)
			if err != nil {
				t.Fatalf("ListCopies: %v", err)
			}
			ingressCount := 0
			for i := range copies {
				copyRow := &copies[i]
				if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress {
					ingressCount++
					if copyRow.CopyIndex != tc.wantIngress {
						t.Fatalf("ingress copy index = %d, want %d", copyRow.CopyIndex, tc.wantIngress)
					}
				}
			}
			if ingressCount != 1 {
				t.Fatalf("ingress copy count = %d, want 1: %#v", ingressCount, copies)
			}
		})
	}
}

func TestStorageUploadRepo_DiscardFailedCandidateIsAtomicWithSharedReferences(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "failed-candidate-shared-refs")
	first, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: "01J000000000000000FAILED01", ContentSize: 10, Checksum: "failed-candidate", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt first: %v", err)
	}
	second, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: "01J000000000000000FAILED02", ContentSize: 10, Checksum: "failed-candidate", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt second: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "303"), CopyIndex: 0, CreatedByUploadID: first.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	for _, upload := range []*model.StorageUpload{first, second} {
		if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "303"),
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings(%d): %v", upload.ID, err)
		}
	}
	if err := repos.Uploads.MarkDataSetFailed(ctx, binding.ID, "creation rejected"); err != nil {
		t.Fatalf("MarkDataSetFailed: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{UploadID: first.ID, CopyIndex: 0, LastError: "creation rejected"}); err != nil {
		t.Fatalf("MarkUploadCopyFailed: %v", err)
	}
	discarded, err := repos.Uploads.DiscardFailedDataSetCandidate(ctx, first.ID, 0, binding.ID)
	if err != nil || discarded {
		t.Fatalf("DiscardFailedDataSetCandidate: discarded=%t err=%v", discarded, err)
	}
	copyRow, err := repos.Uploads.GetUploadCopy(ctx, first.ID, 0)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("first copy after guarded discard = %#v err=%v", copyRow, err)
	}
	retained, err := repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || retained == nil {
		t.Fatalf("binding after guarded discard = %#v err=%v", retained, err)
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

func startCopyHealthUpload(t *testing.T, repos *repository.Repositories, bucketID int64, versionID string, size int64, checksum string, requestedCopies int) *model.StorageUpload {
	t.Helper()
	upload, err := repos.Uploads.StartObjectUploadAttempt(context.Background(), repository.StartObjectUploadAttemptInput{
		BucketID:        bucketID,
		SourceVersionID: versionID,
		ContentSize:     size,
		Checksum:        checksum,
		RequestedCopies: requestedCopies,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	return upload
}

func ensureCopyHealthBinding(t *testing.T, repos *repository.Repositories, bucketID int64, uploadID int64, copyIndex int, providerID string) *model.StorageDataSet {
	t.Helper()
	binding, err := repos.Uploads.EnsureDataSetBinding(context.Background(), repository.EnsureDataSetBindingInput{
		BucketID:          bucketID,
		ProviderID:        onChainID(t, providerID),
		CopyIndex:         copyIndex,
		CreatedByUploadID: uploadID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	return binding
}

func commitStorageHealthCopy(t *testing.T, repos *repository.Repositories, bucketID int64, uploadID int64, copyIndex int, providerID, dataSetID, pieceID, retrievalURL string) *model.StorageDataSet {
	t.Helper()
	binding := ensureCopyHealthBinding(t, repos, bucketID, uploadID, copyIndex, providerID)
	if err := repos.Uploads.MarkDataSetReady(context.Background(), repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  uploadID,
		DataSetID: onChainID(t, dataSetID),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(context.Background(), uploadID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        copyIndex,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, providerID),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(context.Background(), repository.MarkUploadCopyCommittedInput{
		UploadID:     uploadID,
		CopyIndex:    copyIndex,
		PieceCID:     "bafk2bzacestorhealth",
		PieceID:      onChainIDPtr(t, pieceID),
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	got, err := repos.Uploads.GetDataSetBindingByID(context.Background(), binding.ID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID: %v", err)
	}
	return got
}

func bindStorageHealthVersion(t *testing.T, repos *repository.Repositories, bucketID int64, uploadID int64, version *model.ObjectVersion) {
	t.Helper()
	if err := repos.Objects.UpdateVersionState(context.Background(), version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateVersionState uploading: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForVersion(context.Background(), repository.BindReadableUploadForVersionInput{
		UploadID:    uploadID,
		BucketID:    bucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
		VersionID:   version.VersionID,
	}); err != nil {
		t.Fatalf("BindReadableUploadForVersion: %v", err)
	}
}

func hasStorageHealthReason(reasons []observability.ReasonCode, want observability.ReasonCode) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}

func affectedVersionIDs(versions []repository.BucketStorageHealthAffectedVersion) []string {
	out := make([]string, 0, len(versions))
	for _, version := range versions {
		out = append(out, version.Version.VersionID)
	}
	return out
}

func affectedVersionByID(versions []repository.BucketStorageHealthAffectedVersion, versionID string) *repository.BucketStorageHealthAffectedVersion {
	for i := range versions {
		if versions[i].Version.VersionID == versionID {
			return &versions[i]
		}
	}
	return nil
}

func strPtr(v string) *string {
	return &v
}

// A provider replacement gives one replica slot two data set generations that
// can both hold a committed copy. Counting them as two replicas would release
// cache while only one provider actually holds the data.
func TestStorageUploadRepo_DurabilityCountsDistinctSlotsAcrossGenerations(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "generation-durability-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000GEN01", 10)
	version.Checksum = "generation-durability-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 3)
	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "1001", "2001", "https://one.example/piece")
	commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 1, "202", "2002", "2002", "https://two.example/piece")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)

	source, err := repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || source == nil {
		t.Fatalf("GetDataSetBindingByCopyIndex = %#v err=%v", source, err)
	}
	// Stand in for an activated replacement until the replacement repository
	// owns this transition.
	mustExec(t, db, `UPDATE storage_data_sets SET is_current = FALSE, status = ? WHERE id = ?`,
		model.StorageDataSetStatusDraining, source.ID)
	mustExec(t, db, `INSERT INTO storage_data_sets (bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_at, updated_at)
		VALUES (?, '909', 0, 2, TRUE, '9009', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		bucket.ID, model.StorageDataSetStatusReady)
	target, err := repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || target == nil || target.ID == source.ID || target.Generation != 2 {
		t.Fatalf("current binding after activation = %#v err=%v, want the second generation", target, err)
	}
	mustExec(t, db, `INSERT INTO storage_upload_copies (upload_id, copy_index, provider_id, piece_id, transfer_method, status, retrieval_url, storage_data_set_id, created_at, updated_at)
		VALUES (?, 0, '909', '9001', ?, ?, 'https://three.example/piece', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		upload.ID, model.StorageCopyTransferMethodPeerPull, model.StorageUploadCopyStatusCommitted, target.ID)

	// All three physical copies stay retrievable, including the retiring one.
	copies, err := repos.Uploads.ListReadableCommittedCopies(ctx, upload.ID)
	if err != nil {
		t.Fatalf("ListReadableCommittedCopies: %v", err)
	}
	if len(copies) != 3 {
		t.Fatalf("readable copies = %d, want 3 physical copies", len(copies))
	}

	// Durability still sees two slots, so the third requested copy is owed.
	done, refs, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet: %v", err)
	}
	if done {
		t.Fatalf("two generations of one slot satisfied a 3-copy target, want them counted as one replica (refs=%#v)", refs)
	}
	gotVersion, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || gotVersion == nil || gotVersion.State == model.ObjectStateStored {
		t.Fatalf("version after activation = %#v err=%v, want it to stay short of the durability threshold", gotVersion, err)
	}
}

// A staged task records the copy it stored to. If the slot's current generation
// changes before the task runs, the write must still land on the generation
// that actually holds the piece rather than on its replacement.
func TestStorageUploadRepo_CopyWritesFollowTheRecordedCopyNotTheCurrentGeneration(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "generation-addressing-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000ADR01", 10)
	version.Checksum = "generation-addressing-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 1)
	source := ensureCopyHealthBinding(t, repos, bucket.ID, upload.ID, 0, "101")
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        source.ID,
		UploadID:  upload.ID,
		DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: source.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	sourceCopy, err := repos.Uploads.GetUploadCopyForDataSet(ctx, upload.ID, source.ID)
	if err != nil || sourceCopy == nil {
		t.Fatalf("GetUploadCopyForDataSet source = %#v err=%v", sourceCopy, err)
	}

	// Stand in for an activated replacement that also staged its own copy.
	mustExec(t, db, `UPDATE storage_data_sets SET is_current = FALSE, status = ? WHERE id = ?`,
		model.StorageDataSetStatusDraining, source.ID)
	mustExec(t, db, `INSERT INTO storage_data_sets (bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_at, updated_at)
		VALUES (?, '909', 0, 2, TRUE, '9009', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		bucket.ID, model.StorageDataSetStatusReady)
	target, err := repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || target == nil || target.ID == source.ID {
		t.Fatalf("current binding after activation = %#v err=%v, want the new generation", target, err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: target.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodPeerPull,
		ProviderID:       onChainID(t, "909"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings target: %v", err)
	}
	targetCopy, err := repos.Uploads.GetUploadCopyForDataSet(ctx, upload.ID, target.ID)
	if err != nil || targetCopy == nil {
		t.Fatalf("GetUploadCopyForDataSet target = %#v err=%v", targetCopy, err)
	}

	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		StorageUploadCopyID: sourceCopy.ID,
		UploadID:            upload.ID,
		CopyIndex:           0,
		PieceCID:            "bafk2bzacegeneration",
		PieceID:             onChainIDPtr(t, "2001"),
		RetrievalURL:        "https://source.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}

	gotSource, err := repos.Uploads.GetUploadCopyByID(ctx, sourceCopy.ID)
	if err != nil || gotSource == nil || gotSource.Status != model.StorageUploadCopyStatusPieceReady {
		t.Fatalf("recorded copy = %#v err=%v, want piece_ready on the generation that stored it", gotSource, err)
	}
	gotTarget, err := repos.Uploads.GetUploadCopyByID(ctx, targetCopy.ID)
	if err != nil || gotTarget == nil || gotTarget.Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("replacement copy = %#v err=%v, want it untouched", gotTarget, err)
	}

	// A task queued before copy ids existed still resolves through its slot,
	// which names the current generation.
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacegeneration",
		PieceID:      onChainIDPtr(t, "9001"),
		RetrievalURL: "https://target.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady legacy: %v", err)
	}
	gotTarget, err = repos.Uploads.GetUploadCopyByID(ctx, targetCopy.ID)
	if err != nil || gotTarget == nil || gotTarget.Status != model.StorageUploadCopyStatusPieceReady {
		t.Fatalf("legacy addressed copy = %#v err=%v, want the current generation", gotTarget, err)
	}
}

func TestStorageUploadRepo_CountCurrentGenerationCopySlotsIgnoresHistoricalCopies(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "current-generation-copy-count")
	upload := startCopyHealthUpload(t, repos, bucket.ID, model.NewVersionID(), 10, "generation-count", 2)
	source := ensureCopyHealthBinding(t, repos, bucket.ID, upload.ID, 0, "101")
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady source: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: source.ID, CopyIndex: 0,
		TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings source: %v", err)
	}

	mustExec(t, db, `UPDATE storage_data_sets SET is_current = FALSE, status = ? WHERE id = ?`,
		model.StorageDataSetStatusDraining, source.ID)
	mustExec(t, db, `INSERT INTO storage_data_sets
		(bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_at, updated_at)
		VALUES (?, '202', 0, 2, TRUE, '2002', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		bucket.ID, model.StorageDataSetStatusReady)
	target, err := repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || target == nil || target.ID == source.ID {
		t.Fatalf("current target = %#v err=%v", target, err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: target.ID, CopyIndex: 0,
		TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings target: %v", err)
	}

	count, err := repos.Uploads.CountCurrentGenerationCopySlots(ctx, upload.ID)
	if err != nil {
		t.Fatalf("CountCurrentGenerationCopySlots: %v", err)
	}
	if count != 1 {
		t.Fatalf("current logical slots = %d, want one occupied slot and one missing slot", count)
	}
}
