package repository_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
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
			{ProviderID: strPtr("101"), DataSetID: strPtr("1001"), PieceID: strPtr("2001"), Role: "primary", RetrievalURL: strPtr("https://primary.example/piece"), IsNewDataSet: true},
			{ProviderID: strPtr("202"), DataSetID: strPtr("2002"), PieceID: strPtr("3001"), Role: "secondary", RetrievalURL: strPtr("https://secondary.example/piece"), IsNewDataSet: true},
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
			{ProviderID: strPtr("101"), DataSetID: strPtr("1001"), PieceID: strPtr("2001"), Role: "primary", RetrievalURL: strPtr("https://primary.example/piece"), IsNewDataSet: true},
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
			{ProviderID: strPtr("101"), DataSetID: strPtr("1001"), PieceID: strPtr("2001"), Role: "primary", RetrievalURL: strPtr("https://provider.example/first")},
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
			{ProviderID: strPtr("101"), DataSetID: strPtr("1001"), PieceID: strPtr("9999"), Role: "primary", RetrievalURL: strPtr("https://provider.example/second")},
			{ProviderID: strPtr("202"), DataSetID: strPtr("2002"), PieceID: strPtr("3001"), Role: "secondary", RetrievalURL: strPtr("https://provider.example/second-copy")},
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
		providerID: "cross-race-provider",
		dataSetID:  "cross-race-data-set",
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
			{ProviderID: strPtr("safe-provider"), DataSetID: strPtr("safe-data-set"), PieceID: strPtr("1"), Role: "primary", RetrievalURL: strPtr("https://provider.example/safe")},
			{ProviderID: strPtr(hook.providerID), DataSetID: strPtr(hook.dataSetID), PieceID: strPtr("2"), Role: "primary", RetrievalURL: strPtr("https://provider.example/cross-race")},
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
		providerID: "race-provider",
		dataSetID:  "race-data-set",
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
			{ProviderID: strPtr(hook.providerID), DataSetID: strPtr(hook.dataSetID), PieceID: strPtr("2"), Role: "primary", RetrievalURL: &retrievalURL},
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
	if dataSet.FirstSeenUploadID != first.ID || dataSet.LastSeenUploadID != second.ID {
		t.Fatalf("data set seen uploads = first:%d last:%d, want first:%d last:%d", dataSet.FirstSeenUploadID, dataSet.LastSeenUploadID, first.ID, second.ID)
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
	dataSet := &model.StorageDataSet{
		BucketID:          h.bucketID,
		ProviderID:        h.providerID,
		DataSetID:         h.dataSetID,
		FirstSeenUploadID: h.uploadID,
		LastSeenUploadID:  h.uploadID,
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
	if err := repos.Uploads.RecordUploadResult(context.Background(), repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        strPtr(pieceCID),
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{
			{ProviderID: strPtr("101"), DataSetID: strPtr("1001-" + version.VersionID), PieceID: strPtr("2001"), Role: "primary", RetrievalURL: strPtr("https://provider.example/" + version.VersionID)},
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
