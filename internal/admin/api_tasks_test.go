package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

type failingTaskProgressUploadRepo struct {
	repository.StorageUploadRepository
}

func (r failingTaskProgressUploadRepo) GetByIDs(_ context.Context, _ []int64) (map[int64]model.StorageUpload, error) {
	return nil, errors.New("progress lookup failed")
}

func TestAPITasksStageFilter(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	primaryCommit := "primary_commit"
	secondaryCommit := "secondary_commit"

	for _, task := range []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			Stage:          &primaryCommit,
			RefType:        "object",
			RefID:          1,
			RefVersionID:   "01J000000000000000TASKA01",
			IdempotencyKey: "api-stage-primary",
			Status:         model.TaskStatusPending,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeUpload,
			Stage:          &secondaryCommit,
			RefType:        "object",
			RefID:          2,
			RefVersionID:   "01J000000000000000TASKA02",
			IdempotencyKey: "api-stage-secondary",
			Status:         model.TaskStatusPending,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          3,
			RefVersionID:   "01J000000000000000TASKA03",
			IdempotencyKey: "api-stage-evict",
			Status:         model.TaskStatusPending,
			ScheduledAt:    time.Now(),
		},
	} {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create task %q: %v", task.IdempotencyKey, err)
		}
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", srv.handleAPITasks)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks?type=upload&stage=primary_commit&status=pending")
	if err != nil {
		t.Fatalf("GET tasks: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body taskListResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if body.Total != 1 || len(body.Tasks) != 1 {
		t.Fatalf("tasks = total:%d items:%#v, want one", body.Total, body.Tasks)
	}
	if body.Tasks[0].Stage == nil || *body.Tasks[0].Stage != primaryCommit {
		t.Fatalf("stage = %#v, want primary_commit", body.Tasks[0].Stage)
	}

	resp, err = http.Get(ts.URL + "/api/v1/tasks?stage=primary_commit")
	if err != nil {
		t.Fatalf("GET tasks without type: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestAPITasksShowsLegacyPayloadStage(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO tasks (type, ref_type, ref_id, ref_version_id, idempotency_key, payload, status, scheduled_at)
		VALUES ('upload', 'object', 1, '01J000000000000000TASKB01', 'legacy-payload-stage', '{"stage":"secondary_pull","upload_id":9,"copy_index":1}', 'pending', CURRENT_TIMESTAMP)
	`)
	if err != nil {
		t.Fatalf("insert legacy task: %v", err)
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", srv.handleAPITasks)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks?type=upload&status=pending")
	if err != nil {
		t.Fatalf("GET tasks: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body taskListResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if body.Total != 1 || len(body.Tasks) != 1 {
		t.Fatalf("tasks = total:%d items:%#v, want one", body.Total, body.Tasks)
	}
	if body.Tasks[0].Stage == nil || *body.Tasks[0].Stage != "secondary_pull" {
		t.Fatalf("stage = %#v, want secondary_pull", body.Tasks[0].Stage)
	}
}

func TestAPITasksIncludesPrimaryTransferProgress(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, "task-progress-bucket")
	objectID, versionID := seedAdminObjectVersion(t, repos, bucket, "uploading.txt", 20, "etag-progress", "checksum-progress", "text/plain", "", model.ObjectStateUploading)
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     20,
		Checksum:        "checksum-progress",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	progressUpload, err := repos.Uploads.BeginPrimaryStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginPrimaryStoreProgress: %v", err)
	}
	if _, err := repos.Uploads.RecordPrimaryStoreProgress(ctx, repository.RecordPrimaryStoreProgressInput{
		UploadID:      upload.ID,
		Attempt:       progressUpload.PrimaryStoreAttempt,
		BytesUploaded: 5,
	}); err != nil {
		t.Fatalf("RecordPrimaryStoreProgress: %v", err)
	}
	stage := "primary_store"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: "task-primary-progress",
		Payload:        map[string]interface{}{"upload_id": upload.ID},
		Status:         model.TaskStatusRunning,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", srv.handleAPITasks)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks?type=upload&status=running")
	if err != nil {
		t.Fatalf("GET tasks: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body taskListResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(body.Tasks) != 1 || body.Tasks[0].Progress == nil {
		t.Fatalf("tasks = %#v, want primary transfer progress", body.Tasks)
	}
	progress := body.Tasks[0].Progress
	if progress.Scope != "primary_store" || progress.Attempt != progressUpload.PrimaryStoreAttempt || progress.UploadedBytes != 5 || progress.TotalBytes != 20 || progress.Percent == nil || *progress.Percent != 25 || progress.Done {
		t.Fatalf("progress = %#v, want 5/20 primary transfer progress", progress)
	}
}

func TestAPITasksSkipsProgressWhenLookupFails(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	stage := "primary_store"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J000000000000000TASKP01",
		IdempotencyKey: "task-progress-lookup-fails",
		Payload:        map[string]interface{}{"upload_id": int64(42)},
		Status:         model.TaskStatusRunning,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	repos.Uploads = failingTaskProgressUploadRepo{StorageUploadRepository: repos.Uploads}

	srv := New(":0", db, nil, 0, repos, nil, nil, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", srv.handleAPITasks)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks?type=upload&status=running")
	if err != nil {
		t.Fatalf("GET tasks: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body taskListResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(body.Tasks) != 1 {
		t.Fatalf("tasks = %#v, want one task", body.Tasks)
	}
	if body.Tasks[0].Progress != nil {
		t.Fatalf("progress = %#v, want omitted progress after lookup failure", body.Tasks[0].Progress)
	}
}

func TestAPITaskRefDetailObject(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, "task-ref-bucket")
	objectID, versionID := seedAdminObjectVersion(t, repos, bucket, "folder/file.txt", 123, "etag", "checksum", "text/plain", "", model.ObjectStateStored)
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: "task-ref-object",
		Status:         model.TaskStatusCompleted,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	body, status := getTaskRefDetail(t, db, repos, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.RefType != "object" || body.RefID != objectID || body.RefVersionID != versionID {
		t.Fatalf("ref detail = %#v, want object ref", body)
	}
	if body.Object == nil {
		t.Fatal("object detail is nil")
	}
	if body.Object.BucketName != bucket.Name || body.Object.Key != "folder/file.txt" || body.Object.VersionID != versionID {
		t.Fatalf("object detail = %#v, want bucket/key/version", body.Object)
	}
	if body.Object.Size != 123 || body.Object.State != string(model.ObjectStateStored) || body.Object.Status != "success" {
		t.Fatalf("object detail = %#v, want stored success size 123", body.Object)
	}
	if !body.Object.Location.Cache || !body.Object.Location.Filecoin {
		t.Fatalf("location = %#v, want cache and filecoin", body.Object.Location)
	}
	rawBody, rawStatus := getTaskRefDetailUploadStatus(t, db, repos, task.ID)
	if rawStatus != http.StatusOK {
		t.Fatalf("raw status = %d, want 200", rawStatus)
	}
	if rawBody.Object == nil || rawBody.Object.UploadStatus != string(model.StorageUploadStatusAllCopiesCommitted) {
		t.Fatalf("task object upload_status = %#v, want all_copies_committed", rawBody.Object)
	}
}

func TestAPITaskRefDetailNotFound(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	if _, status := getTaskRefDetail(t, db, repos, 999); status != http.StatusNotFound {
		t.Fatalf("missing task status = %d, want 404", status)
	}

	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          123,
		RefVersionID:   "01J000000000000000MISSING",
		IdempotencyKey: "task-ref-missing-object",
		Status:         model.TaskStatusPending,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	if _, status := getTaskRefDetail(t, db, repos, task.ID); status != http.StatusNotFound {
		t.Fatalf("missing object status = %d, want 404", status)
	}
}

func TestAPITaskRefDetailNonObject(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	bucket := testutil.SeedBucket(t, db, "task-ref-non-object-bucket")
	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "bucket",
		RefID:          bucket.ID,
		RefVersionID:   "",
		IdempotencyKey: "task-ref-bucket",
		Status:         model.TaskStatusPending,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	body, status := getTaskRefDetail(t, db, repos, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.RefType != "bucket" || body.RefID != bucket.ID || body.Object != nil {
		t.Fatalf("ref detail = %#v, want bucket ref without object", body)
	}
}

func getTaskRefDetail(t *testing.T, db *bun.DB, repos *repository.Repositories, taskID int64) (taskRefDetailResponse, int) {
	t.Helper()
	srv := New(":0", db, nil, 0, repos, nil, nil, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks/{id}/ref-detail", srv.handleAPITaskRefDetail)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks/" + strconv.FormatInt(taskID, 10) + "/ref-detail")
	if err != nil {
		t.Fatalf("GET task ref detail: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body taskRefDetailResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}
	return body, resp.StatusCode
}

func getTaskRefDetailUploadStatus(t *testing.T, db *bun.DB, repos *repository.Repositories, taskID int64) (struct {
	Object *struct {
		UploadStatus string `json:"upload_status"`
	} `json:"object"`
}, int,
) {
	t.Helper()
	srv := New(":0", db, nil, 0, repos, nil, nil, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks/{id}/ref-detail", srv.handleAPITaskRefDetail)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks/" + strconv.FormatInt(taskID, 10) + "/ref-detail")
	if err != nil {
		t.Fatalf("GET task ref detail upload status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Object *struct {
			UploadStatus string `json:"upload_status"`
		} `json:"object"`
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("Decode upload status: %v", err)
		}
	}
	return body, resp.StatusCode
}
