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

	"github.com/strahe/synaps3/internal/config"
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

func TestAPIListExhaustedUsesTaskListDTO(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J000000000000000TASKE1",
		IdempotencyKey: "api-exhausted-list",
		Status:         model.TaskStatusExhausted,
		ScheduledAt:    time.Now(),
		MaxRetries:     3,
		RetryCount:     3,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/exhausted-tasks", srv.handleListExhausted)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/exhausted-tasks", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var tasks []taskListItem
	if err := json.NewDecoder(rr.Body).Decode(&tasks); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(tasks))
	}
	if tasks[0].ID != task.ID {
		t.Fatalf("task id = %d, want %d", tasks[0].ID, task.ID)
	}
	if tasks[0].Status != string(model.TaskStatusExhausted) {
		t.Fatalf("task status = %q, want exhausted", tasks[0].Status)
	}
	if tasks[0].Type != string(model.TaskTypeUpload) {
		t.Fatalf("task type = %q, want upload", tasks[0].Type)
	}
}

func TestAPIRetryExhaustedHTTPStatuses(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          2,
		RefVersionID:   "01J000000000000000TASKE2",
		IdempotencyKey: "api-exhausted-retry",
		Status:         model.TaskStatusExhausted,
		ScheduledAt:    time.Now(),
		MaxRetries:     3,
		RetryCount:     3,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/exhausted-tasks/{id}/retry", srv.handleRetryExhausted)

	for _, tc := range []struct {
		name       string
		path       string
		wantStatus int
	}{
		{name: "invalid id", path: "/admin/exhausted-tasks/not-a-number/retry", wantStatus: http.StatusBadRequest},
		{name: "missing task", path: "/admin/exhausted-tasks/999999/retry", wantStatus: http.StatusNotFound},
		{name: "exhausted task", path: "/admin/exhausted-tasks/" + strconv.FormatInt(task.ID, 10) + "/retry", wantStatus: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, tc.path, nil))
			if rr.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tc.wantStatus, rr.Body.String())
			}
		})
	}

	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got == nil {
		t.Fatal("retried task was not found")
	}
	if got.Status != model.TaskStatusQueued {
		t.Fatalf("task status = %s, want queued", got.Status)
	}
}

func TestAPITaskStats(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	tasks := []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          1,
			RefVersionID:   "01J000000000000000STAT01",
			IdempotencyKey: "api-task-stats-queued-1",
			Status:         model.TaskStatusQueued,
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          2,
			RefVersionID:   "01J000000000000000STAT02",
			IdempotencyKey: "api-task-stats-queued-2",
			Status:         model.TaskStatusQueued,
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          3,
			RefVersionID:   "01J000000000000000STAT03",
			IdempotencyKey: "api-task-stats-running",
			Status:         model.TaskStatusRunning,
		},
	}
	for _, task := range tasks {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create task %q: %v", task.IdempotencyKey, err)
		}
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
	rr := httptest.NewRecorder()
	srv.handleAPITaskStats(rr, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/stats", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var body []repository.TaskStatusCount
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode stats: %v", err)
	}
	counts := make(map[[2]string]int64, len(body))
	for _, count := range body {
		counts[[2]string{count.Type, count.Status}] = count.Count
	}
	if got := counts[[2]string{string(model.TaskTypeUpload), string(model.TaskStatusQueued)}]; got != 2 {
		t.Fatalf("queued upload count = %d, want 2", got)
	}
	if got := counts[[2]string{string(model.TaskTypeUpload), string(model.TaskStatusRunning)}]; got != 1 {
		t.Fatalf("running upload count = %d, want 1", got)
	}
}

func TestAPITasksStageFilter(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	ingressCommit := "ingress_commit"
	peerCommit := "peer_commit"

	for _, task := range []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			Stage:          &ingressCommit,
			RefType:        "object",
			RefID:          1,
			RefVersionID:   "01J000000000000000TASKA01",
			IdempotencyKey: "api-stage-primary",
			Status:         model.TaskStatusQueued,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeUpload,
			Stage:          &peerCommit,
			RefType:        "object",
			RefID:          2,
			RefVersionID:   "01J000000000000000TASKA02",
			IdempotencyKey: "api-stage-secondary",
			Status:         model.TaskStatusQueued,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          3,
			RefVersionID:   "01J000000000000000TASKA03",
			IdempotencyKey: "api-stage-evict",
			Status:         model.TaskStatusQueued,
			ScheduledAt:    time.Now(),
		},
	} {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create task %q: %v", task.IdempotencyKey, err)
		}
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", srv.handleAPITasks)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks?type=upload&stage=ingress_commit&status=queued")
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
	if body.Tasks[0].Stage == nil || *body.Tasks[0].Stage != ingressCommit {
		t.Fatalf("stage = %#v, want ingress_commit", body.Tasks[0].Stage)
	}

	resp, err = http.Get(ts.URL + "/api/v1/tasks?stage=ingress_commit")
	if err != nil {
		t.Fatalf("GET tasks without type: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestAPITasksReturnsWaitingDetails(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	waitReason := model.TaskWaitReasonDependency
	statusMessage := "waiting for all copies to commit"

	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J000000000000000TASKW01",
		IdempotencyKey: "api-task-waiting",
		Status:         model.TaskStatusWaiting,
		WaitReason:     &waitReason,
		StatusMessage:  &statusMessage,
		ScheduledAt:    time.Now().Add(time.Minute),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", srv.handleAPITasks)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks?status=waiting")
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
	got := body.Tasks[0]
	if got.Status != string(model.TaskStatusWaiting) {
		t.Fatalf("task status = %s, want waiting", got.Status)
	}
	if got.WaitReason == nil || *got.WaitReason != string(waitReason) {
		t.Fatalf("wait_reason = %v, want %s", got.WaitReason, waitReason)
	}
	if got.StatusMessage == nil || *got.StatusMessage != statusMessage {
		t.Fatalf("status_message = %v, want %s", got.StatusMessage, statusMessage)
	}
	if got.LastError != nil {
		t.Fatalf("last_error = %v, want nil", got.LastError)
	}
}

func TestAPITasksShowsLegacyPayloadStage(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO tasks (type, ref_type, ref_id, ref_version_id, idempotency_key, payload, status, scheduled_at)
		VALUES ('upload', 'object', 1, '01J000000000000000TASKB01', 'legacy-payload-stage', '{"stage":"peer_pull","upload_id":9,"copy_index":1}', 'queued', CURRENT_TIMESTAMP)
	`)
	if err != nil {
		t.Fatalf("insert legacy task: %v", err)
	}

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks", srv.handleAPITasks)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/tasks?type=upload&status=queued")
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
	if body.Tasks[0].Stage == nil || *body.Tasks[0].Stage != "peer_pull" {
		t.Fatalf("stage = %#v, want peer_pull", body.Tasks[0].Stage)
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
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	progressUpload, err := repos.Uploads.BeginIngressStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginIngressStoreProgress: %v", err)
	}
	if _, err := repos.Uploads.RecordIngressStoreProgress(ctx, repository.RecordIngressStoreProgressInput{
		UploadID:      upload.ID,
		Attempt:       progressUpload.IngressStoreAttempt,
		BytesUploaded: 5,
	}); err != nil {
		t.Fatalf("RecordIngressStoreProgress: %v", err)
	}
	stage := "ingress_store"
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

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
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
	if progress.Scope != "ingress_store" || progress.Attempt != progressUpload.IngressStoreAttempt || progress.UploadedBytes != 5 || progress.TotalBytes != 20 || progress.Percent == nil || *progress.Percent != 25 || progress.Done {
		t.Fatalf("progress = %#v, want 5/20 primary transfer progress", progress)
	}
}

func TestAPITasksSkipsProgressWhenLookupFails(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	stage := "ingress_store"
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

	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
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
	if rawBody.Object == nil || rawBody.Object.UploadStatus != string(model.StorageUploadStatusComplete) {
		t.Fatalf("task object upload_status = %#v, want complete", rawBody.Object)
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
		Status:         model.TaskStatusQueued,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	if _, status := getTaskRefDetail(t, db, repos, task.ID); status != http.StatusNotFound {
		t.Fatalf("missing object status = %d, want 404", status)
	}
}

func TestAPITaskRefDetailStorageCleanupIncludesDeletedVersionSnapshot(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, "task-ref-storage-cleanup-bucket")
	uploadID := int64(98)
	versionID := "01J000000000000000DELETE1"
	now := time.Now().UTC()

	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		RefVersionID:   "",
		IdempotencyKey: "task-ref-storage-cleanup",
		Payload: map[string]interface{}{
			"storage_upload_id":       uploadID,
			"deleted_source_version":  versionID,
			"deleted_source_versions": []string{versionID},
		},
		Status:      model.TaskStatusQueued,
		MaxRetries:  5,
		ScheduledAt: now,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	deletion := &model.ObjectDeletion{
		BucketID:           bucket.ID,
		ObjectID:           55,
		Key:                "folder/deleted.txt",
		VersionID:          versionID,
		CacheKey:           "cache/deleted",
		StorageUploadID:    &uploadID,
		Size:               456,
		Checksum:           "checksum-deleted",
		CacheCleanupStatus: model.CacheCleanupStatusSkipped,
		CreatedAt:          now,
		UpdatedAt:          now,
		DeletedAt:          now,
	}
	if _, err := db.NewInsert().Model(deletion).Exec(ctx); err != nil {
		t.Fatalf("Insert object deletion: %v", err)
	}
	copy := &model.StorageCleanupCopy{
		TaskID:      task.ID,
		UploadID:    uploadID,
		CopyIndex:   0,
		PieceCID:    "bafkzcibedeleted",
		Status:      model.StorageCleanupCopyStatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
		ScheduledAt: &now,
	}
	if _, err := db.NewInsert().Model(copy).Exec(ctx); err != nil {
		t.Fatalf("Insert storage cleanup copy: %v", err)
	}

	body, status := getTaskRefDetail(t, db, repos, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.StorageCleanup == nil {
		t.Fatal("storage cleanup detail is nil")
	}
	if body.StorageCleanup.UploadID != uploadID || len(body.StorageCleanup.Copies) != 1 {
		t.Fatalf("storage cleanup detail = %#v, want upload and copy snapshot", body.StorageCleanup)
	}
	if len(body.StorageCleanup.DeletedVersions) != 1 {
		t.Fatalf("deleted versions = %#v, want one deleted version snapshot", body.StorageCleanup.DeletedVersions)
	}
	deleted := body.StorageCleanup.DeletedVersions[0]
	if deleted.BucketName != bucket.Name || deleted.Key != "folder/deleted.txt" || deleted.VersionID != versionID {
		t.Fatalf("deleted version = %#v, want bucket/key/version snapshot", deleted)
	}
	if deleted.Size != 456 || deleted.DeletedAt == "" {
		t.Fatalf("deleted version = %#v, want size and deletion timestamp", deleted)
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
		Status:         model.TaskStatusQueued,
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
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
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
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
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
