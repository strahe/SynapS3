package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestAPIOverviewIncludesAttentionAndActivePipeline(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := overviewSeedBucket(t, db, "overview-bucket")

	healthy := overviewObjectVersion(bucket.ID, "healthy.txt", "01J00000000000000000000B01")
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, healthy); err != nil {
		t.Fatalf("seed healthy object: %v", err)
	}
	failed := overviewObjectVersion(bucket.ID, "failed.txt", "01J00000000000000000000B02")
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, failed); err != nil {
		t.Fatalf("seed failed object: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, failed.VersionID, model.ObjectStateCached, "upload failed"); err != nil {
		t.Fatalf("mark failed object: %v", err)
	}
	unavailable := overviewObjectVersion(bucket.ID, "unavailable.txt", "01J00000000000000000000B03")
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, unavailable); err != nil {
		t.Fatalf("seed unavailable object: %v", err)
	}
	overviewMustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status) VALUES (?, ?, ?, ?, ?)`,
		bucket.ID, unavailable.VersionID, unavailable.Size, unavailable.Checksum, model.StorageUploadStatusComplete)
	overviewMustExec(t, db, `UPDATE object_versions SET state = ?, storage_upload_id = (SELECT MAX(id) FROM storage_uploads), in_cache = FALSE WHERE version_id = ?`,
		model.ObjectStateStored, unavailable.VersionID)

	overviewSeedTask(t, repos, model.TaskTypeUpload, "prepare_upload", model.TaskStatusQueued)
	overviewSeedTask(t, repos, model.TaskTypeUpload, "ingress_store", model.TaskStatusRunning)
	overviewSeedTask(t, repos, model.TaskTypeUpload, "peer_commit", model.TaskStatusWaiting)
	overviewSeedTask(t, repos, model.TaskTypeEvictCache, "", model.TaskStatusScheduled)
	overviewSeedTask(t, repos, model.TaskTypeStorageCleanup, "", model.TaskStatusRunning)
	overviewSeedTask(t, repos, model.TaskTypeUpload, "ingress_store", model.TaskStatusCompleted)
	overviewSeedTask(t, repos, model.TaskTypeUpload, "ingress_store", model.TaskStatusFailed)
	overviewSeedTask(t, repos, model.TaskTypeEvictCache, "", model.TaskStatusExhausted)

	srv := New(":0", db, &stubCache{rootDir: t.TempDir(), usedByte: 42}, 100, repos, &stubWorkerHealth{health: map[string]bool{"uploader": true}}, nil, testLogger())
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/overview", nil)
	srv.handleAPIOverview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}

	var body struct {
		Objects struct {
			ByState   map[string]int64 `json:"by_state"`
			Attention struct {
				NeedsAttention int64 `json:"needs_attention"`
				Unavailable    int64 `json:"unavailable"`
			} `json:"attention"`
		} `json:"objects"`
		Tasks struct {
			ByStatus  map[string]int64 `json:"by_status"`
			Attention struct {
				Failed    int64 `json:"failed"`
				Exhausted int64 `json:"exhausted"`
			} `json:"attention"`
			ActivePipeline []struct {
				Pipeline string           `json:"pipeline"`
				ByStatus map[string]int64 `json:"by_status"`
				Total    int64            `json:"total"`
			} `json:"active_pipeline"`
		} `json:"tasks"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if body.Objects.ByState[string(model.ObjectStateCached)] == 0 {
		t.Fatal("overview should keep legacy object state counts")
	}
	if body.Tasks.ByStatus[string(model.TaskStatusCompleted)] == 0 {
		t.Fatal("overview should keep legacy task status counts")
	}
	if body.Objects.Attention.NeedsAttention != 1 {
		t.Fatalf("needs_attention = %d, want 1", body.Objects.Attention.NeedsAttention)
	}
	if body.Objects.Attention.Unavailable != 1 {
		t.Fatalf("unavailable = %d, want 1", body.Objects.Attention.Unavailable)
	}
	if body.Tasks.Attention.Failed != 1 || body.Tasks.Attention.Exhausted != 1 {
		t.Fatalf("task attention = failed:%d exhausted:%d, want 1/1", body.Tasks.Attention.Failed, body.Tasks.Attention.Exhausted)
	}
	pipeline := make(map[string]struct {
		total    int64
		byStatus map[string]int64
	})
	for _, row := range body.Tasks.ActivePipeline {
		pipeline[row.Pipeline] = struct {
			total    int64
			byStatus map[string]int64
		}{total: row.Total, byStatus: row.ByStatus}
	}
	if pipeline["prepare"].byStatus[string(model.TaskStatusQueued)] != 1 {
		t.Fatalf("prepare queued = %d, want 1", pipeline["prepare"].byStatus[string(model.TaskStatusQueued)])
	}
	if pipeline["upload"].byStatus[string(model.TaskStatusRunning)] != 1 {
		t.Fatalf("upload running = %d, want 1", pipeline["upload"].byStatus[string(model.TaskStatusRunning)])
	}
	if pipeline["sync"].byStatus[string(model.TaskStatusWaiting)] != 1 {
		t.Fatalf("sync waiting = %d, want 1", pipeline["sync"].byStatus[string(model.TaskStatusWaiting)])
	}
	if pipeline["evict"].byStatus[string(model.TaskStatusScheduled)] != 1 {
		t.Fatalf("evict scheduled = %d, want 1", pipeline["evict"].byStatus[string(model.TaskStatusScheduled)])
	}
	if pipeline["cleanup"].byStatus[string(model.TaskStatusRunning)] != 1 {
		t.Fatalf("cleanup running = %d, want 1", pipeline["cleanup"].byStatus[string(model.TaskStatusRunning)])
	}
	if pipeline["upload"].total != 1 {
		t.Fatalf("upload total = %d, want only active tasks", pipeline["upload"].total)
	}
}

func overviewSeedBucket(t *testing.T, db *bun.DB, name string) *model.Bucket {
	t.Helper()
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive}
	if _, err := db.NewInsert().Model(bucket).Exec(context.Background()); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	return bucket
}

func overviewObjectVersion(bucketID int64, key, versionID string) *model.ObjectVersion {
	return &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucketID,
		Key:         key,
		Size:        10,
		ETag:        "etag-" + versionID,
		Checksum:    "checksum-" + versionID,
		ContentType: "text/plain",
		CacheKey:    ".versions/" + versionID,
		State:       model.ObjectStateCached,
	}
}

func overviewSeedTask(t *testing.T, repos *repository.Repositories, taskType model.TaskType, stage string, status model.TaskStatus) {
	t.Helper()
	task := &model.Task{
		Type:           taskType,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J0000000000000000000TASK",
		IdempotencyKey: string(taskType) + ":" + stage + ":" + string(status),
		Status:         status,
	}
	if stage != "" {
		task.Stage = &stage
	}
	if taskType == model.TaskTypeStorageCleanup {
		task.RefType = "storage_upload"
		task.RefVersionID = ""
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("seed task: %v", err)
	}
}

func overviewMustExec(t *testing.T, db *bun.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.NewRaw(query, args...).Exec(context.Background()); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}
