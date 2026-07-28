package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestCacheEvictionRepo_EnsureAfterUploadOnlyRequeuesCancelledTask(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	key := "evict_cache:01J0000000000000000000CR01"
	original := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          10,
		RefVersionID:   "01J0000000000000000000CR01",
		IdempotencyKey: key,
		Status:         model.TaskStatusQueued,
		MaxRetries:     2,
		ScheduledAt:    time.Now().Add(-time.Hour),
	}
	if err := repos.Tasks.Create(ctx, original); err != nil {
		t.Fatalf("Create original task: %v", err)
	}
	mustExec(
		t,
		db,
		`UPDATE tasks
		 SET status = ?, retry_count = 2, last_error = 'old error',
		     completed_at = ?, status_message = 'old message'
		 WHERE id = ?`,
		model.TaskStatusCancelled,
		time.Now(),
		original.ID,
	)

	activated, err := repos.CacheEvictions.EnsureAfterUploadTask(
		ctx,
		11,
		original.RefVersionID,
		7,
	)
	if err != nil {
		t.Fatalf("EnsureAfterUploadTask(cancelled): %v", err)
	}
	if !activated {
		t.Fatal("EnsureAfterUploadTask(cancelled) activated = false, want true")
	}
	got, err := repos.Tasks.GetByID(ctx, original.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID reactivated: task=%v err=%v", got, err)
	}
	if got.Status != model.TaskStatusQueued ||
		got.Stage == nil ||
		*got.Stage != cacheeviction.StageAfterUpload ||
		got.RefID != 11 ||
		got.MaxRetries != 7 ||
		got.RetryCount != 0 ||
		got.CompletedAt != nil ||
		got.LastError != nil ||
		got.StatusMessage != nil {
		t.Fatalf("reactivated task = %#v, want reset queued replacement", got)
	}

	mustExec(t, db, `UPDATE tasks SET status = ?, completed_at = ? WHERE id = ?`, model.TaskStatusCompleted, time.Now(), original.ID)
	activated, err = repos.CacheEvictions.EnsureAfterUploadTask(
		ctx,
		11,
		original.RefVersionID,
		7,
	)
	if err != nil {
		t.Fatalf("EnsureAfterUploadTask(completed): %v", err)
	}
	if activated {
		t.Fatal("EnsureAfterUploadTask(completed) activated = true, want false")
	}
	got, err = repos.Tasks.GetByID(ctx, original.ID)
	if err != nil || got == nil || got.Status != model.TaskStatusCompleted {
		t.Fatalf("completed task after reactivation attempt = %#v err=%v", got, err)
	}
}

func TestCacheEvictionRepo_PlanLRUHonorsTerminalCooldown(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	stage := cacheeviction.StageLRU
	completedAt := time.Now()
	lastError := "permission denied"
	original := &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          12,
		RefVersionID:   "01J0000000000000000000CR02",
		IdempotencyKey: "evict_cache:lru:01J0000000000000000000CR02",
		Status:         model.TaskStatusExhausted,
		RetryCount:     3,
		MaxRetries:     3,
		LastError:      &lastError,
		ScheduledAt:    completedAt,
		CompletedAt:    &completedAt,
	}
	if err := repos.Tasks.Create(ctx, original); err != nil {
		t.Fatalf("Create exhausted task: %v", err)
	}
	candidate := cacheeviction.Candidate{
		ObjectID:   original.RefID,
		VersionID:  original.RefVersionID,
		Size:       10,
		AccessedAt: time.Now(),
	}

	activated, err := repos.CacheEvictions.PlanLRU(
		ctx,
		candidate,
		5,
		time.Now().Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("PlanLRU(recent): %v", err)
	}
	if activated {
		t.Fatal("recent exhausted task activated before cooldown")
	}

	mustExec(t, db, `UPDATE tasks SET completed_at = ? WHERE id = ?`, time.Now().Add(-2*time.Hour), original.ID)
	activated, err = repos.CacheEvictions.PlanLRU(
		ctx,
		candidate,
		5,
		time.Now().Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("PlanLRU(cooled): %v", err)
	}
	if !activated {
		t.Fatal("cooled exhausted task activated = false, want true")
	}
	got, err := repos.Tasks.GetByID(ctx, original.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID reactivated recoverable: task=%v err=%v", got, err)
	}
	if got.Status != model.TaskStatusQueued ||
		got.RetryCount != 0 ||
		got.MaxRetries != 5 ||
		got.CompletedAt != nil ||
		got.LastError != nil {
		t.Fatalf("reactivated recoverable task = %#v, want reset queued task", got)
	}

	completedAt = time.Now()
	mustExec(
		t,
		db,
		`UPDATE tasks SET status = ?, completed_at = ? WHERE id = ?`,
		model.TaskStatusCompleted,
		completedAt,
		original.ID,
	)
	activated, err = repos.CacheEvictions.PlanLRU(
		ctx,
		candidate,
		5,
		time.Now().Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("PlanLRU(completed): %v", err)
	}
	if !activated {
		t.Fatal("completed LRU task activated = false, want true")
	}
}

func TestCacheEvictionRepo_CancelActiveTasksExceptPreservesMatchingStageAndTerminalHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	lruStage := cacheeviction.StageLRU
	afterUploadStage := cacheeviction.StageAfterUpload
	tasks := []*model.Task{
		{
			Type:           model.TaskTypeEvictCache,
			Stage:          &lruStage,
			RefType:        "object",
			RefVersionID:   "01J0000000000000000000CS01",
			IdempotencyKey: "cancel-stage-lru",
			Status:         model.TaskStatusQueued,
		},
		{
			Type:           model.TaskTypeEvictCache,
			Stage:          &afterUploadStage,
			RefType:        "object",
			RefVersionID:   "01J0000000000000000000CS02",
			IdempotencyKey: "cancel-stage-after-upload",
			Status:         model.TaskStatusQueued,
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefVersionID:   "01J0000000000000000000CS03",
			IdempotencyKey: "cancel-stage-legacy",
			Status:         model.TaskStatusWaiting,
		},
		{
			Type:           model.TaskTypeEvictCache,
			Stage:          &lruStage,
			RefType:        "object",
			RefVersionID:   "01J0000000000000000000CS04",
			IdempotencyKey: "cancel-stage-terminal",
			Status:         model.TaskStatusFailed,
		},
	}
	for _, task := range tasks {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create(%s): %v", task.IdempotencyKey, err)
		}
	}

	cancelled, err := repos.CacheEvictions.CancelActiveTasksExcept(ctx, lruStage, "policy changed")
	if err != nil {
		t.Fatalf("CancelActiveTasksExcept: %v", err)
	}
	if cancelled != 2 {
		t.Fatalf("cancelled count = %d, want 2", cancelled)
	}
	wantStatuses := []model.TaskStatus{
		model.TaskStatusQueued,
		model.TaskStatusCancelled,
		model.TaskStatusCancelled,
		model.TaskStatusFailed,
	}
	for index, task := range tasks {
		got, err := repos.Tasks.GetByID(ctx, task.ID)
		if err != nil || got == nil {
			t.Fatalf("GetByID(%d): task=%v err=%v", task.ID, got, err)
		}
		if got.Status != wantStatuses[index] {
			t.Fatalf("task %s status = %s, want %s", task.IdempotencyKey, got.Status, wantStatuses[index])
		}
	}
}

func TestCacheEvictionRepo_ListLRUCandidatesOrdersSafeCurrentAndHistoricalVersions(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "lru-candidates-bucket")

	type seededVersion struct {
		version  *model.ObjectVersion
		objectID int64
	}
	seedStored := func(versionID string, size int64) seededVersion {
		t.Helper()
		version := newObjectVersion(bucket.ID, "file.txt", versionID, size)
		objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
		if err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%s): %v", versionID, err)
		}
		if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("UpdateVersionState(%s): %v", versionID, err)
		}
		acceptTestStorageUploadForVersion(t, repos, bucket.ID, version, "piece-"+versionID)
		return seededVersion{version: version, objectID: objectID}
	}

	oldest := seedStored("01J0000000000000000000LR01", 10)
	middle := seedStored("01J0000000000000000000LR02", 20)
	newest := seedStored("01J0000000000000000000LR03", 30)
	unsafe := newObjectVersion(bucket.ID, "unsafe.txt", "01J0000000000000000000LR04", 40)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, unsafe); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(unsafe): %v", err)
	}

	if err := repos.Objects.UpdateVersionState(
		ctx,
		newest.version.VersionID,
		model.ObjectStateStored,
		model.ObjectStateCacheEvicted,
	); err != nil {
		t.Fatalf("mark newest cache_evicted: %v", err)
	}
	base := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	for index, seeded := range []seededVersion{oldest, middle, newest} {
		if err := repos.Objects.RecordVersionCacheCommit(ctx, seeded.version.VersionID, base.Add(time.Duration(index)*time.Hour)); err != nil {
			t.Fatalf("RecordVersionCacheCommit(%s): %v", seeded.version.VersionID, err)
		}
	}

	terminalSince := time.Now().Add(-time.Hour)
	candidates, err := repos.CacheEvictions.ListLRUCandidates(ctx, terminalSince, 10)
	if err != nil {
		t.Fatalf("ListLRUCandidates: %v", err)
	}
	if len(candidates) != 3 {
		t.Fatalf("candidate count = %d, want 3: %#v", len(candidates), candidates)
	}
	for index, want := range []string{oldest.version.VersionID, middle.version.VersionID, newest.version.VersionID} {
		if candidates[index].VersionID != want {
			t.Fatalf("candidate[%d] = %s, want %s; candidates=%#v", index, candidates[index].VersionID, want, candidates)
		}
	}

	// A NULL access time is not eligible. The migration initializes all existing
	// rows, while newly committed cache entries always record an access time.
	mustExec(t, db, `UPDATE object_versions SET cache_accessed_at = NULL, created_at = ? WHERE version_id = ?`, base.Add(-time.Hour), oldest.version.VersionID)
	stage := cacheeviction.StageLRU
	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          middle.objectID,
		RefVersionID:   middle.version.VersionID,
		IdempotencyKey: "evict_cache:lru:test:" + middle.version.VersionID,
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create active eviction task: %v", err)
	}

	candidates, err = repos.CacheEvictions.ListLRUCandidates(ctx, terminalSince, 10)
	if err != nil {
		t.Fatalf("ListLRUCandidates after active task: %v", err)
	}
	if len(candidates) != 1 || candidates[0].VersionID != newest.version.VersionID {
		t.Fatalf("candidates with active task = %#v, want newest only", candidates)
	}
	activeBytes, err := repos.CacheEvictions.ActiveLRUBytes(ctx)
	if err != nil {
		t.Fatalf("ActiveLRUBytes: %v", err)
	}
	if activeBytes != middle.version.Size {
		t.Fatalf("active eviction bytes = %d, want %d", activeBytes, middle.version.Size)
	}
	if err := repos.Objects.RecordVersionCacheAccess(ctx, oldest.version.VersionID, base.Add(-time.Hour)); err != nil {
		t.Fatalf("RecordVersionCacheAccess(restored): %v", err)
	}

	exhaustedAt := time.Now()
	exhausted := &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          oldest.objectID,
		RefVersionID:   oldest.version.VersionID,
		IdempotencyKey: "evict_cache:lru:" + oldest.version.VersionID,
		Status:         model.TaskStatusExhausted,
		MaxRetries:     3,
		ScheduledAt:    exhaustedAt,
		CompletedAt:    &exhaustedAt,
	}
	if err := repos.Tasks.Create(ctx, exhausted); err != nil {
		t.Fatalf("Create exhausted LRU task: %v", err)
	}
	candidates, err = repos.CacheEvictions.ListLRUCandidates(ctx, terminalSince, 10)
	if err != nil {
		t.Fatalf("ListLRUCandidates after exhausted task: %v", err)
	}
	if len(candidates) != 1 || candidates[0].VersionID != newest.version.VersionID {
		t.Fatalf("candidates with active and exhausted tasks = %#v, want newest only", candidates)
	}

	mustExec(
		t,
		db,
		`UPDATE tasks SET completed_at = ? WHERE id = ?`,
		terminalSince.Add(-time.Second),
		exhausted.ID,
	)
	candidates, err = repos.CacheEvictions.ListLRUCandidates(ctx, terminalSince, 10)
	if err != nil {
		t.Fatalf("ListLRUCandidates after exhausted cooldown: %v", err)
	}
	if len(candidates) != 2 ||
		candidates[0].VersionID != oldest.version.VersionID ||
		candidates[1].VersionID != newest.version.VersionID {
		t.Fatalf("candidates after exhausted cooldown = %#v, want oldest/newest", candidates)
	}

	mustExec(t, db, `UPDATE tasks SET completed_at = ? WHERE id = ?`, time.Now(), exhausted.ID)
	newAccess := base.Add(4 * time.Hour)
	if err := repos.Objects.RecordVersionCacheAccess(ctx, oldest.version.VersionID, newAccess); err != nil {
		t.Fatalf("RecordVersionCacheAccess(newer access): %v", err)
	}
	candidates, err = repos.CacheEvictions.ListLRUCandidates(ctx, terminalSince, 10)
	if err != nil {
		t.Fatalf("ListLRUCandidates after newer access: %v", err)
	}
	if len(candidates) != 1 || candidates[0].VersionID != newest.version.VersionID {
		t.Fatalf("candidates after newer access = %#v, want recent exhausted task to remain cooling down", candidates)
	}
}
