package worker_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestEvictor_Preconditions(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task
		wantLastError string
	}{
		{
			name: "MissingVersion",
			setup: func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task {
				_, objID, _ := seedStoredObject(t, env)
				stage := cacheeviction.StageAfterUpload
				task := &model.Task{
					Type:           model.TaskTypeEvictCache,
					Stage:          &stage,
					RefType:        "object",
					RefID:          objID,
					RefVersionID:   "01J000000000000000MISSING1",
					IdempotencyKey: "evict_cache:missing",
					Status:         model.TaskStatusQueued,
					MaxRetries:     5,
					ScheduledAt:    time.Now(),
				}
				if err := env.repos.Tasks.Create(ctx, task); err != nil {
					t.Fatalf("creating task: %v", err)
				}
				return task
			},
			wantLastError: "object not found",
		},
		{
			name: "WrongState",
			setup: func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task {
				_, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
				if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
					t.Fatalf("transition: %v", err)
				}
				return seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)
			},
			wantLastError: "not stored",
		},
		{
			name: "NoReadableCopies",
			setup: func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task {
				_, objID, versionID := seedStoredObject(t, env)
				version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
				if err != nil || version == nil || version.StorageUploadID == nil {
					t.Fatalf("stored version upload: version=%v err=%v", version, err)
				}
				if _, err := env.db.NewDelete().Model((*model.StorageUploadCopy)(nil)).Where("upload_id = ?", *version.StorageUploadID).Exec(ctx); err != nil {
					t.Fatalf("remove readable copies: %v", err)
				}
				return seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)
			},
			wantLastError: "no readable upload copies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &testutil.MockCache{}
			env := newTestWorkerEnvWithMockCache(t, mc)
			ctx := context.Background()

			task := tt.setup(ctx, t, env)

			evictor := newAfterUploadEvictor(env, 1, 50*time.Millisecond)
			runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

			got, err := env.repos.Tasks.GetByID(ctx, task.ID)
			if err != nil {
				t.Fatalf("getting task: %v", err)
			}
			if got == nil {
				t.Fatalf("task %d not found", task.ID)
			}
			if got.Status != model.TaskStatusFailed {
				t.Errorf("expected task failed, got %s", got.Status)
			}
			if got.LastError == nil || !strings.Contains(*got.LastError, tt.wantLastError) {
				t.Errorf("expected last error to contain %q, got %v", tt.wantLastError, got.LastError)
			}
		})
	}
}

func TestEvictor_CacheDeleteFailureLeavesObjectUnchangedAndKeepsTaskRecoverable(t *testing.T) {
	for _, tc := range []struct {
		name        string
		retryCount  int
		wantStatus  model.TaskStatus
		wantRetries int
	}{
		{name: "requeue", retryCount: 0, wantStatus: model.TaskStatusScheduled, wantRetries: 1},
		{name: "exhausted", retryCount: 4, wantStatus: model.TaskStatusExhausted, wantRetries: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := &testutil.MockCache{
				DeleteFunc: func(_ context.Context, _, _ string) error {
					return errors.New("permission denied")
				},
				GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
					return nil, nil, errors.New("not needed")
				},
			}
			env := newTestWorkerEnvWithMockCache(t, mc)
			_, objID, versionID := seedStoredObject(t, env)

			task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, tc.retryCount)

			evictor := newAfterUploadEvictor(env, 1, 50*time.Millisecond)
			if tc.wantStatus == model.TaskStatusScheduled {
				runWorkerUntilTaskRetryCount(t, env, evictor, task.ID, tc.wantRetries, 5*time.Second)
			} else {
				runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)
			}

			got, err := env.repos.Tasks.GetByID(context.Background(), task.ID)
			if err != nil || got == nil {
				t.Fatalf("get task after cache delete failure: task=%v err=%v", got, err)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("expected task %s after cache delete failure, got %s", tc.wantStatus, got.Status)
			}
			if got.RetryCount != tc.wantRetries {
				t.Errorf("expected retry_count=%d, got %d", tc.wantRetries, got.RetryCount)
			}

			obj, err := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
			if err != nil || obj == nil {
				t.Fatalf("get object after cache delete failure: object=%v err=%v", obj, err)
			}
			if obj.State != model.ObjectStateStored {
				t.Errorf("expected object state stored after cache delete failure, got %s", obj.State)
			}
			if !obj.InCache {
				t.Error("expected object cache location to remain true after cache delete failure")
			}
		})
	}
}
