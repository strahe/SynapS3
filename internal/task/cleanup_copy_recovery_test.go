package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestCleanupContinuesAcrossDeletedCopies(t *testing.T) {
	var mu sync.Mutex
	deleted := map[string]int{}
	client := &testutil.MockStorageClient{
		OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePiece: func(_ context.Context, piece sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				mu.Lock()
				defer mu.Unlock()
				deleted[piece.String()]++
				return &sdktypes.WriteResult{Hash: common.HexToHash("0x1")}, nil
			}}, nil
		},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: client, deletionState: func(_ context.Context, _, piece sdktypes.BigInt) (synapse.CleanupPieceState, error) {
		mu.Lock()
		defer mu.Unlock()
		return synapse.CleanupPieceState{Live: deleted[piece.String()] == 0}, nil
	}})
	content, row := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending, model.StorageCleanupCopyStatusPending)
	for range 5 {
		status := model.TaskStatusPending
		if lenDeleted := func() int { mu.Lock(); defer mu.Unlock(); return len(deleted) }(); lenDeleted == 2 {
			status = model.TaskStatusCompleted
		}
		row = runOneStorageTask(t, runtime, row, status)
		if row.Status == model.TaskStatusCompleted {
			break
		}
		if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if row.Status != model.TaskStatusCompleted || row.RetryCount != 0 || len(deleted) != 2 {
		t.Fatalf("cleanup = %#v, requests = %v", row, deleted)
	}
	for piece, calls := range deleted {
		if calls != 1 {
			t.Fatalf("piece %s deleted %d times", piece, calls)
		}
	}
	if current, err := runtime.repos.Contents.GetByID(t.Context(), content.ID); err != nil || current != nil {
		t.Fatalf("content after cleanup = %#v, %v", current, err)
	}
}

func TestCleanupDropsPrecedingCheckpointWithoutResettingNextClock(t *testing.T) {
	for _, age := range []time.Duration{23 * time.Hour, 25 * time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
				return synapse.CleanupPieceState{Live: true}, nil
			}})
			content, row := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusRemoved, model.StorageCleanupCopyStatusPending)
			copies, err := runtime.repos.StorageCleanup.AuthorizeTask(t.Context(), content.ID, content.CleanupGeneration, row.ID)
			if err != nil || len(copies) != 2 {
				t.Fatalf("cleanup copies = %#v, %v", copies, err)
			}
			if err := runtime.repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), copies[1].ID, common.HexToHash("0x1").Hex()); err != nil {
				t.Fatal(err)
			}
			started := time.Now().UTC().Add(-age)
			runtimeState := model.TaskRuntime{OperationKey: fmt.Sprintf("cleanup:%d", copies[1].ID), OperationStartedAt: &started, LastAdmittedAttempt: 1}
			rawRuntime, err := json.Marshal(runtimeState)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("resume_mode = ?", model.TaskResumeModeRecover).
				Set("runtime_json = ?", json.RawMessage(rawRuntime)).
				Set("checkpoint_json = ?", json.RawMessage(fmt.Sprintf(`{"copy_id":%d,"attempted_at":%q}`, copies[0].ID, started.Format(time.RFC3339Nano)))).
				Where("id = ?", row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			want := model.TaskStatusPending
			if age > 24*time.Hour {
				want = model.TaskStatusFailed
			}
			result := runOneStorageTask(t, runtime, row, want)
			var actual model.TaskRuntime
			var checkpoint struct {
				CopyID int64 `json:"copy_id"`
			}
			if err := json.Unmarshal(result.Runtime, &actual); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(result.Checkpoint, &checkpoint); err != nil {
				t.Fatal(err)
			}
			if actual.OperationKey != runtimeState.OperationKey || actual.OperationStartedAt == nil || !actual.OperationStartedAt.Equal(started) || actual.LastAdmittedAttempt != 1 || checkpoint.CopyID != 0 || result.RetryCount != 0 {
				t.Fatalf("next operation was reset: task=%#v runtime=%#v checkpoint=%#v", result, actual, checkpoint)
			}
			if want == model.TaskStatusFailed && (result.FailureReason == nil || *result.FailureReason != "cleanup_outcome_unknown") {
				t.Fatalf("expired next-copy observation = %#v", result)
			}
		})
	}
}
