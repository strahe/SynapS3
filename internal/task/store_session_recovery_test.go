package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
)

func TestStoreSessionRecoveryPreservesOriginalRequest(t *testing.T) {
	for _, scenario := range []string{"lost PUT response", "busy upload", "missing session", "missing session with published piece"} {
		t.Run(scenario, func(t *testing.T) {
			payload := strings.Repeat("s", 128)
			var ready atomic.Bool
			var creates, uploads, finalizes, opens atomic.Int64
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				opens.Add(1)
				return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
			}}
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				if ready.Load() {
					return synapse.ParkedPieceReady, nil
				}
				return synapse.ParkedPieceMissing, nil
			})
			original := "00000000-0000-4000-8000-000000000001"
			fresh := "00000000-0000-4000-8000-000000000002"
			originalURL := "https://store.example"
			target := &testutil.MockStorageTarget{ServiceURLValue: originalURL}
			target.CreateStoreSessionFunc = func(context.Context) (string, error) { creates.Add(1); return fresh, nil }
			target.UploadStoreSessionFunc = func(_ context.Context, url, session string, body io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
				uploads.Add(1)
				if url != originalURL {
					t.Errorf("session moved to %q", url)
				}
				if scenario == "busy upload" {
					if session != original {
						t.Errorf("busy upload changed session to %q", session)
					}
					return nil, synapse.ErrStoreSessionClaimed
				}
				if session != fresh {
					t.Errorf("replacement upload used %q", session)
				}
				data, err := io.ReadAll(body)
				return &storage.StoreResult{PieceCID: opts.PieceCID, Size: int64(len(data))}, err
			}
			target.FinalizeStoreSessionFunc = func(_ context.Context, url, session string, _ cid.Cid) error {
				finalizes.Add(1)
				if url != originalURL {
					t.Errorf("finalize moved to %q", url)
				}
				if scenario == "busy upload" {
					return synapse.ErrStoreSessionIncomplete
				}
				if scenario == "missing session" && session == original {
					return synapse.ErrStoreSessionMissing
				}
				if scenario == "missing session with published piece" {
					ready.Store(true)
					return synapse.ErrStoreSessionMissing
				}
				ready.Store(true)
				return nil
			}
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 3, payload, parked, cacheStore, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, originalURL, time.Now())
			if scenario == "busy upload" {
				target.ServiceURLValue = "https://changed-store.example"
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if row.ResumeMode != model.TaskResumeModeExecute || creates.Load()+uploads.Load()+finalizes.Load() != 0 {
				t.Fatal("recovery issued a mutating request")
			}
			if scenario == "missing session with published piece" {
				runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
				if creates.Load() != 0 || uploads.Load() != 0 || finalizes.Load() != 1 || opens.Load() != 0 {
					t.Fatal("published piece after session expiry created a replacement upload")
				}
				return
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			switch scenario {
			case "lost PUT response":
				if creates.Load() != 0 || uploads.Load() != 0 || finalizes.Load() != 1 || opens.Load() != 0 {
					t.Fatal("finalize recovery retransferred bytes")
				}
			case "busy upload":
				var evidence map[string]any
				if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil {
					t.Fatal(err)
				}
				clock := evidence["observation_started_at"]
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				if row.RetryCount != 1 || creates.Load() != 0 || uploads.Load() != 1 || finalizes.Load() != 1 {
					t.Fatal("busy session bypassed effect admission or opened a new session")
				}
				if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil {
					t.Fatal(err)
				}
				if evidence["session_id"] != original || evidence["provider_service_url"] != originalURL || evidence["observation_started_at"] != clock {
					t.Fatal("busy recovery changed identity or observation clock")
				}
				for retry := 1; retry <= 3; retry++ {
					row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
					row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
					row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
					status := model.TaskStatusPending
					if retry == 3 {
						status = model.TaskStatusFailed
					}
					row = runOneStorageTask(t, runtime, row, status)
				}
				if row.FailureReason == nil || *row.FailureReason != "attempts_exhausted" || row.RetryCount != 3 || creates.Load() != 0 || uploads.Load() != 4 || finalizes.Load() != 4 {
					t.Fatalf("busy upload budget: task=%+v requests=%d/%d/%d", row, creates.Load(), uploads.Load(), finalizes.Load())
				}
				return
			case "missing session":
				if row.RetryCount != 1 || creates.Load() != 0 || uploads.Load() != 0 {
					t.Fatal("missing session was replaced without a retry opportunity")
				}
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				if creates.Load() != 1 || uploads.Load() != 1 {
					t.Fatal("confirmed missing session did not resume with one new session")
				}
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
			if row.Status != model.TaskStatusCompleted {
				t.Fatal("confirmed session did not complete")
			}
		})
	}
}

func TestStoreSessionCheckpointFailureStopsNextRequest(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		condition string
		creates   int64
		uploads   int64
		exhausted bool
	}{
		{name: "before create", condition: "NEW.checkpoint_json->>'stage' = 'create' AND OLD.checkpoint_json->>'stage' = 'create'"},
		{name: "session identity", condition: "NEW.checkpoint_json->>'session_id' <> '' AND COALESCE(OLD.checkpoint_json->>'session_id', '') = ''", creates: 1},
		{name: "before upload", condition: "NEW.checkpoint_json->>'ingress_attempt' = '1' AND COALESCE(OLD.checkpoint_json->>'ingress_attempt', '0') = '0'", creates: 1, exhausted: true},
		{name: "before finalize", condition: "NEW.checkpoint_json->>'stage' = 'finalize' AND OLD.checkpoint_json->>'stage' = 'upload'", creates: 1, uploads: 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			payload := strings.Repeat("c", 128)
			var creates, uploads, finalizes atomic.Int64
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
			}}
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				return synapse.ParkedPieceMissing, nil
			})
			target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
			target.CreateStoreSessionFunc = func(context.Context) (string, error) {
				creates.Add(1)
				return "00000000-0000-4000-8000-000000000001", nil
			}
			target.UploadStoreSessionFunc = func(_ context.Context, _, _ string, body io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
				uploads.Add(1)
				data, err := io.ReadAll(body)
				return &storage.StoreResult{PieceCID: opts.PieceCID, Size: int64(len(data))}, err
			}
			target.FinalizeStoreSessionFunc = func(context.Context, string, string, cid.Cid) error {
				finalizes.Add(1)
				return nil
			}
			retries := 3
			status, retryCount := model.TaskStatusPending, 1
			if scenario.exhausted {
				retries, status, retryCount = 0, model.TaskStatusFailed, 0
			}
			runtime, pipeline, _ := storeRecoveryFixture(t, retries, payload, parked, cacheStore, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			if _, err := runtime.db.ExecContext(t.Context(), "CREATE SEQUENCE test_store_stage_writes"); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := runtime.db.ExecContext(t.Context(), "DROP SEQUENCE test_store_stage_writes"); err != nil {
					t.Fatal(err)
				}
			}()
			condition := fmt.Sprintf("CASE WHEN NEW.id = %d AND (%s) THEN nextval('test_store_stage_writes') = 1 ELSE false END", row.ID, scenario.condition)
			restore := testutil.FailWrites(t, runtime.db, "tasks", "UPDATE OF checkpoint_json", condition, "temporary session checkpoint failure")
			defer restore()
			row = runOneStorageTask(t, runtime, row, status)
			if creates.Load() != scenario.creates || uploads.Load() != scenario.uploads || finalizes.Load() != 0 {
				t.Fatalf("request passed failed checkpoint: create=%d upload=%d finalize=%d", creates.Load(), uploads.Load(), finalizes.Load())
			}
			if row.RetryCount != retryCount || row.ResumeMode != model.TaskResumeModeRecover || len(row.Checkpoint) == 0 {
				t.Fatalf("checkpoint failure lost recovery: %+v", row)
			}
			var evidence struct {
				SessionID      string `json:"session_id"`
				IngressAttempt int    `json:"ingress_attempt"`
			}
			if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil {
				t.Fatal(err)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow == nil {
				t.Fatalf("read copy progress after checkpoint failure: %v", err)
			}
			if evidence.IngressAttempt != copyRow.IngressStoreAttempt || copyRow.IngressStoreAttempt != int(scenario.uploads) {
				t.Errorf("checkpoint failure changed upload attempt without its progress: checkpoint=%d copy=%d uploads=%d", evidence.IngressAttempt, copyRow.IngressStoreAttempt, scenario.uploads)
			}
			if scenario.exhausted {
				if row.FailureReason == nil || *row.FailureReason != "attempts_exhausted" {
					t.Fatalf("checkpoint failure did not exhaust the automatic budget: %+v", row)
				}
				child, err := runtime.service.Retry(t.Context(), row.ID)
				if err != nil {
					t.Fatalf("manual retry after checkpoint failure: %v", err)
				}
				var inherited struct {
					SessionID      string `json:"session_id"`
					IngressAttempt int    `json:"ingress_attempt"`
				}
				if err := json.Unmarshal(child.Checkpoint, &inherited); err != nil {
					t.Fatal(err)
				}
				if inherited.SessionID == "" || inherited.SessionID != evidence.SessionID || inherited.IngressAttempt != copyRow.IngressStoreAttempt || child.ResumeMode != model.TaskResumeModeRecover || child.RetryCount != 0 {
					t.Fatalf("manual retry lost the saved upload session: checkpoint=%+v task=%+v", inherited, child)
				}
			}
		})
	}
}

func TestStoreSessionObservationStartsAfterUploadAdmission(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown=%t", unknown), func(t *testing.T) {
			payload := strings.Repeat("o", 128)
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				if unknown {
					return "", errors.New("provider status unavailable")
				}
				return synapse.ParkedPieceProcessing, nil
			})
			target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 3, payload, parked, nil, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			old := time.Now().UTC().Add(-time.Hour)
			seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), old)
			rawRuntime, err := json.Marshal(map[string]any{"operation_key": "store:" + pieceCID.String(), "operation_started_at": old})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = checkpoint_json - 'observation_started_at', runtime_json = ? WHERE id = ?", json.RawMessage(rawRuntime), row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			before := time.Now().UTC()
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			var evidence struct {
				AttemptedAt          time.Time `json:"attempted_at"`
				ObservationStartedAt time.Time `json:"observation_started_at"`
			}
			if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil {
				t.Fatal(err)
			}
			started := evidence.ObservationStartedAt
			if started.Before(before) || started.After(time.Now().UTC()) || !evidence.AttemptedAt.Equal(old) {
				t.Fatalf("upload admission consumed observation window: %+v", evidence)
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil || !evidence.ObservationStartedAt.Equal(started) {
				t.Fatalf("automatic recovery reset observation start: %+v, err=%v", evidence, err)
			}
		})
	}
}

func TestStoreFinalizeLostResponseCompletesWithoutReplay(t *testing.T) {
	payload := strings.Repeat("f", 128)
	var ready atomic.Bool
	var creates, uploads, finalizes atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		if ready.Load() {
			return synapse.ParkedPieceReady, nil
		}
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
	target.CreateStoreSessionFunc = func(context.Context) (string, error) {
		creates.Add(1)
		return "00000000-0000-4000-8000-000000000001", nil
	}
	target.UploadStoreSessionFunc = func(_ context.Context, _, _ string, body io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
		uploads.Add(1)
		data, err := io.ReadAll(body)
		return &storage.StoreResult{PieceCID: opts.PieceCID, Size: int64(len(data))}, err
	}
	target.FinalizeStoreSessionFunc = func(context.Context, string, string, cid.Cid) error {
		finalizes.Add(1)
		ready.Store(true)
		return errors.New("response lost after publication")
	}
	runtime, pipeline, _ := storeRecoveryFixture(t, 3, payload, parked, cacheStore, target)
	row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if row.RetryCount != 1 || len(row.Checkpoint) == 0 {
		t.Fatal("unknown finalize result lost recovery evidence")
	}
	runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
	if creates.Load() != 1 || uploads.Load() != 1 || finalizes.Load() != 1 {
		t.Fatal("published upload was replayed")
	}
}
