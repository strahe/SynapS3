package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
)

func TestStoreSessionRecoveryPreservesOriginalRequest(t *testing.T) {
	for _, scenario := range []string{"lost PUT response", "busy upload", "incomplete upload claim", "complete upload claim", "missing session", "missing session with published piece", "unreachable original URL"} {
		t.Run(scenario, func(t *testing.T) {
			payload := strings.Repeat("s", 128)
			originalURL := "https://store.example"
			var ready atomic.Bool
			var queryUnavailable atomic.Bool
			var creates, uploads, finalizes, opens atomic.Int64
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				opens.Add(1)
				return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
			}}
			parked := parkedPieceCheckerFunc(func(_ context.Context, url string, _ cid.Cid) (synapse.ParkedPieceState, error) {
				if queryUnavailable.Load() {
					return "", errors.New("provider status temporarily unavailable")
				}
				if scenario == "unreachable original URL" && url == originalURL {
					return "", errors.New("original provider endpoint is unavailable")
				}
				if ready.Load() {
					return synapse.ParkedPieceReady, nil
				}
				return synapse.ParkedPieceMissing, nil
			})
			original := "00000000-0000-4000-8000-000000000001"
			fresh := "00000000-0000-4000-8000-000000000002"
			target := &testutil.MockStorageTarget{ServiceURLValue: originalURL}
			target.CreateStoreSessionFunc = func(context.Context) (string, error) { creates.Add(1); return fresh, nil }
			target.UploadStoreSessionFunc = func(_ context.Context, url, session string, body io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
				upload := uploads.Add(1)
				if url != originalURL {
					t.Errorf("session moved to %q", url)
				}
				if scenario == "busy upload" || (scenario == "incomplete upload claim" || scenario == "complete upload claim") && upload == 2 {
					if session != original {
						t.Errorf("busy upload changed session to %q", session)
					}
					return nil, errors.Join(synapse.ErrStoreSessionClaimed, &pdp.HTTPError{StatusCode: 409, Body: "Data has already been uploaded"})
				}
				if (scenario == "incomplete upload claim" || scenario == "complete upload claim") && upload == 1 {
					if session != original {
						t.Errorf("first upload changed session to %q", session)
					}
					if scenario == "incomplete upload claim" {
						return nil, errors.Join(errors.New("upload response lost"), synapse.ErrStoreUploadTruncated)
					}
					if _, err := io.Copy(io.Discard, body); err != nil {
						return nil, err
					}
					return nil, errors.New("upload response lost")
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
				if scenario == "busy upload" || (scenario == "incomplete upload claim" || scenario == "complete upload claim") && session == original {
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
			retries := 3
			if scenario == "unreachable original URL" {
				retries = 0
			}
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, retries, payload, parked, cacheStore, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, originalURL, time.Now())
			if scenario == "incomplete upload claim" || scenario == "complete upload claim" {
				if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = jsonb_set(checkpoint_json, '{uploads_incomplete}', 'true') WHERE id = ?", row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "busy upload" || scenario == "unreachable original URL" {
				target.ServiceURLValue = "https://changed-store.example"
			}
			if scenario == "unreachable original URL" {
				for range 2 {
					row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
					if row.FailureReason == nil || *row.FailureReason != "attempts_exhausted" {
						t.Fatalf("unreachable provider failure lost its bounded retry: %+v", row)
					}
					var err error
					row, err = runtime.service.Retry(t.Context(), row.ID)
					if err != nil {
						t.Fatal(err)
					}
					var evidence map[string]any
					if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil || evidence["session_id"] != original || evidence["provider_service_url"] != originalURL {
						t.Fatalf("manual retry discarded unresolved session identity: %s, err=%v", row.Checkpoint, err)
					}
				}
				ready.Store(true)
				runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
				if creates.Load()+uploads.Load()+finalizes.Load()+opens.Load() != 0 {
					t.Fatal("unreachable original endpoint caused retransmission or prevented current ready recovery")
				}
				return
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if row.ResumeMode != model.TaskResumeModeExecute || creates.Load()+uploads.Load()+finalizes.Load() != 0 {
				t.Fatal("recovery issued a mutating request")
			}
			if scenario == "lost PUT response" || scenario == "missing session with published piece" {
				if scenario == "lost PUT response" {
					old := time.Now().UTC().Add(-45 * time.Minute).Format(time.RFC3339Nano)
					if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = jsonb_set(checkpoint_json, '{observation_started_at}', to_jsonb(?::text)) WHERE id = ?", old, row.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
				if creates.Load() != 0 || uploads.Load() != 0 || finalizes.Load() != 1 || opens.Load() != 0 {
					t.Fatal("confirmed upload created another session or retransferred bytes")
				}
				return
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			switch scenario {
			case "busy upload":
				if row.RetryCount != 1 || creates.Load() != 0 || uploads.Load() != 1 || finalizes.Load() != 1 {
					t.Fatal("busy session bypassed effect admission or opened a new session")
				}
				var evidence struct {
					Session string    `json:"session_id"`
					URL     string    `json:"provider_service_url"`
					Started time.Time `json:"observation_started_at"`
				}
				if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil || evidence.Session != original || evidence.URL != originalURL || !evidence.Started.IsZero() || row.FailureReason == nil || *row.FailureReason != "store_resend_required" || row.LastError == nil || (!strings.Contains(*row.LastError, "HTTP 409: Data has already been uploaded") || !strings.Contains(*row.LastError, synapse.ErrStoreSessionClaimed.Error())) {
					t.Fatalf("busy recovery lost session or HTTP evidence: checkpoint=%+v task=%+v err=%v", evidence, row, err)
				}
				for retry := 1; retry <= 3; retry++ {
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
			case "incomplete upload claim", "complete upload claim":
				var evidence struct {
					Session    string    `json:"session_id"`
					Stage      string    `json:"stage"`
					Incomplete bool      `json:"uploads_incomplete"`
					Started    time.Time `json:"observation_started_at"`
				}
				incomplete := scenario == "incomplete upload claim"
				if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil || evidence.Session != original || evidence.Incomplete != incomplete || !evidence.Started.IsZero() || row.RetryCount != 1 || row.FailureReason == nil || *row.FailureReason != "provider_confirmation" {
					t.Fatalf("upload result lost completion evidence: checkpoint=%+v task=%+v err=%v", evidence, row, err)
				}
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				stage, reason := "upload", "store_resend_required"
				if incomplete {
					stage, reason = "expired", "provider_confirmation"
				}
				if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil || evidence.Session != original || evidence.Stage != stage || evidence.Incomplete != incomplete || !evidence.Started.IsZero() || row.RetryCount != 2 || row.FailureReason == nil || *row.FailureReason != reason || row.LastError == nil || (!strings.Contains(*row.LastError, "HTTP 409: Data has already been uploaded") || !strings.Contains(*row.LastError, synapse.ErrStoreSessionClaimed.Error())) || creates.Load() != 0 || uploads.Load() != 2 {
					t.Fatalf("claimed upload ignored completion evidence: checkpoint=%+v task=%+v create=%d uploads=%d err=%v", evidence, row, creates.Load(), uploads.Load(), err)
				}
				if !incomplete {
					return
				}
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
				if creates.Load() != 1 || uploads.Load() != 3 || finalizes.Load() != 3 {
					t.Fatalf("incomplete session recovery requests: create=%d uploads=%d finalize=%d", creates.Load(), uploads.Load(), finalizes.Load())
				}
				return
			case "missing session":
				if row.RetryCount != 1 || creates.Load() != 0 || uploads.Load() != 0 {
					t.Fatal("missing session was replaced without a retry opportunity")
				}
				queryUnavailable.Store(true)
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				var expired struct {
					Stage   string    `json:"stage"`
					Session string    `json:"session_id"`
					Started time.Time `json:"observation_started_at"`
				}
				if err := json.Unmarshal(row.Checkpoint, &expired); err != nil || expired.Stage != "expired" || expired.Session != original || !expired.Started.IsZero() || row.RetryCount != 2 {
					t.Fatalf("expired session query failure invented pending work: checkpoint=%+v retry=%d err=%v", expired, row.RetryCount, err)
				}
				if creates.Load()+uploads.Load() != 0 || finalizes.Load() != 1 {
					t.Fatal("expired session query failure issued a mutating request")
				}
				queryUnavailable.Store(false)
				old := time.Now().UTC().Add(-45 * time.Minute).Format(time.RFC3339Nano)
				if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = jsonb_set(checkpoint_json, '{observation_started_at}', to_jsonb(?::text)) WHERE id = ?", old, row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				if row.ResumeMode != model.TaskResumeModeExecute || row.RetryCount != 2 || creates.Load()+uploads.Load() != 0 {
					t.Fatal("confirmed expired session was blocked by an unrelated observation clock")
				}
				runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
				if creates.Load() != 1 || uploads.Load() != 1 {
					t.Fatal("confirmed missing session did not resume with one new session")
				}
				return
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
		finalizes int64
		exhausted bool
		existing  bool
	}{
		{name: "before create", condition: "NEW.checkpoint_json->>'stage' = 'create' AND OLD.checkpoint_json->>'stage' = 'create'"},
		{name: "session identity", condition: "NEW.checkpoint_json->>'session_id' <> '' AND COALESCE(OLD.checkpoint_json->>'session_id', '') = ''", creates: 1},
		{name: "before upload", condition: "NEW.checkpoint_json->>'ingress_attempt' = '1' AND COALESCE(OLD.checkpoint_json->>'ingress_attempt', '0') = '0'", creates: 1, exhausted: true},
		{name: "before finalize", condition: "NEW.checkpoint_json->>'stage' = 'finalize' AND OLD.checkpoint_json->>'stage' = 'upload'", creates: 1, uploads: 1},
		{name: "resume admission", condition: "OLD.checkpoint_json->>'observation_started_at' IS NOT NULL AND (NEW.checkpoint_json->>'observation_started_at' IS NULL OR NEW.checkpoint_json->>'observation_started_at' = '0001-01-01T00:00:00Z')", existing: true},
		{name: "before resume finalize", condition: "NEW.checkpoint_json->>'stage' = 'finalize' AND OLD.checkpoint_json->>'stage' = 'upload'", existing: true},
		{name: "published observation", condition: "NEW.checkpoint_json->>'stage' = 'observe' AND OLD.checkpoint_json->>'stage' = 'finalize'", creates: 1, uploads: 1, finalizes: 1},
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
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, retries, payload, parked, cacheStore, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			var oldClock time.Time
			if scenario.existing {
				oldClock = time.Now().UTC().Add(-45 * time.Minute)
				seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), oldClock)
				if _, err := runtime.db.NewRaw("UPDATE tasks SET resume_mode = 'execute' WHERE id = ?", row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
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
			if creates.Load() != scenario.creates || uploads.Load() != scenario.uploads || finalizes.Load() != scenario.finalizes {
				t.Fatalf("request passed failed checkpoint: create=%d upload=%d finalize=%d", creates.Load(), uploads.Load(), finalizes.Load())
			}
			if row.RetryCount != retryCount || row.ResumeMode != model.TaskResumeModeRecover || len(row.Checkpoint) == 0 {
				t.Fatalf("checkpoint failure lost recovery: %+v", row)
			}
			var evidence struct {
				SessionID            string    `json:"session_id"`
				IngressAttempt       int       `json:"ingress_attempt"`
				ObservationStartedAt time.Time `json:"observation_started_at"`
			}
			if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil {
				t.Fatal(err)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow == nil {
				t.Fatalf("read copy progress after checkpoint failure: %v", err)
			}
			wantAttempt := int(scenario.uploads)
			if scenario.existing {
				wantAttempt++
			}
			if evidence.IngressAttempt != copyRow.IngressStoreAttempt || copyRow.IngressStoreAttempt != wantAttempt {
				t.Errorf("checkpoint failure changed upload attempt without its progress: checkpoint=%d copy=%d uploads=%d", evidence.IngressAttempt, copyRow.IngressStoreAttempt, scenario.uploads)
			}
			if scenario.creates+scenario.uploads+scenario.finalizes == 0 {
				if !evidence.ObservationStartedAt.Equal(oldClock) {
					t.Fatalf("request preparation failure changed the observation window: old=%v checkpoint=%v", oldClock, evidence.ObservationStartedAt)
				}
			} else if !evidence.ObservationStartedAt.IsZero() {
				t.Fatalf("request result began observation before provider confirmation: %v", evidence.ObservationStartedAt)
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
			if !evidence.AttemptedAt.Equal(old) {
				t.Fatalf("provider query changed upload admission: %+v", evidence)
			}
			if unknown {
				if !started.IsZero() || row.RetryCount != 1 || row.FailureReason == nil || *row.FailureReason != "store_check_failed" {
					t.Fatalf("failed provider query began observation or lost retry: checkpoint=%+v task=%+v", evidence, row)
				}
			} else if started.Before(before) || started.After(time.Now().UTC()) || row.RetryCount != 0 {
				t.Fatalf("provider processing did not begin observation without retry: checkpoint=%+v task=%+v", evidence, row)
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil || !evidence.ObservationStartedAt.Equal(started) {
				t.Fatalf("automatic recovery reset observation start: %+v, err=%v", evidence, err)
			}
			if unknown && (row.RetryCount != 2 || row.FailureReason == nil || *row.FailureReason != "store_check_failed") {
				t.Fatalf("provider query failures did not consume retries: %+v", row)
			}
		})
	}
	for _, outcome := range []string{"unknown", "processing", "contention", "canceled invocation", "canceled settlement rollback"} {
		t.Run("resubmission/"+outcome, func(t *testing.T) {
			payload := strings.Repeat("o", 128)
			var state atomic.Value
			state.Store(synapse.ParkedPieceMissing)
			var uploads, finalizes atomic.Int64
			requestEnded := make(chan time.Time, 1)
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
			}}
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				return state.Load().(synapse.ParkedPieceState), nil
			})
			var runtime handlerTestRuntime
			var taskID int64
			assertObservationPaused := func() {
				inFlight, err := runtime.repos.Tasks.GetByID(t.Context(), taskID)
				if err != nil || inFlight == nil {
					t.Errorf("read admitted upload: %v", err)
					return
				}
				var clock struct {
					Started time.Time `json:"observation_started_at"`
				}
				if err := json.Unmarshal(inFlight.Checkpoint, &clock); err != nil || !clock.Started.IsZero() {
					t.Errorf("upload began with an active observation window: %v, err=%v", clock.Started, err)
				}
			}
			target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
			target.FinalizeStoreSessionFunc = func(context.Context, string, string, cid.Cid) error {
				assertObservationPaused()
				if finalizes.Add(1) == 1 {
					if outcome == "contention" {
						state.Store(synapse.ParkedPieceProcessing)
						requestEnded <- time.Now().UTC()
					}
					return synapse.ErrStoreSessionIncomplete
				}
				return nil
			}
			target.UploadStoreSessionFunc = func(ctx context.Context, url, session string, body io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
				uploads.Add(1)
				assertObservationPaused()
				if url != target.ServiceURL() || session != "00000000-0000-4000-8000-000000000001" {
					t.Errorf("resubmission changed upload identity: %s %s", url, session)
				}
				defer func() { requestEnded <- time.Now().UTC() }()
				state.Store(synapse.ParkedPieceProcessing)
				switch outcome {
				case "unknown":
					return nil, errors.New("response lost during upload")
				case "canceled invocation", "canceled settlement rollback":
					<-ctx.Done()
					return nil, ctx.Err()
				default:
					data, err := io.ReadAll(body)
					return &storage.StoreResult{PieceCID: opts.PieceCID, Size: int64(len(data))}, err
				}
			}
			var pipeline seededCopyPipeline
			var pieceCID cid.Cid
			runtime, pipeline, pieceCID = storeRecoveryFixture(t, 3, payload, parked, cacheStore, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			taskID = row.ID
			old := time.Now().UTC().Add(-45 * time.Minute)
			seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), old)
			if _, err := runtime.db.NewRaw("UPDATE tasks SET resume_mode = 'execute' WHERE id = ?", row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if outcome == "contention" {
				for _, statement := range []string{
					"CREATE SEQUENCE test_store_checkpoint_contention",
					"CREATE FUNCTION test_store_checkpoint_contention() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('test_store_checkpoint_contention') <= 20 THEN RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'checkpoint transaction contention'; END IF; RETURN NEW; END $$",
					fmt.Sprintf("CREATE TRIGGER test_store_checkpoint_contention BEFORE UPDATE OF checkpoint_json ON tasks FOR EACH ROW WHEN (NEW.id = %d AND NEW.checkpoint_json->>'stage' = 'upload' AND OLD.checkpoint_json->>'stage' = 'finalize' AND NEW.checkpoint_json->>'ingress_attempt' = '2') EXECUTE FUNCTION test_store_checkpoint_contention()", row.ID),
				} {
					if _, err := runtime.db.ExecContext(t.Context(), statement); err != nil {
						t.Fatal(err)
					}
				}
				defer func() {
					for _, statement := range []string{
						"DROP TRIGGER test_store_checkpoint_contention ON tasks",
						"DROP FUNCTION test_store_checkpoint_contention()",
						"DROP SEQUENCE test_store_checkpoint_contention",
					} {
						if _, err := runtime.db.ExecContext(t.Context(), statement); err != nil {
							t.Fatal(err)
						}
					}
				}()
			}
			if strings.HasPrefix(outcome, "canceled") {
				registry := taskengine.NewRegistry()
				for _, taskType := range runtime.registry.Types() {
					handler, _ := runtime.registry.Handler(taskType)
					definition, _ := runtime.registry.Definition(taskType)
					if taskType == model.TaskTypeStorageStore {
						definition.Policy.InvocationTimeout = handlerTestLeaseDuration
					}
					if err := registry.Register(handlerPolicyFixture{Handler: handler, definition: definition}); err != nil {
						t.Fatal(err)
					}
				}
				runtime.registry = registry
			}
			if outcome == "canceled settlement rollback" {
				var excluded []int64
				if err := runtime.db.NewSelect().Model((*model.Task)(nil)).Column("id").Where("id <> ?", row.ID).Scan(t.Context(), &excluded); err != nil {
					t.Fatal(err)
				}
				repos := *runtime.repos
				repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1, excludedTaskIDs: excluded}
				engine, err := taskengine.NewEngine(taskengine.EngineConfig{
					Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
				}, &repos, handlerExecutionRegistry(t, runtime), slog.Default())
				if err != nil {
					t.Fatal(err)
				}
				restore := testutil.FailWrites(t, runtime.db, "tasks", "UPDATE", fmt.Sprintf("NEW.id = %d AND OLD.status = 'running' AND NEW.status = 'pending'", row.ID), "observation settlement rejected")
				restored := false
				defer func() {
					if !restored {
						restore()
					}
				}()
				cancel, done := runEngine(t, engine)
				defer stopHandlerEngine(t, cancel, done)
				row = waitForTask(t, runtime.repos, row.ID, func(current *model.Task) bool {
					return current.Status == model.TaskStatusRunning && current.ResumeMode == model.TaskResumeModeRecover && current.LeaseUntil != nil && current.LeaseUntil.Before(time.Now().Add(2*handlerTestPollInterval))
				}, 3*handlerTestLeaseDuration)
				stopHandlerEngine(t, cancel, done)
				var beforeRecovery struct {
					Started time.Time `json:"observation_started_at"`
					Session string    `json:"session_id"`
				}
				if err := json.Unmarshal(row.Checkpoint, &beforeRecovery); err != nil || !beforeRecovery.Started.IsZero() || beforeRecovery.Session != "00000000-0000-4000-8000-000000000001" {
					t.Fatalf("rolled-back settlement lost admission evidence: %+v, err=%v", beforeRecovery, err)
				}
				restore()
				restored = true
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			} else {
				row = runStorageTaskClaims(t, runtime, row, model.TaskStatusPending, 1, 3*handlerTestLeaseDuration)
			}
			var finished time.Time
			select {
			case finished = <-requestEnded:
			default:
				t.Fatal("resubmission did not reach its upload request")
			}
			var clock struct {
				Started time.Time `json:"observation_started_at"`
			}
			if err := json.Unmarshal(row.Checkpoint, &clock); err != nil {
				t.Fatal(err)
			}
			observed := outcome == "processing" || outcome == "canceled settlement rollback"
			if observed && (clock.Started.Before(finished) || clock.Started.After(time.Now().UTC())) || !observed && !clock.Started.IsZero() {
				t.Fatalf("upload result observation clock: outcome=%s started=%v finished=%v", outcome, clock.Started, finished)
			}
			wantUploads := int64(1)
			if outcome == "contention" {
				wantUploads = 0
			}
			if row.ResumeMode != model.TaskResumeModeRecover || uploads.Load() != wantUploads {
				t.Fatalf("resubmission did not enter recovery: task=%+v uploads=%d", row, uploads.Load())
			}
			if outcome == "contention" && (row.RetryCount != 0 || row.LastError != nil) {
				t.Fatalf("database contention consumed a retry or returned an error-bearing wait: %+v", row)
			}
			if outcome == "contention" {
				copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
				if err != nil || copyRow.IngressStoreAttempt != 1 || row.WaitReason == nil || *row.WaitReason != "database_contention" {
					t.Fatalf("contended upload checkpoint changed progress or wait semantics: copy=%+v task=%+v err=%v", copyRow, row, err)
				}
			}
			if !observed {
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				if err := json.Unmarshal(row.Checkpoint, &clock); err != nil || clock.Started.Before(finished) || clock.Started.After(time.Now().UTC()) || uploads.Load() != wantUploads {
					t.Fatalf("provider confirmation did not start observation after request completion: clock=%v uploads=%d err=%v", clock.Started, uploads.Load(), err)
				}
			}
			started := clock.Started
			wakeTask(t, runtime, row.ID)
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if err := json.Unmarshal(row.Checkpoint, &clock); err != nil || !clock.Started.Equal(started) || uploads.Load() != wantUploads {
				t.Fatalf("provider query refreshed observation or resent bytes: clock=%v uploads=%d err=%v", clock.Started, uploads.Load(), err)
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
