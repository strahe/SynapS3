package task_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
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
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestStoreIntegrityBeforeProviderAccess(t *testing.T) {
	for _, mismatch := range []string{"checksum", "known CID", "previous attempt without checkpoint"} {
		t.Run(mismatch, func(t *testing.T) {
			payload := strings.Repeat("a", 128)
			cached := strings.Repeat("b", len(payload))
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				return io.NopCloser(strings.NewReader(cached)), &cache.ObjectInfo{Size: int64(len(cached))}, nil
			}}
			runtime, pipeline, _ := storeRecoveryFixture(t, 3, payload, nil, cacheStore, &testutil.MockStorageTarget{})
			checksumPayload := payload
			if mismatch == "known CID" {
				checksumPayload = cached
			}
			if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("checksum = ?", testutil.StorageChecksum(checksumPayload)).Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			var opened atomic.Int64
			runtime.storage.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				opened.Add(1)
				return &testutil.MockStorageTarget{}, nil
			}
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			if mismatch == "previous attempt without checkpoint" {
				if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("ingress_store_attempt = 1").Where("id = ?", pipeline.target.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			failed := runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			if opened.Load() != 0 || failed.FailureReason == nil || *failed.FailureReason != "store_identity_mismatch" || len(failed.Checkpoint) != 0 {
				t.Fatalf("provider opens=%d, task=%#v; corrupt cache must fail before provider access or checkpoint admission", opened.Load(), failed)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if mismatch == "previous attempt without checkpoint" {
				if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID || copyRow.IngressStoreAttempt != 1 {
					t.Fatalf("copy=%#v err=%v; an earlier attempt must retain its owner", copyRow, err)
				}
			} else if err != nil || copyRow.Status != model.StorageCopyStatusFailed || copyRow.IngressStoreAttempt != 0 {
				t.Fatalf("copy=%#v err=%v; no external attempt may be admitted", copyRow, err)
			}
		})
	}
}

func TestStoreUnverifiedCheckpointRequiresCacheEvidence(t *testing.T) {
	for _, scenario := range []string{"valid cache", "corrupt cache", "missing cache", "verified without cache"} {
		t.Run(scenario, func(t *testing.T) {
			payload := strings.Repeat("v", 128)
			var reads, opened atomic.Int64
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				reads.Add(1)
				if scenario == "missing cache" || scenario == "verified without cache" {
					return nil, nil, os.ErrNotExist
				}
				cached := payload
				if scenario == "corrupt cache" {
					cached = strings.Repeat("x", len(payload))
				}
				return io.NopCloser(strings.NewReader(cached)), &cache.ObjectInfo{Size: int64(len(cached))}, nil
			}}
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				return synapse.ParkedPieceProcessing, nil
			})
			target := &testutil.MockStorageTarget{ServiceURLValue: "https://integrity.example"}
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 3, payload, parked, cacheStore, target)
			checksum := testutil.StorageChecksum(payload)
			if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("checksum = ?", checksum).Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			runtime.storage.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				opened.Add(1)
				return target, nil
			}
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now())
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = checkpoint_json - 'verified_checksum' WHERE id = ?`, row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			identity := testStorageExecutionIdentity()
			raw, err := json.Marshal(identity)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = checkpoint_json || ?::jsonb WHERE id = ?`, string(raw), row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if scenario == "verified without cache" {
				if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = checkpoint_json || jsonb_build_object('verified_checksum', ?::text) WHERE id = ?`, checksum, row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			wantStatus := model.TaskStatusFailed
			if scenario == "valid cache" || scenario == "verified without cache" {
				wantStatus = model.TaskStatusPending
			}
			result := runOneStorageTask(t, runtime, row, wantStatus)
			var checkpoint struct {
				VerifiedChecksum string `json:"verified_checksum"`
				SessionID        string `json:"session_id"`
			}
			if err := json.Unmarshal(result.Checkpoint, &checkpoint); err != nil {
				t.Fatal(err)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID || copyRow.Status != model.StorageCopyStatusPending || checkpoint.SessionID == "" {
				t.Fatalf("copy=%#v task=%#v err=%v; interrupted effect must retain its owner and session", copyRow, result, err)
			}
			if wantStatus == model.TaskStatusPending {
				if checkpoint.VerifiedChecksum != checksum || opened.Load() != 1 {
					t.Fatalf("verified=%q opens=%d; expected read-only recovery with verified evidence", checkpoint.VerifiedChecksum, opened.Load())
				}
				if scenario == "verified without cache" && reads.Load() != 0 {
					t.Fatal("verified recovery unexpectedly required cache data")
				}
			} else {
				wantReason := "store_identity_mismatch"
				if scenario == "missing cache" {
					wantReason = "store_identity_unverified"
				}
				if checkpoint.VerifiedChecksum != "" || opened.Load() != 0 || result.FailureReason == nil || *result.FailureReason != wantReason {
					t.Fatalf("task=%#v opens=%d; unverified evidence must stop before provider access", result, opened.Load())
				}
			}
		})
	}
}
