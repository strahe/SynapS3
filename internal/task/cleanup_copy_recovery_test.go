package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storageremoval"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestCleanupAdoptsUnresolvedCopyEvidence(t *testing.T) {
	for _, kind := range []string{"scheduled", "pending hash", "unknown attempt"} {
		t.Run(kind, func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
			content, row := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
			var copyRow model.StorageCleanupCopy
			if err := runtime.db.NewSelect().Model(&copyRow).Where("content_id = ?", content.ID).Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			attempted := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
			hash := common.HexToHash("0x123").Hex()
			update := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Where("id = ?", copyRow.ID)
			if kind != "unknown attempt" {
				update.Set("delete_tx_hash = ?", hash).Set("scheduled_at = ?", attempted)
			}
			if kind == "scheduled" {
				update.Set("status = ?", model.StorageCleanupCopyStatusDeleteScheduled)
			}
			if kind != "unknown attempt" {
				if _, err := update.Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			cp := map[string]any{"copy_id": copyRow.ID, "attempted_at": attempted, "identity": runtime.storage.ContextIdentity(), "pdp_verifier": runtime.storage.PDPVerifierAddress()}
			encoded, err := json.Marshal(cp)
			if err != nil {
				t.Fatal(err)
			}
			operation := model.TaskRuntime{OperationKey: fmt.Sprintf("cleanup:%d", copyRow.ID), OperationStartedAt: &attempted, LastAdmittedAttempt: 1}
			rawRuntime, err := json.Marshal(operation)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(encoded)).Set("runtime_json = ?", json.RawMessage(rawRuntime)).Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			var adopted model.StorageCleanupCopy
			if err := runtime.db.NewSelect().Model(&adopted).Where("id = ?", copyRow.ID).Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			if adopted.RemovalRequestID == nil {
				t.Fatal("unresolved copy was not assigned to a batch")
			}
			request, err := runtime.repos.StorageRemoval.GetRemovalRequest(t.Context(), *adopted.RemovalRequestID)
			if err != nil || request.Status != storageremoval.RequestStatusSubmitted {
				t.Fatalf("adopted request = %#v, %v", request, err)
			}
			sends, err := runtime.repos.StorageRemoval.ListRemovalSends(t.Context(), request.RequestID)
			if err != nil || len(sends) != 1 || sends[0].TaskID == nil || *sends[0].TaskID != row.ID || sends[0].AttemptedAt == nil || !sends[0].AttemptedAt.Equal(attempted) || len(sends[0].Pieces) != 1 || sends[0].Pieces[0].CleanupCopyID != copyRow.ID {
				t.Fatalf("adopted send = %#v, %v", sends, err)
			}
			if kind != "unknown attempt" && (adopted.DeleteTxHash == nil || *adopted.DeleteTxHash != hash || sends[0].TxHash == nil || *sends[0].TxHash != hash || adopted.ScheduledAt == nil || !adopted.ScheduledAt.Equal(attempted)) {
				t.Fatalf("adoption changed legacy evidence: %#v, %#v", adopted, sends)
			}
			var checkpoint map[string]any
			if err := json.Unmarshal(row.Checkpoint, &checkpoint); err != nil || len(checkpoint) != 0 {
				t.Fatalf("adopted task kept the old checkpoint: %s, %v", row.Checkpoint, err)
			}
		})
	}
}

func TestCleanupEnqueuesHealthyCopiesBeforeFailAndReauthorizesOnRetry(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	content, row := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusFailed, model.StorageCleanupCopyStatusPending)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
	if row.FailureReason == nil || *row.FailureReason != "cleanup_copy_failed" {
		t.Fatalf("failed cleanup = %#v", row)
	}
	var copies []model.StorageCleanupCopy
	if err := runtime.db.NewSelect().Model(&copies).Where("content_id = ?", content.ID).Order("copy_index").Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if copies[0].Status != model.StorageCleanupCopyStatusFailed || copies[0].RemovalRequestID != nil || copies[1].RemovalRequestID == nil {
		t.Fatalf("failed cleanup did not preserve independent progress: %#v", copies)
	}
	message := "provider did not recognize the piece"
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("last_error = ?", message).Where("id = ?", copies[0].ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	var err error
	row, err = runtime.service.Retry(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if err := runtime.db.NewSelect().Model(&copies).Where("content_id = ?", content.ID).Order("copy_index").Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if copies[0].Status != model.StorageCleanupCopyStatusPending || copies[0].RemovalRequestID == nil || copies[0].LastError == nil || *copies[0].LastError != message || copies[1].RemovalRequestID == nil {
		t.Fatalf("manual retry did not retain diagnosis or enqueue all copies: %#v", copies)
	}
}

func TestCleanupRetryPreservesPaidEvidenceAndAdoptsIt(t *testing.T) {
	for _, scheduled := range []bool{false, true} {
		t.Run(fmt.Sprintf("scheduled=%t", scheduled), func(t *testing.T) {
			var calls atomic.Int64
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{OpenCleanupContextFunc: func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						calls.Add(1)
						return nil, removalUnknownError()
					}}, nil
				}},
				deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					return synapse.CleanupPieceState{Status: synapse.CleanupPiecePresent, BlockNumber: 100}, nil
				},
				receipts: testReceiptChecker{check: func(context.Context, common.Hash) (*ethtypes.Receipt, error) {
					return nil, ethereum.NotFound
				}},
			})
			content, source := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusFailed)
			hash := common.HexToHash("0x555").Hex()
			var scheduledAt *time.Time
			if scheduled {
				at := time.Now().UTC().Truncate(time.Microsecond)
				scheduledAt = &at
			}
			if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("delete_tx_hash = ?", hash).Set("scheduled_at = ?", scheduledAt).Set("last_error = ?", "unconfirmed removal").Where("content_id = ?", content.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			source = runOneStorageTask(t, runtime, source, model.TaskStatusFailed)
			next, err := runtime.service.Retry(t.Context(), source.ID)
			if err != nil {
				t.Fatal(err)
			}
			runOneStorageTask(t, runtime, next, model.TaskStatusPending)
			var copyRow model.StorageCleanupCopy
			if err := runtime.db.NewSelect().Model(&copyRow).Where("content_id = ?", content.ID).Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			if copyRow.DeleteTxHash == nil || *copyRow.DeleteTxHash != hash || copyRow.RemovalRequestID == nil || copyRow.LastError == nil || *copyRow.LastError != "unconfirmed removal" {
				t.Fatalf("manual retry cleared paid evidence: %#v", copyRow)
			}
			sends, err := runtime.repos.StorageRemoval.ListRemovalSends(t.Context(), *copyRow.RemovalRequestID)
			if err != nil || len(sends) != 1 || sends[0].TxHash == nil || *sends[0].TxHash != hash || sends[0].AttemptedAt != nil {
				t.Fatalf("manual retry did not adopt paid evidence: %#v, %v", sends, err)
			}
			request, err := runtime.repos.StorageRemoval.GetRemovalRequest(t.Context(), *copyRow.RemovalRequestID)
			if err != nil {
				t.Fatal(err)
			}
			if scheduledAt != nil && (request.SealedAt == nil || !request.SealedAt.Equal(*scheduledAt)) {
				t.Fatalf("adoption lost scheduling time: %#v", request)
			}
			row, err := runtime.repos.Tasks.GetByID(t.Context(), *request.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			f := &removalFixture{runtime: runtime, request: request, row: row}
			f.step(t, model.TaskStatusPending)
			if calls.Load() != 0 || f.row.RetryCount != 0 || len(f.sends(t)) != 1 {
				t.Fatalf("hash-only evidence resent before policy deadline: task=%#v calls=%d", f.row, calls.Load())
			}
			if _, err := runtime.db.NewUpdate().Model((*storageremoval.Request)(nil)).Set("sealed_at = ?", time.Now().Add(-time.Hour)).Where("request_id = ?", request.RequestID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			f.send(t)
			sends = f.sends(t)
			if calls.Load() != 1 || len(sends) != 2 || sends[0].AttemptedAt != nil || sends[0].TxHash == nil || *sends[0].TxHash != hash || f.row.RetryCount != 0 {
				t.Fatalf("hash-only recovery changed evidence or spent an unused attempt: task=%#v sends=%#v calls=%d", f.row, sends, calls.Load())
			}
		})
	}
}

func TestCleanupRequiresMatchingRecordedDataSetBeforeObservation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		field  string
		value  any
		reason string
	}{
		{name: "missing creation client", field: "client_data_set_id", reason: "storage_identity_missing"},
		{name: "changed creation client", field: "client_data_set_id", value: "999999", reason: "storage_identity_changed"},
		{name: "changed data set", field: "data_set_id", value: "999999", reason: "storage_identity_changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observations atomic.Int64
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
				observations.Add(1)
				return synapse.CleanupPieceState{Status: synapse.CleanupPieceRemoved}, nil
			}})
			content, row := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
			if _, err := runtime.db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).
				Set(tc.field+" = ?", tc.value).Where("content_id = ?", content.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			failed := runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			if observations.Load() != 0 || failed.FailureReason == nil || *failed.FailureReason != tc.reason {
				t.Fatalf("cleanup accepted unproven data set: observations=%d task=%#v", observations.Load(), failed)
			}
			copies, err := runtime.repos.StorageCleanup.AuthorizeTask(t.Context(), content.ID, content.CleanupGeneration, row.ID)
			if err != nil || len(copies) != 1 || copies[0].Status != model.StorageCleanupCopyStatusPending {
				t.Fatalf("cleanup evidence changed: %#v, %v", copies, err)
			}
		})
	}
}
