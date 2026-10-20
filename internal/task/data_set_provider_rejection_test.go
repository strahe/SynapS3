package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestDataSetCreationErrorsUseFiniteBudgetAndPreserveRequest(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		priorRefusal bool
		pending      bool
		statusOnly   bool
		admitted     bool
		failEvidence bool
		wrongResult  bool
	}{
		{name: "400", status: 400},
		{name: "401", status: 401},
		{name: "403", status: 403},
		{name: "500", status: 500},
		{name: "recorded refusal", status: 403, priorRefusal: true},
		{name: "submitted and pending", pending: true},
		{name: "status address and pending", pending: true, statusOnly: true},
		{name: "admitted opportunity after crash", status: 500, admitted: true},
		{name: "submission evidence write failed", pending: true, failEvidence: true},
		{name: "success belongs to another request", wrongResult: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID := testOnChainID(t, 51000+sequence)
			var creates atomic.Int64
			var confirmations atomic.Int64
			var sentID atomic.Value
			var removeFault func()
			var faultRemoved atomic.Bool
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					creates.Add(1)
					if prior := sentID.Load(); prior != nil && prior.(string) != options.ClientDataSetID.String() {
						t.Errorf("request ID changed: %s -> %s", prior, options.ClientDataSetID.String())
					}
					sentID.Store(options.ClientDataSetID.String())
					if tc.wrongResult {
						other := options.ClientDataSetID.Big()
						other.Xor(other, big.NewInt(1))
						otherID, err := sdktypes.BigIntFromBig(other)
						if err != nil {
							return nil, err
						}
						ref, err := storage.NewDataSetRef(providerID.SDK(), sdktypes.NewBigInt(1200), otherID)
						return &storage.CreateDataSetResult{DataSet: ref, TransactionID: "0xsubmitted", ConfirmedTransactionID: "0xconfirmed"}, errors.Join(err, errors.New("provider response interrupted"))
					}
					if tc.pending {
						transactionID := "0xpending"
						if tc.statusOnly {
							transactionID = ""
						}
						options.OnSubmitted(storage.CreateDataSetSubmission{TransactionID: transactionID, StatusURL: "https://provider.example/pdp/data-sets/created/pending"})
						if removeFault != nil && faultRemoved.CompareAndSwap(false, true) {
							removeFault()
						}
						return nil, context.Canceled
					}
					return nil, &pdp.HTTPError{
						Method: http.MethodPost, URL: "https://provider.example/pdp/data-sets", StatusCode: tc.status,
					}
				},
				WaitDataSetFunc: func(ctx context.Context, _ string, _ sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					confirmations.Add(1)
					if ctx.Err() != nil {
						t.Errorf("confirmation inherited canceled submission context: %v", ctx.Err())
					}
					return nil, nil
				},
				FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					return storage.DataSetRef{}, false, nil
				},
			}
			budget := 3
			if tc.failEvidence {
				budget = 1
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					return target, nil
				}},
				policy: cache.EvictionPolicyNone, maxAttempts: &budget,
			})
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("creation-budget-%d", sequence))
			if tc.priorRefusal || tc.admitted {
				clientID := testOnChainID(t, 61000+sequence)
				sentID.Store(clientID.String())
				if err := runtime.repos.Contents.RecordDataSetClientID(t.Context(), fixture.binding.ID, fixture.ensureTask.ID, clientID); err != nil {
					t.Fatal(err)
				}
				fields := map[string]any{
					"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(),
					"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress, "sends": 1,
				}
				if tc.priorRefusal {
					fields["provider_rejection"] = map[string]any{"status_code": tc.status, "rejected_at": time.Now().UTC().Add(-time.Minute)}
				}
				checkpoint, err := json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = ?, resume_mode = ? WHERE id = ?", string(checkpoint), model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				if tc.admitted {
					operation := fmt.Sprintf(`{"operation_key":"ensure:%d","last_admitted_attempt":1}`, fixture.binding.ID)
					if _, err := runtime.db.NewRaw("UPDATE tasks SET runtime_json = ? WHERE id = ?", operation, fixture.ensureTask.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.failEvidence {
				removeFault = testutil.FailWrites(t, runtime.db, "tasks", "UPDATE OF checkpoint_json", fmt.Sprintf("NEW.id = %d AND NEW.checkpoint_json ? 'transaction_id'", fixture.ensureTask.ID), "submission checkpoint unavailable")
				t.Cleanup(func() {
					if faultRemoved.CompareAndSwap(false, true) {
						removeFault()
					}
				})
			}
			var terminal *model.Task
			if tc.pending && !tc.failEvidence {
				waiting := runOneStorageTask(t, runtime, fixture.ensureTask, model.TaskStatusPending)
				if waiting.RetryCount != 0 || confirmations.Load() != 1 || creates.Load() != 1 {
					t.Fatalf("submitted creation consumed its confirmation budget: task=%#v creates=%d confirmations=%d", waiting, creates.Load(), confirmations.Load())
				}
				waiting = setDataSetConfirmationStart(t, runtime, waiting, time.Now().UTC().Add(-31*time.Minute))
				terminal = runStorageTaskClaims(t, runtime, waiting, model.TaskStatusFailed, int64(budget))
			} else {
				terminal = func() *model.Task {
					cancel, done := runHandlerEngine(t, runtime)
					defer stopHandlerEngine(t, cancel, done)
					return waitForTask(t, runtime.repos, fixture.ensureTask.ID, func(task *model.Task) bool {
						return task.Status == model.TaskStatusFailed
					})
				}()
			}
			binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
			expectedCreates := int64(budget)
			expectedRetries, expectedReason := budget-1, "attempts_exhausted"
			if tc.admitted {
				expectedCreates--
			}
			if tc.pending {
				expectedCreates = 1
			}
			if tc.wrongResult {
				expectedCreates, expectedRetries, expectedReason = 1, 0, "dataset_identity_mismatch"
			}
			if err != nil || terminal.FailureReason == nil || *terminal.FailureReason != expectedReason || terminal.RetryCount != expectedRetries || creates.Load() != expectedCreates {
				t.Fatalf("creation outcome: failure=%v retries=%d calls=%d err=%v", terminal.FailureReason, terminal.RetryCount, creates.Load(), err)
			}
			if tc.pending && !tc.failEvidence && confirmations.Load() != int64(budget+1) {
				t.Fatalf("confirmation attempts=%d, want one observation plus %d expired opportunities", confirmations.Load(), budget)
			}
			if tc.pending && !tc.failEvidence {
				var operation model.TaskRuntime
				if err := json.Unmarshal(terminal.Runtime, &operation); err != nil || operation.OperationStartedAt == nil || time.Since(*operation.OperationStartedAt) < 31*time.Minute {
					t.Fatalf("automatic retries extended the confirmation window: runtime=%#v err=%v", operation, err)
				}
			}
			if tc.failEvidence && (confirmations.Load() != 0 || binding.TxHash == nil || *binding.TxHash != "0xpending" || binding.CreateStatusURL == nil) {
				t.Fatalf("submission facts lost at exhaustion: binding=%#v confirmations=%d", binding, confirmations.Load())
			}
			if !binding.IsCurrent || binding.EnsureTaskID == nil || binding.ClientDataSetID == nil || binding.ClientDataSetID.String() != sentID.Load().(string) || len(binding.CreationRejection) != 0 {
				t.Fatalf("request ownership or identity lost: %#v", binding)
			}
			if binding.DataSetID != nil || binding.Status == model.StorageDataSetStatusReady {
				t.Fatalf("unconfirmed creation became ready: %#v", binding)
			}
			if tc.wrongResult && (binding.TxHash == nil || *binding.TxHash != "0xsubmitted" || binding.ConfirmedTxHash == nil || *binding.ConfirmedTxHash != "0xconfirmed" || binding.LastError == nil || !strings.Contains(*binding.LastError, "provider response interrupted")) {
				t.Fatalf("identity failure discarded creation facts: %#v", binding)
			}
			if _, local, err := runtime.repos.Replacements.SourceEligibility(t.Context(), binding.ID); err != nil || !local {
				t.Fatalf("failed source eligibility: local=%v err=%v", local, err)
			}
			next, err := runtime.service.Retry(t.Context(), terminal.ID)
			if err != nil || next.Status != model.TaskStatusPending || next.RetryCount != 0 || string(next.Checkpoint) != string(terminal.Checkpoint) {
				t.Fatalf("manual retry: next=%#v err=%v", next, err)
			}
			current, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
			if err != nil || current.EnsureTaskID == nil || *current.EnsureTaskID != next.ID || !current.ClientDataSetID.Equal(*binding.ClientDataSetID) {
				t.Fatalf("retry owner or request changed: %#v err=%v", current, err)
			}
			if _, _, err := runtime.repos.Replacements.SourceEligibility(t.Context(), binding.ID); err == nil {
				t.Fatal("provider replacement allowed during manual retry")
			}
		})
	}
}

func TestRejectedDataSetCreationKeepsEvidenceUntilANewSubmission(t *testing.T) {
	const submitted, confirmed = "0xrejected", "0xrejected-confirmed"
	const originalURL = "https://provider.example/rejected"
	for _, tc := range []struct {
		name                 string
		initialConfirmedHash string
		rejectionResult      *storage.CreateDataSetResult
		nextSubmission       *storage.CreateDataSetSubmission
		nextResult           *storage.CreateDataSetResult
		nextDataSetID        uint64
		nextError            error
		wantTxHash           string
		wantConfirmedHash    string
		wantRejected         bool
		wantTaskStatus       model.TaskStatus
	}{
		{
			name:            "error without evidence",
			rejectionResult: &storage.CreateDataSetResult{TransactionID: submitted, ConfirmedTransactionID: confirmed},
			nextError:       errors.New("provider unavailable before a new submission"),
			wantTxHash:      submitted, wantConfirmedHash: confirmed, wantRejected: true, wantTaskStatus: model.TaskStatusPending,
		},
		{
			name: "rejected without result", initialConfirmedHash: confirmed,
			nextError:  errors.New("provider unavailable before a new submission"),
			wantTxHash: submitted, wantConfirmedHash: confirmed, wantRejected: true, wantTaskStatus: model.TaskStatusPending,
		},
		{
			name:            "repeated submission",
			rejectionResult: &storage.CreateDataSetResult{TransactionID: submitted, ConfirmedTransactionID: confirmed},
			nextSubmission:  &storage.CreateDataSetSubmission{TransactionID: submitted, StatusURL: originalURL},
			wantTxHash:      submitted, wantConfirmedHash: confirmed, wantRejected: true, wantTaskStatus: model.TaskStatusPending,
		},
		{
			name:            "new submission",
			rejectionResult: &storage.CreateDataSetResult{TransactionID: submitted, ConfirmedTransactionID: confirmed},
			nextSubmission:  &storage.CreateDataSetSubmission{TransactionID: "0xnew", StatusURL: "https://provider.example/new"},
			wantTxHash:      "0xnew", wantTaskStatus: model.TaskStatusPending,
		},
		{
			name:            "new confirmed result",
			rejectionResult: &storage.CreateDataSetResult{TransactionID: submitted, ConfirmedTransactionID: confirmed},
			nextResult:      &storage.CreateDataSetResult{TransactionID: "0xnew", ConfirmedTransactionID: "0xnew-confirmed"}, nextDataSetID: 7001,
			wantTxHash: "0xnew", wantConfirmedHash: "0xnew-confirmed", wantTaskStatus: model.TaskStatusCompleted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID, clientID := testOnChainID(t, 51000+sequence), testOnChainID(t, 61000+sequence)
			if tc.nextDataSetID != 0 {
				ref, err := storage.NewDataSetRef(providerID.SDK(), sdktypes.NewBigInt(tc.nextDataSetID), clientID.SDK())
				if err != nil {
					t.Fatal(err)
				}
				tc.nextResult.DataSet = ref
			}
			var creates atomic.Int64
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				WaitDataSetFunc: func(_ context.Context, statusURL string, id sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					if !id.Equal(clientID.SDK()) {
						t.Errorf("observation changed request identity: %s", id.String())
					}
					if statusURL != originalURL {
						return nil, nil
					}
					return tc.rejectionResult, synapse.ErrProviderTransactionRejected
				},
				FindDataSetByClientIDFunc: func(_ context.Context, id sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					if !id.Equal(clientID.SDK()) {
						t.Errorf("lookup changed request identity: %s", id.String())
					}
					return storage.DataSetRef{}, false, nil
				},
				CreateDataSetFunc: func(ctx context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					creates.Add(1)
					if !options.ClientDataSetID.Equal(clientID.SDK()) {
						t.Errorf("retry changed request identity: %s", options.ClientDataSetID.String())
					}
					if tc.nextSubmission != nil {
						options.OnSubmitted(*tc.nextSubmission)
						return nil, ctx.Err()
					}
					return tc.nextResult, tc.nextError
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return target, nil
			}}, policy: cache.EvictionPolicyNone})
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("rejected-creation-%d", sequence))
			initialEvidence := repository.MarkDataSetCreatingInput{
				ID: fixture.binding.ID, TxHash: submitted, ConfirmedTxHash: tc.initialConfirmedHash, StatusURL: originalURL, ClientDataSetID: &clientID,
			}
			if err := runtime.repos.Contents.MarkDataSetCreating(t.Context(), initialEvidence); err != nil {
				t.Fatal(err)
			}
			checkpointFields := map[string]any{
				"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(), "sends": 1,
				"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress,
				"transaction_id": submitted, "status_url": originalURL,
				"confirmed_tx_hash": tc.initialConfirmedHash,
			}
			checkpoint, err := json.Marshal(checkpointFields)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = ?, resume_mode = ? WHERE id = ?", string(checkpoint), model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			pending := runOneStorageTask(t, runtime, fixture.ensureTask, model.TaskStatusPending)
			assertPair := func(txHash, confirmedHash string) {
				t.Helper()
				current, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.TxHash == nil {
					t.Errorf("ledger submitted hash is missing, want %q", txHash)
				} else if *current.TxHash != txHash {
					t.Errorf("ledger submitted hash = %q, want %q", *current.TxHash, txHash)
				}
				if confirmedHash == "" {
					if current.ConfirmedTxHash != nil {
						t.Errorf("ledger confirmed hash = %q, want nil", *current.ConfirmedTxHash)
					}
				} else if current.ConfirmedTxHash == nil {
					t.Errorf("ledger confirmed hash is missing, want %q", confirmedHash)
				} else if *current.ConfirmedTxHash != confirmedHash {
					t.Errorf("ledger confirmed hash = %q, want %q", *current.ConfirmedTxHash, confirmedHash)
				}
			}
			assertRecovery := func(txHash, confirmedHash string, rejected bool) {
				t.Helper()
				var stored struct {
					TxHash          string `json:"transaction_id"`
					ConfirmedTxHash string `json:"confirmed_tx_hash"`
					Rejected        bool   `json:"transaction_rejected"`
				}
				if err := json.Unmarshal(pending.Checkpoint, &stored); err != nil {
					t.Fatal(err)
				}
				if stored.TxHash != txHash {
					t.Errorf("checkpoint submitted hash = %q, want %q", stored.TxHash, txHash)
				}
				if stored.ConfirmedTxHash != confirmedHash {
					t.Errorf("checkpoint confirmed hash = %q, want %q", stored.ConfirmedTxHash, confirmedHash)
				}
				if stored.Rejected != rejected {
					t.Errorf("checkpoint rejected = %t, want %t", stored.Rejected, rejected)
				}
			}
			assertPair(submitted, confirmed)
			assertRecovery(submitted, confirmed, true)
			if creates.Load() != 0 || pending.RetryCount != 1 {
				t.Fatalf("rejected observation sent requests=%d task=%#v", creates.Load(), pending)
			}
			wakeTask(t, runtime, pending.ID)
			pending = runOneStorageTask(t, runtime, pending, model.TaskStatusPending)
			if pending.ResumeMode != model.TaskResumeModeExecute {
				t.Fatalf("retry was not admitted for execution: %#v", pending)
			}
			wakeTask(t, runtime, pending.ID)
			pending = runOneStorageTask(t, runtime, pending, tc.wantTaskStatus)
			if creates.Load() != 1 {
				t.Fatalf("create requests=%d", creates.Load())
			}
			assertPair(tc.wantTxHash, tc.wantConfirmedHash)
			if tc.wantTaskStatus == model.TaskStatusPending {
				assertRecovery(tc.wantTxHash, tc.wantConfirmedHash, tc.wantRejected)
			}
			if tc.wantTaskStatus == model.TaskStatusCompleted {
				current, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.Status != model.StorageDataSetStatusReady {
					t.Errorf("data set status = %q, want %q", current.Status, model.StorageDataSetStatusReady)
				}
			}
		})
	}
}

func TestDataSetCreationPreservesEvidenceOnConflictingResults(t *testing.T) {
	for _, source := range []string{"create result after callback", "confirmation observation", "confirmation identity mismatch"} {
		t.Run(source, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			providerID, clientID := testOnChainID(t, 51000+sequence), testOnChainID(t, 61000+sequence)
			const submitted, confirmed = "0xoriginal", "0xoriginal-confirmed"
			const statusURL = "https://provider.example/original"
			cause := errors.New("provider response stopped after confirmation")
			var creates atomic.Int64
			result := func(id sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
				txHash := "0xother"
				if source == "confirmation identity mismatch" {
					txHash, id = submitted, sdktypes.NewBigInt(9999)
				}
				ref, err := storage.NewDataSetRef(providerID.SDK(), sdktypes.NewBigInt(7001), id)
				return &storage.CreateDataSetResult{TransactionID: txHash, ConfirmedTransactionID: "0xother-confirmed", DataSet: ref}, errors.Join(err, cause)
			}
			target := &testutil.MockStorageTarget{
				ProviderIDValue: providerID.SDK(),
				CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					creates.Add(1)
					options.OnSubmitted(storage.CreateDataSetSubmission{TransactionID: submitted, StatusURL: statusURL})
					return result(*options.ClientDataSetID)
				},
				WaitDataSetFunc: func(_ context.Context, _ string, id sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					return result(id)
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return target, nil
			}}, policy: cache.EvictionPolicyNone})
			fixture := seedDataSetEnsure(t, runtime, providerID, fmt.Sprintf("creation-hash-conflict-%d", sequence))
			expectedConfirmed, expectedCreates := "", int64(1)
			if source != "create result after callback" {
				expectedConfirmed, expectedCreates = confirmed, 0
				initialConfirmed := confirmed
				if source == "confirmation identity mismatch" {
					initialConfirmed, expectedConfirmed = "", "0xother-confirmed"
				}
				if err := runtime.repos.Contents.MarkDataSetCreating(t.Context(), repository.MarkDataSetCreatingInput{
					ID: fixture.binding.ID, TxHash: submitted, ConfirmedTxHash: initialConfirmed, StatusURL: statusURL, ClientDataSetID: &clientID,
				}); err != nil {
					t.Fatal(err)
				}
				checkpoint, err := json.Marshal(map[string]any{
					"attempted_at": time.Now().UTC().Add(-time.Hour), "client_data_set_id": clientID.String(), "sends": 1,
					"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress,
					"transaction_id": submitted, "confirmed_tx_hash": initialConfirmed, "status_url": statusURL,
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = ?, resume_mode = ? WHERE id = ?", string(checkpoint), model.TaskResumeModeRecover, fixture.ensureTask.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			failed := runOneStorageTask(t, runtime, fixture.ensureTask, model.TaskStatusFailed)
			current, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), fixture.binding.ID)
			if err != nil || current.TxHash == nil || *current.TxHash != submitted || expectedConfirmed == "" && current.ConfirmedTxHash != nil || expectedConfirmed != "" && (current.ConfirmedTxHash == nil || *current.ConfirmedTxHash != expectedConfirmed) || current.DataSetID != nil {
				t.Fatalf("creation pair after conflicting result=%#v err=%v", current, err)
			}
			var recovery struct {
				TxHash          string `json:"transaction_id"`
				ConfirmedTxHash string `json:"confirmed_tx_hash"`
			}
			if err := json.Unmarshal(failed.Checkpoint, &recovery); err != nil || recovery.TxHash != submitted || recovery.ConfirmedTxHash != expectedConfirmed {
				t.Fatalf("creation recovery after conflict=%#v err=%v", recovery, err)
			}
			if creates.Load() != expectedCreates || failed.FailureReason == nil || *failed.FailureReason != "dataset_identity_mismatch" {
				t.Fatalf("creation conflict=%#v sends=%d", failed, creates.Load())
			}
			for _, diagnostic := range []*string{failed.LastError, current.LastError} {
				if diagnostic == nil || !strings.Contains(*diagnostic, cause.Error()) || source != "confirmation identity mismatch" && (!strings.Contains(*diagnostic, submitted) || !strings.Contains(*diagnostic, "0xother")) {
					t.Fatalf("creation conflict diagnostic=%v", diagnostic)
				}
			}
		})
	}
}

func TestUncreatedReplacementCoordinatorWaitsForReadyTarget(t *testing.T) {
	for _, state := range []string{"already ready", "pending", "provider rejected", "chain rejected", "retryable task stopped", "previous replacement failed", "cancelled", "nonretryable task stopped", "slot missing"} {
		t.Run(state, func(t *testing.T) {
			sequence := storedObjectSequence.Add(1)
			sourceProvider, targetProvider := testOnChainID(t, 71000+sequence), testOnChainID(t, 81000+sequence)
			var creates atomic.Int64
			createStarted, allowCreate := make(chan struct{}), make(chan struct{})
			terminator := &testServiceTerminator{err: errors.New("unexpected remote retirement")}
			target := &testutil.MockStorageTarget{
				ProviderIDValue: targetProvider.SDK(),
				WaitDataSetFunc: func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
					return nil, synapse.ErrProviderTransactionRejected
				},
				FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					return storage.DataSetRef{}, false, nil
				},
				CreateDataSetFunc: func(_ context.Context, options *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
					creates.Add(1)
					if state == "pending" {
						close(createStarted)
						select {
						case <-allowCreate:
						case <-t.Context().Done():
							return nil, t.Context().Err()
						}
					}
					if state == "provider rejected" {
						return nil, errors.New("provider returned HTTP 403")
					}
					ref, err := storage.NewDataSetRef(targetProvider.SDK(), sdktypes.NewBigInt(2002), *options.ClientDataSetID)
					return &storage.CreateDataSetResult{DataSet: ref}, err
				},
			}
			attempts := 2
			if state == "provider rejected" {
				attempts = 1
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				maxAttempts: &attempts,
				storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(_ context.Context, provider sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					if provider.String() != targetProvider.String() {
						return nil, errors.New("old provider must not receive requests")
					}
					return target, nil
				}}, terminator: terminator,
			})
			bucket := &model.Bucket{Name: fmt.Sprintf("uncreated-coordinate-%d", sequence), Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
			if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
			source, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: sourceProvider, CopyIndex: 0})
			if err != nil {
				t.Fatal(err)
			}
			enqueueEnsure := func(id int64) *model.Task {
				task, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
					Type: model.TaskTypeStorageDataSetEnsure, Input: storagepipeline.DataSetInput{ExecutionIdentity: testStorageExecutionIdentity(), DataSetID: id},
				}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
					return repos.Contents.BindDataSetEnsureTask(ctx, id, task.ID)
				})
				if err != nil {
					t.Fatal(err)
				}
				return task
			}
			oldEnsure := enqueueEnsure(source.ID)
			settleHandlerTaskFixture(t, runtime, oldEnsure.ID, model.TaskStatusFailed, new("retry_exhausted"))
			row, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID,
				SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: targetProvider, ClientRequestID: bucket.Name,
			})
			if err != nil {
				t.Fatal(err)
			}
			var targetEnsure *model.Task
			if state == "already ready" {
				if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{ID: row.TargetDataSetID, DataSetID: testOnChainID(t, 2002)}); err != nil {
					t.Fatal(err)
				}
			} else {
				targetEnsure = enqueueEnsure(row.TargetDataSetID)
				if state == "chain rejected" {
					clientID := testOnChainID(t, 909)
					statusURL := "https://provider.example/pdp/data-sets/created/0xdead"
					if err := runtime.repos.Contents.MarkDataSetCreating(t.Context(), repository.MarkDataSetCreatingInput{ID: row.TargetDataSetID, TxHash: "0xdead", StatusURL: statusURL, ClientDataSetID: &clientID}); err != nil {
						t.Fatal(err)
					}
					checkpoint, err := json.Marshal(map[string]any{"attempted_at": time.Now().UTC().Add(-time.Minute), "client_data_set_id": clientID.String(), "identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress, "sends": 1, "transaction_id": "0xdead", "status_url": statusURL})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = ? WHERE id = ?", string(checkpoint), targetEnsure.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
					if _, err := runtime.db.NewRaw("UPDATE tasks SET resume_mode = ? WHERE id = ?", model.TaskResumeModeRecover, targetEnsure.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				if state == "retryable task stopped" || state == "previous replacement failed" || state == "nonretryable task stopped" || state == "cancelled" {
					status, reason := model.TaskStatusFailed, any("handler_panic")
					switch state {
					case "nonretryable task stopped":
						reason = "dataset_correlation_conflict"
					case "cancelled":
						status, reason = model.TaskStatusCancelled, nil
					}
					var failureReason *string
					if reason != nil {
						failureReason = new(reason.(string))
					}
					settleHandlerTaskFixture(t, runtime, targetEnsure.ID, status, failureReason)
				}
			}
			switch state {
			case "slot missing":
				if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("is_current = ?", false).Where("id = ?", source.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "previous replacement failed":
				if err := runtime.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "old coordinator stopped"); err != nil {
					t.Fatal(err)
				}
			}
			enqueueCoordinator := func() *model.Task {
				coordinator, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
					Type:  model.TaskTypeProviderReplacementCoordinate,
					Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration},
				}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
					return repos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, task.ID)
				})
				if err != nil {
					t.Fatal(err)
				}
				return coordinator
			}
			coordinator := enqueueCoordinator()
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			if state == "retryable task stopped" || state == "previous replacement failed" {
				waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusFailed
				})
				stopHandlerEngine(t, cancel, done)
				if err := runtime.repos.Replacements.RetryEligibility(t.Context(), row.ID); err != nil {
					t.Fatalf("failed setup could not be recovered: %v", err)
				}
				coordinator, err = runtime.service.Retry(t.Context(), coordinator.ID)
				if err != nil {
					t.Fatal(err)
				}
				recoveringTarget, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
				if err != nil || recoveringTarget.EnsureTaskID == nil || *recoveringTarget.EnsureTaskID == targetEnsure.ID {
					t.Fatalf("parent retry did not restore setup: %#v, %v", recoveringTarget, err)
				}
				targetEnsure, err = runtime.repos.Tasks.GetByID(t.Context(), *recoveringTarget.EnsureTaskID)
				if err != nil || targetEnsure.Status != model.TaskStatusPending || targetEnsure.RetryCount != 0 {
					t.Fatalf("new setup round = %#v, %v", targetEnsure, err)
				}
				cancel, done = runHandlerEngine(t, runtime)
				defer stopHandlerEngine(t, cancel, done)
			}
			if state == "pending" {
				select {
				case <-createStarted:
				case <-time.After(5 * time.Second):
					close(allowCreate)
					t.Fatal("target creation did not start")
				}
				held, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
				preparing, loadErr := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
				close(allowCreate)
				if err != nil || loadErr != nil || !held.IsCurrent || preparing.IsCurrent {
					t.Fatalf("pending target took the slot: A=%#v B=%#v %v %v", held, preparing, err, loadErr)
				}
			}
			oldEnsure = waitForTask(t, runtime.repos, oldEnsure.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
			oldSource, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
			if err != nil || oldSource.EnsureTaskID != nil || oldEnsure.ClaimGeneration != 1 {
				t.Fatalf("replaced source retained execution authority: %#v, task=%#v, %v", oldSource, oldEnsure, err)
			}
			if targetEnsure != nil {
				waitForTask(t, runtime.repos, targetEnsure.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusCompleted || task.Status == model.TaskStatusCancelled
				})
				waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool { return task.Status != model.TaskStatusRunning })
				wakeTask(t, runtime, coordinator.ID)
			}
			waitForTask(t, runtime.repos, coordinator.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusCompleted
			})
			stored, err := runtime.repos.Replacements.GetByID(t.Context(), row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if state == "provider rejected" {
				if stored.Status != storagereplacement.StatusFailed {
					t.Fatalf("rejected target=%#v", stored)
				}
			} else if state == "cancelled" || state == "nonretryable task stopped" || state == "slot missing" {
				if stored.Status != storagereplacement.StatusFailed {
					t.Fatalf("terminal target replacement=%#v", stored)
				}
				if state == "nonretryable task stopped" {
					if _, err := runtime.service.Retry(t.Context(), coordinator.ID); !errors.Is(err, taskengine.ErrRetryUnsupported) {
						t.Fatalf("identity conflict reopened setup: %v", err)
					}
				} else if err := runtime.repos.Replacements.RetryEligibility(t.Context(), row.ID); !errors.Is(err, storagereplacement.ErrNotRetryable) {
					t.Fatalf("terminal target retry: %v", err)
				}
			} else if stored.Status != storagereplacement.StatusCompleted || stored.TerminationEpoch != nil {
				t.Fatalf("completed replacement=%#v", stored)
			}
			if state == "provider rejected" || state == "nonretryable task stopped" {
				retained, _, err := runtime.repos.Replacements.SourceEligibility(t.Context(), source.ID)
				if err != nil || !retained.IsCurrent {
					t.Fatalf("original source lost its slot: %#v %v", retained, err)
				}
				check, err := runtime.repos.Replacements.Preflight(t.Context(), source.ID)
				if err != nil {
					t.Fatal(err)
				}
				next, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 91000+sequence), ClientRequestID: "another-provider", Preflight: check})
				if err != nil || next.SourceDataSetID != source.ID {
					t.Fatalf("original source could not choose C: %#v %v", next, err)
				}
			}
			if terminator.calls.Load() != 0 {
				t.Fatal("local replacement retired a remote service")
			}
			wantCreates := int64(1)
			if state == "already ready" || state == "cancelled" || state == "nonretryable task stopped" {
				wantCreates = 0
			}
			if creates.Load() != wantCreates {
				t.Fatalf("create calls=%d, want %d", creates.Load(), wantCreates)
			}
		})
	}
}

func TestReadyLocalTargetSchedulesRequiredContentRecovery(t *testing.T) {
	var opened atomic.Int64
	noRetries := 0
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		maxAttempts: taskTestMaxAttempts(&noRetries),
		cache:       &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return false }},
		storage: &testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			opened.Add(1)
			return nil, errors.New("unexpected creation")
		}},
	})
	fixture := seedDataSetEnsure(t, runtime, testOnChainID(t, 101), "local-content-recovery")
	copies, err := runtime.repos.Contents.ListIncompleteCopiesForDataSet(t.Context(), fixture.binding.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("source copies: %#v %v", copies, err)
	}
	oldCopyTask := bindCopyTask(t, runtime, &copies[0], model.TaskTypeStorageTransferPlan)
	reason := "attempts_exhausted"
	settleHandlerTaskFixture(t, runtime, fixture.ensureTask.ID, model.TaskStatusFailed, &reason)
	row, _, err := runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: fixture.binding.BucketID, SourceDataSetID: fixture.binding.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 202), ClientRequestID: "recover"})
	if err != nil {
		t.Fatal(err)
	}
	if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{ID: row.TargetDataSetID, DataSetID: testOnChainID(t, 2002)}); err != nil {
		t.Fatal(err)
	}
	coordinator, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeProviderReplacementCoordinate, Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration}}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, task.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator = runStorageTaskClaims(t, runtime, coordinator, model.TaskStatusPending, 2)
	settled := make(chan struct{}, 1)
	runtime.engine, err = taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
		OnTaskSettled: func(claimed *model.Task, _ repository.TaskTransition) {
			if claimed.ID == coordinator.ID {
				select {
				case settled <- struct{}{}:
				default:
				}
			}
		},
	}, runtime.repos, handlerExecutionRegistry(t, runtime), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failedSource, err := runtime.repos.Tasks.GetByID(t.Context(), fixture.ensureTask.ID)
	if err != nil || failedSource.Status != model.TaskStatusFailed || failedSource.FailureReason == nil || *failedSource.FailureReason != reason {
		t.Fatalf("source creation failure evidence = %#v, %v", failedSource, err)
	}
	waitForTask(t, runtime.repos, oldCopyTask.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCancelled })
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 2*handlerTestLeaseDuration)
	defer cancelWait()
	for failed := false; !failed; {
		select {
		case <-settled:
			observed, err := runtime.repos.Tasks.GetByID(t.Context(), coordinator.ID)
			if err != nil || observed == nil {
				t.Fatalf("read settled coordinator: %v", err)
			}
			failed = observed.Status == model.TaskStatusFailed
			if observed.Status == model.TaskStatusPending {
				wakeTask(t, runtime, coordinator.ID)
			}
		case <-waitCtx.Done():
			t.Fatal("coordinator did not settle required content recovery")
		}
	}
	item, err := runtime.repos.Replacements.ReplacementExecution(t.Context(), row.ID)
	if err != nil || !item.HasFailed {
		t.Fatalf("required content did not request attention: %#v %v", item, err)
	}
	var cancelled int
	if err := runtime.db.NewSelect().Model((*storagereplacement.Item)(nil)).Where("replacement_id = ? AND status = ?", row.ID, storagereplacement.ItemStatusCancelled).ColumnExpr("COUNT(*)").Scan(t.Context(), &cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled != 0 || opened.Load() != 0 {
		t.Fatalf("recovery was cancelled or old provider reopened: cancelled=%d opened=%d", cancelled, opened.Load())
	}
}
