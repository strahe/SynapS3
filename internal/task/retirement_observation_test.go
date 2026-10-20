package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	bindings "github.com/strahe/synaps3/internal/task/binding"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/pdp"
	sdktypes "github.com/strahe/synapse-go/types"
)

type observingRetirementTerminator struct {
	testServiceTerminator
	observations atomic.Int64
	pending      bool
	observeErr   error
	observed     *synapse.TerminationResult
}

func TestRetirementObservationRateLimitCanResumeBeforeAnySend(t *testing.T) {
	terminator := &observingRetirementTerminator{
		testServiceTerminator: testServiceTerminator{result: &synapse.TerminationResult{EndEpoch: new(int64(84))}},
		observeErr:            &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true})
	_, row := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	var checkpoint struct {
		storagepipeline.ExecutionIdentity
		Sends                     int       `json:"sends,omitempty"`
		AttemptedAt               time.Time `json:"attempted_at"`
		ProtocolNextObservationAt time.Time `json:"protocol_next_observation_at,omitzero"`
	}
	if err := json.Unmarshal(row.Checkpoint, &checkpoint); err != nil || !checkpoint.Complete() || checkpoint.Sends != 0 || !checkpoint.AttemptedAt.IsZero() || checkpoint.ProtocolNextObservationAt.IsZero() || terminator.calls.Load() != 0 {
		t.Fatalf("observation throttle changed send evidence: checkpoint=%s requests=%d err=%v", row.Checkpoint, terminator.calls.Load(), err)
	}
	checkpoint.ProtocolNextObservationAt = time.Now().UTC().Add(-time.Second)
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = ?, available_at = ? WHERE id = ?", json.RawMessage(raw), time.Now().Add(-time.Second), row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	terminator.observeErr = nil
	terminator.pending = true
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if terminator.observations.Load() != 2 || terminator.calls.Load() != 0 || row.WaitReason == nil || *row.WaitReason != "provider_confirmation" {
		t.Fatalf("rate-limited observation could not resume: task=%#v observations=%d requests=%d", row, terminator.observations.Load(), terminator.calls.Load())
	}
	terminator.pending = false
	wakeTask(t, runtime, row.ID)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if row.ResumeMode != model.TaskResumeModeExecute || terminator.calls.Load() != 0 {
		t.Fatalf("pure observation was treated as a sent request: %#v requests=%d", row, terminator.calls.Load())
	}
	wakeTask(t, runtime, row.ID)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if terminator.calls.Load() != 1 {
		t.Fatalf("first retirement request was not admitted once: task=%#v requests=%d", row, terminator.calls.Load())
	}
}

func (t *observingRetirementTerminator) ObserveTermination(context.Context, sdktypes.BigInt, synapse.TerminationResult) (*synapse.TerminationResult, bool, error) {
	t.observations.Add(1)
	return t.observed, t.pending, t.observeErr
}

type failedRetirementTerminator struct {
	testServiceTerminator
	directCalls        atomic.Int64
	directObservations atomic.Int64
	payerChecks        atomic.Int64
	endedExternally    atomic.Bool
	directSucceeded    atomic.Bool
}

func (f *failedRetirementTerminator) VerifyServicePayer(context.Context, sdktypes.BigInt) error {
	f.payerChecks.Add(1)
	return nil
}

func (f *failedRetirementTerminator) ObserveTermination(_ context.Context, _ sdktypes.BigInt, known synapse.TerminationResult) (*synapse.TerminationResult, bool, error) {
	if f.endedExternally.Load() {
		return &synapse.TerminationResult{EndEpoch: new(int64(84))}, false, nil
	}
	if known.TxHash == "" {
		return nil, false, nil
	}
	// The provider keeps its first request after a direct replacement transaction.
	return &synapse.TerminationResult{TxHash: "0xattempt-a", ConfirmedTxHash: "0xmined-a", ReceiptStatus: new(uint64(0))}, false, synapse.ErrTerminationTransactionFailed
}

func (f *failedRetirementTerminator) TerminateServiceDirect(_ context.Context, _ sdktypes.BigInt, onSubmitted func(string) error) (*synapse.TerminationResult, error) {
	f.directCalls.Add(1)
	if f.payerChecks.Load() < 2 {
		return nil, errors.New("direct termination was sent without checking the service payer again")
	}
	result := &synapse.TerminationResult{TxHash: "0xattempt-b"}
	return result, errors.Join(context.DeadlineExceeded, onSubmitted(result.TxHash))
}

func (f *failedRetirementTerminator) ObserveTerminationDirect(_ context.Context, _ sdktypes.BigInt, known synapse.TerminationResult) (*synapse.TerminationResult, bool, error) {
	f.directObservations.Add(1)
	if known.TxHash != "0xattempt-b" {
		return nil, false, errors.New("direct observation did not use the second transaction")
	}
	if !f.directSucceeded.Load() {
		return &known, true, nil
	}
	return &synapse.TerminationResult{TxHash: "0xattempt-b", ConfirmedTxHash: "0xmined-b", ReceiptStatus: new(uint64(1)), EndEpoch: new(int64(84))}, false, nil
}

func expireRetirementObservation(t *testing.T, runtime handlerTestRuntime, row *model.Task) *model.Task {
	t.Helper()
	var state model.TaskRuntime
	if err := json.Unmarshal(row.Runtime, &state); err != nil || state.Observation == nil {
		t.Fatalf("retirement observation window missing: %s err=%v", row.Runtime, err)
	}
	state.Observation.StartedAt = time.Now().Add(-time.Hour)
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET runtime_json = ?, resume_mode = 'execute', available_at = ? WHERE id = ?`, json.RawMessage(raw), time.Now().Add(-time.Second), row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	return row
}

func seedFailedRetirement(t *testing.T, abandoned bool, terminator *failedRetirementTerminator) (handlerTestRuntime, *storagereplacement.Replacement, *model.Task) {
	t.Helper()
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true})
	seed := seedSourceRetirement
	if abandoned {
		seed = seedAbandonedTargetRetirement
	}
	replacement, row := seed(t, runtime, storedObjectSequence.Add(1))
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
	if row.FailureReason == nil || *row.FailureReason != "termination_confirmation_failed" {
		t.Fatalf("reverted transaction did not stop for manual recovery: %#v", row)
	}
	if terminator.calls.Load() != 1 || terminator.directCalls.Load() != 0 {
		t.Fatalf("failed transaction was resent: provider=%d direct=%d", terminator.calls.Load(), terminator.directCalls.Load())
	}
	return runtime, replacement, row
}

func TestRetirementManualRetryKeepsFailedAttemptAndObservesDirectTransaction(t *testing.T) {
	for _, tc := range []struct {
		name             string
		abandoned        bool
		failCallbackSave bool
	}{
		{name: "source"},
		{name: "abandoned target", abandoned: true},
		{name: "source callback persistence fails once", failCallbackSave: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminator := &failedRetirementTerminator{testServiceTerminator: testServiceTerminator{
				result: &synapse.TerminationResult{TxHash: "0xattempt-a"}, err: context.DeadlineExceeded,
			}}
			runtime, replacement, failed := seedFailedRetirement(t, tc.abandoned, terminator)
			failedCheckpoint := string(failed.Checkpoint)
			if tc.failCallbackSave {
				if _, err := runtime.db.ExecContext(t.Context(), "CREATE SEQUENCE direct_termination_save_fault"); err != nil {
					t.Fatal(err)
				}
				restore := testutil.FailWrites(t, runtime.db, "storage_data_set_termination_attempts", "UPDATE",
					"NEW.attempt_no = 2 AND NEW.tx_hash = '0xattempt-b' AND nextval('direct_termination_save_fault') = 1", "injected direct termination evidence failure")
				defer restore()
			}
			next, err := runtime.service.Retry(t.Context(), failed.ID)
			if err != nil {
				t.Fatal(err)
			}
			next = runOneStorageTask(t, runtime, next, model.TaskStatusPending)
			if next.ResumeMode != model.TaskResumeModeExecute || terminator.directCalls.Load() != 0 {
				t.Fatalf("manual recovery bypassed its read-only check: task=%#v direct=%d", next, terminator.directCalls.Load())
			}
			next = runOneStorageTask(t, runtime, next, model.TaskStatusPending)
			if terminator.directCalls.Load() != 1 || terminator.calls.Load() != 1 {
				t.Fatalf("manual recovery did not send exactly one direct transaction: provider=%d direct=%d", terminator.calls.Load(), terminator.directCalls.Load())
			}
			if tc.failCallbackSave && (next.LastError == nil || !strings.Contains(*next.LastError, "injected direct termination evidence failure")) {
				t.Fatalf("callback persistence failure was not retained: %v", next.LastError)
			}
			var recovery struct {
				AttemptNo              int64  `json:"attempt_no"`
				TxHash                 string `json:"transaction_hash"`
				RetryFailedTransaction bool   `json:"retry_failed_transaction"`
			}
			if err := json.Unmarshal(next.Checkpoint, &recovery); err != nil {
				t.Fatal(err)
			}
			if recovery.AttemptNo != 2 || recovery.TxHash != "0xattempt-b" || recovery.RetryFailedTransaction {
				t.Fatalf("manual authorization or second submission was not settled: %#v", recovery)
			}
			wakeTask(t, runtime, next.ID)
			next = runOneStorageTask(t, runtime, next, model.TaskStatusPending)
			if terminator.directObservations.Load() != 1 || terminator.directCalls.Load() != 1 || next.RetryCount != 1 {
				t.Fatalf("second transaction did not remain in direct observation: task=%#v reads=%d sends=%d", next, terminator.directObservations.Load(), terminator.directCalls.Load())
			}
			terminator.directSucceeded.Store(true)
			wakeTask(t, runtime, next.ID)
			next = runOneStorageTask(t, runtime, next, model.TaskStatusPending)
			wakeTask(t, runtime, next.ID)
			runOneStorageTask(t, runtime, next, model.TaskStatusCompleted)
			attempts, err := runtime.repos.Replacements.ListTerminationAttempts(t.Context(), []int64{replacement.ID})
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 2 {
				t.Fatalf("permanent termination history has %d rows, want 2", len(attempts))
			}
			for i, expected := range []struct {
				taskID int64
				txHash string
				mined  string
				status uint64
			}{
				{taskID: failed.ID, txHash: "0xattempt-a", mined: "0xmined-a", status: 0},
				{taskID: next.ID, txHash: "0xattempt-b", mined: "0xmined-b", status: 1},
			} {
				attempt := attempts[i]
				if attempt.AttemptNo != int64(i+1) || attempt.TaskID == nil || *attempt.TaskID != expected.taskID || attempt.AttemptedAt == nil {
					t.Fatalf("attempt %d lost its originating task or send time: %#v", i+1, attempt)
				}
				if attempt.TxHash == nil || *attempt.TxHash != expected.txHash || attempt.ConfirmedTxHash == nil || *attempt.ConfirmedTxHash != expected.mined {
					t.Fatalf("attempt %d lost its transaction pair: %#v", i+1, attempt)
				}
				if attempt.ReceiptStatus == nil || *attempt.ReceiptStatus != expected.status {
					t.Fatalf("attempt %d receipt = %v, want %d", i+1, attempt.ReceiptStatus, expected.status)
				}
			}
			history, err := runtime.repos.Tasks.GetByID(t.Context(), failed.ID)
			if err != nil || history.Status != model.TaskStatusFailed || string(history.Checkpoint) != failedCheckpoint {
				t.Fatalf("manual retry changed failed task history: %#v err=%v", history, err)
			}
			if terminator.directCalls.Load() != 1 || terminator.calls.Load() != 1 {
				t.Fatalf("retirement paid again after the authorized attempt: provider=%d direct=%d", terminator.calls.Load(), terminator.directCalls.Load())
			}
		})
	}
}

func TestRetirementManualRetryFindsServiceEndedWithoutChangingFailedReceipt(t *testing.T) {
	terminator := &failedRetirementTerminator{testServiceTerminator: testServiceTerminator{
		result: &synapse.TerminationResult{TxHash: "0xattempt-a"}, err: context.DeadlineExceeded,
	}}
	runtime, replacement, failed := seedFailedRetirement(t, false, terminator)
	terminator.endedExternally.Store(true)
	next, err := runtime.service.Retry(t.Context(), failed.ID)
	if err != nil {
		t.Fatal(err)
	}
	next = runOneStorageTask(t, runtime, next, model.TaskStatusPending)
	wakeTask(t, runtime, next.ID)
	runOneStorageTask(t, runtime, next, model.TaskStatusCompleted)
	stored, err := runtime.repos.Replacements.GetByID(t.Context(), replacement.ID)
	if err != nil || stored.TerminationEpoch == nil || *stored.TerminationEpoch != 84 {
		t.Fatalf("external completion did not record the service epoch: %#v err=%v", stored, err)
	}
	attempts, err := runtime.repos.Replacements.ListTerminationAttempts(t.Context(), []int64{replacement.ID})
	if err != nil || len(attempts) != 1 {
		t.Fatalf("external completion created another transaction: %#v err=%v", attempts, err)
	}
	if attempts[0].ReceiptStatus == nil || *attempts[0].ReceiptStatus != 0 || attempts[0].ConfirmedTxHash == nil || *attempts[0].ConfirmedTxHash != "0xmined-a" {
		t.Fatalf("external completion changed the failed receipt: %#v", attempts[0])
	}
	if terminator.calls.Load() != 1 || terminator.directCalls.Load() != 0 {
		t.Fatalf("external completion caused a new send: provider=%d direct=%d", terminator.calls.Load(), terminator.directCalls.Load())
	}
}

func TestRetirementRecoversLegacyUnknownSubmissionWithoutSending(t *testing.T) {
	for _, tc := range []struct {
		name       string
		diagnostic error
	}{
		{name: "provider request not found"},
		{name: "provider unavailable", diagnostic: synapse.ErrTerminationObservationUnavailable},
		{name: "RPC unavailable", diagnostic: retirementRPCDiagnosticError{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminator := &observingRetirementTerminator{observeErr: tc.diagnostic}
			budget := 1
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true, maxAttempts: &budget})
			replacement, row := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
			var input storagereplacement.RetireInput
			if err := json.Unmarshal(row.Input, &input); err != nil {
				t.Fatal(err)
			}
			binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), input.DataSetID)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := bindings.RecordedIdentity(t.Context(), runtime.repos, binding)
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, err := json.Marshal(map[string]any{
				"identity": identity.Identity, "pdp_verifier": identity.PDPVerifier,
				"sends": 1, "attempted_at": time.Now().Add(-time.Hour), "next_resend_at": time.Now().Add(-time.Minute),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ?, resume_mode = 'execute' WHERE id = ?`, json.RawMessage(checkpoint), row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if row.RetryCount != 0 || row.ResumeMode != model.TaskResumeModeRecover {
				t.Fatalf("unknown submission was not observed first: %#v", row)
			}
			row = expireRetirementObservation(t, runtime, row)
			row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			if row.FailureReason == nil || *row.FailureReason != "attempts_exhausted" {
				t.Fatalf("legacy unknown submission did not stop within its budget: %#v", row)
			}
			if terminator.calls.Load() != 0 || terminator.observations.Load() != 2 {
				t.Fatalf("legacy recovery sent another request: requests=%d observations=%d", terminator.calls.Load(), terminator.observations.Load())
			}
			attempts, err := runtime.repos.Replacements.ListTerminationAttempts(t.Context(), []int64{replacement.ID})
			if err != nil || len(attempts) != 0 {
				t.Fatalf("legacy recovery invented a transaction attempt: %#v err=%v", attempts, err)
			}
		})
	}
}

func TestRetirementObservationPreservesSafeTerminationAndProtocolWait(t *testing.T) {
	tests := []struct {
		name         string
		pending      bool
		observeErr   error
		observed     *synapse.TerminationResult
		wantRequests int64
		wantRetries  int
		wantFailed   bool
		minimumDelay time.Duration
	}{
		{name: "provider observation unavailable", observeErr: errors.Join(synapse.ErrTerminationObservationUnavailable, &synapse.ProviderUnavailableError{Cause: errors.New("provider unavailable")}), wantRequests: 1},
		{name: "no provider request", wantRequests: 1},
		{name: "pending provider relay", pending: true},
		{name: "backpressure", observeErr: &synapse.ProviderUnavailableError{Cause: &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour}}, minimumDelay: time.Hour},
		{name: "unavailable with Retry-After", observeErr: errors.Join(synapse.ErrTerminationObservationUnavailable, &pdp.HTTPError{StatusCode: 503, RetryAfter: time.Hour}), minimumDelay: time.Hour},
		{name: "chain state unknown", observeErr: errors.New("reading storage service state: RPC unavailable"), wantRetries: 2, wantFailed: true},
		{name: "different payer", observeErr: synapse.ErrServicePaidByAnother, wantRetries: 2, wantFailed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			terminator := &observingRetirementTerminator{
				testServiceTerminator: testServiceTerminator{result: &synapse.TerminationResult{EndEpoch: new(int64(84))}},
				pending:               tt.pending,
				observeErr:            tt.observeErr,
			}
			maxAttempts := 3
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true, maxAttempts: &maxAttempts})
			_, taskRow := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
			started := time.Now()
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)

			settled := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				wantStatus := model.TaskStatusPending
				if tt.wantFailed {
					wantStatus = model.TaskStatusFailed
				}
				return terminator.observations.Load() > 0 && task.Status == wantStatus && task.RetryCount == tt.wantRetries &&
					(tt.wantRequests == 0 || task.Checkpoint != nil) &&
					(tt.minimumDelay == 0 || !task.AvailableAt.Before(started.Add(tt.minimumDelay)))
			})
			if got := terminator.calls.Load(); got != tt.wantRequests {
				t.Fatalf("termination requests = %d, want %d", got, tt.wantRequests)
			}
			if tt.minimumDelay > 0 {
				deadline, observations := settled.AvailableAt, terminator.observations.Load()
				wakeTask(t, runtime, taskRow.ID)
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusPending && task.ClaimGeneration > settled.ClaimGeneration && task.AvailableAt.Equal(deadline)
				})
				if terminator.observations.Load() != observations || terminator.calls.Load() != 0 {
					t.Fatal("early wake bypassed the provider observation deadline")
				}
			}
			if tt.wantRequests > 0 {
				wakeTask(t, runtime, taskRow.ID)
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
				if got := terminator.calls.Load(); got != tt.wantRequests {
					t.Fatalf("termination requests after completion = %d, want %d", got, tt.wantRequests)
				}
			}
		})
	}
}

func TestRetirementSubmittedEvidenceSurvivesObservationLossAndManualRetry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pending bool
		err     error
	}{
		{name: "provider request not found"},
		{name: "provider observation unavailable", err: errors.Join(synapse.ErrTerminationObservationUnavailable, errors.New("provider is offline"))},
		{name: "pending transaction", pending: true},
		{name: "provider backpressure", err: &pdp.HTTPError{StatusCode: 429, Body: "provider relay is rate limited", RetryAfter: time.Hour}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminator := &observingRetirementTerminator{
				testServiceTerminator: testServiceTerminator{result: &synapse.TerminationResult{TxHash: "0xoriginal"}, err: context.DeadlineExceeded},
			}
			maxAttempts := 2
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true, maxAttempts: &maxAttempts})
			replacement, row := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if terminator.calls.Load() != 1 || row.RetryCount != 1 {
				t.Fatalf("initial submission task=%#v requests=%d", row, terminator.calls.Load())
			}

			var checkpoint map[string]any
			if err := json.Unmarshal(row.Checkpoint, &checkpoint); err != nil {
				t.Fatal(err)
			}
			delete(checkpoint, "transaction_hash")
			checkpoint["next_resend_at"] = time.Now().Add(-time.Hour)
			raw, err := json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ?, resume_mode = 'execute', available_at = ? WHERE id = ?`, json.RawMessage(raw), time.Now().Add(-time.Second), row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			terminator.pending, terminator.observeErr = tc.pending, tc.err
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if terminator.calls.Load() != 1 || row.RetryCount != 1 || row.ResumeMode != model.TaskResumeModeRecover || !strings.Contains(string(row.Checkpoint), `"transaction_hash": "0xoriginal"`) {
				t.Fatalf("saved submission was not restored: task=%#v requests=%d", row, terminator.calls.Load())
			}
			if throttled, ok := errors.AsType[*pdp.HTTPError](tc.err); ok {
				observations := terminator.observations.Load()
				wakeTask(t, runtime, row.ID)
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				if terminator.observations.Load() != observations || row.LastError == nil || !strings.Contains(*row.LastError, throttled.Body) {
					t.Fatalf("skipped observation lost its diagnostic: task=%#v observations=%d", row, terminator.observations.Load())
				}
			}

			var state model.TaskRuntime
			if err := json.Unmarshal(row.Runtime, &state); err != nil || state.Observation == nil {
				t.Fatalf("observation window missing: %s err=%v", row.Runtime, err)
			}
			state.Observation.StartedAt = time.Now().Add(-time.Hour)
			raw, err = json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET runtime_json = ?, available_at = ? WHERE id = ?`, json.RawMessage(raw), time.Now().Add(-time.Second), row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			if row.LastError == nil || !strings.Contains(*row.LastError, "still unconfirmed") || terminator.calls.Load() != 1 {
				t.Fatalf("unconfirmed transaction did not stop safely: task=%#v requests=%d", row, terminator.calls.Load())
			}
			if tc.err != nil && !strings.Contains(*row.LastError, tc.err.Error()) {
				t.Fatalf("observation timeout lost the provider error: %s", *row.LastError)
			}
			row, err = runtime.service.Retry(t.Context(), row.ID)
			if err != nil {
				t.Fatal(err)
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if terminator.calls.Load() != 1 || row.RetryCount != 0 {
				t.Fatalf("manual retry sent another transaction: task=%#v requests=%d", row, terminator.calls.Load())
			}
			terminator.observeErr = nil
			terminator.observed = &synapse.TerminationResult{TxHash: "0xoriginal", ConfirmedTxHash: "0xconfirmed", EndEpoch: new(int64(84))}
			if err := json.Unmarshal(row.Checkpoint, &checkpoint); err != nil {
				t.Fatal(err)
			}
			checkpoint["protocol_next_observation_at"] = time.Now().Add(-time.Second)
			raw, err = json.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, json.RawMessage(raw), row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			wakeTask(t, runtime, row.ID)
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			if strings.Contains(string(row.Checkpoint), "observation_diagnostic") {
				t.Fatalf("successful observation retained the old diagnostic: %s", row.Checkpoint)
			}
			wakeTask(t, runtime, row.ID)
			row = runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
			stored, err := runtime.repos.Replacements.GetByID(t.Context(), replacement.ID)
			if err != nil || stored.AbandonedTerminationTxHash == nil || *stored.AbandonedTerminationTxHash != "0xoriginal" || stored.AbandonedTerminationConfirmedTxHash == nil || *stored.AbandonedTerminationConfirmedTxHash != "0xconfirmed" || stored.AbandonedTerminationEpoch == nil || *stored.AbandonedTerminationEpoch != 84 || terminator.calls.Load() != 1 {
				t.Fatalf("confirmed evidence=%#v error=%v requests=%d task=%#v", stored, err, terminator.calls.Load(), row)
			}
		})
	}
}

func TestRetirementRejectsConflictingCheckpointAndLedger(t *testing.T) {
	terminator := &observingRetirementTerminator{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, retireOnly: true})
	replacement, row := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	var input storagereplacement.RetireInput
	if err := json.Unmarshal(row.Input, &input); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Replacements.RecordTerminationEvidence(t.Context(), repository.RecordTerminationEvidenceInput{
		ReplacementID: replacement.ID, DataSetID: input.DataSetID, Generation: input.Generation, TaskID: row.ID,
		Role: storagereplacement.TerminationRoleAbandonedTarget, TxHash: "0xledger",
	}); err != nil {
		t.Fatal(err)
	}
	binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), input.DataSetID)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := bindings.RecordedIdentity(t.Context(), runtime.repos, binding)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := map[string]any{"transaction_hash": "0xcheckpoint", "identity": identity.Identity, "pdp_verifier": identity.PDPVerifier}
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, json.RawMessage(raw), row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
	if row.FailureReason == nil || *row.FailureReason != "termination_evidence_conflict" || terminator.calls.Load() != 0 || terminator.observations.Load() != 0 {
		t.Fatalf("conflicting evidence was used: task=%#v requests=%d observations=%d", row, terminator.calls.Load(), terminator.observations.Load())
	}
}

type retirementRPCDiagnosticError struct{}

func (retirementRPCDiagnosticError) Error() string  { return "termination receipt RPC unavailable" }
func (retirementRPCDiagnosticError) ErrorCode() int { return 3 }
func (retirementRPCDiagnosticError) ErrorData() any { return "0xdeadbeef" }

func TestSourceRetirementObservationExhaustionRetainsEvidenceAndDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name       string
		diagnostic error
	}{
		{name: "provider not found", diagnostic: &pdp.HTTPError{StatusCode: 404, Body: "termination record is missing"}},
		{name: "provider unavailable", diagnostic: synapse.ErrTerminationObservationUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminator := &observingRetirementTerminator{
				testServiceTerminator: testServiceTerminator{
					result: &synapse.TerminationResult{TxHash: "0xoriginal", ConfirmedTxHash: "0xconfirmed"}, err: context.DeadlineExceeded,
				},
			}
			maxAttempts := 2
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, retireOnly: true, maxAttempts: &maxAttempts})
			replacement, row := seedSourceRetirement(t, runtime, storedObjectSequence.Add(1))
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			terminator.observeErr = errors.Join(tc.diagnostic, retirementRPCDiagnosticError{})
			wakeTask(t, runtime, row.ID)
			row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			var state model.TaskRuntime
			if err := json.Unmarshal(row.Runtime, &state); err != nil || state.Observation == nil {
				t.Fatalf("observation window missing: %s err=%v", row.Runtime, err)
			}
			state.Observation.StartedAt = time.Now().Add(-time.Hour)
			raw, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET runtime_json = ?, available_at = ? WHERE id = ?`, json.RawMessage(raw), time.Now().Add(-time.Second), row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			stored, err := runtime.repos.Replacements.GetByID(t.Context(), replacement.ID)
			if err != nil || stored.Status != storagereplacement.StatusCleanupAttention || stored.TerminationEpoch != nil || stored.TerminationTxHash == nil || *stored.TerminationTxHash != "0xoriginal" || stored.TerminationConfirmedTxHash == nil || *stored.TerminationConfirmedTxHash != "0xconfirmed" || terminator.calls.Load() != 1 {
				t.Fatalf("source retirement stopped without its evidence: replacement=%#v error=%v requests=%d", stored, err, terminator.calls.Load())
			}
			for _, diagnostic := range []*string{row.LastError, stored.LastError} {
				if diagnostic == nil || !strings.Contains(*diagnostic, "still unconfirmed") || !strings.Contains(*diagnostic, tc.diagnostic.Error()) || !strings.Contains(*diagnostic, "RPC code: 3") || !strings.Contains(*diagnostic, "Revert data: 0xdeadbeef") {
					t.Fatalf("source retirement lost its diagnostic: %v", diagnostic)
				}
			}
		})
	}
}
