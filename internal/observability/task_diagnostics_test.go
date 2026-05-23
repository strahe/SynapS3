package observability

import (
	"reflect"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

func TestTaskDiagnosticFromFactsClassifiesUploadEvidence(t *testing.T) {
	now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		facts         TaskDiagnosticFacts
		wantState     TaskDiagnosticCurrentState
		wantStatus    Status
		wantReasons   []ReasonCode
		wantNext      TaskDiagnosticNextAction
		wantOperation TaskDiagnosticOperation
	}{
		{
			name:          "insufficient funds",
			facts:         taskDiagnosticFactsWithLastError("USDFC available funds = 0"),
			wantState:     TaskDiagnosticStateUnavailable,
			wantStatus:    StatusUnavailable,
			wantReasons:   []ReasonCode{ReasonTaskInsufficientFunds},
			wantNext:      TaskDiagnosticActionCheckWalletFunds,
			wantOperation: TaskDiagnosticOperationAddPieces,
		},
		{
			name:          "approval missing",
			facts:         taskDiagnosticFactsWithLastError("wallet allowance approval missing"),
			wantState:     TaskDiagnosticStateUnavailable,
			wantStatus:    StatusUnavailable,
			wantReasons:   []ReasonCode{ReasonTaskMissingApproval},
			wantNext:      TaskDiagnosticActionCheckWalletApproval,
			wantOperation: TaskDiagnosticOperationAddPieces,
		},
		{
			name:          "rpc timeout",
			facts:         taskDiagnosticFactsWithLastError("rpc deadline exceeded while reading transaction receipt"),
			wantState:     TaskDiagnosticStateUnknown,
			wantStatus:    StatusUnknown,
			wantReasons:   []ReasonCode{ReasonTaskRPCUnavailable},
			wantNext:      TaskDiagnosticActionInspectProvider,
			wantOperation: TaskDiagnosticOperationAddPieces,
		},
		{
			name: "external confirmation wait",
			facts: TaskDiagnosticFacts{
				Task: TaskDiagnosticTaskFacts{
					Type:       model.TaskTypeUpload,
					Stage:      "ensure_dataset",
					Status:     model.TaskStatusWaiting,
					WaitReason: taskWaitReasonPtr(model.TaskWaitReasonExternalConfirmation),
				},
				DataSet: &TaskDiagnosticDataSetFacts{Status: model.StorageDataSetStatusCreating},
			},
			wantState:     TaskDiagnosticStateWaitingForChain,
			wantStatus:    StatusDegraded,
			wantReasons:   []ReasonCode{ReasonTaskChainPending},
			wantNext:      TaskDiagnosticActionWait,
			wantOperation: TaskDiagnosticOperationCreateDataSet,
		},
		{
			name:          "non upload",
			facts:         TaskDiagnosticFacts{Task: TaskDiagnosticTaskFacts{Type: model.TaskTypeEvictCache, Status: model.TaskStatusQueued}},
			wantState:     TaskDiagnosticStateNotApplicable,
			wantStatus:    StatusAvailable,
			wantReasons:   []ReasonCode{ReasonTaskNotApplicable},
			wantNext:      TaskDiagnosticActionNone,
			wantOperation: TaskDiagnosticOperationNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TaskDiagnosticFromFacts(tt.facts, nil, now)
			if got.CurrentState != tt.wantState || got.Signal.Status != tt.wantStatus || got.NextAction != tt.wantNext || got.Evidence.Operation != tt.wantOperation {
				t.Fatalf("diagnostic = state:%s status:%s next:%s op:%s, want state:%s status:%s next:%s op:%s",
					got.CurrentState, got.Signal.Status, got.NextAction, got.Evidence.Operation,
					tt.wantState, tt.wantStatus, tt.wantNext, tt.wantOperation)
			}
			if !reflect.DeepEqual(got.Signal.ReasonCodes, tt.wantReasons) {
				t.Fatalf("reasons = %#v, want %#v", got.Signal.ReasonCodes, tt.wantReasons)
			}
		})
	}
}

func TestTaskDiagnosticFromFactsClassifiesLiveStatus(t *testing.T) {
	now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	baseFacts := taskDiagnosticFactsWithLastError("")

	tests := []struct {
		name        string
		live        TaskDiagnosticLiveCheck
		wantState   TaskDiagnosticCurrentState
		wantStatus  Status
		wantReasons []ReasonCode
		wantNext    TaskDiagnosticNextAction
	}{
		{
			name:        "pending",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLivePending, TxStatus: "pending"},
			wantState:   TaskDiagnosticStateWaitingForChain,
			wantStatus:  StatusDegraded,
			wantReasons: []ReasonCode{ReasonTaskChainPending},
			wantNext:    TaskDiagnosticActionWait,
		},
		{
			name:        "confirmed",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLiveConfirmed, TxStatus: "confirmed", PiecesAdded: boolPtr(true), PieceCount: intPtr(1)},
			wantState:   TaskDiagnosticStateConfirmed,
			wantStatus:  StatusAvailable,
			wantReasons: []ReasonCode{ReasonTaskChainConfirmed},
			wantNext:    TaskDiagnosticActionNone,
		},
		{
			name:        "rejected",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLiveRejected, TxStatus: "rejected"},
			wantState:   TaskDiagnosticStateRejected,
			wantStatus:  StatusUnavailable,
			wantReasons: []ReasonCode{ReasonTaskTransactionRejected},
			wantNext:    TaskDiagnosticActionRetryTask,
		},
		{
			name:        "mismatch",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLiveMismatch, TxStatus: "confirmed", PiecesAdded: boolPtr(false)},
			wantState:   TaskDiagnosticStateMismatch,
			wantStatus:  StatusUnavailable,
			wantReasons: []ReasonCode{ReasonTaskPieceStatusMismatch},
			wantNext:    TaskDiagnosticActionInspectProvider,
		},
		{
			name:        "timeout unavailable",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLiveUnavailable, Error: "context deadline exceeded"},
			wantState:   TaskDiagnosticStateUnknown,
			wantStatus:  StatusUnknown,
			wantReasons: []ReasonCode{ReasonTaskRPCUnavailable},
			wantNext:    TaskDiagnosticActionInspectProvider,
		},
		{
			name:        "missing service URL unavailable",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLiveUnavailable, Error: "missing service URL"},
			wantState:   TaskDiagnosticStateUnavailable,
			wantStatus:  StatusUnavailable,
			wantReasons: []ReasonCode{ReasonTaskDiagnosticUnavailable},
			wantNext:    TaskDiagnosticActionInspectProvider,
		},
		{
			name:        "unavailable with parsed wallet error",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLiveUnavailable, Error: "insufficient funds for add pieces"},
			wantState:   TaskDiagnosticStateUnavailable,
			wantStatus:  StatusUnavailable,
			wantReasons: []ReasonCode{ReasonTaskInsufficientFunds},
			wantNext:    TaskDiagnosticActionCheckWalletFunds,
		},
		{
			name:        "unknown response",
			live:        TaskDiagnosticLiveCheck{State: TaskDiagnosticLiveUnknown, TxStatus: "queued"},
			wantState:   TaskDiagnosticStateUnknown,
			wantStatus:  StatusUnknown,
			wantReasons: []ReasonCode{ReasonTaskUnknownStatus},
			wantNext:    TaskDiagnosticActionInspectTask,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TaskDiagnosticFromFacts(baseFacts, &tt.live, now)
			if got.CurrentState != tt.wantState || got.Signal.Status != tt.wantStatus || got.NextAction != tt.wantNext {
				t.Fatalf("diagnostic = state:%s status:%s next:%s, want state:%s status:%s next:%s",
					got.CurrentState, got.Signal.Status, got.NextAction, tt.wantState, tt.wantStatus, tt.wantNext)
			}
			if !reflect.DeepEqual(got.Signal.ReasonCodes, tt.wantReasons) {
				t.Fatalf("reasons = %#v, want %#v", got.Signal.ReasonCodes, tt.wantReasons)
			}
		})
	}
}

func taskDiagnosticFactsWithLastError(lastError string) TaskDiagnosticFacts {
	var lastErrorPtr *string
	if lastError != "" {
		lastErrorPtr = &lastError
	}
	return TaskDiagnosticFacts{
		Task: TaskDiagnosticTaskFacts{
			Type:      model.TaskTypeUpload,
			Stage:     "ingress_commit",
			Status:    model.TaskStatusFailed,
			LastError: lastErrorPtr,
		},
		Copy: &TaskDiagnosticCopyFacts{
			Status: model.StorageUploadCopyStatusCommitting,
		},
		Transaction: &TaskDiagnosticTransactionFacts{
			Kind:          TaskDiagnosticOperationAddPieces,
			TransactionID: "0xcommit",
		},
	}
}

func taskWaitReasonPtr(value model.TaskWaitReason) *model.TaskWaitReason {
	return &value
}

func intPtr(value int) *int {
	return &value
}
