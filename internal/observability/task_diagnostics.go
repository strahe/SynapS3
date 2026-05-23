package observability

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
)

type TaskDiagnosticCurrentState string

const (
	TaskDiagnosticStateNotApplicable   TaskDiagnosticCurrentState = "not_applicable"
	TaskDiagnosticStatePreparing       TaskDiagnosticCurrentState = "preparing"
	TaskDiagnosticStateTransferring    TaskDiagnosticCurrentState = "transferring"
	TaskDiagnosticStateWaitingForChain TaskDiagnosticCurrentState = "waiting_for_chain"
	TaskDiagnosticStateConfirmed       TaskDiagnosticCurrentState = "confirmed"
	TaskDiagnosticStateRejected        TaskDiagnosticCurrentState = "rejected"
	TaskDiagnosticStateMismatch        TaskDiagnosticCurrentState = "mismatch"
	TaskDiagnosticStateUnavailable     TaskDiagnosticCurrentState = "unavailable"
	TaskDiagnosticStateUnknown         TaskDiagnosticCurrentState = "unknown"
)

type TaskDiagnosticNextAction string

const (
	TaskDiagnosticActionNone                TaskDiagnosticNextAction = "none"
	TaskDiagnosticActionWait                TaskDiagnosticNextAction = "wait"
	TaskDiagnosticActionRetryTask           TaskDiagnosticNextAction = "retry_task"
	TaskDiagnosticActionCheckWalletFunds    TaskDiagnosticNextAction = "check_wallet_funds"
	TaskDiagnosticActionCheckWalletApproval TaskDiagnosticNextAction = "check_wallet_approval"
	TaskDiagnosticActionInspectProvider     TaskDiagnosticNextAction = "inspect_provider"
	TaskDiagnosticActionInspectTask         TaskDiagnosticNextAction = "inspect_task"
)

type TaskDiagnosticOperation string

const (
	TaskDiagnosticOperationNone          TaskDiagnosticOperation = "none"
	TaskDiagnosticOperationPrepareUpload TaskDiagnosticOperation = "prepare_upload"
	TaskDiagnosticOperationTransferPiece TaskDiagnosticOperation = "transfer_piece"
	TaskDiagnosticOperationCreateDataSet TaskDiagnosticOperation = "create_data_set"
	TaskDiagnosticOperationAddPieces     TaskDiagnosticOperation = "add_pieces"
)

type TaskDiagnosticLiveState string

const (
	TaskDiagnosticLiveSkipped     TaskDiagnosticLiveState = "skipped"
	TaskDiagnosticLivePending     TaskDiagnosticLiveState = "pending"
	TaskDiagnosticLiveConfirmed   TaskDiagnosticLiveState = "confirmed"
	TaskDiagnosticLiveRejected    TaskDiagnosticLiveState = "rejected"
	TaskDiagnosticLiveMismatch    TaskDiagnosticLiveState = "mismatch"
	TaskDiagnosticLiveUnavailable TaskDiagnosticLiveState = "unavailable"
	TaskDiagnosticLiveUnknown     TaskDiagnosticLiveState = "unknown"
)

type TaskDiagnostic struct {
	CheckedAt    time.Time                  `json:"checked_at"`
	CurrentState TaskDiagnosticCurrentState `json:"current_state"`
	Signal       Signal                     `json:"signal"`
	ReasonCodes  []ReasonCode               `json:"reason_codes"`
	NextAction   TaskDiagnosticNextAction   `json:"next_action"`
	Evidence     TaskDiagnosticEvidence     `json:"evidence"`
}

type TaskDiagnosticEvidence struct {
	Task        TaskDiagnosticTaskFacts         `json:"task"`
	Upload      *TaskDiagnosticUploadFacts      `json:"upload,omitempty"`
	Copy        *TaskDiagnosticCopyFacts        `json:"copy,omitempty"`
	DataSet     *TaskDiagnosticDataSetFacts     `json:"data_set,omitempty"`
	Provider    *TaskDiagnosticProviderFacts    `json:"provider,omitempty"`
	Transaction *TaskDiagnosticTransactionFacts `json:"transaction,omitempty"`
	LiveCheck   *TaskDiagnosticLiveCheck        `json:"live_check,omitempty"`
	Operation   TaskDiagnosticOperation         `json:"operation"`
}

type TaskDiagnosticFacts struct {
	Task        TaskDiagnosticTaskFacts
	Upload      *TaskDiagnosticUploadFacts
	Copy        *TaskDiagnosticCopyFacts
	DataSet     *TaskDiagnosticDataSetFacts
	Provider    *TaskDiagnosticProviderFacts
	Transaction *TaskDiagnosticTransactionFacts
}

type TaskDiagnosticTaskFacts struct {
	ID            int64                 `json:"id,omitempty"`
	Type          model.TaskType        `json:"type"`
	Stage         string                `json:"stage,omitempty"`
	Status        model.TaskStatus      `json:"status"`
	RetryCount    int                   `json:"retry_count"`
	MaxRetries    int                   `json:"max_retries"`
	LastError     *string               `json:"last_error,omitempty"`
	StatusMessage *string               `json:"status_message,omitempty"`
	WaitReason    *model.TaskWaitReason `json:"wait_reason,omitempty"`
	ScheduledAt   *time.Time            `json:"scheduled_at,omitempty"`
}

type TaskDiagnosticUploadFacts struct {
	ID              int64                     `json:"id,omitempty"`
	Status          model.StorageUploadStatus `json:"status,omitempty"`
	RequestedCopies int                       `json:"requested_copies,omitempty"`
	ErrorMessage    *string                   `json:"error_message,omitempty"`
	AcceptError     *string                   `json:"accept_error,omitempty"`
}

type TaskDiagnosticCopyFacts struct {
	UploadID            int64                           `json:"upload_id,omitempty"`
	CopyIndex           int                             `json:"copy_index,omitempty"`
	Status              model.StorageUploadCopyStatus   `json:"status,omitempty"`
	ProviderID          *types.OnChainID                `json:"provider_id,omitempty"`
	StorageDataSetID    *int64                          `json:"storage_data_set_id,omitempty"`
	ChainDataSetID      *types.OnChainID                `json:"chain_data_set_id,omitempty"`
	PieceID             *types.OnChainID                `json:"piece_id,omitempty"`
	TransferMethod      model.StorageCopyTransferMethod `json:"transfer_method,omitempty"`
	CommitTransactionID *string                         `json:"commit_transaction_id,omitempty"`
	LastError           *string                         `json:"last_error,omitempty"`
}

type TaskDiagnosticDataSetFacts struct {
	ID                  int64                      `json:"id,omitempty"`
	Status              model.StorageDataSetStatus `json:"status,omitempty"`
	ProviderID          types.OnChainID            `json:"provider_id,omitempty"`
	CopyIndex           int                        `json:"copy_index,omitempty"`
	ChainDataSetID      *types.OnChainID           `json:"chain_data_set_id,omitempty"`
	ClientDataSetID     *types.OnChainID           `json:"client_data_set_id,omitempty"`
	CreateTransactionID *string                    `json:"create_transaction_id,omitempty"`
	CreateStatusURL     *string                    `json:"create_status_url,omitempty"`
	LastError           *string                    `json:"last_error,omitempty"`
}

type TaskDiagnosticProviderFacts struct {
	ProviderID   types.OnChainID `json:"provider_id,omitempty"`
	Status       Status          `json:"status,omitempty"`
	ReasonCodes  []ReasonCode    `json:"reason_codes,omitempty"`
	ServiceURL   *string         `json:"service_url,omitempty"`
	HealthStatus *string         `json:"health_status,omitempty"`
	LastError    *string         `json:"last_error,omitempty"`
}

type TaskDiagnosticTransactionFacts struct {
	Kind          TaskDiagnosticOperation `json:"kind"`
	StatusURL     string                  `json:"status_url,omitempty"`
	ServiceURL    string                  `json:"service_url,omitempty"`
	DataSetID     string                  `json:"data_set_id,omitempty"`
	TransactionID string                  `json:"transaction_id,omitempty"`
	PieceCount    int                     `json:"piece_count,omitempty"`
}

type TaskDiagnosticLiveCheck struct {
	State             TaskDiagnosticLiveState `json:"state"`
	StatusURL         string                  `json:"status_url,omitempty"`
	TxStatus          string                  `json:"tx_status,omitempty"`
	DataSetID         string                  `json:"data_set_id,omitempty"`
	DataSetCreated    *bool                   `json:"data_set_created,omitempty"`
	PiecesAdded       *bool                   `json:"pieces_added,omitempty"`
	PieceCount        *int                    `json:"piece_count,omitempty"`
	ConfirmedPieceIDs []string                `json:"confirmed_piece_ids,omitempty"`
	Error             string                  `json:"error,omitempty"`

	confirmedPieceIDsSet bool
}

// SetConfirmedPieceIDs records provider evidence even when the provider returns an empty list.
func (live *TaskDiagnosticLiveCheck) SetConfirmedPieceIDs(ids []string) {
	live.ConfirmedPieceIDs = ids
	if live.ConfirmedPieceIDs == nil {
		live.ConfirmedPieceIDs = []string{}
	}
	live.confirmedPieceIDsSet = true
}

func (live TaskDiagnosticLiveCheck) MarshalJSON() ([]byte, error) {
	type taskDiagnosticLiveCheckJSON struct {
		State             TaskDiagnosticLiveState `json:"state"`
		StatusURL         string                  `json:"status_url,omitempty"`
		TxStatus          string                  `json:"tx_status,omitempty"`
		DataSetID         string                  `json:"data_set_id,omitempty"`
		DataSetCreated    *bool                   `json:"data_set_created,omitempty"`
		PiecesAdded       *bool                   `json:"pieces_added,omitempty"`
		PieceCount        *int                    `json:"piece_count,omitempty"`
		ConfirmedPieceIDs any                     `json:"confirmed_piece_ids,omitempty"`
		Error             string                  `json:"error,omitempty"`
	}
	ids := any(nil)
	if live.confirmedPieceIDsSet {
		confirmed := live.ConfirmedPieceIDs
		if confirmed == nil {
			confirmed = []string{}
		}
		ids = confirmed
	}
	return json.Marshal(taskDiagnosticLiveCheckJSON{
		State:             live.State,
		StatusURL:         live.StatusURL,
		TxStatus:          live.TxStatus,
		DataSetID:         live.DataSetID,
		DataSetCreated:    live.DataSetCreated,
		PiecesAdded:       live.PiecesAdded,
		PieceCount:        live.PieceCount,
		ConfirmedPieceIDs: ids,
		Error:             live.Error,
	})
}

func TaskDiagnosticFromFacts(facts TaskDiagnosticFacts, live *TaskDiagnosticLiveCheck, now time.Time) TaskDiagnostic {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	operation := taskDiagnosticOperation(facts)
	state, status, reasons, next := taskDiagnosticAssessment(facts, live, operation)
	signal := BuildSignal(status, reasons, taskDiagnosticLastError(facts, live), &now, time.Hour, now)
	return TaskDiagnostic{
		CheckedAt:    now,
		CurrentState: state,
		Signal:       signal,
		ReasonCodes:  signal.ReasonCodes,
		NextAction:   next,
		Evidence: TaskDiagnosticEvidence{
			Task:        facts.Task,
			Upload:      facts.Upload,
			Copy:        facts.Copy,
			DataSet:     facts.DataSet,
			Provider:    facts.Provider,
			Transaction: facts.Transaction,
			LiveCheck:   live,
			Operation:   operation,
		},
	}
}

func taskDiagnosticAssessment(facts TaskDiagnosticFacts, live *TaskDiagnosticLiveCheck, operation TaskDiagnosticOperation) (TaskDiagnosticCurrentState, Status, []ReasonCode, TaskDiagnosticNextAction) {
	if facts.Task.Type != model.TaskTypeUpload {
		return TaskDiagnosticStateNotApplicable, StatusAvailable, []ReasonCode{ReasonTaskNotApplicable}, TaskDiagnosticActionNone
	}
	if live != nil && live.State != "" && live.State != TaskDiagnosticLiveSkipped {
		return taskDiagnosticFromLive(*live)
	}
	if state, status, reason, next, ok := taskDiagnosticFromError(taskDiagnosticTextEvidence(facts)); ok {
		return state, status, []ReasonCode{reason}, next
	}
	if facts.Task.WaitReason != nil && *facts.Task.WaitReason == model.TaskWaitReasonExternalConfirmation {
		return TaskDiagnosticStateWaitingForChain, StatusDegraded, []ReasonCode{ReasonTaskChainPending}, TaskDiagnosticActionWait
	}
	if facts.DataSet != nil {
		switch facts.DataSet.Status {
		case model.StorageDataSetStatusCreating:
			return TaskDiagnosticStateWaitingForChain, StatusDegraded, []ReasonCode{ReasonTaskChainPending}, TaskDiagnosticActionWait
		case model.StorageDataSetStatusFailed, model.StorageDataSetStatusUnavailable:
			return TaskDiagnosticStateUnavailable, StatusUnavailable, []ReasonCode{ReasonTaskDiagnosticUnavailable}, TaskDiagnosticActionInspectProvider
		case model.StorageDataSetStatusReady:
			if operation == TaskDiagnosticOperationCreateDataSet {
				return TaskDiagnosticStateConfirmed, StatusAvailable, []ReasonCode{ReasonTaskChainConfirmed}, TaskDiagnosticActionNone
			}
		}
	}
	if facts.Copy != nil {
		switch facts.Copy.Status {
		case model.StorageUploadCopyStatusCommitting:
			return TaskDiagnosticStateWaitingForChain, StatusDegraded, []ReasonCode{ReasonTaskChainPending}, TaskDiagnosticActionWait
		case model.StorageUploadCopyStatusCommitted:
			return TaskDiagnosticStateConfirmed, StatusAvailable, []ReasonCode{ReasonTaskChainConfirmed}, TaskDiagnosticActionNone
		case model.StorageUploadCopyStatusFailed:
			return TaskDiagnosticStateUnavailable, StatusUnavailable, []ReasonCode{ReasonTaskDiagnosticUnavailable}, TaskDiagnosticActionInspectProvider
		}
	}
	switch operation {
	case TaskDiagnosticOperationPrepareUpload:
		return TaskDiagnosticStatePreparing, StatusDegraded, []ReasonCode{ReasonCopyPending}, TaskDiagnosticActionWait
	case TaskDiagnosticOperationTransferPiece:
		return TaskDiagnosticStateTransferring, StatusDegraded, []ReasonCode{ReasonCopyPending}, TaskDiagnosticActionWait
	case TaskDiagnosticOperationCreateDataSet, TaskDiagnosticOperationAddPieces:
		return TaskDiagnosticStateUnknown, StatusUnknown, []ReasonCode{ReasonTaskMissingEvidence}, TaskDiagnosticActionInspectTask
	default:
		return TaskDiagnosticStateUnknown, StatusUnknown, []ReasonCode{ReasonTaskUnknownStatus}, TaskDiagnosticActionInspectTask
	}
}

func taskDiagnosticFromLive(live TaskDiagnosticLiveCheck) (TaskDiagnosticCurrentState, Status, []ReasonCode, TaskDiagnosticNextAction) {
	switch live.State {
	case TaskDiagnosticLivePending:
		return TaskDiagnosticStateWaitingForChain, StatusDegraded, []ReasonCode{ReasonTaskChainPending}, TaskDiagnosticActionWait
	case TaskDiagnosticLiveConfirmed:
		return TaskDiagnosticStateConfirmed, StatusAvailable, []ReasonCode{ReasonTaskChainConfirmed}, TaskDiagnosticActionNone
	case TaskDiagnosticLiveRejected:
		return TaskDiagnosticStateRejected, StatusUnavailable, []ReasonCode{ReasonTaskTransactionRejected}, TaskDiagnosticActionRetryTask
	case TaskDiagnosticLiveMismatch:
		return TaskDiagnosticStateMismatch, StatusUnavailable, []ReasonCode{ReasonTaskPieceStatusMismatch}, TaskDiagnosticActionInspectProvider
	case TaskDiagnosticLiveUnavailable:
		if state, status, reason, next, ok := taskDiagnosticFromError(live.Error); ok {
			return state, status, []ReasonCode{reason}, next
		}
		return TaskDiagnosticStateUnknown, StatusUnknown, []ReasonCode{ReasonTaskDiagnosticUnavailable}, TaskDiagnosticActionInspectProvider
	default:
		return TaskDiagnosticStateUnknown, StatusUnknown, []ReasonCode{ReasonTaskUnknownStatus}, TaskDiagnosticActionInspectTask
	}
}

func taskDiagnosticFromError(text string) (TaskDiagnosticCurrentState, Status, ReasonCode, TaskDiagnosticNextAction, bool) {
	lower := strings.ToLower(text)
	for _, matcher := range taskDiagnosticErrorMatchers {
		if matcher.matches(lower) {
			return matcher.state, matcher.status, matcher.reason, matcher.next, true
		}
	}
	return "", "", "", "", false
}

type taskDiagnosticErrorMatcher struct {
	needles []string
	state   TaskDiagnosticCurrentState
	status  Status
	reason  ReasonCode
	next    TaskDiagnosticNextAction
}

var taskDiagnosticErrorMatchers = []taskDiagnosticErrorMatcher{
	{
		needles: []string{"available funds = 0", "insufficient funds", "not enough funds"},
		state:   TaskDiagnosticStateUnavailable,
		status:  StatusUnavailable,
		reason:  ReasonTaskInsufficientFunds,
		next:    TaskDiagnosticActionCheckWalletFunds,
	},
	{
		needles: []string{"approval", "allowance"},
		state:   TaskDiagnosticStateUnavailable,
		status:  StatusUnavailable,
		reason:  ReasonTaskMissingApproval,
		next:    TaskDiagnosticActionCheckWalletApproval,
	},
	{
		needles: []string{"transaction rejected", "tx rejected", "pdp: transaction rejected"},
		state:   TaskDiagnosticStateRejected,
		status:  StatusUnavailable,
		reason:  ReasonTaskTransactionRejected,
		next:    TaskDiagnosticActionRetryTask,
	},
	{
		needles: []string{"mismatch", "confirmed without", "piecesadded=false"},
		state:   TaskDiagnosticStateMismatch,
		status:  StatusUnavailable,
		reason:  ReasonTaskPieceStatusMismatch,
		next:    TaskDiagnosticActionInspectProvider,
	},
	{
		needles: []string{"timeout", "deadline exceeded", "rpc"},
		state:   TaskDiagnosticStateUnknown,
		status:  StatusUnknown,
		reason:  ReasonTaskRPCUnavailable,
		next:    TaskDiagnosticActionInspectProvider,
	},
	{
		needles: []string{"missing service url", "missing status url"},
		state:   TaskDiagnosticStateUnavailable,
		status:  StatusUnavailable,
		reason:  ReasonTaskDiagnosticUnavailable,
		next:    TaskDiagnosticActionInspectProvider,
	},
	{
		needles: []string{"missing data set id", "missing transaction id"},
		state:   TaskDiagnosticStateUnavailable,
		status:  StatusUnavailable,
		reason:  ReasonTaskDiagnosticUnavailable,
		next:    TaskDiagnosticActionInspectTask,
	},
}

func (m taskDiagnosticErrorMatcher) matches(text string) bool {
	for _, needle := range m.needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func taskDiagnosticOperation(facts TaskDiagnosticFacts) TaskDiagnosticOperation {
	return TaskDiagnosticOperationForTask(facts.Task.Type, facts.Task.Stage)
}

// TaskDiagnosticOperationForTask returns the diagnostic operation represented by a task type and stage.
func TaskDiagnosticOperationForTask(taskType model.TaskType, stage string) TaskDiagnosticOperation {
	if taskType != model.TaskTypeUpload {
		return TaskDiagnosticOperationNone
	}
	switch stage {
	case "", "prepare_upload":
		return TaskDiagnosticOperationPrepareUpload
	case "ensure_dataset":
		return TaskDiagnosticOperationCreateDataSet
	case "ingress_store", "peer_pull":
		return TaskDiagnosticOperationTransferPiece
	case "ingress_commit", "peer_commit":
		return TaskDiagnosticOperationAddPieces
	default:
		return TaskDiagnosticOperationNone
	}
}

func taskDiagnosticTextEvidence(facts TaskDiagnosticFacts) string {
	var parts []string
	if facts.Task.LastError != nil {
		parts = append(parts, *facts.Task.LastError)
	}
	if facts.Task.StatusMessage != nil {
		parts = append(parts, *facts.Task.StatusMessage)
	}
	if facts.Upload != nil {
		if facts.Upload.ErrorMessage != nil {
			parts = append(parts, *facts.Upload.ErrorMessage)
		}
		if facts.Upload.AcceptError != nil {
			parts = append(parts, *facts.Upload.AcceptError)
		}
	}
	if facts.Copy != nil && facts.Copy.LastError != nil {
		parts = append(parts, *facts.Copy.LastError)
	}
	if facts.DataSet != nil && facts.DataSet.LastError != nil {
		parts = append(parts, *facts.DataSet.LastError)
	}
	if facts.Provider != nil && facts.Provider.LastError != nil {
		parts = append(parts, *facts.Provider.LastError)
	}
	return strings.Join(parts, "\n")
}

func taskDiagnosticLastError(facts TaskDiagnosticFacts, live *TaskDiagnosticLiveCheck) *string {
	if live != nil && live.Error != "" {
		return &live.Error
	}
	for _, value := range []*string{
		facts.Task.LastError,
		facts.Task.StatusMessage,
	} {
		if value != nil && *value != "" {
			return value
		}
	}
	if facts.Upload != nil {
		if facts.Upload.ErrorMessage != nil && *facts.Upload.ErrorMessage != "" {
			return facts.Upload.ErrorMessage
		}
		if facts.Upload.AcceptError != nil && *facts.Upload.AcceptError != "" {
			return facts.Upload.AcceptError
		}
	}
	if facts.Copy != nil && facts.Copy.LastError != nil && *facts.Copy.LastError != "" {
		return facts.Copy.LastError
	}
	if facts.DataSet != nil && facts.DataSet.LastError != nil && *facts.DataSet.LastError != "" {
		return facts.DataSet.LastError
	}
	if facts.Provider != nil && facts.Provider.LastError != nil && *facts.Provider.LastError != "" {
		return facts.Provider.LastError
	}
	return nil
}
