package admin

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/types"
)

type taskDiagnosticStatusChecker interface {
	CheckDataSetCreationStatus(context.Context, synapse.DataSetCreationStatusInput) synapse.PDPStatusResult
	CheckAddPiecesStatus(context.Context, synapse.AddPiecesStatusInput) synapse.PDPStatusResult
}

// SynapS3 currently commits one PieceInput per upload copy.
const taskDiagnosticAddPiecesExpectedPieceCount = 1

func (s *Server) WithTaskDiagnosticStatusChecker(checker taskDiagnosticStatusChecker) *Server {
	if checker != nil {
		s.taskDiagnosticChecker = checker
	}
	return s
}

func (s *Server) handleAPITaskDiagnostic(w http.ResponseWriter, r *http.Request) {
	s.handleAPITaskDiagnosticCommon(w, r, false)
}

func (s *Server) handleAPITaskDiagnosticRefresh(w http.ResponseWriter, r *http.Request) {
	s.handleAPITaskDiagnosticCommon(w, r, true)
}

func (s *Server) handleAPITaskDiagnosticCommon(w http.ResponseWriter, r *http.Request, refresh bool) {
	ctx := r.Context()
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	task, err := s.repos.Tasks.GetByID(ctx, id)
	if err != nil {
		s.logger.Error("api: failed to get task diagnostic", "error", err, "taskID", id)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if task == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	facts, err := s.taskDiagnosticFacts(ctx, task)
	if err != nil {
		s.logger.Error("api: failed to build task diagnostic facts", "error", err, "taskID", id)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	var live *observability.TaskDiagnosticLiveCheck
	if refresh {
		live = s.taskDiagnosticLiveCheck(ctx, facts)
	}
	writeJSON(w, http.StatusOK, observability.TaskDiagnosticFromFacts(facts, live, time.Now().UTC()))
}

func (s *Server) taskDiagnosticFacts(ctx context.Context, task *model.Task) (observability.TaskDiagnosticFacts, error) {
	stage := ""
	if ptr := taskStage(task); ptr != nil {
		stage = *ptr
	}
	scheduledAt := task.ScheduledAt
	facts := observability.TaskDiagnosticFacts{
		Task: observability.TaskDiagnosticTaskFacts{
			ID:            task.ID,
			Type:          task.Type,
			Stage:         stage,
			Status:        task.Status,
			RetryCount:    task.RetryCount,
			MaxRetries:    task.MaxRetries,
			LastError:     task.LastError,
			StatusMessage: task.StatusMessage,
			WaitReason:    task.WaitReason,
			ScheduledAt:   &scheduledAt,
		},
	}
	if task.Type != model.TaskTypeUpload || s.repos == nil || s.repos.Uploads == nil {
		return facts, nil
	}

	upload, err := s.taskDiagnosticUpload(ctx, task)
	if err != nil {
		return facts, err
	}
	if upload != nil {
		facts.Upload = &observability.TaskDiagnosticUploadFacts{
			ID:              upload.ID,
			Status:          upload.Status,
			RequestedCopies: upload.RequestedCopies,
			ErrorMessage:    upload.ErrorMessage,
			AcceptError:     upload.AcceptError,
		}
	}

	copyIndex := taskPayloadInt(task.Payload, "copy_index")
	var copyRow *model.StorageUploadCopy
	if upload != nil && copyIndex != nil {
		copyRow, err = s.repos.Uploads.GetUploadCopy(ctx, upload.ID, *copyIndex)
		if err != nil {
			return facts, err
		}
		if copyRow != nil {
			facts.Copy = taskDiagnosticCopyFacts(copyRow)
		}
	}

	binding, err := s.taskDiagnosticDataSet(ctx, upload, copyRow, copyIndex)
	if err != nil {
		return facts, err
	}
	if binding != nil {
		facts.DataSet = taskDiagnosticDataSetFacts(binding)
	}

	providerID := taskDiagnosticProviderID(copyRow, binding)
	if providerID != nil {
		facts.Provider = s.taskDiagnosticProviderFacts(ctx, *providerID)
	}
	if facts.Provider == nil && providerID != nil {
		facts.Provider = &observability.TaskDiagnosticProviderFacts{ProviderID: *providerID}
	}
	facts.Transaction = taskDiagnosticTransactionFacts(facts)
	return facts, nil
}

func (s *Server) taskDiagnosticUpload(ctx context.Context, task *model.Task) (*model.StorageUpload, error) {
	if uploadID := taskPayloadInt64(task.Payload, "upload_id"); uploadID != nil {
		return s.repos.Uploads.GetByID(ctx, *uploadID)
	}
	return nil, nil
}

func (s *Server) taskDiagnosticDataSet(ctx context.Context, upload *model.StorageUpload, copyRow *model.StorageUploadCopy, copyIndex *int) (*model.StorageDataSet, error) {
	if copyRow != nil && copyRow.StorageDataSetID != nil {
		return s.repos.Uploads.GetDataSetBindingByID(ctx, *copyRow.StorageDataSetID)
	}
	if upload != nil && copyIndex != nil {
		return s.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, upload.BucketID, *copyIndex)
	}
	return nil, nil
}

func (s *Server) taskDiagnosticProviderFacts(ctx context.Context, providerID types.OnChainID) *observability.TaskDiagnosticProviderFacts {
	if s.observability == nil {
		return nil
	}
	page, err := s.observability.ListProviderObservations(ctx, observability.ListOptions{ProviderID: &providerID, Limit: 1})
	if err != nil || len(page.Items) == 0 {
		return nil
	}
	observation := page.Items[0]
	return &observability.TaskDiagnosticProviderFacts{
		ProviderID:   observation.Facts.ProviderID,
		Status:       observation.Signal.Status,
		ReasonCodes:  observation.Signal.ReasonCodes,
		ServiceURL:   observation.Facts.ServiceURL,
		HealthStatus: observation.Facts.HealthStatus,
		LastError:    observation.Signal.LastError,
	}
}

func (s *Server) taskDiagnosticLiveCheck(ctx context.Context, facts observability.TaskDiagnosticFacts) *observability.TaskDiagnosticLiveCheck {
	if facts.Transaction == nil {
		return &observability.TaskDiagnosticLiveCheck{State: observability.TaskDiagnosticLiveSkipped}
	}
	checker := s.taskDiagnosticChecker
	if checker == nil {
		return &observability.TaskDiagnosticLiveCheck{State: observability.TaskDiagnosticLiveUnavailable, Error: "task diagnostic checker not configured"}
	}
	switch facts.Transaction.Kind {
	case observability.TaskDiagnosticOperationCreateDataSet:
		if facts.Transaction.StatusURL == "" {
			return &observability.TaskDiagnosticLiveCheck{State: observability.TaskDiagnosticLiveUnavailable, Error: "missing data set creation status URL"}
		}
		result := checker.CheckDataSetCreationStatus(ctx, synapse.DataSetCreationStatusInput{
			StatusURL:         facts.Transaction.StatusURL,
			TransactionID:     facts.Transaction.TransactionID,
			ExpectedDataSetID: facts.Transaction.DataSetID,
		})
		return taskDiagnosticLiveCheckFromSynapse(result, observability.TaskDiagnosticOperationCreateDataSet)
	case observability.TaskDiagnosticOperationAddPieces:
		result := checker.CheckAddPiecesStatus(ctx, synapse.AddPiecesStatusInput{
			ServiceURL:         facts.Transaction.ServiceURL,
			StatusURL:          facts.Transaction.StatusURL,
			DataSetID:          facts.Transaction.DataSetID,
			TransactionID:      facts.Transaction.TransactionID,
			ExpectedPieceCount: facts.Transaction.PieceCount,
		})
		return taskDiagnosticLiveCheckFromSynapse(result, observability.TaskDiagnosticOperationAddPieces)
	default:
		return &observability.TaskDiagnosticLiveCheck{State: observability.TaskDiagnosticLiveSkipped}
	}
}

func taskDiagnosticCopyFacts(copyRow *model.StorageUploadCopy) *observability.TaskDiagnosticCopyFacts {
	return &observability.TaskDiagnosticCopyFacts{
		UploadID:            copyRow.UploadID,
		CopyIndex:           copyRow.CopyIndex,
		Status:              copyRow.Status,
		ProviderID:          copyRow.ProviderID,
		StorageDataSetID:    copyRow.StorageDataSetID,
		ChainDataSetID:      copyRow.DataSetID,
		PieceID:             copyRow.PieceID,
		TransferMethod:      copyRow.TransferMethod,
		CommitTransactionID: copyRow.CommitTransactionID,
		LastError:           copyRow.LastError,
	}
}

func taskDiagnosticDataSetFacts(binding *model.StorageDataSet) *observability.TaskDiagnosticDataSetFacts {
	return &observability.TaskDiagnosticDataSetFacts{
		ID:                  binding.ID,
		Status:              binding.Status,
		ProviderID:          binding.ProviderID,
		CopyIndex:           binding.CopyIndex,
		ChainDataSetID:      binding.DataSetID,
		ClientDataSetID:     binding.ClientDataSetID,
		CreateTransactionID: binding.CreateTransactionID,
		CreateStatusURL:     binding.CreateStatusURL,
		LastError:           binding.LastError,
	}
}

func taskDiagnosticProviderID(copyRow *model.StorageUploadCopy, binding *model.StorageDataSet) *types.OnChainID {
	if copyRow != nil && copyRow.ProviderID != nil {
		return copyRow.ProviderID
	}
	if binding != nil {
		return &binding.ProviderID
	}
	return nil
}

func taskDiagnosticTransactionFacts(facts observability.TaskDiagnosticFacts) *observability.TaskDiagnosticTransactionFacts {
	switch observability.TaskDiagnosticOperationForTask(facts.Task.Type, facts.Task.Stage) {
	case observability.TaskDiagnosticOperationCreateDataSet:
		if facts.DataSet == nil {
			return nil
		}
		tx := &observability.TaskDiagnosticTransactionFacts{
			Kind:      observability.TaskDiagnosticOperationCreateDataSet,
			StatusURL: derefString(facts.DataSet.CreateStatusURL),
		}
		if facts.DataSet.CreateTransactionID != nil {
			tx.TransactionID = *facts.DataSet.CreateTransactionID
		}
		if facts.DataSet.ChainDataSetID != nil {
			tx.DataSetID = facts.DataSet.ChainDataSetID.String()
		}
		if facts.Provider != nil && facts.Provider.ServiceURL != nil {
			tx.ServiceURL = *facts.Provider.ServiceURL
		}
		return tx
	case observability.TaskDiagnosticOperationAddPieces:
		if facts.Copy == nil {
			return nil
		}
		transactionID := derefString(facts.Copy.CommitTransactionID)
		if transactionID == "" {
			return nil
		}
		tx := &observability.TaskDiagnosticTransactionFacts{
			Kind:          observability.TaskDiagnosticOperationAddPieces,
			TransactionID: transactionID,
			PieceCount:    taskDiagnosticAddPiecesExpectedPieceCount,
		}
		if facts.DataSet != nil && facts.DataSet.ChainDataSetID != nil {
			tx.DataSetID = facts.DataSet.ChainDataSetID.String()
		}
		if facts.Provider != nil && facts.Provider.ServiceURL != nil {
			tx.ServiceURL = *facts.Provider.ServiceURL
		}
		return tx
	default:
		return nil
	}
}

func taskDiagnosticLiveCheckFromSynapse(result synapse.PDPStatusResult, operation observability.TaskDiagnosticOperation) *observability.TaskDiagnosticLiveCheck {
	live := &observability.TaskDiagnosticLiveCheck{
		State:     taskDiagnosticLiveStateFromSynapse(result.State),
		StatusURL: result.StatusURL,
		TxStatus:  result.TxStatus,
		DataSetID: result.DataSetID,
		Error:     result.Error,
	}
	if result.State == synapse.PDPStatusUnavailable || result.State == synapse.PDPStatusUnknown {
		return live
	}
	switch operation {
	case observability.TaskDiagnosticOperationCreateDataSet:
		live.DataSetCreated = boolPtr(result.DataSetCreated)
	case observability.TaskDiagnosticOperationAddPieces:
		live.PiecesAdded = boolPtr(result.PiecesAdded)
		live.PieceCount = intPtr(result.PieceCount)
		live.SetConfirmedPieceIDs(result.ConfirmedPieceIDs)
	}
	return live
}

func taskDiagnosticLiveStateFromSynapse(state synapse.PDPStatusState) observability.TaskDiagnosticLiveState {
	switch state {
	case synapse.PDPStatusPending:
		return observability.TaskDiagnosticLivePending
	case synapse.PDPStatusConfirmed:
		return observability.TaskDiagnosticLiveConfirmed
	case synapse.PDPStatusRejected:
		return observability.TaskDiagnosticLiveRejected
	case synapse.PDPStatusMismatch:
		return observability.TaskDiagnosticLiveMismatch
	case synapse.PDPStatusUnavailable:
		return observability.TaskDiagnosticLiveUnavailable
	default:
		return observability.TaskDiagnosticLiveUnknown
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func intPtr(value int) *int {
	return &value
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
