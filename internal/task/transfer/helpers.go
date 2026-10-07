package transfer

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
)

func copyDefinition(taskType model.TaskType, retryLimit *int) taskengine.Definition {
	return taskengine.Definition{
		Type: taskType, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.CopyGenerationInput) error {
			return storagepipeline.ValidateCopyGenerationInput(*input)
		}),
		RetryLimit: retryLimit, AllowRetry: true,
		// A copy task that the Engine failed itself still holds its copy, and
		// only a retry can release it. Failures a handler settles keep their
		// own policy.
		CanManualRetry: func(task *model.Task) bool {
			if task == nil || task.FailureReason == nil {
				return false
			}
			return taskengine.RecoverableEngineFailure(*task.FailureReason) ||
				(task.Type == model.TaskTypeStoragePull && (*task.FailureReason == storagepull.FailureOutcomeUnknown || *task.FailureReason == storagepull.FailureCancelOutcomeUnknown || *task.FailureReason == storagepull.FailureRecoveryBlocked))
		},
		Subject: taskengine.SubjectFromInput(model.TaskSubjectStorageCopy, func(input storagepipeline.CopyGenerationInput) int64 {
			return input.CopyID
		}),
	}
}

func copyTaskKey(taskType model.TaskType, copyID, generation int64) string {
	switch taskType {
	case model.TaskTypeStorageTransferPlan:
		return storagepipeline.TransferPlanKey(copyID, generation)
	case model.TaskTypeStorageStore:
		return storagepipeline.StoreKey(copyID, generation)
	case model.TaskTypeStoragePull:
		return storagepipeline.PullKey(copyID, generation)
	default:
		panic(fmt.Sprintf("unsupported copy task type %q", taskType))
	}
}

func waitForCommitBacklog() taskengine.Result {
	return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "commit_backlog",
		"Waiting for earlier transfers to this storage service to be registered", nil)
}

func validateStoreCheckpoint(checkpoint storeCheckpoint, contentSize int64) error {
	if checkpoint.AttemptedAt.IsZero() || checkpoint.IntendedPieceCID == "" || checkpoint.ProviderServiceURL == "" {
		return errors.New("storage transfer checkpoint is incomplete")
	}
	pieceCID, err := cid.Parse(checkpoint.IntendedPieceCID)
	if err != nil {
		return err
	}
	pieceInfo, err := piece.ParseV2(pieceCID)
	if err != nil {
		return err
	}
	if contentSize < 0 || pieceInfo.RawSize != uint64(contentSize) {
		return fmt.Errorf("checkpointed storage identity has size %d, expected %d", pieceInfo.RawSize, contentSize)
	}
	return nil
}

func pullResultStatus(result *storage.PullResult, pieceCID cid.Cid) (storage.PullStatus, error) {
	if result == nil {
		return "", errors.New("provider returned no pull status")
	}
	switch result.Status {
	case storage.PullStatusPending, storage.PullStatusInProgress, storage.PullStatusRetrying, storage.PullStatusComplete, storage.PullStatusFailed:
	default:
		return "", fmt.Errorf("unknown overall pull status %q", result.Status)
	}
	var status storage.PullStatus
	found := false
	for _, piece := range result.Pieces {
		if !piece.PieceCID.Equals(pieceCID) {
			continue
		}
		if found {
			return "", errors.New("provider returned duplicate pull piece statuses")
		}
		found = true
		status = piece.Status
	}
	if !found {
		if len(result.Pieces) == 0 && (result.Status == storage.PullStatusPending || result.Status == storage.PullStatusInProgress || result.Status == storage.PullStatusRetrying) {
			return result.Status, nil
		}
		return "", errors.New("provider pull status does not identify the requested piece")
	}
	switch status {
	case storage.PullStatusPending, storage.PullStatusInProgress, storage.PullStatusRetrying, storage.PullStatusComplete, storage.PullStatusFailed:
	default:
		return "", fmt.Errorf("unknown pull piece status %q", status)
	}
	if (result.Status == storage.PullStatusFailed && status != storage.PullStatusFailed) ||
		(result.Status == storage.PullStatusComplete && status != storage.PullStatusComplete && status != storage.PullStatusFailed) {
		return "", errors.New("provider returned conflicting pull statuses")
	}
	return status, nil
}

func choosePullSource(sources []repository.ReadableStorageCopy, last *storagepull.Attempt) repository.ReadableStorageCopy {
	if last != nil {
		for _, source := range sources {
			if !source.ProviderID.Equal(last.SourceProviderID) || !source.DataSetID.Equal(last.SourceDataSetID) {
				return source
			}
		}
	}
	return sources[0]
}

func retryPullDependency(execution taskengine.Execution, err error, reason string) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	if execution.CancellationRequested() {
		return taskengine.Fail(err, storagepull.FailureRecoveryBlocked, nil)
	}
	return failPullOutcome(execution, err)
}

func retryPullOutcome(execution taskengine.Execution, err error, reason string) taskengine.Result {
	if execution.CancellationRequested() || execution.RetryWillFail() {
		return failPullOutcome(execution, err)
	}
	return retryTask(err, reason)
}

func failPullOutcome(execution taskengine.Execution, err error) taskengine.Result {
	reason := storagepull.FailureOutcomeUnknown
	if execution.CancellationRequested() {
		reason = storagepull.FailureCancelOutcomeUnknown
	}
	return taskengine.Fail(err, reason, nil)
}

func pullAttemptRequest(attempt *storagepull.Attempt) (cid.Cid, []byte, error) {
	if attempt.Status != storagepull.AttemptStatusAttempted || attempt.SourceRetrievalURL == "" {
		return cid.Undef, nil, errors.New("pull attempt has invalid request evidence")
	}
	pieceCID, err := cid.Parse(attempt.SourcePieceCID)
	if err != nil {
		return cid.Undef, nil, err
	}
	extra, err := hex.DecodeString(attempt.ExtraDataHex)
	if err == nil {
		_, err = storagecommit.ExtraDataNonce(extra)
	}
	return pieceCID, extra, err
}

func pullReservation(input storagepipeline.CopyGenerationInput, taskID int64, attempt *storagepull.Attempt) repository.ReservePullRequestInput {
	return repository.ReservePullRequestInput{
		CopyID: input.CopyID, Generation: input.Generation, TaskID: taskID, AttemptID: attempt.AttemptID,
		SourceProviderID: &attempt.SourceProviderID, SourceDataSetID: &attempt.SourceDataSetID, SourcePieceID: &attempt.SourcePieceID,
		SourcePieceCID: attempt.SourcePieceCID, SourceRetrievalURL: attempt.SourceRetrievalURL, ExtraDataHex: attempt.ExtraDataHex,
	}
}

func newAttemptID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("creating request identity: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
