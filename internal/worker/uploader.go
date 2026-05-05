package worker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const submittedCommitPollInterval = 4 * time.Second

var errCommitRejected = errors.New("commit transaction rejected")

var submittedCommitRequestTimeout = 15 * time.Second

var submittedCommitMaxWait = 5 * time.Minute

// Uploader claims upload tasks, persists upload provenance, and accepts complete
// uploads for object versions.
type Uploader struct {
	repos           *repository.Repositories
	cache           cache.Cache
	storage         synapse.StorageClient
	wallet          synapse.WalletQuerier // optional; nil skips balance pre-check
	stateMachine    *state.Machine
	autoEvict       bool
	evictMaxRetries int
	targetCopies    int
	concurrency     int
	pollInterval    time.Duration
	leaseTTL        time.Duration
	logger          *slog.Logger
	*livenessTracker
}

const (
	defaultEvictMaxRetries  = 3
	uploadPollJitterDivisor = 5
	defaultTargetCopies     = 2
	maxTargetCopies         = 8

	uploadStagePrepare         = "prepare_upload"
	uploadStageEnsureDataSet   = "ensure_dataset"
	uploadStagePrimaryStore    = "primary_store"
	uploadStagePrimaryCommit   = "primary_commit"
	uploadStageSecondaryPull   = "secondary_pull"
	uploadStageSecondaryCommit = "secondary_commit"
	uploadStageLegacy          = "legacy_upload"
)

// UploaderOption configures uploader behavior.
type UploaderOption func(*Uploader)

// WithEvictMaxRetries configures max retries for cache eviction tasks created after upload.
func WithEvictMaxRetries(maxRetries int) UploaderOption {
	return func(u *Uploader) {
		u.evictMaxRetries = maxRetries
	}
}

// WithTargetCopies configures the target number of storage copies for staged uploads.
func WithTargetCopies(copies int) UploaderOption {
	return func(u *Uploader) {
		if copies > 0 {
			u.targetCopies = boundedTargetCopies(copies)
		}
	}
}

func boundedTargetCopies(copies int) int {
	if copies <= 0 {
		return defaultTargetCopies
	}
	if copies > maxTargetCopies {
		return maxTargetCopies
	}
	return copies
}

// NewUploader creates a new upload worker.
func NewUploader(repos *repository.Repositories, c cache.Cache, sc synapse.StorageClient, wallet synapse.WalletQuerier, sm *state.Machine, autoEvict bool, concurrency int, pollInterval time.Duration, logger *slog.Logger, opts ...UploaderOption) *Uploader {
	u := &Uploader{
		repos:           repos,
		cache:           c,
		storage:         sc,
		wallet:          wallet,
		stateMachine:    sm,
		autoEvict:       autoEvict,
		evictMaxRetries: defaultEvictMaxRetries,
		targetCopies:    defaultTargetCopies,
		concurrency:     concurrency,
		pollInterval:    pollInterval,
		leaseTTL:        10 * time.Minute,
		logger:          logger,
		livenessTracker: newLivenessTracker(pollInterval),
	}
	for _, opt := range opts {
		opt(u)
	}
	return u
}

func (u *Uploader) Name() string { return "uploader" }

func (u *Uploader) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for range u.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u.runSlot(ctx)
		}()
	}

	wg.Wait()
	return ctx.Err()
}

func (u *Uploader) runSlot(ctx context.Context) {
	if !sleepUntilNextUploadPoll(ctx, u.pollInterval) {
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}

		u.recordTick()
		task, err := u.repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, u.leaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			u.logger.Error("claiming upload task", "error", err)
			if !sleepUntilNextUploadPoll(ctx, u.pollInterval) {
				return
			}
			continue
		}
		if task == nil {
			if !sleepUntilNextUploadPoll(ctx, u.pollInterval) {
				return
			}
			continue
		}

		u.recordWorkStarted()
		func() {
			defer u.recordWorkFinished()
			u.processTask(ctx, task)
		}()
	}
}

func sleepUntilNextUploadPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(uploadPollSleepDuration(interval))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func uploadPollSleepDuration(interval time.Duration) time.Duration {
	if interval <= 0 {
		return interval
	}

	maxJitter := interval / uploadPollJitterDivisor
	if maxJitter <= 0 {
		return interval
	}
	return interval + time.Duration(rand.Int63n(int64(maxJitter)+1))
}

// Healthy returns true if the worker has ticked recently.
func (u *Uploader) Healthy() bool { return u.healthy() }

func (u *Uploader) processTask(ctx context.Context, task *model.Task) {
	start := time.Now()
	defer func() {
		admin.WorkerTaskDuration.WithLabelValues("uploader").Observe(time.Since(start).Seconds())
	}()

	logger := u.logger.With("taskID", task.ID, "objectID", task.RefID, "versionID", task.RefVersionID)

	if u.storage == nil {
		logger.Warn("storage client not configured, failing task")
		_ = u.repos.Tasks.Fail(ctx, task.ID, "storage client not configured")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	version, err := u.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil || version == nil {
		logger.Warn("object version not found for upload task", "error", err)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "object not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	bucket, err := u.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "bucketID", version.BucketID, "error", err)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	if version.State == model.ObjectStateStored || version.State == model.ObjectStateCacheEvicted {
		if version.State == model.ObjectStateStored {
			u.enqueueEvictTask(ctx, logger, task.RefID, task.RefVersionID)
		}
		_ = u.repos.Tasks.Complete(ctx, task.ID)
		logger.Info("upload task already satisfied", "state", version.State)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
		return
	}

	u.processStagedTask(ctx, task, version, bucket, uploadTaskStage(task), logger)
}

func (u *Uploader) processLegacyUploadTask(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, logger *slog.Logger) {
	// Transition cached → uploading (on retry, object may already be in uploading state)
	if version.State != model.ObjectStateUploading {
		if err := state.TransitionState(ctx, u.stateMachine, u.repos.Objects, task.RefVersionID,
			model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			logger.Warn("state transition cached→uploading failed", "error", err)
			_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
			admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
			return
		}
	}

	pendingAccept, err := u.repos.Uploads.FindAcceptableUploadAttempt(ctx, task.ID, task.RefVersionID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "find acceptable upload", err)
		return
	}
	if pendingAccept != nil {
		u.acceptUploadAttempt(ctx, task, version, pendingAccept, logger)
		return
	}

	if err := u.checkPaymentBalancePreflight(ctx, logger); err != nil {
		u.handleBalancePreflightFailure(ctx, task, logger, err)
		return
	}

	// Read from cache
	rc, _, err := u.cache.Get(ctx, bucket.Name, version.CacheKey)
	if err != nil {
		if os.IsNotExist(err) && version.InCache {
			if markErr := u.repos.Objects.SetVersionCachePresence(ctx, task.RefVersionID, false); markErr != nil {
				logger.Warn("failed to mark cache location absent", "error", markErr)
			}
		}
		u.handleFailure(ctx, task, version, logger, "cache read", err)
		return
	}
	defer func() { _ = rc.Close() }()

	attempt, err := u.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceTaskID:    task.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "start upload attempt", err)
		return
	}

	// Upload to provider (SDK handles store + on-chain commit)
	uploadOpts := &storage.UploadOptions{
		DataSetMetadata: map[string]string{"bucket": bucket.Name},
	}
	result, err := u.storage.Upload(ctx, rc, uploadOpts)
	if err != nil {
		msg := decodeRevertReason(err.Error())
		u.recordUploadFailure(ctx, logger, attempt.ID, msg)
		u.handleFailure(ctx, task, version, logger, "upload",
			fmt.Errorf("%s", msg))
		return
	}
	if result == nil {
		u.recordUploadFailure(ctx, logger, attempt.ID, "upload returned nil result")
		u.handleFailure(ctx, task, version, logger, "upload", errors.New("upload returned nil result"))
		return
	}

	recordInput := recordUploadResultInput(attempt.ID, result)
	if !result.Complete {
		msg := fmt.Sprintf("upload incomplete: %d/%d copies committed", result.SuccessCount(), requestedCopies(result))
		recordInput.ErrorMessage = &msg
	}
	if err := u.repos.Uploads.RecordUploadResult(ctx, recordInput); err != nil {
		u.handleTaskFailure(ctx, task, logger, "record upload result", err)
		return
	}

	savedAttempt, err := u.repos.Uploads.GetByID(ctx, attempt.ID)
	if err != nil || savedAttempt == nil {
		if err == nil {
			err = errors.New("upload attempt not found after result record")
		}
		u.handleTaskFailure(ctx, task, logger, "load upload attempt", err)
		return
	}
	if savedAttempt.Status != model.StorageUploadStatusComplete {
		err := fmt.Errorf("upload result status %s", savedAttempt.Status)
		if savedAttempt.AcceptError != nil && *savedAttempt.AcceptError != "" {
			err = fmt.Errorf("upload result status %s: %s", savedAttempt.Status, *savedAttempt.AcceptError)
		}
		u.handleFailure(ctx, task, version, logger, "upload", err)
		return
	}

	u.acceptUploadAttempt(ctx, task, version, savedAttempt, logger)
}

func (u *Uploader) processStagedTask(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, stage string, logger *slog.Logger) {
	switch stage {
	case uploadStagePrepare:
		u.prepareStagedUpload(ctx, task, version, bucket, logger)
	case uploadStageLegacy:
		u.processLegacyUploadTask(ctx, task, version, bucket, logger)
	case uploadStageEnsureDataSet:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.ensureUploadDataSet(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStagePrimaryStore:
		uploadID, _, err := uploadStageIDs(task, false)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.primaryStore(ctx, task, version, bucket, uploadID, logger)
	case uploadStagePrimaryCommit:
		uploadID, _, err := uploadStageIDs(task, false)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.primaryCommit(ctx, task, version, bucket, uploadID, logger)
	case uploadStageSecondaryPull:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.secondaryPull(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStageSecondaryCommit:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.secondaryCommit(ctx, task, version, bucket, uploadID, copyIndex, logger)
	default:
		u.handleTaskFailure(ctx, task, logger, "parse upload task payload", fmt.Errorf("unknown upload stage %q", stage))
	}
}

func (u *Uploader) prepareStagedUpload(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, logger *slog.Logger) {
	if version.State == model.ObjectStateCached {
		if err := state.TransitionState(ctx, u.stateMachine, u.repos.Objects, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			u.handleTaskFailure(ctx, task, logger, "state transition cached→uploading", err)
			return
		}
	} else if version.State != model.ObjectStateUploading {
		u.handleTaskFailure(ctx, task, logger, "prepare upload", fmt.Errorf("object state %s is not uploadable", version.State))
		return
	}

	if err := u.checkPaymentBalancePreflight(ctx, logger); err != nil {
		u.handleBalancePreflightFailure(ctx, task, logger, err)
		return
	}

	upload, err := u.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceTaskID:    task.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "start upload attempt", err)
		return
	}
	bindings, err := u.ensureBucketProviderBindings(ctx, bucket, upload.ID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "ensure provider bindings", err)
		return
	}
	copyInputs := make([]repository.UploadCopyBindingInput, 0, len(bindings))
	for _, binding := range bindings {
		role := string(storage.CopyRoleSecondary)
		if binding.CopyIndex == 0 {
			role = string(storage.CopyRolePrimary)
		}
		copyInputs = append(copyInputs, repository.UploadCopyBindingInput{
			StorageDataSetID: binding.ID,
			CopyIndex:        binding.CopyIndex,
			Role:             role,
			ProviderID:       binding.ProviderID,
		})
	}
	if err := u.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, copyInputs); err != nil {
		u.handleTaskFailure(ctx, task, logger, "create upload copy rows", err)
		return
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStageEnsureDataSet, upload.ID, 0, true); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue primary dataset task", err)
		return
	}
	_ = u.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) ensureBucketProviderBindings(ctx context.Context, bucket *model.Bucket, uploadID int64) ([]model.StorageDataSet, error) {
	bindings, err := u.repos.Uploads.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	existing := make(map[int]struct{}, len(bindings))
	targetCopies := boundedTargetCopies(u.targetCopies)
	selected := make([]model.StorageDataSet, 0, targetCopies)
	excluded := make([]sdktypes.BigInt, 0, len(bindings))
	for _, binding := range bindings {
		existing[binding.CopyIndex] = struct{}{}
		excluded = append(excluded, binding.ProviderID.SDK())
		if binding.CopyIndex < targetCopies {
			selected = append(selected, binding)
		}
	}
	if len(selected) >= targetCopies {
		sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
		return selected, nil
	}
	missing := targetCopies - len(selected)
	contexts, err := u.storage.CreateContexts(ctx, &storage.CreateContextsOptions{
		Copies:             missing,
		ExcludeProviderIDs: excluded,
		DataSetMetadata:    map[string]string{"bucket": bucket.Name},
	})
	if err != nil {
		return nil, err
	}
	if len(contexts) != missing {
		return nil, fmt.Errorf("CreateContexts returned %d contexts, want %d", len(contexts), missing)
	}
	nextCopyIndex := 0
	for _, storageCtx := range contexts {
		for {
			if _, ok := existing[nextCopyIndex]; !ok {
				break
			}
			nextCopyIndex++
		}
		binding, err := u.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID:          bucket.ID,
			ProviderID:        idtypes.OnChainIDFromSDK(storageCtx.ProviderID()),
			CopyIndex:         nextCopyIndex,
			CreatedByUploadID: uploadID,
		})
		if err != nil {
			return nil, err
		}
		if dataSetID := storageCtx.DataSetID(); dataSetID != nil {
			if err := u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID:        binding.ID,
				UploadID:  uploadID,
				DataSetID: idtypes.OnChainIDFromSDK(*dataSetID),
			}); err != nil {
				return nil, err
			}
		}
		selected = append(selected, *binding)
		existing[nextCopyIndex] = struct{}{}
		nextCopyIndex++
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
	return selected, nil
}

func (u *Uploader) ensureUploadDataSet(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger) {
	binding, err := u.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, copyIndex)
	if err != nil || binding == nil {
		if err == nil {
			err = fmt.Errorf("dataset binding for copy_index %d not found", copyIndex)
		}
		u.handleTaskFailure(ctx, task, logger, "load dataset binding", err)
		return
	}
	if binding.Status != model.StorageDataSetStatusReady {
		storageCtx, err := u.contextForBindingProvider(ctx, binding, bucket.Name)
		if err != nil {
			u.markDataSetStageFailed(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "create dataset context", err)
			return
		}
		if dataSetID := storageCtx.DataSetID(); dataSetID != nil {
			if err := u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID:        binding.ID,
				UploadID:  uploadID,
				DataSetID: idtypes.OnChainIDFromSDK(*dataSetID),
			}); err != nil {
				u.handleTaskFailure(ctx, task, logger, "mark existing dataset ready", err)
				return
			}
		} else {
			switch binding.Status {
			case model.StorageDataSetStatusPending, model.StorageDataSetStatusFailed:
				var submitted storage.CreateDataSetSubmission
				var submitErr error
				result, err := storageCtx.CreateDataSet(ctx, &storage.CreateDataSetOptions{
					OnSubmitted: func(sub storage.CreateDataSetSubmission) {
						submitted = sub
						submitErr = u.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
							ID:              binding.ID,
							UploadID:        uploadID,
							TransactionID:   sub.TransactionID,
							StatusURL:       sub.StatusURL,
							ClientDataSetID: onChainIDPtrFromSDKPtr(sub.ClientDataSetID),
						})
					},
				})
				if err != nil {
					if submitted.TransactionID != "" {
						if submitErr != nil {
							u.handleTaskFailure(ctx, task, logger, "save dataset submission", submitErr)
							return
						}
						u.handleTaskFailure(ctx, task, logger, "wait dataset", err)
						return
					}
					u.markDataSetStageFailed(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "create dataset", err)
					return
				}
				if submitted.TransactionID != "" {
					binding.CreateTransactionID = &submitted.TransactionID
				}
				if err := u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
					ID:              binding.ID,
					UploadID:        uploadID,
					DataSetID:       idtypes.OnChainIDFromSDK(result.DataSetID),
					ClientDataSetID: onChainIDPtrFromSDK(result.ClientDataSetID),
				}); err != nil {
					u.handleTaskFailure(ctx, task, logger, "mark dataset ready", err)
					return
				}
			case model.StorageDataSetStatusCreating:
				if binding.CreateTransactionID == nil || binding.CreateStatusURL == nil || binding.ClientDataSetID == nil {
					u.markDataSetStageFailed(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "wait dataset", errors.New("dataset creation submission is incomplete"))
					return
				}
				clientDataSetID := sdkBigIntPtr(binding.ClientDataSetID)
				result, err := storageCtx.WaitForDataSetCreated(ctx, storage.CreateDataSetSubmission{
					TransactionID:   *binding.CreateTransactionID,
					StatusURL:       *binding.CreateStatusURL,
					ClientDataSetID: clientDataSetID,
				})
				if err != nil {
					if errors.Is(err, pdp.ErrTxRejected) {
						u.markDataSetStageFailed(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "wait dataset", err)
						return
					}
					u.handleTaskFailure(ctx, task, logger, "wait dataset", err)
					return
				}
				if err := u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
					ID:              binding.ID,
					UploadID:        uploadID,
					DataSetID:       idtypes.OnChainIDFromSDK(result.DataSetID),
					ClientDataSetID: onChainIDPtrFromSDK(result.ClientDataSetID),
				}); err != nil {
					u.handleTaskFailure(ctx, task, logger, "mark dataset ready", err)
					return
				}
			default:
				u.handleTaskFailure(ctx, task, logger, "ensure dataset", fmt.Errorf("dataset binding status %s cannot be ensured", binding.Status))
				return
			}
		}
	}
	nextStage := uploadStageSecondaryPull
	if copyIndex == 0 {
		nextStage = uploadStagePrimaryStore
	}
	if err := u.enqueueUploadStage(ctx, task, nextStage, uploadID, copyIndex, copyIndex != 0); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue next upload stage", err)
		return
	}
	_ = u.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) primaryStore(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, logger *slog.Logger) {
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, 0)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "primary context", err)
		return
	}
	if binding == nil {
		u.handleTaskFailure(ctx, task, logger, "primary context", errors.New("primary dataset binding not found"))
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, 0)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load primary copy", err)
		return
	}
	if copyHasPiece(copyRow) {
		if version.State == model.ObjectStateUploading {
			if err := state.TransitionState(ctx, u.stateMachine, u.repos.Objects, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
				u.handleTaskFailure(ctx, task, logger, "state transition uploading→committing", err)
				return
			}
		}
		if err := u.enqueueUploadStage(ctx, task, uploadStagePrimaryCommit, uploadID, 0, false); err != nil {
			u.handleTaskFailure(ctx, task, logger, "enqueue primary commit", err)
			return
		}
		_ = u.repos.Tasks.Complete(ctx, task.ID)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
		return
	}
	rc, _, err := u.cache.Get(ctx, bucket.Name, version.CacheKey)
	if err != nil {
		if os.IsNotExist(err) && version.InCache {
			if markErr := u.repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); markErr != nil {
				logger.Warn("failed to mark cache location absent", "error", markErr)
			}
		}
		u.handlePrimaryFailure(ctx, task, version, uploadID, logger, "cache read", err)
		return
	}
	defer func() { _ = rc.Close() }()
	result, err := storageCtx.Store(ctx, rc, nil)
	if err != nil {
		u.handlePrimaryFailure(ctx, task, version, uploadID, logger, "primary store", err)
		return
	}
	pieceCID := result.PieceCID.String()
	if err := u.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     uploadID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		RetrievalURL: storageCtx.PieceURL(result.PieceCID),
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark primary piece ready", err)
		return
	}
	if version.State == model.ObjectStateUploading {
		if err := state.TransitionState(ctx, u.stateMachine, u.repos.Objects, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
			u.handleTaskFailure(ctx, task, logger, "state transition uploading→committing", err)
			return
		}
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStagePrimaryCommit, uploadID, 0, false); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue primary commit", err)
		return
	}
	_ = u.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) primaryCommit(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, logger *slog.Logger) {
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, 0)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "primary commit context", err)
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, 0)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load primary copy", err)
		return
	}
	if copyCommitted(copyRow) {
		u.finishPrimaryCommitted(ctx, task, version, uploadID, logger)
		return
	}
	upload, err := u.repos.Uploads.GetByID(ctx, uploadID)
	if err != nil || upload == nil || upload.PieceCID == nil {
		if err == nil {
			err = errors.New("upload has no primary piece cid")
		}
		u.handleTaskFailure(ctx, task, logger, "load primary upload", err)
		return
	}
	pieceCID, err := cid.Decode(*upload.PieceCID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "decode primary piece cid", err)
		return
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	if copyCommitSubmitted(copyRow) {
		result, err := u.waitForSubmittedCommit(ctx, storageCtx, binding, *copyRow.CommitTransactionID, len(pieces))
		if err != nil {
			if errors.Is(err, errCommitRejected) {
				u.handlePrimaryFailure(ctx, task, version, uploadID, logger, "primary commit", err)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait primary commit", err)
			return
		}
		var pieceID *idtypes.OnChainID
		if len(result.PieceIDs) > 0 {
			pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
		}
		if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID:            uploadID,
			CopyIndex:           0,
			PieceCID:            *upload.PieceCID,
			PieceID:             pieceID,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
			CommitExtraDataHex:  derefString(copyRow.CommitExtraDataHex),
			CommitTransactionID: result.TransactionID,
		}); err != nil {
			u.handleTaskFailure(ctx, task, logger, "mark primary committed", err)
			return
		}
		u.finishPrimaryCommitted(ctx, task, version, uploadID, logger)
		return
	}
	extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, uploadID, 0, pieces)
	if err != nil {
		u.handlePrimaryFailure(ctx, task, version, uploadID, logger, "primary presign", err)
		return
	}
	var submittedTx string
	var submitErr error
	result, err := storageCtx.Commit(ctx, storage.CommitRequest{
		Pieces:    pieces,
		ExtraData: extraData,
		OnSubmitted: func(txHash string) {
			submittedTx = txHash
			submitErr = u.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
				UploadID:            uploadID,
				CopyIndex:           0,
				CommitExtraDataHex:  extraHex,
				CommitTransactionID: txHash,
			})
		},
	})
	if err != nil {
		if submittedTx != "" {
			if submitErr != nil {
				u.handleTaskFailure(ctx, task, logger, "save primary commit submission", submitErr)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait primary commit", err)
			return
		}
		u.handlePrimaryFailure(ctx, task, version, uploadID, logger, "primary commit", err)
		return
	}
	var pieceID *idtypes.OnChainID
	if len(result.PieceIDs) > 0 {
		pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
	}
	if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:            uploadID,
		CopyIndex:           0,
		PieceCID:            *upload.PieceCID,
		PieceID:             pieceID,
		RetrievalURL:        storageCtx.PieceURL(pieceCID),
		CommitExtraDataHex:  extraHex,
		CommitTransactionID: result.TransactionID,
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark primary committed", err)
		return
	}
	u.finishPrimaryCommitted(ctx, task, version, uploadID, logger)
}

func (u *Uploader) finishPrimaryCommitted(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, logger *slog.Logger) {
	refs, err := u.repos.Uploads.BindPrimaryCommittedUploadForContent(ctx, repository.BindPrimaryCommittedUploadInput{
		UploadID:    uploadID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "bind primary committed", err)
		return
	}
	u.enqueueEvictTasksForRefs(ctx, logger, refs)
	copies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "list upload copies", err)
		return
	}
	for _, copyRow := range copies {
		if copyRow.CopyIndex == 0 {
			continue
		}
		if err := u.enqueueUploadStage(ctx, task, uploadStageEnsureDataSet, uploadID, copyRow.CopyIndex, true); err != nil {
			u.handleTaskFailure(ctx, task, logger, "enqueue secondary dataset", err)
			return
		}
	}
	if len(copies) == 1 {
		finalized, storedRefs, err := u.repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize single-copy upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, storedRefs)
		}
	}
	_ = u.repos.Tasks.Complete(ctx, task.ID)
	logger.Info("primary upload committed", "uploadID", uploadID, "versions", len(refs))
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) repairPrimaryCommittedBinding(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, logger *slog.Logger, stage string) bool {
	_, err := u.repos.Uploads.BindPrimaryCommittedUploadForContent(ctx, repository.BindPrimaryCommittedUploadInput{
		UploadID:    uploadID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, stage, err)
		return false
	}
	return true
}

func (u *Uploader) secondaryPull(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger) {
	_, storageCtx, err := u.readyContextForCopy(ctx, bucket, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "secondary pull context", err)
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load secondary copy", err)
		return
	}
	if !u.repairPrimaryCommittedBinding(ctx, task, version, uploadID, logger, "repair primary binding") {
		return
	}
	if copyCommitted(copyRow) {
		finalized, refs, err := u.repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, refs)
		}
		_ = u.repos.Tasks.Complete(ctx, task.ID)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
		return
	}
	if copyHasPiece(copyRow) {
		if err := u.enqueueUploadStage(ctx, task, uploadStageSecondaryCommit, uploadID, copyIndex, true); err != nil {
			u.handleTaskFailure(ctx, task, logger, "enqueue secondary commit", err)
			return
		}
		_ = u.repos.Tasks.Complete(ctx, task.ID)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
		return
	}
	primaryCopies, err := u.repos.Uploads.ListReadablePrimaryCopy(ctx, uploadID)
	if err != nil || len(primaryCopies) == 0 {
		if err == nil {
			err = errors.New("primary readable copy not found")
		}
		u.handleTaskFailure(ctx, task, logger, "load primary readable copy", err)
		return
	}
	pieceCID, err := cid.Decode(primaryCopies[0].PieceCID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "decode primary piece cid", err)
		return
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, uploadID, copyIndex, pieces)
	if err != nil {
		u.markSecondaryFailed(ctx, task, uploadID, copyIndex, logger, "secondary presign", err)
		return
	}
	if _, err := storageCtx.Pull(ctx, storage.PullRequest{
		Pieces:    []cid.Cid{pieceCID},
		ExtraData: extraData,
		From: func(cid.Cid) string {
			return primaryCopies[0].RetrievalURL
		},
	}); err != nil {
		u.markSecondaryFailed(ctx, task, uploadID, copyIndex, logger, "secondary pull", err)
		return
	}
	if err := u.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     uploadID,
		CopyIndex:    copyIndex,
		PieceCID:     primaryCopies[0].PieceCID,
		RetrievalURL: storageCtx.PieceURL(pieceCID),
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark secondary piece ready", err)
		return
	}
	if err := u.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID:           uploadID,
		CopyIndex:          copyIndex,
		CommitExtraDataHex: extraHex,
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "save secondary extra data", err)
		return
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStageSecondaryCommit, uploadID, copyIndex, true); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue secondary commit", err)
		return
	}
	_ = u.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) secondaryCommit(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger) {
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "secondary commit context", err)
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load secondary copy", err)
		return
	}
	if !u.repairPrimaryCommittedBinding(ctx, task, version, uploadID, logger, "repair primary binding") {
		return
	}
	if copyCommitted(copyRow) {
		finalized, refs, err := u.repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, refs)
		}
		_ = u.repos.Tasks.Complete(ctx, task.ID)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
		return
	}
	primaryCopies, err := u.repos.Uploads.ListReadablePrimaryCopy(ctx, uploadID)
	if err != nil || len(primaryCopies) == 0 {
		if err == nil {
			err = errors.New("primary readable copy not found")
		}
		u.handleTaskFailure(ctx, task, logger, "load primary readable copy", err)
		return
	}
	pieceCID, err := cid.Decode(primaryCopies[0].PieceCID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "decode primary piece cid", err)
		return
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	if copyCommitSubmitted(copyRow) {
		result, err := u.waitForSubmittedCommit(ctx, storageCtx, binding, *copyRow.CommitTransactionID, len(pieces))
		if err != nil {
			if errors.Is(err, errCommitRejected) {
				u.markSecondaryFailed(ctx, task, uploadID, copyIndex, logger, "secondary commit", err)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait secondary commit", err)
			return
		}
		var pieceID *idtypes.OnChainID
		if len(result.PieceIDs) > 0 {
			pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
		}
		if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID:            uploadID,
			CopyIndex:           copyIndex,
			PieceCID:            primaryCopies[0].PieceCID,
			PieceID:             pieceID,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
			CommitExtraDataHex:  derefString(copyRow.CommitExtraDataHex),
			CommitTransactionID: result.TransactionID,
		}); err != nil {
			u.handleTaskFailure(ctx, task, logger, "mark secondary committed", err)
			return
		}
		finalized, refs, err := u.repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, refs)
		}
		_ = u.repos.Tasks.Complete(ctx, task.ID)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
		return
	}
	extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, uploadID, copyIndex, pieces)
	if err != nil {
		u.markSecondaryFailed(ctx, task, uploadID, copyIndex, logger, "secondary presign", err)
		return
	}
	var submittedTx string
	var submitErr error
	result, err := storageCtx.Commit(ctx, storage.CommitRequest{
		Pieces:    pieces,
		ExtraData: extraData,
		OnSubmitted: func(txHash string) {
			submittedTx = txHash
			submitErr = u.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
				UploadID:            uploadID,
				CopyIndex:           copyIndex,
				CommitExtraDataHex:  extraHex,
				CommitTransactionID: txHash,
			})
		},
	})
	if err != nil {
		if submittedTx != "" {
			if submitErr != nil {
				u.handleTaskFailure(ctx, task, logger, "save secondary commit submission", submitErr)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait secondary commit", err)
			return
		}
		u.markSecondaryFailed(ctx, task, uploadID, copyIndex, logger, "secondary commit", err)
		return
	}
	var pieceID *idtypes.OnChainID
	if len(result.PieceIDs) > 0 {
		pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
	}
	if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:            uploadID,
		CopyIndex:           copyIndex,
		PieceCID:            primaryCopies[0].PieceCID,
		PieceID:             pieceID,
		RetrievalURL:        storageCtx.PieceURL(pieceCID),
		CommitExtraDataHex:  extraHex,
		CommitTransactionID: result.TransactionID,
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark secondary committed", err)
		return
	}
	finalized, refs, err := u.repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
		return
	}
	if finalized {
		u.enqueueEvictTasksForRefs(ctx, logger, refs)
	}
	_ = u.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) enqueueEvictTask(ctx context.Context, logger *slog.Logger, objectID int64, versionID string) {
	if !u.autoEvict {
		return
	}
	evictTask := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("evict_cache:%s", versionID),
		Status:         model.TaskStatusPending,
		MaxRetries:     u.evictMaxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := u.repos.Tasks.Create(ctx, evictTask); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		logger.Warn("failed to enqueue eviction task (non-fatal)", "error", err, "versionID", versionID)
	}
}

func (u *Uploader) enqueueEvictTasksForRefs(ctx context.Context, logger *slog.Logger, refs []repository.ObjectVersionRef) {
	for _, ref := range refs {
		u.enqueueEvictTask(ctx, logger, ref.ObjectID, ref.VersionID)
	}
}

func (u *Uploader) enqueueUploadStage(ctx context.Context, parent *model.Task, stage string, uploadID int64, copyIndex int, includeCopyIndex bool) error {
	payload := map[string]interface{}{
		"upload_id": uploadID,
	}
	key := fmt.Sprintf("upload:%s:%s:%d", parent.RefVersionID, stage, uploadID)
	if includeCopyIndex {
		payload["copy_index"] = copyIndex
		key = fmt.Sprintf("%s:%d", key, copyIndex)
	}
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          parent.RefID,
		RefVersionID:   parent.RefVersionID,
		IdempotencyKey: key,
		Payload:        payload,
		Status:         model.TaskStatusPending,
		MaxRetries:     parent.MaxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := u.repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		return err
	}
	return nil
}

func uploadTaskStage(task *model.Task) string {
	if task == nil {
		return uploadStagePrepare
	}
	if task.Stage != nil && *task.Stage != "" {
		return *task.Stage
	}
	if task.Payload == nil {
		return uploadStagePrepare
	}
	stage, _ := task.Payload["stage"].(string)
	if stage == "" {
		return uploadStagePrepare
	}
	return stage
}

func uploadStageIDs(task *model.Task, needsCopyIndex bool) (int64, int, error) {
	uploadID, err := payloadInt64(task.Payload, "upload_id")
	if err != nil {
		return 0, 0, err
	}
	copyIndex := 0
	if needsCopyIndex {
		v, err := payloadInt64(task.Payload, "copy_index")
		if err != nil {
			return 0, 0, err
		}
		copyIndex = int(v)
	}
	return uploadID, copyIndex, nil
}

func payloadInt64(payload map[string]interface{}, key string) (int64, error) {
	raw, ok := payload[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	switch v := raw.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case json.Number:
		return v.Int64()
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("%s has unsupported type %T", key, raw)
	}
}

func (u *Uploader) markDataSetStageFailed(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, copyIndex int, dataSetID int64, logger *slog.Logger, stage string, err error) {
	_ = u.repos.Uploads.MarkDataSetFailed(ctx, dataSetID, err.Error())
	if copyIndex > 0 {
		u.markSecondaryFailed(ctx, task, uploadID, copyIndex, logger, stage, err)
		return
	}
	u.handlePrimaryFailure(ctx, task, version, uploadID, logger, stage, err)
}

func (u *Uploader) markSecondaryFailed(ctx context.Context, task *model.Task, uploadID int64, copyIndex int, logger *slog.Logger, stage string, err error) {
	_ = u.repos.Uploads.MarkUploadCopyFailed(ctx, uploadID, copyIndex, fmt.Sprintf("%s: %v", stage, err))
	logger.Error(stage+" failed", "error", err)
	if task.RetryCount+1 >= task.MaxRetries {
		if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
			logger.Error("failed to mark task as dead-letter", "error", ftErr)
		} else {
			admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUpload)).Inc()
		}
	} else {
		_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
		_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) readyContextForCopy(ctx context.Context, bucket *model.Bucket, copyIndex int) (*model.StorageDataSet, synapse.UploadContext, error) {
	binding, err := u.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, copyIndex)
	if err != nil {
		return nil, nil, err
	}
	if binding == nil {
		return nil, nil, nil
	}
	if binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.DataSetID.IsZero() {
		return nil, nil, fmt.Errorf("dataset binding %d is not ready", binding.ID)
	}
	storageCtx, err := u.contextForReadyBinding(ctx, binding, bucket.Name)
	if err != nil {
		return nil, nil, err
	}
	return binding, storageCtx, nil
}

func (u *Uploader) contextForBindingProvider(ctx context.Context, binding *model.StorageDataSet, bucketName string) (synapse.UploadContext, error) {
	return u.storage.CreateContext(ctx, &storage.CreateContextOptions{
		ProviderIDs:     []sdktypes.BigInt{binding.ProviderID.SDK()},
		DataSetMetadata: map[string]string{"bucket": bucketName},
	})
}

func (u *Uploader) contextForReadyBinding(ctx context.Context, binding *model.StorageDataSet, bucketName string) (synapse.UploadContext, error) {
	return u.storage.CreateContext(ctx, &storage.CreateContextOptions{
		DataSetIDs:      []sdktypes.BigInt{binding.DataSetID.SDK()},
		DataSetMetadata: map[string]string{"bucket": bucketName},
	})
}

func (u *Uploader) extraDataForCopy(ctx context.Context, storageCtx synapse.UploadContext, uploadID int64, copyIndex int, pieces []storage.PieceInput) ([]byte, string, error) {
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		return nil, "", err
	}
	if copyRow != nil && copyRow.CommitExtraDataHex != nil && *copyRow.CommitExtraDataHex != "" {
		extraData, err := hex.DecodeString(*copyRow.CommitExtraDataHex)
		return extraData, strings.ToLower(*copyRow.CommitExtraDataHex), err
	}
	extraData, err := storageCtx.PresignForCommit(ctx, pieces)
	if err != nil {
		return nil, "", err
	}
	return extraData, strings.ToLower(hex.EncodeToString(extraData)), nil
}

type submittedCommitStatus struct {
	TxHash            string            `json:"txHash"`
	TxStatus          string            `json:"txStatus"`
	DataSetID         json.RawMessage   `json:"dataSetId"`
	PiecesAdded       bool              `json:"piecesAdded"`
	ConfirmedPieceIDs []json.RawMessage `json:"confirmedPieceIds,omitempty"`
}

func (u *Uploader) waitForSubmittedCommit(ctx context.Context, storageCtx synapse.UploadContext, binding *model.StorageDataSet, txHash string, pieceCount int) (*storage.CommitResult, error) {
	if binding == nil || binding.DataSetID == nil || binding.DataSetID.IsZero() {
		return nil, errors.New("commit dataset binding is not ready")
	}
	if submittedCommitMaxWait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, submittedCommitMaxWait)
		defer cancel()
	}
	statusURL, err := submittedCommitStatusURL(storageCtx, binding.DataSetID.String(), txHash)
	if err != nil {
		return nil, err
	}
	for {
		status, err := getSubmittedCommitStatus(ctx, statusURL)
		if err != nil {
			return nil, err
		}
		switch status.TxStatus {
		case "confirmed":
			if status.PiecesAdded {
				return status.commitResult(*binding.DataSetID, txHash, pieceCount)
			}
			return nil, fmt.Errorf("%w: commit %s confirmed without adding pieces", errCommitRejected, txHash)
		case "rejected":
			return nil, fmt.Errorf("%w: %s", errCommitRejected, txHash)
		case "", "pending":
		default:
			return nil, fmt.Errorf("commit status %q for %s", status.TxStatus, txHash)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(submittedCommitPollInterval):
		}
	}
}

func submittedCommitStatusURL(storageCtx synapse.UploadContext, dataSetID string, txHash string) (string, error) {
	serviceURL := strings.TrimSpace(storageCtx.ServiceURL())
	if serviceURL == "" {
		return "", errors.New("storage context has empty provider service URL")
	}
	statusURL, err := url.JoinPath(serviceURL, "pdp", "data-sets", dataSetID, "pieces", "added", txHash)
	if err != nil {
		return "", fmt.Errorf("building commit status URL: %w", err)
	}
	return statusURL, nil
}

func getSubmittedCommitStatus(ctx context.Context, statusURL string) (*submittedCommitStatus, error) {
	if submittedCommitRequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, submittedCommitRequestTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("commit status %s returned %s", statusURL, resp.Status)
	}
	var status submittedCommitStatus
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&status); err != nil {
		return nil, fmt.Errorf("decoding commit status: %w", err)
	}
	return &status, nil
}

func (s *submittedCommitStatus) commitResult(dataSetID idtypes.OnChainID, txHash string, pieceCount int) (*storage.CommitResult, error) {
	if len(s.DataSetID) == 0 {
		return nil, errors.New("commit status missing dataSetId")
	}
	gotDataSetID, err := parseJSONOnChainID("dataSetId", s.DataSetID)
	if err != nil {
		return nil, err
	}
	if !gotDataSetID.Equal(dataSetID) {
		return nil, fmt.Errorf("commit status dataSetId = %s, want %s", gotDataSetID.String(), dataSetID.String())
	}
	if len(s.ConfirmedPieceIDs) != pieceCount {
		return nil, fmt.Errorf("commit status confirmedPieceIds = %d, want %d", len(s.ConfirmedPieceIDs), pieceCount)
	}
	pieceIDs := make([]sdktypes.BigInt, 0, len(s.ConfirmedPieceIDs))
	for _, raw := range s.ConfirmedPieceIDs {
		pieceID, err := parseJSONOnChainID("confirmedPieceId", raw)
		if err != nil {
			return nil, err
		}
		pieceIDs = append(pieceIDs, pieceID.SDK())
	}
	if s.TxHash != "" {
		txHash = s.TxHash
	}
	return &storage.CommitResult{
		TransactionID: txHash,
		DataSetID:     dataSetID.SDK(),
		PieceIDs:      pieceIDs,
		IsNewDataSet:  false,
	}, nil
}

func parseJSONOnChainID(name string, raw json.RawMessage) (idtypes.OnChainID, error) {
	var id idtypes.OnChainID
	if err := json.Unmarshal(raw, &id); err != nil {
		return idtypes.OnChainID{}, fmt.Errorf("bad %s: %w", name, err)
	}
	return id, nil
}

func onChainIDPtrFromSDK(value sdktypes.BigInt) *idtypes.OnChainID {
	id := idtypes.OnChainIDFromSDK(value)
	return &id
}

func requiredOnChainIDPtrFromSDK(value sdktypes.BigInt) *idtypes.OnChainID {
	if value.IsZero() {
		return nil
	}
	return onChainIDPtrFromSDK(value)
}

func onChainIDPtrFromSDKPtr(value *sdktypes.BigInt) *idtypes.OnChainID {
	if value == nil {
		return nil
	}
	return onChainIDPtrFromSDK(*value)
}

func sdkBigIntPtr(value *idtypes.OnChainID) *sdktypes.BigInt {
	if value == nil {
		return nil
	}
	id := value.SDK()
	return &id
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (u *Uploader) acceptUploadAttempt(ctx context.Context, task *model.Task, version *model.ObjectVersion, upload *model.StorageUpload, logger *slog.Logger) {
	refs, err := u.repos.Uploads.AcceptCompleteUploadForContent(ctx, repository.AcceptCompleteUploadInput{
		UploadID:        upload.ID,
		TaskID:          task.ID,
		BucketID:        version.BucketID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		AutoEvict:       u.autoEvict,
		EvictMaxRetries: u.evictMaxRetries,
	})
	if err != nil {
		if setErr := u.repos.Uploads.SetAcceptError(ctx, upload.ID, err.Error()); setErr != nil {
			logger.Warn("failed to record upload accept error", "uploadID", upload.ID, "error", setErr)
		}
		u.handleTaskFailure(ctx, task, logger, "accept upload", err)
		return
	}
	logger.Info("upload accepted", "uploadID", upload.ID, "versions", len(refs))
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) recordUploadFailure(ctx context.Context, logger *slog.Logger, uploadID int64, message string) {
	if err := u.repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:      uploadID,
		ErrorMessage:  &message,
		RawResultJSON: rawUploadErrorJSON(message),
	}); err != nil {
		logger.Warn("failed to record upload failure", "uploadID", uploadID, "error", err)
	}
}

func requestedCopies(result *storage.UploadResult) int {
	if result == nil {
		return 0
	}
	if result.RequestedCopies > 0 {
		return result.RequestedCopies
	}
	return len(result.Copies)
}

func recordUploadResultInput(uploadID int64, result *storage.UploadResult) repository.RecordUploadResultInput {
	input := repository.RecordUploadResultInput{
		UploadID:        uploadID,
		Complete:        result.Complete,
		RequestedCopies: requestedCopies(result),
		RawResultJSON:   rawUploadResultJSON(result),
	}
	if result.PieceCID.Defined() {
		pieceCID := result.PieceCID.String()
		input.PieceCID = &pieceCID
	}
	input.Copies = make([]repository.StorageUploadCopyInput, 0, len(result.Copies))
	for _, copy := range result.Copies {
		input.Copies = append(input.Copies, repository.StorageUploadCopyInput{
			ProviderID:   requiredOnChainIDPtrFromSDK(copy.ProviderID),
			DataSetID:    requiredOnChainIDPtrFromSDK(copy.DataSetID),
			PieceID:      onChainIDPtrFromSDK(copy.PieceID),
			Role:         string(copy.Role),
			RetrievalURL: nonEmptyStringPtr(copy.RetrievalURL),
			IsNewDataSet: copy.IsNewDataSet,
		})
	}
	input.Failures = make([]repository.StorageUploadFailureInput, 0, len(result.FailedAttempts))
	for _, failure := range result.FailedAttempts {
		input.Failures = append(input.Failures, repository.StorageUploadFailureInput{
			ProviderID:   requiredOnChainIDPtrFromSDK(failure.ProviderID),
			Role:         string(failure.Role),
			Stage:        nonEmptyStringPtr(string(failure.Stage)),
			ErrorMessage: errorStringPtr(failure.Err),
			Explicit:     failure.Explicit,
		})
	}
	return input
}

type uploadResultJSON struct {
	PieceCID        string              `json:"piece_cid,omitempty"`
	Size            int64               `json:"size"`
	RequestedCopies int                 `json:"requested_copies"`
	Complete        bool                `json:"complete"`
	Copies          []uploadCopyJSON    `json:"copies,omitempty"`
	FailedAttempts  []uploadFailureJSON `json:"failed_attempts,omitempty"`
}

type uploadCopyJSON struct {
	ProviderID   string `json:"provider_id"`
	DataSetID    string `json:"data_set_id"`
	PieceID      string `json:"piece_id"`
	Role         string `json:"role"`
	RetrievalURL string `json:"retrieval_url,omitempty"`
	IsNewDataSet bool   `json:"is_new_data_set"`
}

type uploadFailureJSON struct {
	ProviderID string `json:"provider_id"`
	Role       string `json:"role"`
	Stage      string `json:"stage,omitempty"`
	Error      string `json:"error,omitempty"`
	Explicit   bool   `json:"explicit"`
}

func rawUploadResultJSON(result *storage.UploadResult) []byte {
	dto := uploadResultJSON{
		Size:            result.Size,
		RequestedCopies: requestedCopies(result),
		Complete:        result.Complete,
	}
	if result.PieceCID.Defined() {
		dto.PieceCID = result.PieceCID.String()
	}
	for _, copy := range result.Copies {
		dto.Copies = append(dto.Copies, uploadCopyJSON{
			ProviderID:   copy.ProviderID.String(),
			DataSetID:    copy.DataSetID.String(),
			PieceID:      copy.PieceID.String(),
			Role:         string(copy.Role),
			RetrievalURL: copy.RetrievalURL,
			IsNewDataSet: copy.IsNewDataSet,
		})
	}
	for _, failure := range result.FailedAttempts {
		dto.FailedAttempts = append(dto.FailedAttempts, uploadFailureJSON{
			ProviderID: failure.ProviderID.String(),
			Role:       string(failure.Role),
			Stage:      string(failure.Stage),
			Error:      errorString(failure.Err),
			Explicit:   failure.Explicit,
		})
	}
	raw, err := json.Marshal(dto)
	if err != nil {
		return nil
	}
	return raw
}

func rawUploadErrorJSON(message string) []byte {
	raw, err := json.Marshal(map[string]string{"error": message})
	if err != nil {
		return nil
	}
	return raw
}

func nonEmptyStringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func errorStringPtr(err error) *string {
	value := errorString(err)
	if value == "" {
		return nil
	}
	return &value
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func copyHasPiece(copyRow *model.StorageUploadCopy) bool {
	if copyRow == nil {
		return false
	}
	switch copyRow.Status {
	case model.StorageUploadCopyStatusPieceReady, model.StorageUploadCopyStatusCommitting, model.StorageUploadCopyStatusCommitted:
		return true
	default:
		return false
	}
}

func copyCommitSubmitted(copyRow *model.StorageUploadCopy) bool {
	return copyRow != nil &&
		copyRow.Status == model.StorageUploadCopyStatusCommitting &&
		copyRow.CommitTransactionID != nil &&
		*copyRow.CommitTransactionID != ""
}

func copyCommitted(copyRow *model.StorageUploadCopy) bool {
	return copyRow != nil && copyRow.Status == model.StorageUploadCopyStatusCommitted
}

func availableUSDFCFunds(acct *synapse.TokenAccountInfo) *big.Int {
	if acct == nil {
		return nil
	}
	if acct.AvailableFunds != nil {
		available := new(big.Int).Set(acct.AvailableFunds)
		if available.Sign() < 0 {
			return big.NewInt(0)
		}
		return available
	}
	if acct.Funds == nil || acct.LockupCurrent == nil {
		return nil
	}
	available := new(big.Int).Sub(acct.Funds, acct.LockupCurrent)
	if available.Sign() < 0 {
		return big.NewInt(0)
	}
	return available
}

func (u *Uploader) checkPaymentBalancePreflight(ctx context.Context, logger *slog.Logger) error {
	if u.wallet == nil {
		return nil
	}
	info, err := u.wallet.GetWalletInfo(ctx)
	if err != nil {
		logger.Warn("wallet balance pre-check failed, proceeding with upload", "error", err)
		return nil
	}
	if info == nil {
		return nil
	}
	available := availableUSDFCFunds(info.USDFCAccount)
	if available == nil || available.Sign() != 0 {
		return nil
	}
	return errors.New("insufficient payment account balance: USDFC available funds = 0; deposit USDFC into your payment account or wait for locked funds to become available before uploading")
}

func (u *Uploader) handleBalancePreflightFailure(ctx context.Context, task *model.Task, logger *slog.Logger, err error) {
	logger.Warn("wallet balance pre-check failed", "error", err)
	if task.RetryCount+1 >= task.MaxRetries {
		if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
			logger.Error("failed to mark task as dead-letter", "error", ftErr)
		} else {
			admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUpload)).Inc()
		}
	} else {
		_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
		_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) handleTaskFailure(ctx context.Context, task *model.Task, logger *slog.Logger, stage string, err error) {
	logger.Error(stage+" failed", "error", err)
	if task.RetryCount+1 >= task.MaxRetries {
		if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
			logger.Error("failed to mark task as dead-letter", "error", ftErr)
		} else {
			admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUpload)).Inc()
		}
	} else {
		_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
		_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) handleFailure(ctx context.Context, task *model.Task, version *model.ObjectVersion, logger *slog.Logger, stage string, err error) {
	logger.Error(stage+" failed", "error", err)
	if task.RetryCount+1 >= task.MaxRetries {
		u.failUploadingContent(ctx, task, version, logger, fmt.Sprintf("%s: %v (max retries reached)", stage, err))
		if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
			logger.Error("failed to mark task as dead-letter", "error", ftErr)
		} else {
			admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUpload)).Inc()
		}
	} else {
		_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
		_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) handlePrimaryFailure(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, logger *slog.Logger, stage string, err error) {
	if task.RetryCount+1 >= task.MaxRetries {
		if markErr := u.repos.Uploads.MarkUploadCopyFailed(ctx, uploadID, 0, fmt.Sprintf("%s: %v", stage, err)); markErr != nil {
			logger.Warn("failed to mark primary upload copy failed", "uploadID", uploadID, "error", markErr)
		}
	}
	u.handleFailure(ctx, task, version, logger, stage, err)
}

func (u *Uploader) failUploadingContent(ctx context.Context, task *model.Task, version *model.ObjectVersion, logger *slog.Logger, lastError string) {
	refs, err := u.repos.Objects.FailUploadingContentFollowers(ctx, version.BucketID, version.Size, version.Checksum, task.RefVersionID, lastError)
	if err == nil {
		logger.Info("marked matching active upload versions failed", "count", len(refs))
		return
	}
	logger.Warn("failed to mark matching active upload versions failed", "error", err)
	from := version.State
	if from != model.ObjectStateUploading && from != model.ObjectStateCommitting {
		logger.Warn("cannot transition non-primary upload state to failed", "state", from)
		return
	}
	_ = state.TransitionToFailed(ctx, u.stateMachine, u.repos.Objects, task.RefVersionID, from, lastError)
}
