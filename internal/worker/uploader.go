package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
	"golang.org/x/sync/errgroup"
)

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
	concurrency     int
	pollInterval    time.Duration
	leaseTTL        time.Duration
	logger          *slog.Logger
	*livenessTracker
}

const defaultEvictMaxRetries = 3

// UploaderOption configures uploader behavior.
type UploaderOption func(*Uploader)

// WithEvictMaxRetries configures max retries for cache eviction tasks created after upload.
func WithEvictMaxRetries(maxRetries int) UploaderOption {
	return func(u *Uploader) {
		u.evictMaxRetries = maxRetries
	}
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
	return pollLoop(ctx, u.pollInterval, u.tick)
}

func (u *Uploader) tick(ctx context.Context) error {
	u.recordTick()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(u.concurrency)

	for range u.concurrency {
		task, err := u.repos.Tasks.ClaimPending(gctx, model.TaskTypeUpload, u.leaseTTL)
		if err != nil {
			u.logger.Error("claiming upload task", "error", err)
			break
		}
		if task == nil {
			break
		}

		g.Go(func() error {
			u.processTask(gctx, task)
			return nil
		})
	}

	err := g.Wait()
	return err
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

	// Pre-flight: check payment account has deposited funds.
	if u.wallet != nil {
		info, wErr := u.wallet.GetWalletInfo(ctx)
		if wErr != nil {
			logger.Warn("wallet balance pre-check failed, proceeding with upload", "error", wErr)
		} else if info != nil {
			available := availableUSDFCFunds(info.USDFCAccount)
			if available != nil && available.Sign() == 0 {
				msg := "insufficient payment account balance: USDFC available funds = 0; deposit USDFC into your payment account or wait for locked funds to become available before uploading"
				u.handleBalancePreflightFailure(ctx, task, logger, errors.New(msg))
				return
			}
		}
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
			ProviderID:   uintIDString(uint64(copy.ProviderID)),
			DataSetID:    uintIDString(uint64(copy.DataSetID)),
			PieceID:      uintIDString(uint64(copy.PieceID)),
			Role:         string(copy.Role),
			RetrievalURL: nonEmptyStringPtr(copy.RetrievalURL),
			IsNewDataSet: copy.IsNewDataSet,
		})
	}
	input.Failures = make([]repository.StorageUploadFailureInput, 0, len(result.FailedAttempts))
	for _, failure := range result.FailedAttempts {
		input.Failures = append(input.Failures, repository.StorageUploadFailureInput{
			ProviderID:   uintIDString(uint64(failure.ProviderID)),
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
			ProviderID:   strconv.FormatUint(uint64(copy.ProviderID), 10),
			DataSetID:    strconv.FormatUint(uint64(copy.DataSetID), 10),
			PieceID:      strconv.FormatUint(uint64(copy.PieceID), 10),
			Role:         string(copy.Role),
			RetrievalURL: copy.RetrievalURL,
			IsNewDataSet: copy.IsNewDataSet,
		})
	}
	for _, failure := range result.FailedAttempts {
		dto.FailedAttempts = append(dto.FailedAttempts, uploadFailureJSON{
			ProviderID: strconv.FormatUint(uint64(failure.ProviderID), 10),
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

func uintIDString(id uint64) *string {
	if id == 0 {
		return nil
	}
	value := strconv.FormatUint(id, 10)
	return &value
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

func (u *Uploader) failUploadingContent(ctx context.Context, task *model.Task, version *model.ObjectVersion, logger *slog.Logger, lastError string) {
	refs, err := u.repos.Objects.FailUploadingContentFollowers(ctx, version.BucketID, version.Size, version.Checksum, task.RefVersionID, lastError)
	if err == nil {
		logger.Info("marked matching uploading versions failed", "count", len(refs))
		return
	}
	logger.Warn("failed to mark matching uploading versions failed", "error", err)
	_ = state.TransitionToFailed(ctx, u.stateMachine, u.repos.Objects, task.RefVersionID, model.ObjectStateUploading, lastError)
}
