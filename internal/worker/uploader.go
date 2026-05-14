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

const (
	submittedCommitPollInterval   = 4 * time.Second
	terminalFailureCleanupTimeout = 5 * time.Second
)

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
	eventPublisher  admin.EventPublisher
	concurrency     int
	pollInterval    time.Duration
	leaseTTL        time.Duration
	logger          *slog.Logger
	*livenessTracker
}

const (
	defaultEvictMaxRetries  = 3
	uploadPollJitterDivisor = 5
	uploadProgressTimeout   = 2 * time.Second

	uploadStagePrepare       = "prepare_upload"
	uploadStageEnsureDataSet = "ensure_dataset"
	uploadStageIngressStore  = "ingress_store"
	uploadStageIngressCommit = "ingress_commit"
	uploadStagePeerPull      = "peer_pull"
	uploadStagePeerCommit    = "peer_commit"
)

// UploaderOption configures uploader behavior.
type UploaderOption func(*Uploader)

// WithEvictMaxRetries configures max retries for cache eviction tasks created after upload.
func WithEvictMaxRetries(maxRetries int) UploaderOption {
	return func(u *Uploader) {
		u.evictMaxRetries = maxRetries
	}
}

func WithEventPublisher(publisher admin.EventPublisher) UploaderOption {
	return func(u *Uploader) {
		u.eventPublisher = publisher
	}
}

func boundedTargetCopies(copies int) int {
	return model.ClampStorageCopies(copies)
}

// NewUploader creates a new upload worker.
func NewUploader(repos *repository.Repositories, c cache.Cache, sc synapse.StorageClient, wallet synapse.WalletQuerier, sm *state.Machine, autoEvict bool, targetCopies int, concurrency int, pollInterval time.Duration, logger *slog.Logger, opts ...UploaderOption) *Uploader {
	u := &Uploader{
		repos:           repos,
		cache:           c,
		storage:         sc,
		wallet:          wallet,
		stateMachine:    sm,
		autoEvict:       autoEvict,
		evictMaxRetries: defaultEvictMaxRetries,
		targetCopies:    boundedTargetCopies(targetCopies),
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

type uploadProgressReporter struct {
	ctx           context.Context
	repos         *repository.Repositories
	publisher     admin.EventPublisher
	logger        *slog.Logger
	uploadID      int64
	taskID        int64
	versionID     string
	bucketName    string
	objectKey     string
	attempt       int
	totalBytes    int64
	flushInterval time.Duration

	mu           sync.Mutex
	lastFlush    time.Time
	pendingBytes int64
	pending      bool
	pendingTimer *time.Timer
}

func (u *Uploader) beginIngressProgressReporter(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, logger *slog.Logger) *uploadProgressReporter {
	if u == nil || u.repos == nil || u.repos.Uploads == nil || uploadID == 0 {
		return nil
	}
	upload, err := u.repos.Uploads.BeginIngressStoreProgress(ctx, uploadID)
	if err != nil {
		logger.Warn("failed to begin ingress upload progress", "uploadID", uploadID, "error", err)
		return nil
	}
	reporter := &uploadProgressReporter{
		ctx:           ctx,
		repos:         u.repos,
		publisher:     u.eventPublisher,
		logger:        logger,
		uploadID:      uploadID,
		versionID:     upload.SourceVersionID,
		attempt:       upload.IngressStoreAttempt,
		totalBytes:    upload.ContentSize,
		flushInterval: time.Second,
	}
	if task != nil {
		reporter.taskID = task.ID
		if reporter.versionID == "" {
			reporter.versionID = task.RefVersionID
		}
	}
	if version != nil {
		if reporter.versionID == "" {
			reporter.versionID = version.VersionID
		}
		reporter.objectKey = version.Key
		if reporter.totalBytes == 0 {
			reporter.totalBytes = version.Size
		}
	}
	if bucket != nil {
		reporter.bucketName = bucket.Name
	}
	reporter.record(0, false)
	return reporter
}

func (r *uploadProgressReporter) OnProgress(bytesUploaded int64) {
	if r == nil || r.attempt <= 0 {
		return
	}
	now := time.Now()
	r.mu.Lock()
	if r.flushInterval <= 0 || r.lastFlush.IsZero() || now.Sub(r.lastFlush) >= r.flushInterval {
		r.cancelPendingLocked()
		r.lastFlush = now
		r.mu.Unlock()
		go r.record(bytesUploaded, false)
		return
	}
	r.pendingBytes = bytesUploaded
	r.pending = true
	if r.pendingTimer == nil {
		delay := r.flushInterval - now.Sub(r.lastFlush)
		r.pendingTimer = time.AfterFunc(delay, r.flushPendingProgress)
	}
	r.mu.Unlock()
}

func (r *uploadProgressReporter) Flush(bytesUploaded int64, done bool) {
	if r == nil || r.attempt <= 0 {
		return
	}
	r.mu.Lock()
	r.cancelPendingLocked()
	r.mu.Unlock()
	r.record(bytesUploaded, done)
}

func (r *uploadProgressReporter) flushPendingProgress() {
	r.mu.Lock()
	if !r.pending {
		r.pendingTimer = nil
		r.mu.Unlock()
		return
	}
	bytesUploaded := r.pendingBytes
	r.pending = false
	r.pendingTimer = nil
	r.lastFlush = time.Now()
	r.mu.Unlock()
	r.record(bytesUploaded, false)
}

func (r *uploadProgressReporter) cancelPendingLocked() {
	r.pending = false
	if r.pendingTimer != nil {
		r.pendingTimer.Stop()
		r.pendingTimer = nil
	}
}

func (r *uploadProgressReporter) record(bytesUploaded int64, done bool) {
	if r == nil || r.repos == nil || r.repos.Uploads == nil {
		return
	}
	ctx, cancel := r.recordContext()
	defer cancel()
	upload, err := r.repos.Uploads.RecordIngressStoreProgress(ctx, repository.RecordIngressStoreProgressInput{
		UploadID:      r.uploadID,
		Attempt:       r.attempt,
		BytesUploaded: bytesUploaded,
	})
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("failed to record ingress upload progress", "uploadID", r.uploadID, "attempt", r.attempt, "error", err)
		}
		return
	}
	if r.publisher == nil || upload == nil || upload.ProgressUpdatedAt == nil {
		return
	}
	r.publisher.Publish("upload_progress_updated", map[string]any{
		"upload_id":   r.uploadID,
		"task_id":     nullableTaskID(r.taskID),
		"version_id":  r.versionID,
		"bucket_name": r.bucketName,
		"object_key":  r.objectKey,
		"progress":    uploadProgressEventPayload(upload, done),
	})
}

func (r *uploadProgressReporter) recordContext() (context.Context, context.CancelFunc) {
	baseCtx := context.Background()
	if r != nil && r.ctx != nil {
		baseCtx = r.ctx
	}
	return context.WithTimeout(baseCtx, uploadProgressTimeout)
}

func nullableTaskID(taskID int64) any {
	if taskID == 0 {
		return nil
	}
	return taskID
}

func uploadProgressEventPayload(upload *model.StorageUpload, done bool) map[string]any {
	uploaded := upload.IngressBytesTransferred
	if uploaded < 0 {
		uploaded = 0
	}
	total := upload.ContentSize
	if total < 0 {
		total = 0
	}
	if uploaded > total {
		uploaded = total
	}
	progress := map[string]any{
		"scope":          "ingress_store",
		"attempt":        upload.IngressStoreAttempt,
		"uploaded_bytes": uploaded,
		"total_bytes":    total,
		"done":           done || (total > 0 && uploaded >= total),
		"updated_at":     upload.ProgressUpdatedAt.Format(time.RFC3339),
	}
	if percent := model.UploadProgressPercent(uploaded, total); percent != nil {
		progress["percent"] = *percent
	}
	return progress
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
		task, err := u.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, u.leaseTTL)
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
			stopLeaseRenewal := startTaskLeaseRenewal(u.logger, u.repos, task, u.leaseTTL)
			defer stopLeaseRenewal()
			u.processTask(ctx, task)
		}()
		releaseTaskOnWorkerShutdown(ctx, u.logger, u.repos, task)
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
		_ = u.repos.Tasks.FailRunning(ctx, task, "storage client not configured")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	version, err := u.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil || version == nil {
		logger.Warn("object version not found for upload task", "error", err)
		_ = u.repos.Tasks.FailRunning(ctx, task, "object not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	bucket, err := u.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "bucketID", version.BucketID, "error", err)
		_ = u.repos.Tasks.FailRunning(ctx, task, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}
	defer u.publishUploadStateChanged(task, version, bucket)

	if version.State == model.ObjectStateStored || version.State == model.ObjectStateCacheEvicted {
		if version.State == model.ObjectStateStored {
			u.enqueueEvictTask(ctx, logger, task.RefID, task.RefVersionID)
		}
		if !completeWorkerTask(ctx, u.repos, task, "uploader", logger) {
			return
		}
		logger.Info("upload task already satisfied", "state", version.State)
		return
	}

	u.processStagedTask(ctx, task, version, bucket, uploadTaskStage(task), logger)
}

func (u *Uploader) publishUploadStateChanged(task *model.Task, version *model.ObjectVersion, bucket *model.Bucket) {
	if u == nil || u.eventPublisher == nil || task == nil {
		return
	}
	payload := map[string]any{
		"task_id":    task.ID,
		"version_id": task.RefVersionID,
	}
	if uploadID, err := payloadInt64(task.Payload, "upload_id"); err == nil && uploadID != 0 {
		payload["upload_id"] = uploadID
	}
	if version != nil {
		payload["version_id"] = version.VersionID
		payload["object_key"] = version.Key
	}
	if bucket != nil {
		payload["bucket_name"] = bucket.Name
	}
	u.eventPublisher.Publish("upload_state_changed", payload)
}

func (u *Uploader) processStagedTask(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, stage string, logger *slog.Logger) {
	switch stage {
	case uploadStagePrepare:
		u.prepareStagedUpload(ctx, task, version, bucket, logger)
	case uploadStageEnsureDataSet:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.ensureUploadDataSet(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStageIngressStore:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.ingressStore(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStageIngressCommit:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.ingressCommit(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStagePeerPull:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.peerPull(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStagePeerCommit:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		u.peerCommit(ctx, task, version, bucket, uploadID, copyIndex, logger)
	default:
		u.handleTaskFailure(ctx, task, logger, "parse upload task payload", fmt.Errorf("unknown upload stage %q", stage))
	}
}

func (u *Uploader) prepareStagedUpload(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, logger *slog.Logger) {
	if uploadID, ok := repairUploadID(task); ok {
		u.prepareReadableUploadRepair(ctx, task, version, bucket, uploadID, logger)
		return
	}
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

	targetCopies := u.targetCopiesForBucket(bucket)
	upload, err := u.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceTaskID:    task.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: targetCopies,
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "start upload attempt", err)
		return
	}
	targetCopies = boundedTargetCopies(upload.RequestedCopies)
	bindings, err := u.ensureBucketProviderBindings(ctx, bucket, upload.ID, targetCopies)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "ensure provider bindings", err)
		return
	}
	copyInputs := make([]repository.UploadCopyBindingInput, 0, len(bindings))
	for i, binding := range bindings {
		transferMethod := model.StorageCopyTransferMethodPeerPull
		if i == 0 {
			transferMethod = model.StorageCopyTransferMethodIngress
		}
		copyInputs = append(copyInputs, repository.UploadCopyBindingInput{
			StorageDataSetID: binding.ID,
			CopyIndex:        binding.CopyIndex,
			TransferMethod:   transferMethod,
			ProviderID:       binding.ProviderID,
		})
	}
	if err := u.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, copyInputs); err != nil {
		u.handleTaskFailure(ctx, task, logger, "create upload copy rows", err)
		return
	}
	if len(bindings) == 0 {
		u.handleTaskFailure(ctx, task, logger, "prepare upload", errors.New("no upload dataset bindings selected"))
		return
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStageEnsureDataSet, upload.ID, bindings[0].CopyIndex, model.StorageCopyTransferMethodIngress); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue ingress dataset task", err)
		return
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func repairUploadID(task *model.Task) (int64, bool) {
	if task == nil || task.Payload == nil {
		return 0, false
	}
	uploadID, err := payloadInt64(task.Payload, "upload_id")
	return uploadID, err == nil && uploadID > 0
}

func (u *Uploader) prepareReadableUploadRepair(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, logger *slog.Logger) {
	if version.State != model.ObjectStateReplicating || version.StorageUploadID == nil || *version.StorageUploadID != uploadID {
		u.handleTaskFailure(ctx, task, logger, "prepare upload repair", fmt.Errorf("object state %s is not repairable for upload %d", version.State, uploadID))
		return
	}
	upload, err := u.repos.Uploads.GetByID(ctx, uploadID)
	if err != nil || upload == nil {
		if err == nil {
			err = fmt.Errorf("storage upload %d not found", uploadID)
		}
		u.handleTaskFailure(ctx, task, logger, "load repair upload", err)
		return
	}
	if upload.BucketID != version.BucketID || upload.ContentSize != version.Size || upload.Checksum != version.Checksum {
		u.handleTaskFailure(ctx, task, logger, "prepare upload repair", fmt.Errorf("upload %d does not match object version %s", uploadID, version.VersionID))
		return
	}
	readableCopies, err := u.repos.Uploads.ListReadableCommittedCopies(ctx, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "list readable repair copies", err)
		return
	}
	if len(readableCopies) == 0 {
		u.handleTaskFailure(ctx, task, logger, "prepare upload repair", errors.New("readable source copy not found"))
		return
	}
	finalized, refs, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "finalize repaired upload", err)
		return
	}
	if finalized {
		u.enqueueEvictTasksForRefs(ctx, logger, refs)
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	copies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "list repair upload copies", err)
		return
	}
	pendingPeers := 0
	bindings, err := u.repos.Uploads.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "list repair dataset bindings", err)
		return
	}
	bindingsByCopyIndex := make(map[int]*model.StorageDataSet, len(bindings))
	for i := range bindings {
		bindingsByCopyIndex[bindings[i].CopyIndex] = &bindings[i]
	}
	for i := range copies {
		copyRow := &copies[i]
		if copyRow.TransferMethod == model.StorageCopyTransferMethodPeerPull &&
			!copyCommitted(copyRow) &&
			copyRow.Status != model.StorageUploadCopyStatusFailed &&
			dataSetBindingCanEnsureWrite(bindingsByCopyIndex[copyRow.CopyIndex]) {
			pendingPeers++
		}
	}
	missing := upload.RequestedCopies - len(readableCopies) - pendingPeers
	for missing > 0 {
		if err := u.createReplacementPeerCopy(ctx, task, bucket, uploadID, logger); err != nil {
			u.handleTaskFailure(ctx, task, logger, "create repair peer copy", err)
			return
		}
		missing--
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) targetCopiesForBucket(bucket *model.Bucket) int {
	if bucket != nil && bucket.DefaultCopies != nil {
		return boundedTargetCopies(*bucket.DefaultCopies)
	}
	return boundedTargetCopies(u.targetCopies)
}

func (u *Uploader) ensureBucketProviderBindings(ctx context.Context, bucket *model.Bucket, uploadID int64, targetCopies int) ([]model.StorageDataSet, error) {
	bindings, err := u.repos.Uploads.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	existing := make(map[int]struct{}, len(bindings))
	targetCopies = boundedTargetCopies(targetCopies)
	selected := make([]model.StorageDataSet, 0, targetCopies)
	excluded := make([]sdktypes.BigInt, 0, len(bindings))
	for _, binding := range bindings {
		existing[binding.CopyIndex] = struct{}{}
		excluded = append(excluded, binding.ProviderID.SDK())
		if binding.Status == model.StorageDataSetStatusReady && len(selected) < targetCopies {
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
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil || copyRow == nil {
		if err == nil {
			err = fmt.Errorf("upload copy %d not found", copyIndex)
		}
		u.handleTaskFailure(ctx, task, logger, "load upload copy", err)
		return
	}
	binding, err := u.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, copyIndex)
	if err != nil || binding == nil {
		if err == nil {
			err = fmt.Errorf("dataset binding for copy_index %d not found", copyIndex)
		}
		u.handleTaskFailure(ctx, task, logger, "load dataset binding", err)
		return
	}
	if !dataSetBindingCanEnsureWrite(binding) {
		err := fmt.Errorf("dataset binding status %s is not writable", binding.Status)
		if copyRow.TransferMethod == model.StorageCopyTransferMethodPeerPull {
			u.replacePeerCopy(ctx, task, bucket, uploadID, copyIndex, logger, "ensure dataset", err)
			return
		}
		u.replanIngressUpload(ctx, task, version, uploadID, copyIndex, logger, "ensure dataset", err)
		return
	}
	if binding.Status != model.StorageDataSetStatusReady {
		storageCtx, err := u.contextForBindingProvider(ctx, binding, bucket.Name)
		if err != nil {
			u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "create dataset context", err)
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
					u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "create dataset", err)
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
					u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "wait dataset", errors.New("dataset creation submission is incomplete"))
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
						u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "wait dataset", err)
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
	nextStage := uploadStagePeerPull
	if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress {
		nextStage = uploadStageIngressStore
	}
	if err := u.enqueueUploadStage(ctx, task, nextStage, uploadID, copyIndex, copyRow.TransferMethod); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue next upload stage", err)
		return
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) ingressStore(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger) {
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, copyIndex)
	if err != nil {
		u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "ingress context", err)
		return
	}
	if binding == nil {
		u.handleTaskFailure(ctx, task, logger, "ingress context", errors.New("ingress dataset binding not found"))
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load ingress copy", err)
		return
	}
	if copyHasPiece(copyRow) {
		if version.State == model.ObjectStateUploading {
			if err := state.TransitionState(ctx, u.stateMachine, u.repos.Objects, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
				u.handleTaskFailure(ctx, task, logger, "state transition uploading→committing", err)
				return
			}
		}
		if err := u.enqueueUploadStage(ctx, task, uploadStageIngressCommit, uploadID, copyIndex, model.StorageCopyTransferMethodIngress); err != nil {
			u.handleTaskFailure(ctx, task, logger, "enqueue ingress commit", err)
			return
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	rc, _, err := u.cache.Get(ctx, bucket.Name, version.CacheKey)
	if err != nil {
		if os.IsNotExist(err) && version.InCache {
			if markErr := u.repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); markErr != nil {
				logger.Warn("failed to mark cache location absent", "error", markErr)
			}
		}
		u.handleIngressFailure(ctx, task, version, uploadID, copyIndex, logger, "cache read", err)
		return
	}
	defer func() { _ = rc.Close() }()
	progress := u.beginIngressProgressReporter(ctx, task, version, bucket, uploadID, logger)
	result, err := storageCtx.Store(ctx, rc, &storage.StoreOptions{
		OnProgress: func(bytesUploaded int64) {
			progress.OnProgress(bytesUploaded)
		},
	})
	if err != nil {
		u.handleIngressDataSetFailure(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "ingress store", err)
		return
	}
	progress.Flush(version.Size, true)
	pieceCID := result.PieceCID.String()
	if err := u.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     uploadID,
		CopyIndex:    copyIndex,
		PieceCID:     pieceCID,
		RetrievalURL: storageCtx.PieceURL(result.PieceCID),
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark ingress piece ready", err)
		return
	}
	if version.State == model.ObjectStateUploading {
		if err := state.TransitionState(ctx, u.stateMachine, u.repos.Objects, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
			u.handleTaskFailure(ctx, task, logger, "state transition uploading→committing", err)
			return
		}
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStageIngressCommit, uploadID, copyIndex, model.StorageCopyTransferMethodIngress); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue ingress commit", err)
		return
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) ingressCommit(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger) {
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, copyIndex)
	if err != nil {
		u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "ingress commit context", err)
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load ingress copy", err)
		return
	}
	if copyCommitted(copyRow) {
		u.finishReadable(ctx, task, version, uploadID, logger)
		return
	}
	upload, err := u.repos.Uploads.GetByID(ctx, uploadID)
	if err != nil || upload == nil || upload.PieceCID == nil {
		if err == nil {
			err = errors.New("upload has no ingress piece cid")
		}
		u.handleTaskFailure(ctx, task, logger, "load ingress upload", err)
		return
	}
	pieceCID, err := cid.Decode(*upload.PieceCID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "decode ingress piece cid", err)
		return
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	if copyCommitSubmitted(copyRow) {
		result, err := u.waitForSubmittedCommit(ctx, storageCtx, binding, *copyRow.CommitTransactionID, len(pieces))
		if err != nil {
			if terminalDataSetError(err) {
				u.handleIngressDataSetFailure(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "ingress commit", err)
				return
			}
			if errors.Is(err, errCommitRejected) {
				u.handleIngressFailure(ctx, task, version, uploadID, copyIndex, logger, "ingress commit", err)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait ingress commit", err)
			return
		}
		var pieceID *idtypes.OnChainID
		if len(result.PieceIDs) > 0 {
			pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
		}
		if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID:            uploadID,
			CopyIndex:           copyIndex,
			PieceCID:            *upload.PieceCID,
			PieceID:             pieceID,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
			CommitExtraDataHex:  derefString(copyRow.CommitExtraDataHex),
			CommitTransactionID: result.TransactionID,
		}); err != nil {
			u.handleTaskFailure(ctx, task, logger, "mark ingress committed", err)
			return
		}
		u.finishReadable(ctx, task, version, uploadID, logger)
		return
	}
	extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, uploadID, copyIndex, pieces)
	if err != nil {
		u.handleIngressDataSetFailure(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "ingress presign", err)
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
				u.handleTaskFailure(ctx, task, logger, "save ingress commit submission", submitErr)
				return
			}
			if terminalDataSetError(err) {
				u.handleIngressDataSetFailure(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "ingress commit", err)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait ingress commit", err)
			return
		}
		u.handleIngressDataSetFailure(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "ingress commit", err)
		return
	}
	var pieceID *idtypes.OnChainID
	if len(result.PieceIDs) > 0 {
		pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
	}
	if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:            uploadID,
		CopyIndex:           copyIndex,
		PieceCID:            *upload.PieceCID,
		PieceID:             pieceID,
		RetrievalURL:        storageCtx.PieceURL(pieceCID),
		CommitExtraDataHex:  extraHex,
		CommitTransactionID: result.TransactionID,
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark ingress committed", err)
		return
	}
	u.finishReadable(ctx, task, version, uploadID, logger)
}

func (u *Uploader) finishReadable(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, logger *slog.Logger) {
	refs, err := u.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    uploadID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "bind readable upload", err)
		return
	}
	copies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "list upload copies", err)
		return
	}
	for _, copyRow := range copies {
		if copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull || copyCommitted(&copyRow) || copyRow.Status == model.StorageUploadCopyStatusFailed {
			continue
		}
		if err := u.enqueueUploadStage(ctx, task, uploadStageEnsureDataSet, uploadID, copyRow.CopyIndex, copyRow.TransferMethod); err != nil {
			u.handleTaskFailure(ctx, task, logger, "enqueue peer dataset", err)
			return
		}
	}
	if len(copies) == 1 {
		finalized, storedRefs, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize single-copy upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, storedRefs)
		}
	}
	if !completeWorkerTask(ctx, u.repos, task, "uploader", logger) {
		return
	}
	logger.Info("upload readable copy committed", "uploadID", uploadID, "versions", len(refs))
}

func (u *Uploader) repairReadableBinding(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, logger *slog.Logger, stage string) bool {
	_, err := u.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
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

func (u *Uploader) peerPull(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger) {
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, copyIndex)
	if err != nil {
		u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "peer pull context", err)
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load peer copy", err)
		return
	}
	if !u.repairReadableBinding(ctx, task, version, uploadID, logger, "repair readable binding") {
		return
	}
	if copyCommitted(copyRow) {
		finalized, refs, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, refs)
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	if copyHasPiece(copyRow) {
		if err := u.enqueueUploadStage(ctx, task, uploadStagePeerCommit, uploadID, copyIndex, model.StorageCopyTransferMethodPeerPull); err != nil {
			u.handleTaskFailure(ctx, task, logger, "enqueue peer commit", err)
			return
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	readableCopies, err := u.repos.Uploads.ListReadableCommittedCopies(ctx, uploadID)
	if err != nil || len(readableCopies) == 0 {
		if err == nil {
			err = errors.New("readable source copy not found")
		}
		u.handleTaskFailure(ctx, task, logger, "load readable source copy", err)
		return
	}
	sourceCopy := readableCopies[0]
	pieceCID, err := cid.Decode(sourceCopy.PieceCID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "decode readable piece cid", err)
		return
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, uploadID, copyIndex, pieces)
	if err != nil {
		u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer presign", err)
		return
	}
	if _, err := storageCtx.Pull(ctx, storage.PullRequest{
		Pieces:    []cid.Cid{pieceCID},
		ExtraData: extraData,
		From: func(cid.Cid) string {
			return sourceCopy.RetrievalURL
		},
	}); err != nil {
		u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer pull", err)
		return
	}
	if err := u.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     uploadID,
		CopyIndex:    copyIndex,
		PieceCID:     sourceCopy.PieceCID,
		RetrievalURL: storageCtx.PieceURL(pieceCID),
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark peer piece ready", err)
		return
	}
	if err := u.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID:           uploadID,
		CopyIndex:          copyIndex,
		CommitExtraDataHex: extraHex,
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "save peer extra data", err)
		return
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStagePeerCommit, uploadID, copyIndex, model.StorageCopyTransferMethodPeerPull); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue peer commit", err)
		return
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) peerCommit(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger) {
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, copyIndex)
	if err != nil {
		u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "peer commit context", err)
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load peer copy", err)
		return
	}
	if !u.repairReadableBinding(ctx, task, version, uploadID, logger, "repair readable binding") {
		return
	}
	if copyCommitted(copyRow) {
		finalized, refs, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, refs)
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	readableCopies, err := u.repos.Uploads.ListReadableCommittedCopies(ctx, uploadID)
	if err != nil || len(readableCopies) == 0 {
		if err == nil {
			err = errors.New("readable source copy not found")
		}
		u.handleTaskFailure(ctx, task, logger, "load readable source copy", err)
		return
	}
	sourceCopy := readableCopies[0]
	pieceCID, err := cid.Decode(sourceCopy.PieceCID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "decode readable piece cid", err)
		return
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	if copyCommitSubmitted(copyRow) {
		result, err := u.waitForSubmittedCommit(ctx, storageCtx, binding, *copyRow.CommitTransactionID, len(pieces))
		if err != nil {
			if dataSetWriteBlockedError(err) || terminalDataSetError(err) {
				u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer commit", err)
				return
			}
			if errors.Is(err, errCommitRejected) {
				u.markPeerFailed(ctx, task, uploadID, copyIndex, 0, logger, "peer commit", err)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait peer commit", err)
			return
		}
		var pieceID *idtypes.OnChainID
		if len(result.PieceIDs) > 0 {
			pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
		}
		if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID:            uploadID,
			CopyIndex:           copyIndex,
			PieceCID:            sourceCopy.PieceCID,
			PieceID:             pieceID,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
			CommitExtraDataHex:  derefString(copyRow.CommitExtraDataHex),
			CommitTransactionID: result.TransactionID,
		}); err != nil {
			u.handleTaskFailure(ctx, task, logger, "mark peer committed", err)
			return
		}
		finalized, refs, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
			return
		}
		if finalized {
			u.enqueueEvictTasksForRefs(ctx, logger, refs)
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, uploadID, copyIndex, pieces)
	if err != nil {
		u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer presign", err)
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
				u.handleTaskFailure(ctx, task, logger, "save peer commit submission", submitErr)
				return
			}
			if dataSetWriteBlockedError(err) || terminalDataSetError(err) {
				u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer commit", err)
				return
			}
			u.handleTaskFailure(ctx, task, logger, "wait peer commit", err)
			return
		}
		u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer commit", err)
		return
	}
	var pieceID *idtypes.OnChainID
	if len(result.PieceIDs) > 0 {
		pieceID = onChainIDPtrFromSDK(result.PieceIDs[0])
	}
	if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:            uploadID,
		CopyIndex:           copyIndex,
		PieceCID:            sourceCopy.PieceCID,
		PieceID:             pieceID,
		RetrievalURL:        storageCtx.PieceURL(pieceCID),
		CommitExtraDataHex:  extraHex,
		CommitTransactionID: result.TransactionID,
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark peer committed", err)
		return
	}
	finalized, refs, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: uploadID})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
		return
	}
	if finalized {
		u.enqueueEvictTasksForRefs(ctx, logger, refs)
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
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
		Status:         model.TaskStatusQueued,
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

func (u *Uploader) enqueueUploadStage(ctx context.Context, parent *model.Task, stage string, uploadID int64, copyIndex int, transferMethod model.StorageCopyTransferMethod) error {
	payload := map[string]interface{}{
		"upload_id": uploadID,
	}
	key := fmt.Sprintf("upload:%s:%s:%d", parent.RefVersionID, stage, uploadID)
	if transferMethod != "" {
		payload["copy_index"] = copyIndex
		payload["transfer_method"] = string(transferMethod)
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
		Status:         model.TaskStatusQueued,
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

func (u *Uploader) markDataSetStageFailed(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyIndex int, binding *model.StorageDataSet, logger *slog.Logger, stage string, err error) {
	dataSetID := int64(0)
	if binding != nil {
		dataSetID = binding.ID
	}
	writeBlocked := dataSetWriteBlockedError(err) || dataSetBindingWriteBlocked(binding)
	unavailable := terminalDataSetError(err) || dataSetBindingUnavailable(binding)
	if dataSetID > 0 {
		if dataSetWriteBlockedError(err) {
			_ = u.repos.Uploads.MarkDataSetDraining(ctx, dataSetID, err.Error())
		} else if terminalDataSetError(err) {
			_ = u.repos.Uploads.MarkDataSetUnavailable(ctx, dataSetID, err.Error())
		} else if dataSetCreationRejected(err) {
			_ = u.repos.Uploads.MarkDataSetFailed(ctx, dataSetID, err.Error())
		}
	}
	copyRow, copyErr := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if copyErr != nil {
		u.handleTaskFailure(ctx, task, logger, "load failed upload copy", copyErr)
		return
	}
	if copyRow != nil && copyRow.TransferMethod == model.StorageCopyTransferMethodPeerPull {
		if writeBlocked || unavailable {
			u.replacePeerCopy(ctx, task, bucket, uploadID, copyIndex, logger, stage, err)
			return
		}
		discardDataSetID := int64(0)
		if dataSetID > 0 && dataSetCreationRejected(err) {
			discardDataSetID = dataSetID
		}
		u.markPeerFailed(ctx, task, uploadID, copyIndex, discardDataSetID, logger, stage, err)
		return
	}
	if writeBlocked || unavailable {
		u.replanIngressUpload(ctx, task, version, uploadID, copyIndex, logger, stage, err)
		return
	}
	u.handleIngressFailure(ctx, task, version, uploadID, copyIndex, logger, stage, err)
}

func (u *Uploader) markPeerFailed(ctx context.Context, task *model.Task, uploadID int64, copyIndex int, discardDataSetID int64, logger *slog.Logger, stage string, err error) {
	if appendErr := u.repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
		UploadID:       uploadID,
		CopyIndex:      copyIndex,
		TransferMethod: string(model.StorageCopyTransferMethodPeerPull),
		Stage:          stage,
		ErrorMessage:   err.Error(),
	}); appendErr != nil {
		logger.Warn("failed to append peer upload failure", "uploadID", uploadID, "copyIndex", copyIndex, "error", appendErr)
	}
	logger.Error(stage+" failed", "error", err)
	status := scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
	if status == model.TaskStatusExhausted {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), terminalFailureCleanupTimeout)
		defer cancel()
		if markErr := u.repos.Uploads.MarkUploadCopyFailed(cleanupCtx, uploadID, copyIndex, fmt.Sprintf("%s: %v", stage, err)); markErr != nil {
			logger.Warn("failed to mark peer upload copy failed", "uploadID", uploadID, "copyIndex", copyIndex, "error", markErr)
		}
		if discardDataSetID > 0 {
			if discardErr := u.repos.Uploads.DiscardFailedDataSetCandidate(cleanupCtx, uploadID, copyIndex, discardDataSetID); discardErr != nil {
				logger.Warn("failed to discard failed dataset candidate", "uploadID", uploadID, "copyIndex", copyIndex, "dataSetID", discardDataSetID, "error", discardErr)
			}
		}
		if repairErr := u.enqueueRepairUpload(cleanupCtx, task, uploadID); repairErr != nil {
			logger.Warn("failed to enqueue peer repair task", "uploadID", uploadID, "copyIndex", copyIndex, "error", repairErr)
		}
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) handlePeerDataSetFailure(ctx context.Context, task *model.Task, bucket *model.Bucket, uploadID int64, copyIndex int, dataSetID int64, logger *slog.Logger, stage string, err error) {
	if dataSetID > 0 {
		if dataSetWriteBlockedError(err) {
			if markErr := u.repos.Uploads.MarkDataSetDraining(ctx, dataSetID, err.Error()); markErr != nil {
				logger.Warn("failed to mark peer dataset draining", "uploadID", uploadID, "copyIndex", copyIndex, "dataSetID", dataSetID, "error", markErr)
			}
			u.replacePeerCopy(ctx, task, bucket, uploadID, copyIndex, logger, stage, err)
			return
		}
		if terminalDataSetError(err) {
			if markErr := u.repos.Uploads.MarkDataSetUnavailable(ctx, dataSetID, err.Error()); markErr != nil {
				logger.Warn("failed to mark peer dataset unavailable", "uploadID", uploadID, "copyIndex", copyIndex, "dataSetID", dataSetID, "error", markErr)
			}
			u.replacePeerCopy(ctx, task, bucket, uploadID, copyIndex, logger, stage, err)
			return
		}
	}
	u.markPeerFailed(ctx, task, uploadID, copyIndex, 0, logger, stage, err)
}

func (u *Uploader) enqueueRepairUpload(ctx context.Context, parent *model.Task, uploadID int64) error {
	if parent == nil {
		return errors.New("parent task is required for upload repair")
	}
	stage := uploadStagePrepare
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          parent.RefID,
		RefVersionID:   parent.RefVersionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:%s:%d:repair", parent.RefVersionID, stage, uploadID),
		Payload:        map[string]interface{}{"upload_id": uploadID},
		Status:         model.TaskStatusQueued,
		MaxRetries:     parent.MaxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := u.repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		return err
	}
	return nil
}

func (u *Uploader) replacePeerCopy(ctx context.Context, task *model.Task, bucket *model.Bucket, uploadID int64, copyIndex int, logger *slog.Logger, stage string, err error) {
	if appendErr := u.repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
		UploadID:       uploadID,
		CopyIndex:      copyIndex,
		TransferMethod: string(model.StorageCopyTransferMethodPeerPull),
		Stage:          stage,
		ErrorMessage:   err.Error(),
	}); appendErr != nil {
		logger.Warn("failed to append peer upload failure", "uploadID", uploadID, "copyIndex", copyIndex, "error", appendErr)
	}
	if markErr := u.repos.Uploads.MarkUploadCopyFailed(ctx, uploadID, copyIndex, fmt.Sprintf("%s: %v", stage, err)); markErr != nil {
		u.handleTaskFailure(ctx, task, logger, "mark peer copy failed", markErr)
		return
	}
	if replaceErr := u.createReplacementPeerCopy(ctx, task, bucket, uploadID, logger); replaceErr != nil {
		u.handleTaskFailure(ctx, task, logger, "create replacement peer copy", replaceErr)
		return
	}
	logger.Error(stage+" failed; replacement peer copy queued", "error", err)
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) createReplacementPeerCopy(ctx context.Context, task *model.Task, bucket *model.Bucket, uploadID int64, logger *slog.Logger) error {
	if bucket == nil {
		return errors.New("bucket is required for replacement peer copy")
	}
	binding, err := u.createNewBucketProviderBinding(ctx, bucket, uploadID)
	if err != nil {
		return err
	}
	if err := u.repos.Uploads.CreateUploadCopiesForBindings(ctx, uploadID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        binding.CopyIndex,
		TransferMethod:   model.StorageCopyTransferMethodPeerPull,
		ProviderID:       binding.ProviderID,
	}}); err != nil {
		return err
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStageEnsureDataSet, uploadID, binding.CopyIndex, model.StorageCopyTransferMethodPeerPull); err != nil {
		return err
	}
	if logger != nil {
		logger.Info("queued replacement peer copy", "uploadID", uploadID, "copyIndex", binding.CopyIndex)
	}
	return nil
}

func (u *Uploader) createNewBucketProviderBinding(ctx context.Context, bucket *model.Bucket, uploadID int64) (*model.StorageDataSet, error) {
	bindings, err := u.repos.Uploads.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	existing := make(map[int]struct{}, len(bindings))
	excluded := make([]sdktypes.BigInt, 0, len(bindings))
	for _, binding := range bindings {
		existing[binding.CopyIndex] = struct{}{}
		excluded = append(excluded, binding.ProviderID.SDK())
	}
	contexts, err := u.storage.CreateContexts(ctx, &storage.CreateContextsOptions{
		Copies:             1,
		ExcludeProviderIDs: excluded,
		DataSetMetadata:    map[string]string{"bucket": bucket.Name},
	})
	if err != nil {
		return nil, err
	}
	if len(contexts) != 1 {
		return nil, fmt.Errorf("CreateContexts returned %d contexts, want 1", len(contexts))
	}
	copyIndex := nextAvailableCopyIndex(existing)
	storageCtx := contexts[0]
	binding, err := u.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        idtypes.OnChainIDFromSDK(storageCtx.ProviderID()),
		CopyIndex:         copyIndex,
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
	return binding, nil
}

func nextAvailableCopyIndex(existing map[int]struct{}) int {
	copyIndex := 0
	for {
		if _, ok := existing[copyIndex]; !ok {
			return copyIndex
		}
		copyIndex++
	}
}

func (u *Uploader) replanIngressUpload(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, copyIndex int, logger *slog.Logger, stage string, err error) {
	if stage != "cache read" {
		if appendErr := u.repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
			UploadID:       uploadID,
			CopyIndex:      copyIndex,
			TransferMethod: string(model.StorageCopyTransferMethodIngress),
			Stage:          stage,
			ErrorMessage:   err.Error(),
		}); appendErr != nil {
			logger.Warn("failed to append ingress upload failure", "uploadID", uploadID, "copyIndex", copyIndex, "error", appendErr)
		}
	}
	if markErr := u.repos.Uploads.MarkUploadCopyFailed(ctx, uploadID, copyIndex, fmt.Sprintf("%s: %v", stage, err)); markErr != nil {
		u.handleTaskFailure(ctx, task, logger, "mark ingress copy failed", markErr)
		return
	}
	if version.State == model.ObjectStateCommitting {
		if err := u.repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCommitting, model.ObjectStateUploading); err != nil {
			u.handleTaskFailure(ctx, task, logger, "state transition committing→uploading", err)
			return
		}
	}
	if err := u.enqueuePrepareUpload(ctx, task, version.VersionID, uploadID); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue replacement upload", err)
		return
	}
	logger.Error(stage+" failed; replacement ingress upload queued", "error", err)
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) enqueuePrepareUpload(ctx context.Context, parent *model.Task, versionID string, failedUploadID int64) error {
	stage := uploadStagePrepare
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          parent.RefID,
		RefVersionID:   parent.RefVersionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:%s:%d", versionID, stage, failedUploadID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     parent.MaxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := u.repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		return err
	}
	return nil
}

func terminalDataSetError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, transient := range []string{
		"context canceled",
		"deadline exceeded",
		"timeout",
		"temporary",
		"connection refused",
		"connection reset",
		"no such host",
		"network",
		"rpc",
		"insufficient",
		"balance",
	} {
		if strings.Contains(message, transient) {
			return false
		}
	}
	for _, terminal := range []string{
		"not found",
		"does not exist",
		"terminated",
		"retired",
		"not active",
		"inactive",
		"not live",
	} {
		if strings.Contains(message, terminal) {
			return true
		}
	}
	return false
}

func dataSetWriteBlockedError(err error) bool {
	var blocked *storage.DataSetPDPPaymentTerminatedError
	return errors.As(err, &blocked)
}

func dataSetBindingWriteBlocked(binding *model.StorageDataSet) bool {
	return binding != nil && binding.Status == model.StorageDataSetStatusDraining
}

func dataSetBindingUnavailable(binding *model.StorageDataSet) bool {
	if binding == nil {
		return false
	}
	switch binding.Status {
	case model.StorageDataSetStatusUnavailable, model.StorageDataSetStatusRetired:
		return true
	default:
		return false
	}
}

func dataSetCreationRejected(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, pdp.ErrTxRejected) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "rejected") || strings.Contains(message, "incomplete")
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
		return binding, nil, err
	}
	return binding, storageCtx, nil
}

func (u *Uploader) contextForBindingProvider(ctx context.Context, binding *model.StorageDataSet, bucketName string) (synapse.UploadContext, error) {
	return u.storage.CreateContext(ctx, &storage.CreateContextOptions{
		ProviderID:      sdkBigIntPtr(&binding.ProviderID),
		DataSetMetadata: map[string]string{"bucket": bucketName},
	})
}

func (u *Uploader) contextForReadyBinding(ctx context.Context, binding *model.StorageDataSet, bucketName string) (synapse.UploadContext, error) {
	return u.storage.CreateContext(ctx, &storage.CreateContextOptions{
		DataSetID:       sdkBigIntPtr(binding.DataSetID),
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

func dataSetBindingCanEnsureWrite(binding *model.StorageDataSet) bool {
	return binding != nil && dataSetStatusCanEnsureWrite(binding.Status)
}

func dataSetStatusCanEnsureWrite(status model.StorageDataSetStatus) bool {
	switch status {
	case model.StorageDataSetStatusPending,
		model.StorageDataSetStatusCreating,
		model.StorageDataSetStatusReady,
		model.StorageDataSetStatusFailed:
		return true
	default:
		return false
	}
}

func availableUSDFCFunds(acct *synapse.PaymentAccountInfo) *big.Int {
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
	available := availableUSDFCFunds(info.PaymentAccount)
	if available == nil || available.Sign() != 0 {
		return nil
	}
	return errors.New("insufficient payment account balance: USDFC available funds = 0; deposit USDFC into your payment account or wait for locked funds to become available before uploading")
}

func (u *Uploader) handleBalancePreflightFailure(ctx context.Context, task *model.Task, logger *slog.Logger, err error) {
	logger.Warn("wallet balance pre-check failed", "error", err)
	scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) handleTaskFailure(ctx context.Context, task *model.Task, logger *slog.Logger, stage string, err error) {
	logger.Error(stage+" failed", "error", err)
	scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) handleFailure(ctx context.Context, task *model.Task, version *model.ObjectVersion, logger *slog.Logger, stage string, err error) model.TaskStatus {
	logger.Error(stage+" failed", "error", err)
	status := scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
	if status == model.TaskStatusExhausted {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), terminalFailureCleanupTimeout)
		defer cancel()
		u.failUploadingContent(cleanupCtx, task, version, logger, fmt.Sprintf("%s: %v (max retries reached)", stage, err))
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
	return status
}

func (u *Uploader) handleIngressFailure(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, copyIndex int, logger *slog.Logger, stage string, err error) {
	if stage != "cache read" {
		if appendErr := u.repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
			UploadID:       uploadID,
			CopyIndex:      copyIndex,
			TransferMethod: string(model.StorageCopyTransferMethodIngress),
			Stage:          stage,
			ErrorMessage:   err.Error(),
		}); appendErr != nil {
			logger.Warn("failed to append ingress upload failure", "uploadID", uploadID, "copyIndex", copyIndex, "error", appendErr)
		}
	}
	status := u.handleFailure(ctx, task, version, logger, stage, err)
	if status == model.TaskStatusExhausted {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), terminalFailureCleanupTimeout)
		defer cancel()
		if markErr := u.repos.Uploads.MarkUploadCopyFailed(cleanupCtx, uploadID, copyIndex, fmt.Sprintf("%s: %v", stage, err)); markErr != nil {
			logger.Warn("failed to mark ingress upload copy failed", "uploadID", uploadID, "copyIndex", copyIndex, "error", markErr)
		}
	}
}

func (u *Uploader) handleIngressDataSetFailure(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, copyIndex int, dataSetID int64, logger *slog.Logger, stage string, err error) {
	if dataSetID > 0 {
		if dataSetWriteBlockedError(err) {
			if markErr := u.repos.Uploads.MarkDataSetDraining(ctx, dataSetID, err.Error()); markErr != nil {
				logger.Warn("failed to mark ingress dataset draining", "uploadID", uploadID, "copyIndex", copyIndex, "dataSetID", dataSetID, "error", markErr)
			}
			u.replanIngressUpload(ctx, task, version, uploadID, copyIndex, logger, stage, err)
			return
		}
		if terminalDataSetError(err) {
			if markErr := u.repos.Uploads.MarkDataSetUnavailable(ctx, dataSetID, err.Error()); markErr != nil {
				logger.Warn("failed to mark ingress dataset unavailable", "uploadID", uploadID, "copyIndex", copyIndex, "dataSetID", dataSetID, "error", markErr)
			}
			u.replanIngressUpload(ctx, task, version, uploadID, copyIndex, logger, stage, err)
			return
		}
	}
	u.handleIngressFailure(ctx, task, version, uploadID, copyIndex, logger, stage, err)
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
		logger.Warn("cannot transition non-ingress upload state to failed", "state", from)
		return
	}
	_ = state.TransitionToFailed(ctx, u.stateMachine, u.repos.Objects, task.RefVersionID, from, lastError)
}
