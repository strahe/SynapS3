package worker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
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
	"github.com/strahe/synaps3/internal/objectlimits"
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
	uploadFundingWaitDelay        = time.Minute
	uploadDependencyWaitDelay     = time.Minute
)

var (
	errCommitRejected            = errors.New("commit transaction rejected")
	errSubmittedCommitPending    = errors.New("submitted commit is still pending")
	errDataSetCreationIncomplete = errors.New("data set creation submission is incomplete")
)

var submittedCommitRequestTimeout = 15 * time.Second

var submittedCommitMaxWait = 5 * time.Minute

// Uploader claims upload tasks, persists upload provenance, and accepts complete
// uploads for object versions.
type Uploader struct {
	repos           *repository.Repositories
	cache           cache.Cache
	storage         synapse.StorageClient
	statusChecker   *synapse.PDPStatusChecker
	wallet          synapse.WalletQuerier // optional; nil skips balance pre-check
	stateMachine    *state.Machine
	evictionPolicy  cache.EvictionPolicy
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
	uploadStageRepairReplica = "repair_replica"
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

// WithPDPStatusChecker configures the client used to resume submitted commits.
func WithPDPStatusChecker(checker *synapse.PDPStatusChecker) UploaderOption {
	return func(u *Uploader) {
		if checker != nil {
			u.statusChecker = checker
		}
	}
}

func boundedTargetCopies(copies int) int {
	return model.ClampStorageCopies(copies)
}

// NewUploader creates a new upload worker.
func NewUploader(repos *repository.Repositories, c cache.Cache, sc synapse.StorageClient, wallet synapse.WalletQuerier, sm *state.Machine, evictionPolicy cache.EvictionPolicy, targetCopies int, concurrency int, pollInterval time.Duration, logger *slog.Logger, opts ...UploaderOption) *Uploader {
	u := &Uploader{
		repos:           repos,
		cache:           c,
		storage:         sc,
		statusChecker:   synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{Timeout: submittedCommitRequestTimeout}),
		wallet:          wallet,
		stateMachine:    sm,
		evictionPolicy:  evictionPolicy,
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

func (u *Uploader) finalizeUploadInput(uploadID int64) repository.FinalizeUploadInput {
	return repository.NewFinalizeUploadInput(
		uploadID,
		u.evictionPolicy.EnqueuesAfterUploadEviction(),
		u.evictMaxRetries,
	)
}

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
	if uploadTaskStage(task) == uploadStageRepairReplica {
		u.processReplicaRepairTask(ctx, task, logger)
		return
	}

	version, err := u.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil || version == nil {
		logger.Warn("object version not found for upload task", "error", err)
		_ = u.repos.Tasks.FailRunning(ctx, task, "object not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}
	if task.ClaimedAt == nil {
		return
	}
	uploadID, _ := taskUploadID(task)
	err = u.repos.Uploads.AcquireUploadTask(ctx, repository.AcquireUploadTaskInput{
		TaskID:        task.ID,
		TaskClaimedAt: *task.ClaimedAt,
		UploadID:      uploadID,
		VersionID:     version.VersionID,
	})
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrUploadTaskCancelled):
			completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		case errors.Is(err, repository.ErrTaskClaimLost):
		default:
			u.handleTaskFailure(ctx, task, logger, "acquire upload task", err)
		}
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
		if u.deferToReplicaRepair(ctx, task, bucket.ID, uploadID, copyIndex, logger) {
			return
		}
		u.ingressStore(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStageIngressCommit:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		if u.deferToReplicaRepair(ctx, task, bucket.ID, uploadID, copyIndex, logger) {
			return
		}
		u.ingressCommit(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStagePeerPull:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		if u.deferToReplicaRepair(ctx, task, bucket.ID, uploadID, copyIndex, logger) {
			return
		}
		u.peerPull(ctx, task, version, bucket, uploadID, copyIndex, logger)
	case uploadStagePeerCommit:
		uploadID, copyIndex, err := uploadStageIDs(task, true)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "parse upload task payload", err)
			return
		}
		if u.deferToReplicaRepair(ctx, task, bucket.ID, uploadID, copyIndex, logger) {
			return
		}
		u.peerCommit(ctx, task, version, bucket, uploadID, copyIndex, logger)
	default:
		u.handleTaskFailure(ctx, task, logger, "parse upload task payload", fmt.Errorf("unknown upload stage %q", stage))
	}
}

func (u *Uploader) deferToReplicaRepair(ctx context.Context, task *model.Task, bucketID, uploadID int64, copyIndex int, logger *slog.Logger) bool {
	binding, err := u.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucketID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load upload data set", err)
		return true
	}
	if binding == nil {
		return false
	}
	repairTask, err := u.repos.Tasks.GetByIdempotencyKey(ctx, replicaRepairTaskKey(binding.ID))
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "check replica repair task", err)
		return true
	}
	if repairTask == nil || repairTask.Status != model.TaskStatusRunning {
		return false
	}
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil || copyRow == nil {
		if err == nil {
			err = fmt.Errorf("upload copy %d not found", copyIndex)
		}
		u.handleTaskFailure(ctx, task, logger, "load upload copy for replica repair coordination", err)
		return true
	}
	repairCopyID, err := payloadInt64(repairTask.Payload, replicaRepairCopyIDKey)
	if err == nil && repairCopyID != copyRow.ID {
		return false
	}
	if !taskClaimPrecedes(repairTask, task) {
		return false
	}
	u.waitForStorageDependency(ctx, task, logger, "Waiting for in-place replica recovery")
	return true
}

func taskClaimPrecedes(first, second *model.Task) bool {
	if first == nil || second == nil || first.ClaimedAt == nil || second.ClaimedAt == nil {
		return true
	}
	if first.ClaimedAt.Equal(*second.ClaimedAt) {
		return first.ID < second.ID
	}
	return first.ClaimedAt.Before(*second.ClaimedAt)
}

func (u *Uploader) prepareStagedUpload(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, logger *slog.Logger) {
	if uploadID, ok := taskUploadID(task); ok {
		u.prepareReadableUploadRepair(ctx, task, version, bucket, uploadID, logger)
		return
	}
	if version.State == model.ObjectStateReplicating && version.StorageUploadID != nil {
		u.prepareReadableUploadRepair(ctx, task, version, bucket, *version.StorageUploadID, logger)
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
	plan, planErr := u.ensureBucketProviderBindings(ctx, bucket, upload.ID, targetCopies)
	if planErr != nil && !synapse.IsNoProviderCandidates(planErr) {
		u.handleTaskFailure(ctx, task, logger, "ensure provider bindings", planErr)
		return
	}
	copyInputs, err := u.uploadCopyInputs(ctx, upload.ID, plan.bindings)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "plan upload copy rows", err)
		return
	}
	if err := u.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, copyInputs); err != nil {
		u.handleTaskFailure(ctx, task, logger, "create upload copy rows", err)
		return
	}
	for _, input := range copyInputs {
		binding := plan.byID[input.StorageDataSetID]
		if binding != nil && binding.Status == model.StorageDataSetStatusUnavailable {
			if err := u.ensureReplicaRepairTask(ctx, binding, task.MaxRetries); err != nil {
				u.handleTaskFailure(ctx, task, logger, "ensure unavailable replica repair", err)
				return
			}
		}
	}
	if len(plan.bindings) == 0 {
		u.waitForStorageDependency(ctx, task, logger, "Waiting for an assigned storage provider")
		return
	}
	fundedBindings, fundingReady := u.ensureUploadFundingReady(ctx, task, version.Size, bucket, upload.ID, &plan, logger)
	if !fundingReady {
		return
	}
	ingress, err := u.ensureWritableIngressCopy(ctx, upload.ID, fundedBindings)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "select writable ingress copy", err)
		return
	}
	if ingress == nil {
		u.waitForStorageDependency(ctx, task, logger, "Waiting for an assigned storage provider to recover")
		return
	}
	if err := u.enqueueUploadStage(ctx, task, uploadStageEnsureDataSet, upload.ID, ingress.CopyIndex, model.StorageCopyTransferMethodIngress); err != nil {
		u.handleTaskFailure(ctx, task, logger, "enqueue ingress dataset task", err)
		return
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) uploadCopyInputs(ctx context.Context, uploadID int64, bindings []model.StorageDataSet) ([]repository.UploadCopyBindingInput, error) {
	existingCopies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	existingByIndex := make(map[int]model.StorageUploadCopy, len(existingCopies))
	ingressCopyIndex := -1
	for _, copyRow := range existingCopies {
		existingByIndex[copyRow.CopyIndex] = copyRow
		if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress {
			ingressCopyIndex = copyRow.CopyIndex
		}
	}
	if ingressCopyIndex < 0 {
		for i := range bindings {
			if uploadCanUseDataSetBinding(uploadID, &bindings[i]) {
				ingressCopyIndex = bindings[i].CopyIndex
				break
			}
		}
	}
	if ingressCopyIndex < 0 {
		for i := range bindings {
			if uploadTracksDataSetBinding(uploadID, &bindings[i]) {
				ingressCopyIndex = bindings[i].CopyIndex
				break
			}
		}
	}
	inputs := make([]repository.UploadCopyBindingInput, 0, len(bindings))
	for _, binding := range bindings {
		if _, exists := existingByIndex[binding.CopyIndex]; !exists && !uploadTracksDataSetBinding(uploadID, &binding) {
			continue
		}
		transferMethod := model.StorageCopyTransferMethodPeerPull
		if existing, ok := existingByIndex[binding.CopyIndex]; ok {
			transferMethod = existing.TransferMethod
		} else if binding.CopyIndex == ingressCopyIndex {
			transferMethod = model.StorageCopyTransferMethodIngress
		}
		inputs = append(inputs, repository.UploadCopyBindingInput{
			StorageDataSetID: binding.ID,
			CopyIndex:        binding.CopyIndex,
			TransferMethod:   transferMethod,
			ProviderID:       binding.ProviderID,
		})
	}
	return inputs, nil
}

func taskUploadID(task *model.Task) (int64, bool) {
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
	finalized, _, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(
		ctx,
		u.finalizeUploadInput(uploadID),
	)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "finalize repaired upload", err)
		return
	}
	if finalized {
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	plan, planErr := u.ensureBucketProviderBindings(ctx, bucket, uploadID, boundedTargetCopies(upload.RequestedCopies))
	if planErr != nil && !synapse.IsNoProviderCandidates(planErr) {
		u.handleTaskFailure(ctx, task, logger, "ensure repair provider bindings", planErr)
		return
	}
	inputs, err := u.uploadCopyInputs(ctx, uploadID, plan.bindings)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "plan repair upload copy rows", err)
		return
	}
	if err := u.repos.Uploads.CreateUploadCopiesForBindings(ctx, uploadID, inputs); err != nil {
		u.handleTaskFailure(ctx, task, logger, "create repair upload copies", err)
		return
	}
	fundedBindings := make(map[int]*model.StorageDataSet, len(plan.writable))
	fundingCandidates := make([]model.StorageDataSet, 0, len(plan.writable))
	for i := range plan.writable {
		binding := plan.byID[plan.writable[i].ID]
		if binding == nil {
			continue
		}
		if binding.Status == model.StorageDataSetStatusReady {
			fundedBindings[binding.CopyIndex] = binding
			continue
		}
		fundingCandidates = append(fundingCandidates, *binding)
	}
	if len(fundedBindings) == 0 && len(fundingCandidates) == 0 {
		u.waitForStorageDependency(ctx, task, logger, "Waiting for an assigned storage provider")
		return
	}
	deferredContext := false
	if len(fundingCandidates) > 0 {
		fundingPlan := newBucketBindingPlan(fundingCandidates, uploadID)
		candidateBindings, fundingReady := u.ensureUploadFundingReady(ctx, task, version.Size, bucket, uploadID, &fundingPlan, logger)
		if !fundingReady {
			return
		}
		for copyIndex, binding := range candidateBindings {
			fundedBindings[copyIndex] = binding
		}
		deferredContext = fundingPlan.deferredContext
	}
	copies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "reload repair upload copies", err)
		return
	}
	for i := range copies {
		copyRow := &copies[i]
		if copyCommitted(copyRow) || copyRow.Status == model.StorageUploadCopyStatusFailed {
			continue
		}
		binding := plan.byCopyIndex[copyRow.CopyIndex]
		switch {
		case fundedBindings[copyRow.CopyIndex] != nil && uploadCanUseDataSetBinding(uploadID, binding):
			if err := u.enqueueUploadStage(ctx, task, uploadStageEnsureDataSet, uploadID, copyRow.CopyIndex, model.StorageCopyTransferMethodPeerPull); err != nil {
				u.handleTaskFailure(ctx, task, logger, "enqueue existing repair copy", err)
				return
			}
		case binding != nil && binding.Status == model.StorageDataSetStatusUnavailable:
			if err := u.ensureReplicaRepairTask(ctx, binding, task.MaxRetries); err != nil {
				u.handleTaskFailure(ctx, task, logger, "ensure unavailable replica repair", err)
				return
			}
		}
	}
	if !plan.complete || deferredContext {
		u.waitForStorageDependency(ctx, task, logger, "Waiting for an eligible storage provider")
		return
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) targetCopiesForBucket(bucket *model.Bucket) int {
	if bucket != nil && bucket.DefaultCopies != nil {
		return boundedTargetCopies(*bucket.DefaultCopies)
	}
	return boundedTargetCopies(u.targetCopies)
}

type bucketBindingPlan struct {
	bindings        []model.StorageDataSet
	writable        []model.StorageDataSet
	byID            map[int64]*model.StorageDataSet
	byCopyIndex     map[int]*model.StorageDataSet
	complete        bool
	deferredContext bool
}

func newBucketBindingPlan(bindings []model.StorageDataSet, uploadID int64) bucketBindingPlan {
	plan := bucketBindingPlan{
		bindings:    bindings,
		byID:        make(map[int64]*model.StorageDataSet, len(bindings)),
		byCopyIndex: make(map[int]*model.StorageDataSet, len(bindings)),
	}
	for i := range plan.bindings {
		binding := &plan.bindings[i]
		plan.byID[binding.ID] = binding
		plan.byCopyIndex[binding.CopyIndex] = binding
		if uploadCanUseDataSetBinding(uploadID, binding) {
			plan.writable = append(plan.writable, *binding)
		}
	}
	return plan
}

func (u *Uploader) ensureWritableIngressCopy(ctx context.Context, uploadID int64, fundedBindings map[int]*model.StorageDataSet) (*model.StorageUploadCopy, error) {
	copies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	var ingress *model.StorageUploadCopy
	for i := range copies {
		copyRow := &copies[i]
		if copyRow.TransferMethod != model.StorageCopyTransferMethodIngress || copyCommitted(copyRow) {
			continue
		}
		ingress = copyRow
		if fundedBindings[copyRow.CopyIndex] != nil {
			return ingress, nil
		}
		break
	}
	if ingress == nil {
		return nil, nil
	}
	return u.repos.Uploads.ReassignIngressCopy(ctx, uploadID, ingress.CopyIndex)
}

func (u *Uploader) waitForStorageDependency(ctx context.Context, task *model.Task, logger *slog.Logger, message string) {
	if err := u.repos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonDependency, message, uploadDependencyWaitDelay); err != nil {
		logger.Error("failed to wait for storage dependency", "error", err)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) ensureBucketProviderBindings(ctx context.Context, bucket *model.Bucket, uploadID int64, targetCopies int) (bucketBindingPlan, error) {
	bindings, err := u.repos.Uploads.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return bucketBindingPlan{}, err
	}
	existing := make(map[int]model.StorageDataSet, len(bindings))
	targetCopies = boundedTargetCopies(targetCopies)
	selected := make([]model.StorageDataSet, 0, targetCopies)
	excluded := make([]sdktypes.BigInt, 0, len(bindings))
	for _, binding := range bindings {
		existing[binding.CopyIndex] = binding
		excluded = append(excluded, binding.ProviderID.SDK())
	}
	missingIndexes := make([]int, 0, targetCopies)
	for copyIndex := range targetCopies {
		if binding, ok := existing[copyIndex]; ok {
			selected = append(selected, binding)
		} else {
			missingIndexes = append(missingIndexes, copyIndex)
		}
	}
	if len(missingIndexes) == 0 {
		plan := newBucketBindingPlan(selected, uploadID)
		plan.complete = true
		return plan, nil
	}
	contexts, err := u.storage.CreateContexts(ctx, &storage.CreateContextsOptions{
		Copies:             len(missingIndexes),
		ExcludeProviderIDs: excluded,
		DataSetMetadata:    map[string]string{"bucket": bucket.Name},
	})
	if err != nil {
		return newBucketBindingPlan(selected, uploadID), err
	}
	for i, storageCtx := range contexts {
		if i >= len(missingIndexes) {
			break
		}
		if storageCtx == nil {
			return newBucketBindingPlan(selected, uploadID), errors.New("storage context resolver returned a nil context")
		}
		copyIndex := missingIndexes[i]
		binding, err := u.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID:          bucket.ID,
			ProviderID:        idtypes.OnChainIDFromSDK(storageCtx.ProviderID()),
			CopyIndex:         copyIndex,
			CreatedByUploadID: uploadID,
		})
		if err != nil {
			return newBucketBindingPlan(selected, uploadID), err
		}
		if dataSetID := storageCtx.DataSetID(); dataSetID != nil {
			if err := u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID:        binding.ID,
				UploadID:  uploadID,
				DataSetID: idtypes.OnChainIDFromSDK(*dataSetID),
			}); err != nil {
				return newBucketBindingPlan(selected, uploadID), err
			}
			binding.Status = model.StorageDataSetStatusReady
			binding.DataSetID = onChainIDPtrFromSDK(*dataSetID)
		}
		selected = append(selected, *binding)
		existing[copyIndex] = *binding
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
	if len(contexts) != len(missingIndexes) {
		return newBucketBindingPlan(selected, uploadID), &synapse.NoProviderCandidatesError{
			Cause: fmt.Errorf("CreateContexts returned %d contexts, want %d", len(contexts), len(missingIndexes)),
		}
	}
	plan := newBucketBindingPlan(selected, uploadID)
	plan.complete = true
	return plan, nil
}

func (u *Uploader) ensureUploadFundingReady(
	ctx context.Context,
	task *model.Task,
	contentSize int64,
	bucket *model.Bucket,
	uploadID int64,
	plan *bucketBindingPlan,
	logger *slog.Logger,
) (map[int]*model.StorageDataSet, bool) {
	if plan == nil {
		u.handleTaskFailure(ctx, task, logger, "prepare upload funding contexts", errors.New("missing bucket binding plan"))
		return nil, false
	}
	contexts := make([]synapse.UploadContext, 0, len(plan.writable))
	fundedBindings := make(map[int]*model.StorageDataSet, len(plan.writable))
	for i := range plan.writable {
		binding := plan.byID[plan.writable[i].ID]
		if binding == nil {
			u.handleTaskFailure(ctx, task, logger, "prepare upload funding contexts", errors.New("storage data set binding is missing from plan"))
			return nil, false
		}
		var (
			storageCtx synapse.UploadContext
			err        error
		)
		if binding.Status == model.StorageDataSetStatusReady && binding.DataSetID != nil && !binding.DataSetID.IsZero() {
			storageCtx, err = u.contextForReadyBinding(ctx, binding, bucket.Name)
		} else {
			storageCtx, err = u.contextForBindingProvider(ctx, binding, bucket.Name)
		}
		if err == nil {
			contexts = append(contexts, storageCtx)
			fundedBindings[binding.CopyIndex] = binding
			continue
		}
		switch {
		case binding.Status == model.StorageDataSetStatusReady && dataSetFailureEnded(err, binding):
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusDraining, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark funding data set draining", markErr)
				return nil, false
			}
			binding = latest
			plan.byID[binding.ID] = binding
			if binding.Status == model.StorageDataSetStatusUnavailable {
				if repairErr := u.ensureReplicaRepairTask(ctx, binding, task.MaxRetries); repairErr != nil {
					u.handleTaskFailure(ctx, task, logger, "ensure unavailable replica repair", repairErr)
					return nil, false
				}
			} else if binding.Status == model.StorageDataSetStatusReady {
				plan.deferredContext = true
			} else if !dataSetBindingWriteBlocked(binding) {
				u.handleTaskFailure(ctx, task, logger, "mark funding data set draining", fmt.Errorf("data set status changed to %s", binding.Status))
				return nil, false
			}
		case binding.Status == model.StorageDataSetStatusReady && dataSetFailureUnavailable(err, binding):
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusUnavailable, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark funding data set unavailable", markErr)
				return nil, false
			}
			binding = latest
			plan.byID[binding.ID] = binding
			switch binding.Status {
			case model.StorageDataSetStatusUnavailable:
				if repairErr := u.ensureReplicaRepairTask(ctx, binding, task.MaxRetries); repairErr != nil {
					u.handleTaskFailure(ctx, task, logger, "ensure unavailable replica repair", repairErr)
					return nil, false
				}
			case model.StorageDataSetStatusReady:
				plan.deferredContext = true
			case model.StorageDataSetStatusDraining, model.StorageDataSetStatusRetired:
			default:
				u.handleTaskFailure(ctx, task, logger, "mark funding data set unavailable", fmt.Errorf("data set status changed to %s", binding.Status))
				return nil, false
			}
		case synapse.IsNoProviderCandidates(err):
			plan.deferredContext = true
		case synapse.IsProviderUnavailable(err):
			plan.deferredContext = true
		default:
			u.handleTaskFailure(ctx, task, logger, "prepare upload funding contexts", err)
			return nil, false
		}
	}
	if len(contexts) == 0 {
		u.waitForStorageDependency(ctx, task, logger, "Waiting for an assigned storage provider to recover")
		return nil, false
	}
	dataSize := uint64(objectlimits.MinFOCUploadSize)
	if contentSize > int64(dataSize) {
		dataSize = uint64(contentSize)
	}
	costs, err := u.storage.PrepareUpload(ctx, dataSize, contexts)
	if err != nil {
		if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
			u.waitForStorageDependency(ctx, task, logger, "Waiting for storage providers to become available")
			return nil, false
		}
		u.handleTaskFailure(ctx, task, logger, "prepare upload funding", err)
		return nil, false
	}
	if costs == nil {
		u.handleTaskFailure(ctx, task, logger, "prepare upload funding", errors.New("missing storage cost estimate"))
		return nil, false
	}
	if costs.Ready {
		return fundedBindings, true
	}
	message := uploadFundingWaitMessage(costs)
	if err := u.repos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonDependency, message, uploadFundingWaitDelay); err != nil {
		logger.Error("failed to wait for upload funding", "uploadID", uploadID, "error", err)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return nil, false
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
	logger.Info("upload funding deferred", "uploadID", uploadID, "message", message)
	return nil, false
}

func uploadFundingWaitMessage(costs *storage.MultiContextCosts) string {
	parts := make([]string, 0, 2)
	if costs != nil && costs.DepositNeeded != nil && costs.DepositNeeded.Sign() > 0 {
		parts = append(parts, fmt.Sprintf("deposit %s USDFC base units", costs.DepositNeeded.String()))
	}
	if costs != nil && costs.NeedsFWSSMaxApproval {
		parts = append(parts, "approve FWSS spending")
	}
	if len(parts) == 0 {
		return "Waiting for Filecoin payment funding"
	}
	return "Waiting for Filecoin payment funding: " + strings.Join(parts, "; ")
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
	if dataSetBindingUnavailable(binding) || dataSetBindingWriteBlocked(binding) {
		u.handleAssignedDataSetDependency(ctx, task, version, uploadID, copyRow, binding, logger, "ensure dataset", fmt.Errorf("data set status is %s", binding.Status))
		return
	}
	if !uploadCanUseDataSetBinding(uploadID, binding) {
		u.waitForStorageDependency(ctx, task, logger, "Waiting for the assigned storage service to become writable")
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
						if synapse.IsProviderUnavailable(err) {
							u.waitForStorageDependency(ctx, task, logger, "Waiting for storage service creation")
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
					u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "wait dataset", errDataSetCreationIncomplete)
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
					if synapse.IsProviderUnavailable(err) {
						u.waitForStorageDependency(ctx, task, logger, "Waiting for storage service creation")
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
	copyRow, err := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load ingress copy", err)
		return
	}
	if copyCommitted(copyRow) {
		u.finishCommittedIngress(ctx, task, version, bucket, uploadID, copyRow, logger)
		return
	}
	binding, storageCtx, err := u.readyContextForCopy(ctx, bucket, copyIndex)
	if err != nil {
		u.markDataSetStageFailed(ctx, task, version, bucket, uploadID, copyIndex, binding, logger, "ingress commit context", err)
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
			if u.waitForPendingSubmittedCommit(ctx, task, logger, err) {
				return
			}
			if errors.Is(err, errCommitRejected) {
				if resetErr := u.resetRejectedSubmittedCommit(ctx, uploadID, copyIndex, *copyRow.CommitTransactionID, err); resetErr != nil {
					u.handleTaskFailure(ctx, task, logger, "reset rejected ingress commit", resetErr)
					return
				}
				u.handleIngressFailure(ctx, task, version, uploadID, copyIndex, logger, "ingress commit", err)
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
			u.handleIngressDataSetFailure(ctx, task, version, uploadID, copyIndex, binding.ID, logger, "ingress commit", err)
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

func (u *Uploader) finishCommittedIngress(ctx context.Context, task *model.Task, version *model.ObjectVersion, bucket *model.Bucket, uploadID int64, copyRow *model.StorageUploadCopy, logger *slog.Logger) {
	readable, err := u.repos.Uploads.HasReadableCommittedCopy(ctx, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "check committed ingress readability", err)
		return
	}
	if readable {
		u.finishReadable(ctx, task, version, uploadID, logger)
		return
	}
	if copyRow == nil || copyRow.StorageDataSetID == nil {
		u.handleTaskFailure(ctx, task, logger, "load committed ingress data set", repository.ErrNotFound)
		return
	}
	binding, err := u.repos.Uploads.GetDataSetBindingByID(ctx, *copyRow.StorageDataSetID)
	if err != nil || binding == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		u.handleTaskFailure(ctx, task, logger, "load committed ingress data set", err)
		return
	}
	switch binding.Status {
	case model.StorageDataSetStatusReady:
		u.finishReadable(ctx, task, version, uploadID, logger)
		return
	case model.StorageDataSetStatusUnavailable:
	case model.StorageDataSetStatusDraining, model.StorageDataSetStatusRetired:
		u.waitForStorageDependency(ctx, task, logger, "Waiting for the storage service to be replaced")
		return
	default:
		u.handleTaskFailure(ctx, task, logger, "recover committed ingress data set", fmt.Errorf("data set status %s cannot be recovered in place", binding.Status))
		return
	}
	if binding.DataSetID == nil || binding.DataSetID.IsZero() {
		u.handleTaskFailure(ctx, task, logger, "recover committed ingress data set", errors.New("established data set has no data set ID"))
		return
	}
	if _, err := u.contextForReadyBinding(ctx, binding, bucket.Name); err != nil {
		u.handleCommittedIngressDataSetFailure(ctx, task, binding, logger, err)
		return
	}
	if err := u.ensureReplicaRepairTask(ctx, binding, task.MaxRetries); err != nil {
		u.handleTaskFailure(ctx, task, logger, "schedule data set recovery finalization", err)
		return
	}
	recovered, err := u.repos.Uploads.RecoverDataSet(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  uploadID,
		DataSetID: *binding.DataSetID,
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "recover committed ingress data set", err)
		return
	}
	if !recovered {
		latest, loadErr := u.repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
		if loadErr != nil || latest == nil {
			if loadErr == nil {
				loadErr = repository.ErrNotFound
			}
			u.handleTaskFailure(ctx, task, logger, "reload committed ingress data set", loadErr)
			return
		}
		switch latest.Status {
		case model.StorageDataSetStatusReady:
		case model.StorageDataSetStatusUnavailable:
			u.waitForStorageDependency(ctx, task, logger, "Waiting for the assigned storage provider to recover")
			return
		case model.StorageDataSetStatusDraining, model.StorageDataSetStatusRetired:
			u.waitForStorageDependency(ctx, task, logger, "Waiting for the storage service to be replaced")
			return
		default:
			u.handleTaskFailure(ctx, task, logger, "recover committed ingress data set", fmt.Errorf("data set status changed to %s", latest.Status))
			return
		}
	}
	u.finishReadable(ctx, task, version, uploadID, logger)
}

func (u *Uploader) handleCommittedIngressDataSetFailure(ctx context.Context, task *model.Task, binding *model.StorageDataSet, logger *slog.Logger, err error) {
	status := model.StorageDataSetStatusUnavailable
	if dataSetFailureEnded(err, binding) {
		status = model.StorageDataSetStatusDraining
	} else if !dataSetFailureUnavailable(err, binding) {
		u.handleTaskFailure(ctx, task, logger, "verify committed ingress context", err)
		return
	}
	latest, markErr := u.markDataSetStatus(ctx, binding, status, err.Error())
	if markErr != nil {
		u.handleTaskFailure(ctx, task, logger, "mark committed ingress data set", markErr)
		return
	}
	var message string
	if latest.Status == model.StorageDataSetStatusReady {
		message = "Waiting to retry the storage operation"
	} else if latest.Status == model.StorageDataSetStatusUnavailable {
		message = "Waiting for the assigned storage provider to recover"
	} else if dataSetBindingWriteBlocked(latest) {
		message = "Waiting for the storage service to be replaced"
	} else {
		u.handleTaskFailure(ctx, task, logger, "mark committed ingress data set", fmt.Errorf("data set status changed to %s", latest.Status))
		return
	}
	u.waitForStorageDependency(ctx, task, logger, message)
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
	ref := repository.ObjectVersionRef{ObjectID: version.ObjectID, VersionID: version.VersionID}
	_, needsPreparation, err := u.scheduleRemainingPeerCopies(ctx, ref, version.BucketID, uploadID, task.MaxRetries)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "schedule remaining upload copies", err)
		return
	}
	if needsPreparation {
		if err := u.enqueueRepairUploadForVersion(ctx, ref, task.MaxRetries, uploadID); err != nil {
			u.handleTaskFailure(ctx, task, logger, "schedule upload preparation", err)
			return
		}
	}
	_, _, err = u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(
		ctx,
		u.finalizeUploadInput(uploadID),
	)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "finalize readable upload", err)
		return
	}
	if !completeWorkerTask(ctx, u.repos, task, "uploader", logger) {
		return
	}
	logger.Info("upload readable copy committed", "uploadID", uploadID, "versions", len(refs))
}

func (u *Uploader) scheduleRemainingPeerCopies(
	ctx context.Context,
	ref repository.ObjectVersionRef,
	bucketID int64,
	uploadID int64,
	maxRetries int,
) ([]model.StorageUploadCopy, bool, error) {
	copies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		return nil, false, err
	}
	upload, err := u.repos.Uploads.GetByID(ctx, uploadID)
	if err != nil {
		return nil, false, err
	}
	if upload == nil {
		return nil, false, fmt.Errorf("storage upload %d not found", uploadID)
	}
	needsPreparation := len(copies) < boundedTargetCopies(upload.RequestedCopies)
	for _, copyRow := range copies {
		if copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull || copyCommitted(&copyRow) || copyRow.Status == model.StorageUploadCopyStatusFailed {
			continue
		}
		binding, err := u.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucketID, copyRow.CopyIndex)
		if err != nil {
			return nil, false, err
		}
		switch {
		case binding != nil && binding.Status == model.StorageDataSetStatusReady:
			if err := u.enqueueUploadStageForVersion(ctx, ref, maxRetries, uploadStageEnsureDataSet, uploadID, copyRow.CopyIndex, copyRow.TransferMethod); err != nil {
				return nil, false, err
			}
		case binding != nil && binding.Status == model.StorageDataSetStatusUnavailable:
			if err := u.ensureReplicaRepairTask(ctx, binding, maxRetries); err != nil {
				return nil, false, err
			}
		case uploadCanUseDataSetBinding(uploadID, binding):
			needsPreparation = true
		}
	}
	return copies, needsPreparation, nil
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
	if copyCommitted(copyRow) {
		u.finishPeerCopy(ctx, task, version, uploadID, logger)
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
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load readable source copy", err)
		return
	}
	var pieceCID cid.Cid
	var pieceCIDString string
	var extraHex string
	if len(readableCopies) > 0 {
		if !u.repairReadableBinding(ctx, task, version, uploadID, logger, "repair readable binding") {
			return
		}
		sourceCopy := readableCopies[0]
		pieceCID, err = cid.Decode(sourceCopy.PieceCID)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "decode readable piece cid", err)
			return
		}
		pieceCIDString = sourceCopy.PieceCID
		pieces := []storage.PieceInput{{PieceCID: pieceCID}}
		extraData, encodedExtra, err := u.extraDataForCopy(ctx, storageCtx, uploadID, copyIndex, pieces)
		if err != nil {
			u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer presign", err)
			return
		}
		extraHex = encodedExtra
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
	} else {
		rc, _, err := u.cache.Get(ctx, bucket.Name, version.CacheKey)
		if err != nil {
			if os.IsNotExist(err) {
				if version.InCache {
					if markErr := u.repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); markErr != nil {
						logger.Warn("failed to mark cache location absent", "versionID", version.VersionID, "error", markErr)
					}
				}
				u.waitForStorageDependency(ctx, task, logger, "Waiting for a readable replica or retained cache data")
				return
			}
			u.handleTaskFailure(ctx, task, logger, "open retained cache data", err)
			return
		}
		result, storeErr := storageCtx.Store(ctx, rc, &storage.StoreOptions{})
		closeErr := rc.Close()
		if storeErr != nil {
			u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer cache store", storeErr)
			return
		}
		if closeErr != nil {
			u.handleTaskFailure(ctx, task, logger, "close retained cache data", closeErr)
			return
		}
		if result == nil || !result.PieceCID.Defined() {
			u.handleTaskFailure(ctx, task, logger, "peer cache store", errors.New("store returned no piece CID"))
			return
		}
		pieceCID = result.PieceCID
		pieceCIDString = pieceCID.String()
		_, extraHex, err = u.extraDataForCopy(ctx, storageCtx, uploadID, copyIndex, []storage.PieceInput{{PieceCID: pieceCID}})
		if err != nil {
			u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer presign", err)
			return
		}
	}
	if err := u.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     uploadID,
		CopyIndex:    copyIndex,
		PieceCID:     pieceCIDString,
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
	if copyCommitted(copyRow) {
		u.finishPeerCopy(ctx, task, version, uploadID, logger)
		return
	}
	readableCopies, err := u.repos.Uploads.ListReadableCommittedCopies(ctx, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load readable source copy", err)
		return
	}
	pieceCIDString := ""
	if len(readableCopies) > 0 {
		pieceCIDString = readableCopies[0].PieceCID
	} else if copyHasPiece(copyRow) {
		upload, loadErr := u.repos.Uploads.GetByID(ctx, uploadID)
		if loadErr != nil {
			u.handleTaskFailure(ctx, task, logger, "load peer upload piece", loadErr)
			return
		}
		if upload != nil && upload.PieceCID != nil {
			pieceCIDString = *upload.PieceCID
		}
	}
	if pieceCIDString == "" {
		u.handleTaskFailure(ctx, task, logger, "load peer piece", errors.New("peer copy has no persisted piece CID"))
		return
	}
	pieceCID, err := cid.Decode(pieceCIDString)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "decode readable piece cid", err)
		return
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	if copyCommitSubmitted(copyRow) {
		result, err := u.waitForSubmittedCommit(ctx, storageCtx, binding, *copyRow.CommitTransactionID, len(pieces))
		if err != nil {
			if u.waitForPendingSubmittedCommit(ctx, task, logger, err) {
				return
			}
			if errors.Is(err, errCommitRejected) {
				if resetErr := u.resetRejectedSubmittedCommit(ctx, uploadID, copyIndex, *copyRow.CommitTransactionID, err); resetErr != nil {
					u.handleTaskFailure(ctx, task, logger, "reset rejected peer commit", resetErr)
					return
				}
				u.handleTaskFailure(ctx, task, logger, "peer commit", err)
				return
			}
			if dataSetFailureEnded(err, binding) || dataSetFailureUnavailable(err, binding) {
				u.handlePeerDataSetFailure(ctx, task, bucket, uploadID, copyIndex, binding.ID, logger, "peer commit", err)
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
			PieceCID:            pieceCIDString,
			PieceID:             pieceID,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
			CommitExtraDataHex:  derefString(copyRow.CommitExtraDataHex),
			CommitTransactionID: result.TransactionID,
		}); err != nil {
			u.handleTaskFailure(ctx, task, logger, "mark peer committed", err)
			return
		}
		u.finishPeerCopy(ctx, task, version, uploadID, logger)
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
			if errors.Is(err, errCommitRejected) {
				if resetErr := u.resetRejectedSubmittedCommit(ctx, uploadID, copyIndex, submittedTx, err); resetErr != nil {
					u.handleTaskFailure(ctx, task, logger, "reset rejected peer commit", resetErr)
					return
				}
				u.handleTaskFailure(ctx, task, logger, "peer commit", err)
				return
			}
			if dataSetFailureEnded(err, binding) || dataSetFailureUnavailable(err, binding) {
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
		PieceCID:            pieceCIDString,
		PieceID:             pieceID,
		RetrievalURL:        storageCtx.PieceURL(pieceCID),
		CommitExtraDataHex:  extraHex,
		CommitTransactionID: result.TransactionID,
	}); err != nil {
		u.handleTaskFailure(ctx, task, logger, "mark peer committed", err)
		return
	}
	u.finishPeerCopy(ctx, task, version, uploadID, logger)
}

func (u *Uploader) finishPeerCopy(ctx context.Context, task *model.Task, version *model.ObjectVersion, uploadID int64, logger *slog.Logger) {
	if !u.repairReadableBinding(ctx, task, version, uploadID, logger, "repair readable binding") {
		return
	}
	if _, _, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, u.finalizeUploadInput(uploadID)); err != nil {
		u.handleTaskFailure(ctx, task, logger, "finalize upload", err)
		return
	}
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) enqueueUploadStage(ctx context.Context, parent *model.Task, stage string, uploadID int64, copyIndex int, transferMethod model.StorageCopyTransferMethod) error {
	ref := repository.ObjectVersionRef{ObjectID: parent.RefID, VersionID: parent.RefVersionID}
	return u.enqueueUploadStageForVersion(ctx, ref, parent.MaxRetries, stage, uploadID, copyIndex, transferMethod)
}

func (u *Uploader) enqueueUploadStageForVersion(ctx context.Context, ref repository.ObjectVersionRef, maxRetries int, stage string, uploadID int64, copyIndex int, transferMethod model.StorageCopyTransferMethod) error {
	return enqueueUploadStageForVersion(ctx, u.repos, ref, maxRetries, stage, uploadID, copyIndex, transferMethod)
}

func enqueueUploadStageForVersion(ctx context.Context, repos *repository.Repositories, ref repository.ObjectVersionRef, maxRetries int, stage string, uploadID int64, copyIndex int, transferMethod model.StorageCopyTransferMethod) error {
	task := newUploadStageTask(ref, maxRetries, stage, uploadID, copyIndex, transferMethod)
	if err := repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		return err
	}
	return nil
}

func newUploadStageTask(ref repository.ObjectVersionRef, maxRetries int, stage string, uploadID int64, copyIndex int, transferMethod model.StorageCopyTransferMethod) *model.Task {
	payload := map[string]interface{}{
		"upload_id": uploadID,
	}
	key := fmt.Sprintf("upload:%s:%s:%d", ref.VersionID, stage, uploadID)
	if transferMethod != "" {
		payload["copy_index"] = copyIndex
		payload["transfer_method"] = string(transferMethod)
		key = fmt.Sprintf("%s:%d", key, copyIndex)
	}
	return &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          ref.ObjectID,
		RefVersionID:   ref.VersionID,
		IdempotencyKey: key,
		Payload:        payload,
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	}
}

func ensureIngressHandoffTask(ctx context.Context, repos *repository.Repositories, ref repository.ObjectVersionRef, maxRetries int, uploadID int64, copyIndex int) error {
	task := newUploadStageTask(ref, maxRetries, uploadStageEnsureDataSet, uploadID, copyIndex, model.StorageCopyTransferMethodIngress)
	created, err := repos.Tasks.EnsureRecurring(ctx, task)
	if err != nil || created {
		return err
	}
	existing, err := repos.Tasks.GetByIdempotencyKey(ctx, task.IdempotencyKey)
	if err != nil {
		return err
	}
	if existing == nil || existing.RefType != task.RefType || existing.RefID != task.RefID || existing.RefVersionID != task.RefVersionID || uploadTaskStage(existing) != uploadStageEnsureDataSet {
		return fmt.Errorf("ensuring reassigned ingress task: conflicting task identity: %w", repository.ErrConflict)
	}
	existingUploadID, uploadErr := payloadInt64(existing.Payload, "upload_id")
	existingCopyIndex, copyErr := payloadInt64(existing.Payload, "copy_index")
	if uploadErr != nil || copyErr != nil || existingUploadID != uploadID || int(existingCopyIndex) != copyIndex {
		return fmt.Errorf("ensuring reassigned ingress task: conflicting task payload: %w", repository.ErrConflict)
	}
	switch existing.Status {
	case model.TaskStatusQueued, model.TaskStatusScheduled, model.TaskStatusWaiting:
		return nil
	case model.TaskStatusRunning:
		transferMethod, _ := existing.Payload["transfer_method"].(string)
		if transferMethod == string(model.StorageCopyTransferMethodIngress) {
			return nil
		}
		return fmt.Errorf("ensuring reassigned ingress task: running task owns a different transfer method: %w", repository.ErrConflict)
	default:
		return fmt.Errorf("ensuring reassigned ingress task: task status %s: %w", existing.Status, repository.ErrConflict)
	}
}

func reassignIngressCopyAndSchedule(
	ctx context.Context,
	repos *repository.Repositories,
	stateMachine *state.Machine,
	version *model.ObjectVersion,
	uploadID int64,
	unavailableCopyIndex int,
	maxRetries int,
	runningTask *model.Task,
) (*model.StorageUploadCopy, error) {
	if version == nil {
		return nil, fmt.Errorf("reassigning ingress copy: missing object version: %w", repository.ErrInvalidInput)
	}
	var reassigned *model.StorageUploadCopy
	err := repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		wasCommitting := version.State == model.ObjectStateCommitting
		if wasCommitting {
			if err := state.TransitionState(ctx, stateMachine, txRepos.Objects, version.VersionID, model.ObjectStateCommitting, model.ObjectStateUploading); err != nil {
				return fmt.Errorf("lock committing version before ingress reassignment: %w", err)
			}
		}
		selected, err := txRepos.Uploads.ReassignIngressCopy(ctx, uploadID, unavailableCopyIndex)
		if err != nil {
			return err
		}
		if wasCommitting && (selected == nil || copyHasPiece(selected)) {
			if err := state.TransitionState(ctx, stateMachine, txRepos.Objects, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
				return fmt.Errorf("preserve committing state after ingress reassignment: %w", err)
			}
		}
		if selected == nil {
			return nil
		}
		if runningTask != nil {
			if err := txRepos.Tasks.LockRunningClaim(ctx, runningTask); err != nil {
				return err
			}
		}
		ref := repository.ObjectVersionRef{ObjectID: version.ObjectID, VersionID: version.VersionID}
		if err := ensureIngressHandoffTask(ctx, txRepos, ref, maxRetries, uploadID, selected.CopyIndex); err != nil {
			return fmt.Errorf("enqueue reassigned ingress: %w", err)
		}
		if runningTask != nil {
			if err := txRepos.Tasks.Complete(ctx, runningTask); err != nil {
				return err
			}
		}
		reassigned = selected
		return nil
	})
	return reassigned, err
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
	copyRow, copyErr := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
	if copyErr != nil {
		u.handleTaskFailure(ctx, task, logger, "load failed upload copy", copyErr)
		return
	}
	if copyRow == nil {
		u.handleTaskFailure(ctx, task, logger, stage, err)
		return
	}
	if dataSetFailureEnded(err, binding) {
		if dataSetBindingEstablished(binding) {
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusDraining, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark data set draining", markErr)
				return
			}
			binding = latest
		}
		u.handleAssignedDataSetDependency(ctx, task, version, uploadID, copyRow, binding, logger, stage, err)
		return
	}
	if dataSetFailureUnavailable(err, binding) {
		if dataSetBindingEstablished(binding) {
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusUnavailable, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark data set unavailable", markErr)
				return
			}
			binding = latest
		}
		u.handleAssignedDataSetDependency(ctx, task, version, uploadID, copyRow, binding, logger, stage, err)
		return
	}
	if dataSetCreationRejected(err) && binding != nil {
		latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusFailed, err.Error())
		if markErr != nil {
			u.handleTaskFailure(ctx, task, logger, "mark data set creation failed", markErr)
			return
		}
		if latest.Status != model.StorageDataSetStatusFailed {
			if latest.Status == model.StorageDataSetStatusReady || latest.Status == model.StorageDataSetStatusUnavailable || dataSetBindingWriteBlocked(latest) {
				u.waitForStorageDependency(ctx, task, logger, "Waiting for the storage data set state to settle")
				return
			}
			u.handleTaskFailure(ctx, task, logger, "mark data set creation failed", fmt.Errorf("data set status changed to %s", latest.Status))
			return
		}
		binding = latest
		u.handleDataSetCreationFailure(ctx, task, version, uploadID, copyRow, binding, logger, stage, err)
		return
	}
	if copyRow.TransferMethod == model.StorageCopyTransferMethodPeerPull {
		u.markPeerFailed(ctx, task, uploadID, copyIndex, 0, logger, stage, err)
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
		discarded := false
		if discardDataSetID > 0 {
			var discardErr error
			discarded, discardErr = u.repos.Uploads.DiscardFailedDataSetCandidate(cleanupCtx, uploadID, copyIndex, discardDataSetID)
			if discardErr != nil {
				logger.Warn("failed to discard failed dataset candidate", "uploadID", uploadID, "copyIndex", copyIndex, "dataSetID", discardDataSetID, "error", discardErr)
			}
		}
		if discarded {
			if repairErr := u.enqueueRepairUpload(cleanupCtx, task, uploadID); repairErr != nil {
				logger.Warn("failed to enqueue authorized slot retry", "uploadID", uploadID, "copyIndex", copyIndex, "error", repairErr)
			}
		}
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) handlePeerDataSetFailure(ctx context.Context, task *model.Task, bucket *model.Bucket, uploadID int64, copyIndex int, dataSetID int64, logger *slog.Logger, stage string, err error) {
	var binding *model.StorageDataSet
	if dataSetID > 0 {
		var loadErr error
		binding, loadErr = u.repos.Uploads.GetDataSetBindingByID(ctx, dataSetID)
		if loadErr != nil {
			u.handleTaskFailure(ctx, task, logger, "load failed peer data set", loadErr)
			return
		}
	}
	if dataSetFailureEnded(err, binding) || dataSetFailureUnavailable(err, binding) {
		copyRow, loadErr := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
		if loadErr != nil || copyRow == nil {
			if loadErr == nil {
				loadErr = errors.New("peer upload copy not found")
			}
			u.handleTaskFailure(ctx, task, logger, "load failed peer copy", loadErr)
			return
		}
		if dataSetFailureEnded(err, binding) {
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusDraining, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark peer data set draining", markErr)
				return
			}
			binding = latest
		} else {
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusUnavailable, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark peer data set unavailable", markErr)
				return
			}
			binding = latest
		}
		u.handleAssignedDataSetDependency(ctx, task, nil, uploadID, copyRow, binding, logger, stage, err)
		return
	}
	u.markPeerFailed(ctx, task, uploadID, copyIndex, 0, logger, stage, err)
}

func (u *Uploader) handleAssignedDataSetDependency(
	ctx context.Context,
	task *model.Task,
	version *model.ObjectVersion,
	uploadID int64,
	copyRow *model.StorageUploadCopy,
	binding *model.StorageDataSet,
	logger *slog.Logger,
	stage string,
	err error,
) {
	if copyRow == nil {
		u.handleTaskFailure(ctx, task, logger, stage, err)
		return
	}
	if dataSetBindingEstablished(binding) {
		switch binding.Status {
		case model.StorageDataSetStatusReady:
			u.waitForStorageDependency(ctx, task, logger, "Waiting to retry the storage operation")
			return
		case model.StorageDataSetStatusUnavailable, model.StorageDataSetStatusDraining, model.StorageDataSetStatusRetired:
		default:
			u.handleTaskFailure(ctx, task, logger, stage, fmt.Errorf("data set status changed to %s: %w", binding.Status, err))
			return
		}
	}
	if appendErr := u.repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
		UploadID:       uploadID,
		CopyIndex:      copyRow.CopyIndex,
		TransferMethod: string(copyRow.TransferMethod),
		Stage:          stage,
		ErrorMessage:   err.Error(),
	}); appendErr != nil {
		logger.Warn("failed to append storage dependency failure", "uploadID", uploadID, "copyIndex", copyRow.CopyIndex, "error", appendErr)
	}
	if binding != nil && binding.Status == model.StorageDataSetStatusUnavailable {
		if repairErr := u.ensureReplicaRepairTask(ctx, binding, task.MaxRetries); repairErr != nil {
			u.handleTaskFailure(ctx, task, logger, "ensure unavailable replica repair", repairErr)
			return
		}
	}
	if copyRow.TransferMethod == model.StorageCopyTransferMethodPeerPull {
		if binding != nil && (dataSetBindingUnavailable(binding) || dataSetBindingWriteBlocked(binding)) {
			completeWorkerTask(ctx, u.repos, task, "uploader", logger)
			return
		}
		u.waitForStorageDependency(ctx, task, logger, "Waiting for the assigned storage provider to recover")
		return
	}
	if copyCommitSubmitted(copyRow) {
		u.waitForStorageDependency(ctx, task, logger, "Waiting for storage confirmation")
		return
	}
	reassigned, reassignErr := reassignIngressCopyAndSchedule(ctx, u.repos, u.stateMachine, version, uploadID, copyRow.CopyIndex, task.MaxRetries, task)
	if reassignErr != nil {
		u.handleTaskFailure(ctx, task, logger, "reassign ingress copy", reassignErr)
		return
	}
	if reassigned == nil {
		if binding != nil && binding.Status == model.StorageDataSetStatusUnavailable && dataSetBindingEstablished(binding) {
			completeWorkerTask(ctx, u.repos, task, "uploader", logger)
			return
		}
		message := "Waiting for the assigned storage provider to recover"
		if binding != nil && dataSetBindingWriteBlocked(binding) {
			message = "Waiting for the storage service to be replaced"
		}
		u.waitForStorageDependency(ctx, task, logger, message)
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) handleDataSetCreationFailure(
	ctx context.Context,
	task *model.Task,
	version *model.ObjectVersion,
	uploadID int64,
	copyRow *model.StorageUploadCopy,
	binding *model.StorageDataSet,
	logger *slog.Logger,
	stage string,
	err error,
) {
	if appendErr := u.repos.Uploads.AppendUploadFailure(ctx, repository.AppendUploadFailureInput{
		UploadID:       uploadID,
		CopyIndex:      copyRow.CopyIndex,
		TransferMethod: string(copyRow.TransferMethod),
		Stage:          stage,
		ErrorMessage:   err.Error(),
	}); appendErr != nil {
		logger.Warn("failed to append data set creation failure", "uploadID", uploadID, "copyIndex", copyRow.CopyIndex, "error", appendErr)
	}
	logger.Error(stage+" failed", "error", err)
	status := scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
	if status != model.TaskStatusExhausted {
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), terminalFailureCleanupTimeout)
	defer cancel()
	if markErr := u.repos.Uploads.MarkUploadCopyFailed(cleanupCtx, uploadID, copyRow.CopyIndex, fmt.Sprintf("%s: %v", stage, err)); markErr != nil {
		logger.Warn("failed to mark rejected data set copy failed", "uploadID", uploadID, "copyIndex", copyRow.CopyIndex, "error", markErr)
	}
	discarded, discardErr := u.repos.Uploads.DiscardFailedDataSetCandidate(cleanupCtx, uploadID, copyRow.CopyIndex, binding.ID)
	if discardErr != nil {
		logger.Warn("failed to discard rejected data set candidate", "uploadID", uploadID, "copyIndex", copyRow.CopyIndex, "dataSetID", binding.ID, "error", discardErr)
	}
	if discarded {
		if version != nil && version.State == model.ObjectStateCommitting {
			if stateErr := state.TransitionState(cleanupCtx, u.stateMachine, u.repos.Objects, version.VersionID, model.ObjectStateCommitting, model.ObjectStateUploading); stateErr != nil {
				logger.Warn("failed to resume upload after rejected data set", "versionID", version.VersionID, "error", stateErr)
			}
		}
		var enqueueErr error
		if version != nil && version.State == model.ObjectStateReplicating {
			enqueueErr = u.enqueueRepairUpload(cleanupCtx, task, uploadID)
		} else if version != nil {
			enqueueErr = u.enqueuePrepareUpload(cleanupCtx, task, version.VersionID, uploadID)
		}
		if enqueueErr != nil {
			logger.Warn("failed to retry authorized replica slot", "uploadID", uploadID, "copyIndex", copyRow.CopyIndex, "error", enqueueErr)
		}
	} else if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress {
		reassigned, reassignErr := u.repos.Uploads.ReassignIngressCopy(cleanupCtx, uploadID, copyRow.CopyIndex)
		if reassignErr != nil {
			logger.Warn("failed to reassign ingress after data set rejection", "uploadID", uploadID, "copyIndex", copyRow.CopyIndex, "error", reassignErr)
		} else if reassigned != nil {
			if enqueueErr := u.enqueueUploadStage(cleanupCtx, task, uploadStageEnsureDataSet, uploadID, reassigned.CopyIndex, model.StorageCopyTransferMethodIngress); enqueueErr != nil {
				logger.Warn("failed to enqueue ingress after data set rejection", "uploadID", uploadID, "copyIndex", reassigned.CopyIndex, "error", enqueueErr)
			}
		}
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) enqueueRepairUpload(ctx context.Context, parent *model.Task, uploadID int64) error {
	if parent == nil {
		return errors.New("parent task is required for upload repair")
	}
	ref := repository.ObjectVersionRef{ObjectID: parent.RefID, VersionID: parent.RefVersionID}
	return u.enqueueRepairUploadForVersion(ctx, ref, parent.MaxRetries, uploadID)
}

func (u *Uploader) enqueueRepairUploadForVersion(ctx context.Context, ref repository.ObjectVersionRef, maxRetries int, uploadID int64) error {
	stage := uploadStagePrepare
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          ref.ObjectID,
		RefVersionID:   ref.VersionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:%s:%d:repair", ref.VersionID, stage, uploadID),
		Payload:        map[string]interface{}{"upload_id": uploadID},
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	}
	_, err := u.repos.Tasks.EnsureRecurring(ctx, task)
	return err
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

func dataSetWriteBlockedError(err error) bool {
	var blocked *storage.DataSetPDPPaymentTerminatedError
	return errors.As(err, &blocked)
}

func dataSetBindingWriteBlocked(binding *model.StorageDataSet) bool {
	if binding == nil {
		return false
	}
	return binding.Status == model.StorageDataSetStatusDraining || binding.Status == model.StorageDataSetStatusRetired
}

func dataSetBindingUnavailable(binding *model.StorageDataSet) bool {
	return binding != nil && binding.Status == model.StorageDataSetStatusUnavailable
}

func dataSetBindingEstablished(binding *model.StorageDataSet) bool {
	return binding != nil && binding.DataSetID != nil && !binding.DataSetID.IsZero()
}

func dataSetFailureEnded(err error, binding *model.StorageDataSet) bool {
	return dataSetBindingWriteBlocked(binding) || dataSetWriteBlockedError(err) || synapse.IsDataSetServiceEnded(err)
}

func dataSetFailureUnavailable(err error, binding *model.StorageDataSet) bool {
	return dataSetBindingUnavailable(binding) || synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err)
}

func dataSetCreationRejected(err error) bool {
	return errors.Is(err, pdp.ErrTxRejected) || errors.Is(err, errDataSetCreationIncomplete)
}

func (u *Uploader) markDataSetStatus(ctx context.Context, binding *model.StorageDataSet, status model.StorageDataSetStatus, lastError string) (*model.StorageDataSet, error) {
	if binding == nil || binding.ID <= 0 {
		return nil, fmt.Errorf("marking storage data set status: %w", repository.ErrInvalidInput)
	}
	var err error
	switch status {
	case model.StorageDataSetStatusDraining:
		err = u.repos.Uploads.MarkDataSetDraining(ctx, binding.ID, lastError)
	case model.StorageDataSetStatusUnavailable:
		err = u.repos.Uploads.MarkDataSetUnavailable(ctx, binding.ID, lastError)
	case model.StorageDataSetStatusFailed:
		err = u.repos.Uploads.MarkDataSetFailed(ctx, binding.ID, lastError)
	default:
		return nil, fmt.Errorf("marking storage data set status %s: %w", status, repository.ErrInvalidInput)
	}
	if err == nil {
		binding.Status = status
		binding.LastError = &lastError
		return binding, nil
	}
	if !errors.Is(err, repository.ErrConflict) {
		return nil, err
	}
	latest, loadErr := u.repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if loadErr != nil {
		return nil, loadErr
	}
	if latest == nil {
		return nil, fmt.Errorf("loading storage data set %d after status conflict: %w", binding.ID, repository.ErrNotFound)
	}
	return latest, nil
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
		return binding, nil, fmt.Errorf("dataset binding %d is not ready", binding.ID)
	}
	storageCtx, err := u.contextForReadyBinding(ctx, binding, bucket.Name)
	if err != nil {
		return binding, nil, err
	}
	return binding, storageCtx, nil
}

func (u *Uploader) contextForBindingProvider(ctx context.Context, binding *model.StorageDataSet, bucketName string) (synapse.UploadContext, error) {
	storageCtx, err := u.storage.CreateContext(ctx, &storage.CreateContextOptions{
		ProviderID:      sdkBigIntPtr(&binding.ProviderID),
		DataSetMetadata: map[string]string{"bucket": bucketName},
	})
	if err != nil {
		return nil, err
	}
	if storageCtx == nil {
		return nil, errors.New("storage context resolver returned no context")
	}
	return storageCtx, nil
}

func (u *Uploader) contextForReadyBinding(ctx context.Context, binding *model.StorageDataSet, bucketName string) (synapse.UploadContext, error) {
	storageCtx, err := u.storage.CreateContext(ctx, &storage.CreateContextOptions{
		ProviderID:      sdkBigIntPtr(&binding.ProviderID),
		DataSetID:       sdkBigIntPtr(binding.DataSetID),
		DataSetMetadata: map[string]string{"bucket": bucketName},
	})
	if err != nil {
		return nil, err
	}
	if storageCtx == nil {
		return nil, errors.New("storage context resolver returned no context")
	}
	if got := idtypes.OnChainIDFromSDK(storageCtx.ProviderID()); !got.Equal(binding.ProviderID) {
		return nil, fmt.Errorf("dataset %s resolved provider %s, want %s", binding.DataSetID, got.String(), binding.ProviderID.String())
	}
	resolvedDataSetID := storageCtx.DataSetID()
	if resolvedDataSetID == nil || !idtypes.OnChainIDFromSDK(*resolvedDataSetID).Equal(*binding.DataSetID) {
		return nil, fmt.Errorf("storage context did not resolve requested data set %s", binding.DataSetID.String())
	}
	return storageCtx, nil
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

func (u *Uploader) waitForSubmittedCommit(ctx context.Context, storageCtx synapse.UploadContext, binding *model.StorageDataSet, txHash string, pieceCount int) (*storage.CommitResult, error) {
	if binding == nil || binding.DataSetID == nil || binding.DataSetID.IsZero() {
		return nil, errors.New("commit dataset binding is not ready")
	}
	if submittedCommitMaxWait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(
			ctx,
			submittedCommitMaxWait,
			fmt.Errorf("%w: commit %s", errSubmittedCommitPending, txHash),
		)
		defer cancel()
	}
	checker := u.statusChecker
	if checker == nil {
		checker = synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{Timeout: submittedCommitRequestTimeout})
	}
	for {
		status, err := checker.GetAddPiecesStatus(ctx, synapse.AddPiecesStatusInput{
			ServiceURL:         storageCtx.ServiceURL(),
			DataSetID:          binding.DataSetID.String(),
			TransactionID:      txHash,
			ExpectedPieceCount: pieceCount,
		})
		if err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			return nil, synapse.NormalizeProviderOperationError(ctx, err)
		}
		switch status.State {
		case synapse.PDPStatusConfirmed:
			pieceIDs := make([]sdktypes.BigInt, 0, len(status.ConfirmedPieceIDs))
			for _, raw := range status.ConfirmedPieceIDs {
				pieceID, err := idtypes.ParseOnChainID("confirmed piece ID", raw)
				if err != nil {
					return nil, err
				}
				pieceIDs = append(pieceIDs, pieceID.SDK())
			}
			return &storage.CommitResult{
				TransactionID: txHash,
				DataSetID:     binding.DataSetID.SDK(),
				PieceIDs:      pieceIDs,
				IsNewDataSet:  false,
			}, nil
		case synapse.PDPStatusRejected:
			return nil, fmt.Errorf("%w: %s", errCommitRejected, txHash)
		case synapse.PDPStatusMismatch:
			if status.TxStatus == "confirmed" && !status.PiecesAdded {
				return nil, fmt.Errorf("%w: commit %s confirmed without adding pieces", errCommitRejected, txHash)
			}
			return nil, fmt.Errorf("commit status did not match submission %s", txHash)
		case synapse.PDPStatusUnavailable:
			return nil, &synapse.ProviderUnavailableError{Cause: errors.New("commit status unavailable")}
		case synapse.PDPStatusUnknown:
			return nil, fmt.Errorf("commit status %q for %s", status.TxStatus, txHash)
		case synapse.PDPStatusPending:
		default:
			return nil, fmt.Errorf("unexpected commit status state %q for %s", status.State, txHash)
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(submittedCommitPollInterval):
		}
	}
}

func (u *Uploader) waitForPendingSubmittedCommit(ctx context.Context, task *model.Task, logger *slog.Logger, err error) bool {
	if !errors.Is(err, errSubmittedCommitPending) {
		return false
	}
	u.waitForStorageDependency(ctx, task, logger, "Waiting for storage confirmation")
	return true
}

func (u *Uploader) resetRejectedSubmittedCommit(
	ctx context.Context,
	uploadID int64,
	copyIndex int,
	transactionID string,
	commitErr error,
) error {
	return u.repos.Uploads.ResetRejectedUploadCopyCommit(ctx, repository.ResetRejectedUploadCopyCommitInput{
		UploadID:            uploadID,
		CopyIndex:           copyIndex,
		CommitTransactionID: transactionID,
		LastError:           commitErr.Error(),
	})
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

func (u *Uploader) submittedCommitPresence(ctx context.Context, uploadID int64, copyIndex int) (bool, bool, error) {
	copies, err := u.repos.Uploads.ListCopies(ctx, uploadID)
	if err != nil {
		return false, false, err
	}
	currentSubmitted := false
	otherSubmitted := false
	for i := range copies {
		if !copyCommitSubmitted(&copies[i]) {
			continue
		}
		if copies[i].CopyIndex == copyIndex {
			currentSubmitted = true
		} else {
			otherSubmitted = true
		}
	}
	return currentSubmitted, otherSubmitted, nil
}

func dataSetBindingCanEnsureWrite(binding *model.StorageDataSet) bool {
	return binding != nil && dataSetStatusCanEnsureWrite(binding.Status)
}

func uploadCanUseDataSetBinding(uploadID int64, binding *model.StorageDataSet) bool {
	if binding == nil {
		return false
	}
	if binding.Status == model.StorageDataSetStatusReady {
		return true
	}
	if binding.Status == model.StorageDataSetStatusFailed && dataSetBindingHasCreationEvidence(binding) {
		return false
	}
	return binding.CreatedByUploadID != nil &&
		*binding.CreatedByUploadID == uploadID &&
		dataSetBindingCanEnsureWrite(binding)
}

func uploadTracksDataSetBinding(uploadID int64, binding *model.StorageDataSet) bool {
	if binding == nil {
		return false
	}
	if dataSetBindingHasCreationEvidence(binding) {
		return true
	}
	return binding.CreatedByUploadID != nil && *binding.CreatedByUploadID == uploadID
}

func dataSetBindingHasCreationEvidence(binding *model.StorageDataSet) bool {
	if binding == nil {
		return false
	}
	return dataSetBindingEstablished(binding) ||
		(binding.ClientDataSetID != nil && !binding.ClientDataSetID.IsZero()) ||
		(binding.CreateTransactionID != nil && *binding.CreateTransactionID != "") ||
		(binding.CreateStatusURL != nil && *binding.CreateStatusURL != "")
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
	currentSubmitted, otherSubmitted, checkErr := u.submittedCommitPresence(ctx, uploadID, copyIndex)
	if checkErr != nil {
		u.handleTaskFailure(ctx, task, logger, "check submitted ingress commits", checkErr)
		return
	}
	if currentSubmitted || otherSubmitted {
		logger.Error(stage+" failed while a commit remains submitted", "error", err)
		status := scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
		if status == model.TaskStatusExhausted && !currentSubmitted {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), terminalFailureCleanupTimeout)
			defer cancel()
			if markErr := u.repos.Uploads.MarkUploadCopyFailed(cleanupCtx, uploadID, copyIndex, fmt.Sprintf("%s: %v", stage, err)); markErr != nil {
				logger.Warn("failed to mark alternate ingress upload copy failed", "uploadID", uploadID, "copyIndex", copyIndex, "error", markErr)
			}
		}
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
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
	var binding *model.StorageDataSet
	if dataSetID > 0 {
		var loadErr error
		binding, loadErr = u.repos.Uploads.GetDataSetBindingByID(ctx, dataSetID)
		if loadErr != nil {
			u.handleTaskFailure(ctx, task, logger, "load failed ingress data set", loadErr)
			return
		}
	}
	if dataSetFailureEnded(err, binding) || dataSetFailureUnavailable(err, binding) {
		copyRow, loadErr := u.repos.Uploads.GetUploadCopy(ctx, uploadID, copyIndex)
		if loadErr != nil || copyRow == nil {
			if loadErr == nil {
				loadErr = errors.New("ingress upload copy not found")
			}
			u.handleTaskFailure(ctx, task, logger, "load failed ingress copy", loadErr)
			return
		}
		if dataSetFailureEnded(err, binding) {
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusDraining, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark ingress data set draining", markErr)
				return
			}
			binding = latest
		} else {
			latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusUnavailable, err.Error())
			if markErr != nil {
				u.handleTaskFailure(ctx, task, logger, "mark ingress data set unavailable", markErr)
				return
			}
			binding = latest
		}
		u.handleAssignedDataSetDependency(ctx, task, version, uploadID, copyRow, binding, logger, stage, err)
		return
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
