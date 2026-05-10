package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

// Worker defines a background processing unit.
type Worker interface {
	// Name returns a human-readable identifier.
	Name() string
	// Run starts the worker loop; it should block until ctx is cancelled.
	Run(ctx context.Context) error
	// Healthy returns true if the worker has ticked recently.
	Healthy() bool
}

// Manager coordinates the lifecycle of all background workers.
type Manager struct {
	repos            *repository.Repositories
	workers          []Worker
	logger           *slog.Logger
	autoEvict        bool
	uploadMaxRetries int
	evictMaxRetries  int
}

const (
	defaultUploadMaxRetries = 5
	reconcileBatchSize      = 100
)

const (
	recoveryStagePrepare       = "prepare_upload"
	recoveryStageEnsureDataSet = "ensure_dataset"
	recoveryStageIngressStore  = "ingress_store"
	recoveryStageIngressCommit = "ingress_commit"
	recoveryStagePeerPull      = "peer_pull"
	recoveryStagePeerCommit    = "peer_commit"
)

// NewManager creates a new worker manager.
func NewManager(repos *repository.Repositories, logger *slog.Logger, autoEvict bool, workers ...Worker) *Manager {
	return &Manager{
		repos:            repos,
		workers:          workers,
		logger:           logger,
		autoEvict:        autoEvict,
		uploadMaxRetries: defaultUploadMaxRetries,
		evictMaxRetries:  defaultEvictMaxRetries,
	}
}

// WithTaskMaxRetries configures max retries for tasks recreated during startup reconciliation.
func (m *Manager) WithTaskMaxRetries(uploadMaxRetries, evictMaxRetries int) *Manager {
	m.uploadMaxRetries = uploadMaxRetries
	m.evictMaxRetries = evictMaxRetries
	return m
}

// Start launches all registered workers and blocks until ctx is cancelled.
// It performs startup recovery before launching workers.
func (m *Manager) Start(ctx context.Context) {
	m.recoverOnStartup(ctx)

	var wg sync.WaitGroup

	for _, w := range m.workers {
		wg.Add(1)
		go func(w Worker) {
			defer wg.Done()
			m.logger.Info("starting worker", "worker", w.Name())
			if err := w.Run(ctx); err != nil {
				m.logger.Error("worker exited with error", "worker", w.Name(), "error", err)
			} else {
				m.logger.Info("worker stopped", "worker", w.Name())
			}
		}(w)
	}

	wg.Wait()
}

// recoverOnStartup releases expired task leases and resets objects stuck
// in intermediate states from a previous crash.
func (m *Manager) recoverOnStartup(ctx context.Context) {
	released, err := m.repos.Tasks.ReleaseExpiredLeases(ctx)
	if err != nil {
		m.logger.Error("failed to release expired task leases", "error", err)
	} else if released > 0 {
		m.logger.Info("released expired task leases", "count", released)
	}

	// Reconcile: ensure objects in cached/stored have corresponding tasks.
	m.reconcileTasks(ctx, model.ObjectStateCached, model.TaskTypeUpload, "upload")
	m.reconcileStagedUploads(ctx)
	if m.autoEvict {
		m.reconcileTasks(ctx, model.ObjectStateStored, model.TaskTypeEvictCache, "evict_cache")
	}

	// Log exhausted task count for operator awareness
	exhaustedTasks, err := m.repos.Tasks.ListExhausted(ctx, 100)
	if err != nil {
		m.logger.Error("failed to check exhausted tasks", "error", err)
	} else if len(exhaustedTasks) > 0 {
		m.logger.Warn("exhausted tasks found on startup, review via GET /admin/exhausted-tasks", "count", len(exhaustedTasks))
	}
}

// reconcileTasks finds object versions in the given state and ensures each has a corresponding
// queued task. Uses idempotency keys to safely skip objects that already have tasks.
// keyPrefix must match the prefix used by the normal task creation path for deduplication.
func (m *Manager) reconcileTasks(ctx context.Context, objState model.ObjectState, taskType model.TaskType, keyPrefix string) {
	created := 0
	var cursor versionStateCursor
	for {
		versions, nextCursor, err := m.listVersionStateBatch(ctx, objState, cursor)
		if err != nil {
			return
		}
		if len(versions) == 0 {
			break
		}
		for _, version := range versions {
			stage := taskStageForReconcile(taskType)
			task := &model.Task{
				Type:           taskType,
				Stage:          stage,
				RefType:        "object",
				RefID:          version.ObjectID,
				RefVersionID:   version.VersionID,
				IdempotencyKey: fmt.Sprintf("%s:%s", keyPrefix, version.VersionID),
				Status:         model.TaskStatusQueued,
				MaxRetries:     m.maxRetriesForTaskType(taskType),
				ScheduledAt:    time.Now(),
			}
			if err := m.repos.Tasks.Create(ctx, task); err != nil {
				// Idempotency key collision means task already exists — skip
				continue
			}
			created++
		}
		if len(versions) < reconcileBatchSize {
			break
		}
		cursor = nextCursor
	}
	if created > 0 {
		m.logger.Info("reconciled missing tasks", "state", objState, "type", taskType, "created", created)
	}
}

type versionStateCursor struct {
	updatedAt time.Time
	versionID string
}

func (m *Manager) listVersionStateBatch(ctx context.Context, objState model.ObjectState, cursor versionStateCursor) ([]model.ObjectVersion, versionStateCursor, error) {
	versions, err := m.repos.Objects.ListVersionsByStateAfter(ctx, objState, cursor.updatedAt, cursor.versionID, reconcileBatchSize)
	if err != nil {
		m.logger.Error("failed to list object versions for reconciliation", "state", objState, "error", err)
		return nil, cursor, err
	}
	if len(versions) == 0 {
		return nil, cursor, nil
	}
	last := versions[len(versions)-1]
	return versions, versionStateCursor{updatedAt: last.UpdatedAt, versionID: last.VersionID}, nil
}

func taskStageForReconcile(taskType model.TaskType) *string {
	if taskType != model.TaskTypeUpload {
		return nil
	}
	stage := recoveryStagePrepare
	return &stage
}

func (m *Manager) reconcileStagedUploads(ctx context.Context) {
	for _, objState := range []model.ObjectState{
		model.ObjectStateUploading,
		model.ObjectStateCommitting,
		model.ObjectStateReplicating,
	} {
		var cursor versionStateCursor
		for {
			versions, nextCursor, err := m.listVersionStateBatch(ctx, objState, cursor)
			if err != nil {
				break
			}
			if len(versions) == 0 {
				break
			}
			for _, version := range versions {
				upload, err := m.recoverableUploadForVersion(ctx, version)
				if err != nil {
					m.logger.Error("failed to load staged upload for reconciliation", "versionID", version.VersionID, "error", err)
					continue
				}
				if upload == nil {
					m.reconcileOrphanStagedVersion(ctx, version)
					continue
				}
				if version.State == model.ObjectStateReplicating {
					m.reconcileReplicatingUpload(ctx, version, upload)
					continue
				}
				m.reconcileIngressUpload(ctx, version, upload)
			}
			if len(versions) < reconcileBatchSize {
				break
			}
			cursor = nextCursor
		}
	}
}

func (m *Manager) reconcileOrphanStagedVersion(ctx context.Context, version model.ObjectVersion) {
	switch version.State {
	case model.ObjectStateUploading:
		m.enqueueRecoveredUploadStage(ctx, version, 0, recoveryStagePrepare, 0, "")
	case model.ObjectStateCommitting:
		if err := m.repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCommitting, model.ObjectStateUploading); err != nil {
			m.logger.Error("failed to reset orphan committing version", "versionID", version.VersionID, "error", err)
			return
		}
		version.State = model.ObjectStateUploading
		m.enqueueRecoveredUploadStage(ctx, version, 0, recoveryStagePrepare, 0, "")
	case model.ObjectStateReplicating:
		m.logger.Warn("replicating version has no recoverable storage upload", "versionID", version.VersionID)
	}
}

func (m *Manager) recoverableUploadForVersion(ctx context.Context, version model.ObjectVersion) (*model.StorageUpload, error) {
	if version.StorageUploadID != nil {
		upload, err := m.repos.Uploads.GetByID(ctx, *version.StorageUploadID)
		if err != nil || upload == nil {
			return upload, err
		}
		if isRecoverableUploadStatus(upload.Status) {
			return upload, nil
		}
		return nil, nil
	}
	return m.repos.Uploads.FindActiveUploadBySourceVersion(ctx, version.VersionID)
}

func isRecoverableUploadStatus(status model.StorageUploadStatus) bool {
	switch status {
	case model.StorageUploadStatusRunning,
		model.StorageUploadStatusIngressReady,
		model.StorageUploadStatusReadable,
		model.StorageUploadStatusComplete:
		return true
	default:
		return false
	}
}

func (m *Manager) reconcileIngressUpload(ctx context.Context, version model.ObjectVersion, upload *model.StorageUpload) {
	copies, err := m.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		m.logger.Error("failed to list ingress upload copies for reconciliation", "uploadID", upload.ID, "error", err)
		return
	}
	if len(copies) == 0 {
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStagePrepare, 0, "")
		return
	}
	var ingress *model.StorageUploadCopy
	for i := range copies {
		if copies[i].TransferMethod == model.StorageCopyTransferMethodIngress && !copyCommitted(&copies[i]) {
			ingress = &copies[i]
			break
		}
	}
	if ingress == nil {
		m.reconcileReplicatingUpload(ctx, version, upload)
		return
	}
	binding, err := m.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, version.BucketID, ingress.CopyIndex)
	if err != nil {
		m.logger.Error("failed to load ingress dataset binding for reconciliation", "uploadID", upload.ID, "copyIndex", ingress.CopyIndex, "error", err)
		return
	}
	if binding == nil || binding.Status != model.StorageDataSetStatusReady {
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStageEnsureDataSet, ingress.CopyIndex, ingress.TransferMethod)
		return
	}
	if version.State == model.ObjectStateCommitting && copyHasPiece(ingress) {
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStageIngressCommit, ingress.CopyIndex, ingress.TransferMethod)
		return
	}
	m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStageIngressStore, ingress.CopyIndex, ingress.TransferMethod)
}

func (m *Manager) reconcileReplicatingUpload(ctx context.Context, version model.ObjectVersion, upload *model.StorageUpload) {
	copies, err := m.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		m.logger.Error("failed to list peer upload copies for reconciliation", "uploadID", upload.ID, "error", err)
		return
	}
	if len(copies) == 0 {
		return
	}
	readableCopies, err := m.repos.Uploads.ListReadableCommittedCopies(ctx, upload.ID)
	if err != nil {
		m.logger.Error("failed to list readable upload copies for reconciliation", "uploadID", upload.ID, "error", err)
		return
	}
	if len(readableCopies) > 0 && !versionBoundToUpload(version, upload.ID) {
		_, err := m.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
			UploadID:    upload.ID,
			BucketID:    version.BucketID,
			ContentSize: version.Size,
			Checksum:    version.Checksum,
		})
		if err != nil {
			m.logger.Error("failed to bind recovered readable upload", "uploadID", upload.ID, "versionID", version.VersionID, "error", err)
			return
		}
		version.State = model.ObjectStateReplicating
		version.StorageUploadID = &upload.ID
	}
	finalized, refs, err := m.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
	if err != nil {
		m.logger.Error("failed to finalize recovered upload", "uploadID", upload.ID, "error", err)
		return
	}
	if finalized {
		m.enqueueRecoveredEvictTasks(ctx, refs)
		return
	}
	recoverablePeerCount := 0
	for i := range copies {
		copyRow := &copies[i]
		if copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull || copyCommitted(copyRow) || copyRow.Status == model.StorageUploadCopyStatusFailed {
			continue
		}
		stage := recoveryStageEnsureDataSet
		binding, err := m.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, version.BucketID, copyRow.CopyIndex)
		if err != nil {
			m.logger.Error("failed to load peer dataset binding for reconciliation", "uploadID", upload.ID, "copyIndex", copyRow.CopyIndex, "error", err)
			continue
		}
		if !dataSetBindingCanEnsureWrite(binding) {
			continue
		}
		recoverablePeerCount++
		if binding != nil && binding.Status == model.StorageDataSetStatusReady {
			stage = recoveryStagePeerPull
			if copyHasPiece(copyRow) {
				stage = recoveryStagePeerCommit
			}
		}
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, stage, copyRow.CopyIndex, copyRow.TransferMethod)
	}
	if upload.RequestedCopies > len(readableCopies)+recoverablePeerCount && len(readableCopies) > 0 {
		m.enqueueRecoveredUploadRepair(ctx, version, upload.ID)
	}
}

func versionBoundToUpload(version model.ObjectVersion, uploadID int64) bool {
	return version.State == model.ObjectStateReplicating && version.StorageUploadID != nil && *version.StorageUploadID == uploadID
}

func (m *Manager) enqueueRecoveredUploadStage(ctx context.Context, version model.ObjectVersion, uploadID int64, stage string, copyIndex int, transferMethod model.StorageCopyTransferMethod) {
	payload := map[string]interface{}{"upload_id": uploadID}
	key := fmt.Sprintf("upload:%s:%s:%d", version.VersionID, stage, uploadID)
	if stage == recoveryStagePrepare {
		payload = nil
		key = fmt.Sprintf("upload:%s", version.VersionID)
	}
	if transferMethod != "" {
		payload["copy_index"] = copyIndex
		payload["transfer_method"] = string(transferMethod)
		key = fmt.Sprintf("%s:%d", key, copyIndex)
	}
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          version.ObjectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: key,
		Payload:        payload,
		Status:         model.TaskStatusQueued,
		MaxRetries:     m.uploadMaxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := m.repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		m.logger.Error("failed to enqueue recovered upload stage", "stage", stage, "uploadID", uploadID, "versionID", version.VersionID, "error", err)
	}
}

func (m *Manager) enqueueRecoveredUploadRepair(ctx context.Context, version model.ObjectVersion, uploadID int64) {
	stage := recoveryStagePrepare
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          version.ObjectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:%s:%d:repair", version.VersionID, stage, uploadID),
		Payload:        map[string]interface{}{"upload_id": uploadID},
		Status:         model.TaskStatusQueued,
		MaxRetries:     m.uploadMaxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := m.repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		m.logger.Error("failed to enqueue recovered upload repair", "uploadID", uploadID, "versionID", version.VersionID, "error", err)
	}
}

func (m *Manager) enqueueRecoveredEvictTasks(ctx context.Context, refs []repository.ObjectVersionRef) {
	if !m.autoEvict {
		return
	}
	for _, ref := range refs {
		task := &model.Task{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          ref.ObjectID,
			RefVersionID:   ref.VersionID,
			IdempotencyKey: fmt.Sprintf("evict_cache:%s", ref.VersionID),
			Status:         model.TaskStatusQueued,
			MaxRetries:     m.evictMaxRetries,
			ScheduledAt:    time.Now(),
		}
		if err := m.repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
			m.logger.Error("failed to enqueue recovered eviction task", "versionID", ref.VersionID, "error", err)
		}
	}
}

func (m *Manager) maxRetriesForTaskType(taskType model.TaskType) int {
	if taskType == model.TaskTypeEvictCache {
		return m.evictMaxRetries
	}
	return m.uploadMaxRetries
}

// WorkerHealth returns a map of worker name → healthy status.
func (m *Manager) WorkerHealth() map[string]bool {
	health := make(map[string]bool, len(m.workers))
	for _, w := range m.workers {
		health[w.Name()] = w.Healthy()
	}
	return health
}
