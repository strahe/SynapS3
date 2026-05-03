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
	recoveryStagePrepare         = "prepare_upload"
	recoveryStageEnsureDataSet   = "ensure_dataset"
	recoveryStagePrimaryStore    = "primary_store"
	recoveryStagePrimaryCommit   = "primary_commit"
	recoveryStageSecondaryPull   = "secondary_pull"
	recoveryStageSecondaryCommit = "secondary_commit"
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
	// Release expired task leases
	released, err := m.repos.Tasks.ReleaseExpiredLeases(ctx)
	if err != nil {
		m.logger.Error("failed to release expired leases", "error", err)
	} else if released > 0 {
		m.logger.Info("released expired task leases", "count", released)
	}

	// Reconcile: ensure objects in cached/stored have corresponding tasks.
	m.reconcileTasks(ctx, model.ObjectStateCached, model.TaskTypeUpload, "upload")
	m.reconcileStagedUploads(ctx)
	if m.autoEvict {
		m.reconcileTasks(ctx, model.ObjectStateStored, model.TaskTypeEvictCache, "evict_cache")
		m.reconcileTasks(ctx, model.ObjectStateReplicating, model.TaskTypeEvictCache, "evict_cache")
	}

	// Log dead-letter task count for operator awareness
	deadLetters, err := m.repos.Tasks.ListDeadLetters(ctx, 100)
	if err != nil {
		m.logger.Error("failed to check dead-letter tasks", "error", err)
	} else if len(deadLetters) > 0 {
		m.logger.Warn("dead-letter tasks found on startup, review via GET /admin/dead-letters", "count", len(deadLetters))
	}
}

// reconcileTasks finds object versions in the given state and ensures each has a corresponding
// pending task. Uses idempotency keys to safely skip objects that already have tasks.
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
				Status:         model.TaskStatusPending,
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
				m.reconcilePrimaryUpload(ctx, version, upload)
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
		m.enqueueRecoveredUploadStage(ctx, version, 0, recoveryStagePrepare, 0, false)
	case model.ObjectStateCommitting:
		if err := m.repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCommitting, model.ObjectStateUploading); err != nil {
			m.logger.Error("failed to reset orphan committing version", "versionID", version.VersionID, "error", err)
			return
		}
		version.State = model.ObjectStateUploading
		m.enqueueRecoveredUploadStage(ctx, version, 0, recoveryStagePrepare, 0, false)
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
		model.StorageUploadStatusStoredOnPrimary,
		model.StorageUploadStatusPrimaryCommitted,
		model.StorageUploadStatusPartial,
		model.StorageUploadStatusAllCopiesCommitted:
		return true
	default:
		return false
	}
}

func (m *Manager) reconcilePrimaryUpload(ctx context.Context, version model.ObjectVersion, upload *model.StorageUpload) {
	copies, err := m.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		m.logger.Error("failed to list primary upload copies for reconciliation", "uploadID", upload.ID, "error", err)
		return
	}
	if len(copies) == 0 {
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStagePrepare, 0, false)
		return
	}
	var primary *model.StorageUploadCopy
	for i := range copies {
		if copies[i].CopyIndex == 0 {
			primary = &copies[i]
			break
		}
	}
	binding, err := m.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, version.BucketID, 0)
	if err != nil {
		m.logger.Error("failed to load primary dataset binding for reconciliation", "uploadID", upload.ID, "error", err)
		return
	}
	if primary == nil || binding == nil || binding.Status != model.StorageDataSetStatusReady {
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStageEnsureDataSet, 0, true)
		return
	}
	if version.State == model.ObjectStateCommitting && copyHasPiece(primary) {
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStagePrimaryCommit, 0, false)
		return
	}
	m.enqueueRecoveredUploadStage(ctx, version, upload.ID, recoveryStagePrimaryStore, 0, false)
}

func (m *Manager) reconcileReplicatingUpload(ctx context.Context, version model.ObjectVersion, upload *model.StorageUpload) {
	copies, err := m.repos.Uploads.ListCopies(ctx, upload.ID)
	if err != nil {
		m.logger.Error("failed to list secondary upload copies for reconciliation", "uploadID", upload.ID, "error", err)
		return
	}
	if len(copies) == 0 {
		return
	}
	allCommitted := true
	for i := range copies {
		if !copyCommitted(&copies[i]) {
			allCommitted = false
			break
		}
	}
	if allCommitted {
		finalized, refs, err := m.repos.Uploads.FinalizeUploadIfAllCopiesCommitted(ctx, repository.FinalizeUploadInput{UploadID: upload.ID})
		if err != nil {
			m.logger.Error("failed to finalize recovered upload", "uploadID", upload.ID, "error", err)
			return
		}
		if finalized {
			m.enqueueRecoveredEvictTasks(ctx, refs)
		}
		return
	}
	for i := range copies {
		copyRow := &copies[i]
		if copyRow.CopyIndex == 0 || copyCommitted(copyRow) {
			continue
		}
		stage := recoveryStageEnsureDataSet
		binding, err := m.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, version.BucketID, copyRow.CopyIndex)
		if err != nil {
			m.logger.Error("failed to load secondary dataset binding for reconciliation", "uploadID", upload.ID, "copyIndex", copyRow.CopyIndex, "error", err)
			continue
		}
		if binding != nil && binding.Status == model.StorageDataSetStatusReady {
			stage = recoveryStageSecondaryPull
			if copyHasPiece(copyRow) {
				stage = recoveryStageSecondaryCommit
			}
		}
		m.enqueueRecoveredUploadStage(ctx, version, upload.ID, stage, copyRow.CopyIndex, true)
	}
}

func (m *Manager) enqueueRecoveredUploadStage(ctx context.Context, version model.ObjectVersion, uploadID int64, stage string, copyIndex int, includeCopyIndex bool) {
	payload := map[string]interface{}{"upload_id": uploadID}
	key := fmt.Sprintf("upload:%s:%s:%d", version.VersionID, stage, uploadID)
	if stage == recoveryStagePrepare {
		payload = nil
		key = fmt.Sprintf("upload:%s", version.VersionID)
	}
	if includeCopyIndex {
		payload["copy_index"] = copyIndex
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
		Status:         model.TaskStatusPending,
		MaxRetries:     m.uploadMaxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := m.repos.Tasks.Create(ctx, task); err != nil && !errors.Is(err, repository.ErrAlreadyExists) {
		m.logger.Error("failed to enqueue recovered upload stage", "stage", stage, "uploadID", uploadID, "versionID", version.VersionID, "error", err)
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
			Status:         model.TaskStatusPending,
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

// pollLoop is a helper that calls fn on a fixed interval until ctx is cancelled.
func pollLoop(ctx context.Context, interval time.Duration, fn func(ctx context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := fn(ctx); err != nil {
				slog.Error("poll iteration failed", "error", err)
			}
		}
	}
}
