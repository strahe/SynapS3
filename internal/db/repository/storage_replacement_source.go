package repository

import (
	"context"
	"errors"
	"strconv"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

// SourceEligibility is shared by presentation, preflight, and authorization.
// The bool distinguishes a local handoff from replacement of a remote service.
func (r *BunStorageReplacementRepo) SourceEligibility(ctx context.Context, dataSetID int64) (*model.StorageDataSet, bool, error) {
	source, err := (&BunStorageContentRepo{db: r.db}).GetDataSetBindingByID(ctx, dataSetID)
	if err != nil {
		return nil, false, err
	}
	if source == nil {
		return nil, false, ErrNotFound
	}
	local, err := replacementSourceEligibility(ctx, r.db, source)
	return source, local, err
}

func replacementSourceEligibility(ctx context.Context, db bun.IDB, source *model.StorageDataSet) (bool, error) {
	if !source.IsCurrent {
		return false, storagereplacement.ErrSourceNotCurrent
	}
	initializing := source.Status != model.StorageDataSetStatusReady
	var outgoing []storagereplacement.Replacement
	if err := db.NewSelect().Model(&outgoing).Where("source_data_set_id = ?", source.ID).
		Where("status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).Scan(ctx); err != nil {
		return false, err
	}
	if initializing {
		if len(outgoing) == 0 {
			if err := failedInitializationOwner(ctx, db, source); err != nil {
				return false, err
			}
		} else if _, err := replaceableEarlierTargets(ctx, db, source.ID); err != nil {
			return false, err
		}
	} else {
		for _, row := range outgoing {
			if row.Status != storagereplacement.StatusFailed && row.Status != storagereplacement.StatusCleanupAttention {
				return false, storagereplacement.ErrActiveReplacement
			}
		}
	}
	var incoming []storagereplacement.Replacement
	if err := db.NewSelect().Model(&incoming).Where("target_data_set_id = ?", source.ID).
		Where("status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).Scan(ctx); err != nil {
		return false, err
	}
	if len(incoming) != 0 {
		return false, storagereplacement.ErrActiveReplacement
	}
	if !initializing {
		if _, err := replaceableEarlierTargets(ctx, db, source.ID); err != nil {
			return false, err
		}
	}
	return source.DataSetID == nil, nil
}

func replaceableEarlierTargets(ctx context.Context, db bun.IDB, sourceDataSetID int64) ([]model.StorageDataSet, error) {
	source, err := (&BunStorageContentRepo{db: db}).GetDataSetBindingByID(ctx, sourceDataSetID)
	if err != nil || source == nil {
		return nil, errors.Join(err, ErrNotFound)
	}
	initializing := source.Status != model.StorageDataSetStatusReady
	var targets []model.StorageDataSet
	query := db.NewSelect().Model(&targets).
		Where(`id IN (SELECT target_data_set_id FROM storage_replacements WHERE source_data_set_id = ? AND status NOT IN (?, ?))`,
			sourceDataSetID, storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		Where("status <> ?", model.StorageDataSetStatusRetired)
	if !initializing {
		query = query.Where("data_set_id IS NULL")
	}
	if err := query.Scan(ctx); err != nil {
		return nil, err
	}
	if len(targets) == 0 && initializing {
		return nil, storagereplacement.ErrTargetCreating
	}
	for i := range targets {
		if initializing && targets[i].Status == model.StorageDataSetStatusReady {
			return nil, storagereplacement.ErrActiveReplacement
		}
		check := failedInitializationOwner
		if !initializing {
			check = remoteReplacementTargetEligibility
		}
		if err := check(ctx, db, &targets[i]); err != nil {
			if errors.Is(err, storagereplacement.ErrSourceRunning) || errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
				return nil, storagereplacement.ErrTargetCreating
			}
			return nil, err
		}
	}
	return targets, nil
}

func remoteReplacementTargetEligibility(ctx context.Context, db bun.IDB, binding *model.StorageDataSet) error {
	if binding.EnsureTaskID != nil {
		task, err := (&BunTaskRepo{db: db}).GetByID(ctx, *binding.EnsureTaskID)
		if err != nil {
			return err
		}
		if task == nil {
			return ErrNotFound
		}
		if task.Status == model.TaskStatusPending || task.Status == model.TaskStatusRunning {
			return storagereplacement.ErrSourceRunning
		}
	}
	if binding.CreateTransactionID != nil || binding.CreateStatusURL != nil {
		return &storagereplacement.SourceOutcomeError{Message: "Storage setup has a submitted transaction. Wait for its confirmation before replacing the provider."}
	}
	status := binding.Status
	if status == model.StorageDataSetStatusFailed && binding.ClientDataSetID == nil {
		status = model.StorageDataSetStatusPending
	}
	if (status != model.StorageDataSetStatusPending && status != model.StorageDataSetStatusCreating) || binding.DataSetID != nil {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	evidence, err := binding.CreationRejectionEvidence()
	if err != nil {
		return &storagereplacement.SourceOutcomeError{Message: "Could not verify the recorded setup refusal. Check the setup task before replacing the provider."}
	}
	if binding.ClientDataSetID != nil && (binding.ClientDataSetID.IsZero() || evidence == nil) {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	effects, err := dataSetHasExternalEffects(ctx, db, binding.ID)
	if err != nil {
		return err
	}
	if effects {
		return &storagereplacement.SourceOutcomeError{Message: "This provider already received storage work. Check this replica's tasks and service status before replacing it."}
	}
	return nil
}

func failedInitializationOwner(ctx context.Context, db bun.IDB, binding *model.StorageDataSet) error {
	if binding.Status == model.StorageDataSetStatusReady || binding.Status == model.StorageDataSetStatusRetired || binding.EnsureTaskID == nil {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	task, err := (&BunTaskRepo{db: db}).GetByID(ctx, *binding.EnsureTaskID)
	if err != nil {
		return err
	}
	if task == nil || task.Type != model.TaskTypeStorageDataSetEnsure || task.SubjectType != "storage_data_set" ||
		task.SubjectKey != strconv.FormatInt(binding.ID, 10) || task.SupersededAt != nil {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	if task.Status == model.TaskStatusPending || task.Status == model.TaskStatusRunning {
		return storagereplacement.ErrSourceRunning
	}
	if task.Status != model.TaskStatusFailed {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	effects, err := dataSetHasExternalEffects(ctx, db, binding.ID)
	if err != nil {
		return err
	}
	if effects {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	return nil
}

func uncreatedDataSetEligibility(ctx context.Context, db bun.IDB, source *model.StorageDataSet) error {
	return failedInitializationOwner(ctx, db, source)
}

func locallyEndedDataSet(source *model.StorageDataSet) bool {
	return source.Status == model.StorageDataSetStatusRetired && !source.IsCurrent && source.EnsureTaskID == nil && source.DataSetID == nil
}

func dataSetHasExternalEffects(ctx context.Context, db bun.IDB, dataSetID int64) (bool, error) {
	var exists bool
	err := db.NewRaw(`SELECT
		EXISTS (SELECT 1 FROM storage_copies WHERE storage_data_set_id = ? AND
			(piece_id IS NOT NULL OR commit_position IS NOT NULL OR ingress_store_attempt > 0 OR ingress_bytes_transferred > 0))
		OR EXISTS (SELECT 1 FROM storage_pull_attempts WHERE storage_data_set_id = ?)
		OR EXISTS (SELECT 1 FROM storage_commit_requests WHERE storage_data_set_id = ? AND sealed_at IS NOT NULL)`,
		dataSetID, dataSetID, dataSetID).Scan(ctx, &exists)
	return exists, err
}
