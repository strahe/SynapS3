package repository

import (
	"context"
	"errors"

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
	active, err := db.NewSelect().Model((*storagereplacement.Replacement)(nil)).
		Where("source_data_set_id = ?", source.ID).
		Where("status NOT IN (?, ?, ?, ?)", storagereplacement.StatusFailed, storagereplacement.StatusCleanupAttention,
			storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).Exists(ctx)
	if err != nil {
		return false, err
	}
	if active {
		return false, storagereplacement.ErrActiveReplacement
	}
	local := source.Status != model.StorageDataSetStatusReady
	if local {
		if err := uncreatedDataSetEligibility(ctx, db, source); err != nil {
			return false, err
		}
	}
	var incoming []storagereplacement.Replacement
	if err := db.NewSelect().Model(&incoming).
		Where("target_data_set_id = ?", source.ID).
		Where("status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		Scan(ctx); err != nil {
		return false, err
	}
	for _, row := range incoming {
		// An incoming target still depends on its original source's migration;
		// replacing it would discard that unfinished obligation.
		if !local || row.Status != storagereplacement.StatusFailed {
			return false, storagereplacement.ErrActiveReplacement
		}
		prior, err := (&BunStorageContentRepo{db: db}).GetDataSetBindingByID(ctx, row.SourceDataSetID)
		if err != nil {
			return false, err
		}
		if prior == nil || !locallyEndedDataSet(prior) {
			return false, storagereplacement.ErrActiveReplacement
		}
		effects, err := dataSetHasExternalEffects(ctx, db, prior.ID)
		if err != nil {
			return false, err
		}
		if effects {
			return false, storagereplacement.ErrActiveReplacement
		}
	}
	if _, err := replaceableEarlierTargets(ctx, db, source.ID); err != nil {
		return false, err
	}
	return local, nil
}

func replaceableEarlierTargets(ctx context.Context, db bun.IDB, sourceDataSetID int64) ([]model.StorageDataSet, error) {
	var targets []model.StorageDataSet
	if err := db.NewSelect().Model(&targets).
		Where(`id IN (SELECT target_data_set_id FROM storage_replacements WHERE source_data_set_id = ? AND status NOT IN (?, ?))`,
			sourceDataSetID, storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		Where("data_set_id IS NULL AND status <> ?", model.StorageDataSetStatusRetired).Scan(ctx); err != nil {
		return nil, err
	}
	for _, target := range targets {
		if target.EnsureTaskID != nil {
			live, err := db.NewSelect().Model((*model.Task)(nil)).Where("id = ?", *target.EnsureTaskID).
				Where("status IN (?, ?)", model.TaskStatusPending, model.TaskStatusRunning).Exists(ctx)
			if err != nil {
				return nil, err
			}
			if live {
				return nil, storagereplacement.ErrTargetCreating
			}
		}
		if target.Status == model.StorageDataSetStatusFailed && target.ClientDataSetID == nil {
			target.Status = model.StorageDataSetStatusPending
		}
		if err := uncreatedDataSetEligibility(ctx, db, &target); err != nil {
			if errors.Is(err, storagereplacement.ErrSourceRunning) || errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
				return nil, storagereplacement.ErrTargetCreating
			}
			return nil, err
		}
	}
	return targets, nil
}

func uncreatedDataSetEligibility(ctx context.Context, db bun.IDB, source *model.StorageDataSet) error {
	if source.EnsureTaskID != nil {
		task, err := (&BunTaskRepo{db: db}).GetByID(ctx, *source.EnsureTaskID)
		if err != nil {
			return err
		}
		if task == nil {
			return ErrNotFound
		}
		if task.Status == model.TaskStatusRunning {
			return storagereplacement.ErrSourceRunning
		}
	}
	if source.CreateTransactionID != nil || source.CreateStatusURL != nil {
		return &storagereplacement.SourceOutcomeError{Message: "Storage setup has a submitted transaction. Wait for its confirmation before replacing the provider."}
	}
	if (source.Status != model.StorageDataSetStatusPending && source.Status != model.StorageDataSetStatusCreating) || source.DataSetID != nil {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	evidence, err := source.CreationRejectionEvidence()
	if err != nil {
		return &storagereplacement.SourceOutcomeError{Message: "Could not verify the recorded setup refusal. Check the setup task before replacing the provider."}
	}
	if source.ClientDataSetID != nil && (source.ClientDataSetID.IsZero() || evidence == nil) {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	effects, err := dataSetHasExternalEffects(ctx, db, source.ID)
	if err != nil {
		return err
	}
	if effects {
		return &storagereplacement.SourceOutcomeError{Message: "This provider already received storage work. Check this replica's tasks and service status before replacing it."}
	}
	return nil
}

func locallyEndedDataSet(source *model.StorageDataSet) bool {
	if source.Status != model.StorageDataSetStatusRetired || source.IsCurrent || source.EnsureTaskID != nil ||
		source.DataSetID != nil || source.CreateTransactionID != nil || source.CreateStatusURL != nil {
		return false
	}
	evidence, err := source.CreationRejectionEvidence()
	return err == nil && (source.ClientDataSetID == nil || evidence != nil)
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
