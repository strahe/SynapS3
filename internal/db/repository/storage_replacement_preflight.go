package repository

import (
	"bytes"
	"context"
	"reflect"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func (r *BunStorageReplacementRepo) Preflight(ctx context.Context, dataSetID int64) (*ReplacementPreflight, error) {
	if _, _, err := r.SourceEligibility(ctx, dataSetID); err != nil {
		return nil, err
	}
	return replacementPreflight(ctx, r.db, dataSetID)
}

func replacementPreflight(ctx context.Context, db bun.IDB, dataSetID int64) (*ReplacementPreflight, error) {
	contents := &BunStorageContentRepo{db: db}
	source, err := contents.GetDataSetBindingByID(ctx, dataSetID)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, ErrNotFound
	}
	check := &ReplacementPreflight{Source: *source}
	var rows []storagereplacement.Replacement
	if err := db.NewSelect().Model(&rows).
		Where("source_data_set_id = ? AND status NOT IN (?, ?)", dataSetID, storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		OrderExpr("id ASC").Scan(ctx); err != nil {
		return nil, err
	}
	for _, row := range rows {
		target, err := contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
		if err != nil {
			return nil, err
		}
		if target == nil {
			return nil, ErrNotFound
		}
		check.Targets = append(check.Targets, ReplacementTargetCheck{Replacement: row, DataSet: *target})
	}
	return check, nil
}

func sameReplacementDataSet(a, b model.StorageDataSet) bool {
	return a.ID == b.ID && a.BucketID == b.BucketID && a.CopyIndex == b.CopyIndex && a.Generation == b.Generation &&
		a.IsCurrent == b.IsCurrent && a.Status == b.Status && a.ProviderID.Equal(b.ProviderID) &&
		reflect.DeepEqual(a.DataSetID, b.DataSetID) && reflect.DeepEqual(a.ClientDataSetID, b.ClientDataSetID) &&
		reflect.DeepEqual(a.EnsureTaskID, b.EnsureTaskID) && reflect.DeepEqual(a.CreateTransactionID, b.CreateTransactionID) &&
		reflect.DeepEqual(a.CreateStatusURL, b.CreateStatusURL) && bytes.Equal(a.CreationRejection, b.CreationRejection)
}

func validateReplacementPreflight(ctx context.Context, db bun.IDB, expected ReplacementPreflight) error {
	ids := []int64{expected.Source.ID}
	for _, target := range expected.Targets {
		if _, err := lockReplacementByID(ctx, db, target.Replacement.ID); err != nil {
			return err
		}
		ids = append(ids, target.DataSet.ID)
	}
	if err := lockReplacementDataSets(ctx, db, ids...); err != nil {
		return err
	}
	actual, err := replacementPreflight(ctx, db, expected.Source.ID)
	if err != nil {
		return err
	}
	if !sameReplacementDataSet(actual.Source, expected.Source) || len(actual.Targets) != len(expected.Targets) {
		return storagereplacement.ErrSourceOutcomeUnknown
	}
	for i, target := range actual.Targets {
		previous := expected.Targets[i]
		if target.Replacement.ID != previous.Replacement.ID || target.Replacement.Status != previous.Replacement.Status ||
			target.Replacement.TaskGeneration != previous.Replacement.TaskGeneration ||
			!reflect.DeepEqual(target.Replacement.TaskID, previous.Replacement.TaskID) ||
			!sameReplacementDataSet(target.DataSet, previous.DataSet) {
			return storagereplacement.ErrSourceOutcomeUnknown
		}
	}
	return nil
}
