package binding

import (
	"context"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func ReadReadyDataSet(ctx context.Context, tx *repository.Repositories, id int64) (*model.StorageDataSet, error) {
	binding, err := tx.Contents.GetDataSetBindingByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if binding == nil || binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.DataSetID.IsZero() {
		return nil, repository.ErrConflict
	}
	return binding, nil
}
