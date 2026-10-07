package repository

import (
	"context"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
)

// A local replacement source keeps its slot but permanently gives up creation.
func dataSetCreationStoppedSQL(alias string) string {
	return fmt.Sprintf(`(%[1]s.status IN ('failed', 'retired') OR (%[1]s.data_set_id IS NULL AND EXISTS (
		SELECT 1 FROM storage_replacements AS replacement_owner WHERE replacement_owner.source_data_set_id = %[1]s.id)))`, alias)
}

func (r *BunStorageContentRepo) DataSetCreationStopped(ctx context.Context, id int64) (bool, error) {
	return r.db.NewSelect().Model((*model.StorageDataSet)(nil)).Where("id = ?", id).
		Where(dataSetCreationStoppedSQL("storage_data_set")).Exists(ctx)
}
