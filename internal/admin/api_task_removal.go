package admin

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
)

type taskRemovalBatch struct {
	RequestID        string                    `json:"request_id"`
	Status           string                    `json:"status"`
	AttentionCode    *string                   `json:"attention_code,omitempty"`
	AttentionAt      *string                   `json:"attention_at,omitempty"`
	ProviderID       string                    `json:"provider_id"`
	ProviderIdentity *providerIdentityResponse `json:"provider_identity,omitempty"`
	DataSetID        string                    `json:"data_set_id,omitempty"`
	PieceCount       int                       `json:"piece_count"`
	TxHash           *string                   `json:"tx_hash,omitempty"`
	LastError        *string                   `json:"last_error,omitempty"`
}

func (s *Server) attachTaskRemovalBatches(ctx context.Context, rows []model.Task, items []taskListItem) error {
	var ids []int64
	for _, row := range rows {
		if row.Type == model.TaskTypeStorageRemoval {
			ids = append(ids, row.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	records, err := s.repos.StorageRemoval.ListRemovalBatchesForTasks(ctx, ids)
	if err != nil {
		return err
	}
	byTask := make(map[int64]int, len(rows))
	for i := range rows {
		byTask[rows[i].ID] = i
	}
	for _, record := range records {
		i, ok := byTask[record.TaskID]
		if !ok {
			continue
		}
		batch := &taskRemovalBatch{
			RequestID: record.RequestID, Status: string(record.Status),
			AttentionCode: record.AttentionCode, AttentionAt: formattedTime(record.AttentionAt),
			ProviderID: record.ProviderID.String(), PieceCount: record.PieceCount, TxHash: record.TxHash,
		}
		if record.DataSetID != nil {
			batch.DataSetID = record.DataSetID.String()
		}
		if record.LastError != nil {
			message := errtext.Format(errors.New(*record.LastError))
			batch.LastError = &message
		}
		items[i].Acknowledgeable = false
		items[i].RemovalBatch = batch
	}
	return nil
}
