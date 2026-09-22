package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/types"
)

func (h *TaskHandlers) scheduleMissingProviderSpeedTests(ctx context.Context) error {
	if h.deps.Observability == nil || h.deps.UploadSpeedProbe == nil || h.taskService == nil || h.deps.Repositories.ProviderUploadSpeed == nil {
		return nil
	}
	ids, err := h.deps.Repositories.Contents.ListReadyBucketProviderIDs(ctx)
	if err != nil || len(ids) == 0 {
		return err
	}
	existing, err := h.deps.Repositories.ProviderUploadSpeed.ListByProviderIDs(ctx, ids)
	if err != nil {
		return err
	}
	for _, providerID := range ids {
		if _, tested := existing[providerID]; tested {
			continue
		}
		id, err := types.ParseOnChainID("provider_id", providerID)
		if err != nil {
			return err
		}
		serviceURL, eligible, err := providerbenchmark.CurrentServiceURL(ctx, h.deps.Observability, id)
		if err != nil {
			return err
		}
		if !eligible {
			continue
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		input := providerbenchmark.Input{ProviderID: providerID, ServiceURLHash: providerbenchmark.URLHash(serviceURL)}
		_, _, err = h.taskService.EnqueueTx(ctx, taskengine.EnqueueRequest{
			Type:           model.TaskTypeProviderUploadSpeedTest,
			IdempotencyKey: "provider-upload-speed:auto:" + providerID + ":" + hex.EncodeToString(nonce[:]),
			Input:          input, SubjectType: "provider", SubjectKey: providerID,
		}, func(ctx context.Context, repos *repository.Repositories, taskRow *model.Task, _ bool) error {
			return repos.ProviderUploadSpeed.BeginIfAbsent(ctx, providerID, input.ServiceURLHash, taskRow.ID)
		})
		if err != nil && !errors.Is(err, repository.ErrConflict) {
			return err
		}
	}
	return nil
}
