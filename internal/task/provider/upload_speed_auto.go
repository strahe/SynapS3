package provider

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func (h *ObservabilityHandler) scheduleMissingProviderSpeedTests(ctx context.Context) error {
	if h.deps.Observability == nil || h.deps.UploadSpeedProbe == nil || h.deps.Scheduler == nil || h.deps.Repositories.ProviderUploadSpeed == nil {
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
		err = h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
			taskRow, _, err := h.deps.Scheduler.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
				Type:           model.TaskTypeProviderUploadSpeedTest,
				IdempotencyKey: "provider-upload-speed:auto:" + providerID + ":" + hex.EncodeToString(nonce[:]),
				Input:          input, SubjectType: "provider", SubjectKey: providerID,
			})
			if err != nil {
				return err
			}
			return repos.ProviderUploadSpeed.BeginIfAbsent(ctx, providerID, input.ServiceURLHash, taskRow.ID)
		})
		if err != nil && !errors.Is(err, repository.ErrConflict) {
			return err
		}
	}
	return nil
}
