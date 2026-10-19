package binding

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
)

func RecordedIdentity(ctx context.Context, repos *repository.Repositories, binding *model.StorageDataSet) (storagepipeline.ExecutionIdentity, error) {
	if binding == nil {
		return storagepipeline.ExecutionIdentity{}, storagepipeline.ErrExecutionIdentityMissing
	}
	var task *model.Task
	var err error
	if binding.EnsureTaskID != nil {
		task, err = repos.Tasks.GetByID(ctx, *binding.EnsureTaskID)
	} else {
		task, err = repos.Tasks.LatestForSubject(ctx, "storage_data_set", strconv.FormatInt(binding.ID, 10), model.TaskTypeStorageDataSetEnsure)
	}
	if err != nil {
		return storagepipeline.ExecutionIdentity{}, err
	}
	if task == nil || task.Type != model.TaskTypeStorageDataSetEnsure || task.SubjectType != "storage_data_set" || task.SubjectKey != strconv.FormatInt(binding.ID, 10) {
		return storagepipeline.ExecutionIdentity{}, storagepipeline.ErrExecutionIdentityMissing
	}
	var input storagepipeline.DataSetInput
	if err := json.Unmarshal(task.Input, &input); err != nil || input.DataSetID != binding.ID {
		return storagepipeline.ExecutionIdentity{}, storagepipeline.ErrExecutionIdentityMissing
	}
	var checkpoint struct {
		storagepipeline.ExecutionIdentity
		ClientDataSetID string `json:"client_data_set_id"`
	}
	if len(task.Checkpoint) > 0 {
		if err := json.Unmarshal(task.Checkpoint, &checkpoint); err != nil {
			return storagepipeline.ExecutionIdentity{}, fmt.Errorf("reading recorded storage identity: %w", storagepipeline.ErrExecutionIdentityMissing)
		}
		clientID, err := idtypes.ParseOnChainID("recorded client data set ID", checkpoint.ClientDataSetID)
		if err != nil || binding.ClientDataSetID == nil {
			return storagepipeline.ExecutionIdentity{}, storagepipeline.ErrExecutionIdentityMissing
		}
		if !binding.ClientDataSetID.Equal(clientID) {
			return storagepipeline.ExecutionIdentity{}, storagepipeline.ErrExecutionIdentityChanged
		}
	}
	identity := input.ExecutionIdentity
	if checkpoint.Present() {
		if !checkpoint.Complete() {
			return storagepipeline.ExecutionIdentity{}, storagepipeline.ErrExecutionIdentityMissing
		}
		if identity.Present() {
			if err := identity.Match(checkpoint.ExecutionIdentity); err != nil {
				return storagepipeline.ExecutionIdentity{}, err
			}
		}
		identity = checkpoint.ExecutionIdentity
	}
	if !identity.Complete() {
		return storagepipeline.ExecutionIdentity{}, storagepipeline.ErrExecutionIdentityMissing
	}
	return identity, nil
}

func CurrentIdentity(client synapse.StorageClient) storagepipeline.ExecutionIdentity {
	if client == nil {
		return storagepipeline.ExecutionIdentity{}
	}
	return storagepipeline.NewExecutionIdentity(client.ContextIdentity(), client.PDPVerifierAddress())
}

func (h *Resolver) ExecutionIdentity(ctx context.Context, binding *model.StorageDataSet) (storagepipeline.ExecutionIdentity, error) {
	identity, err := RecordedIdentity(ctx, h.deps.Repositories, binding)
	if err != nil {
		return identity, err
	}
	if err := identity.Match(CurrentIdentity(h.deps.Storage)); err != nil {
		return identity, err
	}
	return identity, nil
}
