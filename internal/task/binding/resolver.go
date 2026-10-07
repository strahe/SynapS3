package binding

import (
	"context"
	"errors"
	"fmt"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
)

type ResolverDependencies struct {
	Repositories *repository.Repositories
	Storage      synapse.StorageClient
}

// Resolver opens execution-side targets and verifies their immutable identity.
type Resolver struct{ deps ResolverDependencies }

func NewResolver(deps ResolverDependencies) *Resolver { return &Resolver{deps: deps} }

func (h *Resolver) OpenBindingTarget(ctx context.Context, bucketName string, binding *model.StorageDataSet) (synapse.StorageTarget, error) {
	if binding == nil {
		return nil, errors.New("storage data set binding is missing")
	}
	if h == nil || h.deps.Storage == nil {
		return nil, errors.New("storage client is unavailable")
	}
	if binding.DataSetID != nil && !binding.DataSetID.IsZero() {
		return h.OpenReadyDataSet(ctx, binding)
	}
	return h.deps.Storage.OpenProviderTarget(ctx, binding.ProviderID.SDK(), storage.NewProviderContextOptions{
		DataSetMetadata: map[string]string{"bucket": bucketName},
	})
}

func (h *Resolver) OpenReadyDataSet(ctx context.Context, binding *model.StorageDataSet) (synapse.DataSetTarget, error) {
	if binding == nil || binding.DataSetID == nil || binding.DataSetID.IsZero() {
		return nil, errors.New("storage data set is not ready")
	}
	if h == nil || h.deps.Storage == nil {
		return nil, errors.New("storage client is unavailable")
	}
	providerID := binding.ProviderID.SDK()
	target, err := h.deps.Storage.OpenDataSetTarget(ctx, binding.DataSetID.SDK(), storage.NewDataSetContextOptions{ProviderID: &providerID})
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, errors.New("storage client returned no data set context")
	}
	if got := idtypes.OnChainIDFromSDK(target.ProviderID()); !got.Equal(binding.ProviderID) {
		return nil, fmt.Errorf("data set resolved provider %s, want %s", got.String(), binding.ProviderID.String())
	}
	ref, ok := target.DataSetRef()
	if !ok {
		return nil, errors.New("storage client did not resolve the requested data set")
	}
	dataSetID, clientDataSetID, err := DataSetRefIDs(binding, ref)
	if err != nil {
		return nil, err
	}
	if !dataSetID.Equal(*binding.DataSetID) {
		return nil, fmt.Errorf("data set resolved %s, want %s", dataSetID.String(), binding.DataSetID.String())
	}
	if binding.ClientDataSetID == nil {
		if err := h.deps.Repositories.Contents.BackfillClientDataSetID(ctx, repository.BackfillClientDataSetIDInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: clientDataSetID,
		}); err != nil {
			return nil, err
		}
	} else if !binding.ClientDataSetID.Equal(clientDataSetID) {
		return nil, errors.New("storage client data set identity changed")
	}
	return target, nil
}

func DataSetRefIDs(binding *model.StorageDataSet, ref storage.DataSetRef) (idtypes.OnChainID, idtypes.OnChainID, error) {
	if binding == nil {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, errors.New("storage data set binding is missing")
	}
	providerID := idtypes.OnChainIDFromSDK(ref.ProviderID())
	if !providerID.Equal(binding.ProviderID) {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, fmt.Errorf("data set resolved provider %s, want %s", providerID.String(), binding.ProviderID.String())
	}
	dataSetID := idtypes.OnChainIDFromSDK(ref.DataSetID())
	if dataSetID.IsZero() {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, errors.New("data set resolved a zero identity")
	}
	clientDataSetID := idtypes.OnChainIDFromSDK(ref.ClientDataSetID())
	// Once a request goes out, this generation owns the ID it used. A data set
	// resolved under any other ID belongs to a different generation, however
	// well its metadata matches.
	if binding.ClientDataSetID != nil && !binding.ClientDataSetID.IsZero() && !clientDataSetID.Equal(*binding.ClientDataSetID) {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, fmt.Errorf(
			"data set resolved client ID %s, want %s", clientDataSetID.String(), binding.ClientDataSetID.String())
	}
	return dataSetID, clientDataSetID, nil
}

func DataSetResultIDs(binding *model.StorageDataSet, result *storage.CreateDataSetResult) (idtypes.OnChainID, idtypes.OnChainID, error) {
	if result == nil {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, errors.New("storage provider returned no data set result")
	}
	return DataSetRefIDs(binding, result.DataSet)
}
