package synapse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

// StorageServiceAdapter adapts synapse-go's concrete immutable storage
// contexts to SynapS3's testable staged storage interface.
type StorageServiceAdapter struct {
	service    *storage.Service
	terminator storageServiceTerminator
}

func AdaptStorageService(service *storage.Service) *StorageServiceAdapter {
	return &StorageServiceAdapter{service: service, terminator: service}
}

func (s *StorageServiceAdapter) Download(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error) {
	return s.service.Download(ctx, pieceCID, opts)
}

func (s *StorageServiceAdapter) PrepareUpload(ctx context.Context, dataSize uint64, targets []StorageTarget) (*storage.MultiContextCosts, error) {
	contexts := make([]storage.StorageContext, 0, len(targets))
	for i, target := range targets {
		adapter, ok := target.(interface{ sdkStorageContext() storage.StorageContext })
		if !ok || adapter.sdkStorageContext() == nil {
			return nil, fmt.Errorf("preparing upload: target %d was not created by the storage adapter", i)
		}
		contexts = append(contexts, adapter.sdkStorageContext())
	}
	prepared, err := s.service.Prepare(ctx, &storage.PrepareOptions{
		DataSize: dataSize,
		Contexts: contexts,
	})
	if err != nil {
		return nil, normalizeResolutionOperationError(err)
	}
	if prepared == nil {
		return nil, nil
	}
	return prepared.Costs, nil
}

func (s *StorageServiceAdapter) SelectUploadTargets(ctx context.Context, opts storage.SelectUploadContextsOptions) ([]StorageTarget, error) {
	opts.AllowUnendorsedPrimary = true
	selection, selectErr := s.service.SelectUploadContexts(ctx, opts)
	if selection == nil {
		if selectErr == nil {
			selectErr = errors.New("storage target selection returned no result")
		}
		return nil, normalizeSelectUploadTargetsError(selectErr)
	}
	out := make([]StorageTarget, 0, len(selection.Contexts))
	for _, storageCtx := range selection.Contexts {
		target, err := wrapStorageTarget(storageCtx)
		if err != nil {
			return nil, err
		}
		out = append(out, target)
	}
	return out, normalizeSelectUploadTargetsError(selectErr)
}

func (s *StorageServiceAdapter) OpenProviderTarget(ctx context.Context, providerID sdktypes.BigInt, opts storage.NewProviderContextOptions) (ProviderTarget, error) {
	storageCtx, err := s.service.NewProviderContext(ctx, providerID, opts)
	if err != nil {
		return nil, normalizeOpenProviderTargetError(ctx, err)
	}
	if storageCtx == nil {
		return nil, errors.New("opening provider target returned no context")
	}
	return newProviderTargetAdapter(storageCtx), nil
}

func (s *StorageServiceAdapter) OpenDataSetTarget(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (DataSetTarget, error) {
	storageCtx, err := s.service.NewDataSetContext(ctx, dataSetID, opts)
	if err != nil {
		return nil, normalizeOpenDataSetTargetError(ctx, err)
	}
	if storageCtx == nil {
		return nil, errors.New("opening data set target returned no context")
	}
	return newDataSetTargetAdapter(storageCtx), nil
}

func (s *StorageServiceAdapter) OpenCleanupContext(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (CleanupContext, error) {
	storageCtx, err := s.service.NewDataSetContext(ctx, dataSetID, opts)
	if err != nil {
		return nil, normalizeOpenDataSetTargetError(ctx, err)
	}
	if storageCtx == nil {
		return nil, errors.New("opening cleanup context returned no context")
	}
	return storageCtx, nil
}

func (s *StorageServiceAdapter) FindMatchingDataSet(
	ctx context.Context,
	providerID sdktypes.BigInt,
	metadata map[string]string,
	withCDN bool,
) (*storage.DataSetRef, error) {
	dataSets, err := s.service.FindDataSets(ctx, &storage.FindDataSetsOptions{OnlyManaged: true})
	if err != nil {
		return nil, normalizeResolutionOperationError(err)
	}
	wanted := cloneDataSetMetadata(metadata)
	wanted["source"] = dataSetSource
	if withCDN {
		wanted["withCDN"] = ""
	} else {
		delete(wanted, "withCDN")
	}
	var best *storage.DataSetDetails
	for _, dataSet := range dataSets {
		if dataSet == nil || dataSet.DataSetInfo == nil || dataSet.DataSetID.IsZero() ||
			!dataSet.ProviderID.Equal(providerID) || dataSet.PDPEndEpoch != 0 ||
			!dataSet.IsLive || !dataSet.IsManaged || !maps.Equal(dataSet.Metadata, wanted) {
			continue
		}
		if best == nil ||
			(dataSet.HasActivePieces && !best.HasActivePieces) ||
			(dataSet.HasActivePieces == best.HasActivePieces && dataSet.DataSetID.Cmp(best.DataSetID) < 0) {
			best = dataSet
		}
	}
	if best == nil {
		return nil, nil
	}
	ref, err := storage.NewDataSetRef(best.ProviderID, best.DataSetID, best.ClientDataSetID)
	if err != nil {
		return nil, fmt.Errorf("building matching data set reference: %w", err)
	}
	return &ref, nil
}

func cloneDataSetMetadata(metadata map[string]string) map[string]string {
	out := make(map[string]string, len(metadata)+2)
	maps.Copy(out, metadata)
	return out
}

func wrapStorageTarget(storageCtx storage.StorageContext) (StorageTarget, error) {
	switch concrete := storageCtx.(type) {
	case *storage.ProviderContext:
		if concrete == nil {
			return nil, errors.New("storage selection returned a nil provider context")
		}
		return newProviderTargetAdapter(concrete), nil
	case *storage.DataSetContext:
		if concrete == nil {
			return nil, errors.New("storage selection returned a nil data set context")
		}
		return newDataSetTargetAdapter(concrete), nil
	default:
		return nil, fmt.Errorf("storage selection returned unsupported context %T", storageCtx)
	}
}

type storageTargetAdapter struct {
	inner storage.StorageContext
}

func (c storageTargetAdapter) sdkStorageContext() storage.StorageContext { return c.inner }

func (c storageTargetAdapter) ProviderID() sdktypes.BigInt { return c.inner.ProviderID() }

func (c storageTargetAdapter) DataSetRef() (storage.DataSetRef, bool) { return c.inner.DataSetRef() }

func (c storageTargetAdapter) GetProviderInfo() storage.Provider {
	return c.inner.GetProviderInfo()
}

func (c storageTargetAdapter) CDNEnabled() bool { return c.inner.CDNEnabled() }

func (c storageTargetAdapter) PieceURL(pieceCID cid.Cid) string {
	return c.inner.PieceURL(pieceCID)
}

func (c storageTargetAdapter) ServiceURL() string { return c.inner.ServiceURL() }

type providerTargetAdapter struct {
	storageTargetAdapter
	provider *storage.ProviderContext
}

func newProviderTargetAdapter(provider *storage.ProviderContext) *providerTargetAdapter {
	if provider == nil {
		return nil
	}
	return &providerTargetAdapter{storageTargetAdapter: storageTargetAdapter{inner: provider}, provider: provider}
}

func (c *providerTargetAdapter) CreateDataSet(ctx context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	result, err := c.provider.CreateDataSet(ctx, opts)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *providerTargetAdapter) WaitForDataSetCreated(ctx context.Context, submission storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error) {
	result, err := c.provider.WaitForDataSetCreated(ctx, submission)
	return result, NormalizeProviderOperationError(ctx, err)
}

type dataSetTargetAdapter struct {
	storageTargetAdapter
	dataSet *storage.DataSetContext
}

func newDataSetTargetAdapter(dataSet *storage.DataSetContext) *dataSetTargetAdapter {
	if dataSet == nil {
		return nil
	}
	return &dataSetTargetAdapter{storageTargetAdapter: storageTargetAdapter{inner: dataSet}, dataSet: dataSet}
}

func (c *dataSetTargetAdapter) Store(ctx context.Context, reader io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
	result, err := c.dataSet.Store(ctx, reader, opts)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *dataSetTargetAdapter) PresignForCommit(ctx context.Context, pieces []storage.PieceInput) ([]byte, error) {
	result, err := c.dataSet.PresignForCommit(ctx, pieces)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *dataSetTargetAdapter) Pull(ctx context.Context, request storage.PullRequest) (*storage.PullResult, error) {
	result, err := c.dataSet.Pull(ctx, request)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *dataSetTargetAdapter) SubmitCommit(ctx context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
	result, err := c.dataSet.SubmitCommit(ctx, request)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *dataSetTargetAdapter) GetCommitStatus(ctx context.Context, submission storage.CommitSubmission) (*storage.CommitStatus, error) {
	result, err := c.dataSet.GetCommitStatus(ctx, submission)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *dataSetTargetAdapter) PieceStatus(ctx context.Context, pieceCID cid.Cid) (*storage.PieceStatus, error) {
	result, err := c.dataSet.PieceStatus(ctx, pieceCID)
	return result, NormalizeProviderOperationError(ctx, err)
}

var (
	_ StorageTarget  = (*storageTargetAdapter)(nil)
	_ ProviderTarget = (*providerTargetAdapter)(nil)
	_ DataSetTarget  = (*dataSetTargetAdapter)(nil)
	_ CleanupContext = (*storage.DataSetContext)(nil)
)
