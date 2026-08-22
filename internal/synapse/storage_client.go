package synapse

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

// StorageServiceAdapter adapts synapse-go's concrete storage service to
// SynapS3's testable staged storage interface. It exists because Go does not
// allow []*storage.Context to satisfy []UploadContext directly.
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

func (s *StorageServiceAdapter) PrepareUpload(ctx context.Context, dataSize uint64, contexts []UploadContext) (*storage.MultiContextCosts, error) {
	uploadContexts := make([]storage.UploadContext, 0, len(contexts))
	for _, c := range contexts {
		uploadContexts = append(uploadContexts, c)
	}
	prepared, err := s.service.Prepare(ctx, &storage.PrepareOptions{
		DataSize: dataSize,
		Contexts: uploadContexts,
	})
	if err != nil {
		return nil, normalizeResolutionOperationError(err)
	}
	if prepared == nil {
		return nil, nil
	}
	return prepared.Costs, nil
}

func (s *StorageServiceAdapter) CreateContexts(ctx context.Context, opts *storage.CreateContextsOptions) ([]UploadContext, error) {
	contexts, err := s.service.CreateContexts(ctx, opts)
	if err != nil {
		return nil, normalizeCreateContextsError(err)
	}
	out := make([]UploadContext, 0, len(contexts))
	for _, c := range contexts {
		out = append(out, &storageUploadContextAdapter{inner: c})
	}
	return out, nil
}

func (s *StorageServiceAdapter) CreateContext(ctx context.Context, opts *storage.CreateContextOptions) (UploadContext, error) {
	storageCtx, err := s.service.CreateContext(ctx, opts)
	if err != nil {
		return nil, normalizeCreateContextError(err, opts)
	}
	return &storageUploadContextAdapter{inner: storageCtx}, nil
}

func (s *StorageServiceAdapter) CreateCleanupContext(ctx context.Context, opts *storage.CreateContextOptions) (CleanupContext, error) {
	storageCtx, err := s.service.CreateContext(ctx, opts)
	if err != nil {
		return nil, normalizeCreateContextError(err, opts)
	}
	return storageCtx, nil
}

type storageUploadContextAdapter struct {
	inner *storage.Context
}

func (c *storageUploadContextAdapter) ProviderID() sdktypes.BigInt { return c.inner.ProviderID() }

func (c *storageUploadContextAdapter) DataSetID() *sdktypes.BigInt { return c.inner.DataSetID() }

func (c *storageUploadContextAdapter) GetProviderInfo() storage.Provider {
	return c.inner.GetProviderInfo()
}

func (c *storageUploadContextAdapter) WithCDN() bool { return c.inner.CDNEnabled() }

func (c *storageUploadContextAdapter) PieceURL(pieceCID cid.Cid) string {
	return c.inner.PieceURL(pieceCID)
}

func (c *storageUploadContextAdapter) ServiceURL() string { return c.inner.ServiceURL() }

func (c *storageUploadContextAdapter) CreateDataSet(ctx context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	result, err := c.inner.CreateDataSet(ctx, opts)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *storageUploadContextAdapter) WaitForDataSetCreated(ctx context.Context, submission storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error) {
	result, err := c.inner.WaitForDataSetCreated(ctx, submission)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *storageUploadContextAdapter) Store(ctx context.Context, reader io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
	result, err := c.inner.Store(ctx, reader, opts)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *storageUploadContextAdapter) PresignForCommit(ctx context.Context, pieces []storage.PieceInput) ([]byte, error) {
	result, err := c.inner.PresignForCommit(ctx, pieces)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *storageUploadContextAdapter) Pull(ctx context.Context, request storage.PullRequest) (*storage.PullResult, error) {
	result, err := c.inner.Pull(ctx, request)
	return result, NormalizeProviderOperationError(ctx, err)
}

func (c *storageUploadContextAdapter) Commit(ctx context.Context, request storage.CommitRequest) (*storage.CommitResult, error) {
	result, err := c.inner.Commit(ctx, request)
	return result, NormalizeProviderOperationError(ctx, err)
}
