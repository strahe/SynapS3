package synapse

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/storage"
)

// StorageServiceAdapter adapts synapse-go's concrete storage service to
// SynapS3's testable staged storage interface. It exists because Go does not
// allow []*storage.Context to satisfy []UploadContext directly.
type StorageServiceAdapter struct {
	service *storage.Service
}

func AdaptStorageService(service *storage.Service) *StorageServiceAdapter {
	return &StorageServiceAdapter{service: service}
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
		return nil, err
	}
	if prepared == nil {
		return nil, nil
	}
	return prepared.Costs, nil
}

func (s *StorageServiceAdapter) CreateContexts(ctx context.Context, opts *storage.CreateContextsOptions) ([]UploadContext, error) {
	contexts, err := s.service.CreateContexts(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := make([]UploadContext, 0, len(contexts))
	for _, c := range contexts {
		out = append(out, c)
	}
	return out, nil
}

func (s *StorageServiceAdapter) CreateContext(ctx context.Context, opts *storage.CreateContextOptions) (UploadContext, error) {
	return s.service.CreateContext(ctx, opts)
}

func (s *StorageServiceAdapter) CreateCleanupContext(ctx context.Context, opts *storage.CreateContextOptions) (CleanupContext, error) {
	return s.service.CreateContext(ctx, opts)
}
