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

func (s *StorageServiceAdapter) Upload(ctx context.Context, r io.Reader, opts *storage.UploadOptions) (*storage.UploadResult, error) {
	return s.service.Upload(ctx, r, opts)
}

func (s *StorageServiceAdapter) Download(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error) {
	return s.service.Download(ctx, pieceCID, opts)
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
