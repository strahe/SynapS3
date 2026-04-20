package testutil

import (
	"context"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
)

// Compile-time interface checks.
var (
	_ synapse.StorageClient = (*MockStorageClient)(nil)
	_ synapse.WalletQuerier = (*MockWalletQuerier)(nil)
	_ cache.Cache           = (*MockCache)(nil)
)

// MockStorageClient is a configurable test double for synapse.StorageClient.
type MockStorageClient struct {
	UploadFunc   func(ctx context.Context, r io.Reader, opts *storage.UploadOptions) (*storage.UploadResult, error)
	DownloadFunc func(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error)
}

func (m *MockStorageClient) Upload(ctx context.Context, r io.Reader, opts *storage.UploadOptions) (*storage.UploadResult, error) {
	if m.UploadFunc != nil {
		return m.UploadFunc(ctx, r, opts)
	}
	return nil, errors.New("MockStorageClient.Upload not configured")
}

func (m *MockStorageClient) Download(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error) {
	if m.DownloadFunc != nil {
		return m.DownloadFunc(ctx, pieceCID, opts)
	}
	return nil, errors.New("MockStorageClient.Download not configured")
}

// MockWalletQuerier is a configurable test double for synapse.WalletQuerier.
type MockWalletQuerier struct {
	GetWalletInfoFunc func(ctx context.Context) (*synapse.WalletInfo, error)
}

func (m *MockWalletQuerier) GetWalletInfo(ctx context.Context) (*synapse.WalletInfo, error) {
	if m.GetWalletInfoFunc != nil {
		return m.GetWalletInfoFunc(ctx)
	}
	return nil, errors.New("MockWalletQuerier.GetWalletInfo not configured")
}

// MockCache is a configurable test double for cache.Cache.
// Use for fault injection tests; for happy-path tests prefer real cache.NewFilesystem.
type MockCache struct {
	PutFunc             func(ctx context.Context, bucket, key string, r io.Reader) (*cache.ObjectInfo, error)
	PutStagedFunc       func(ctx context.Context, bucket, key string, r io.Reader) (*cache.StagedObject, error)
	GetFunc             func(ctx context.Context, bucket, key string) (io.ReadCloser, *cache.ObjectInfo, error)
	DeleteFunc          func(ctx context.Context, bucket, key string) error
	ExistsFunc          func(ctx context.Context, bucket, key string) bool
	UsedBytesFunc       func() int64
	CreateBucketDirFunc func(ctx context.Context, bucket string) error
	DeleteBucketDirFunc func(ctx context.Context, bucket string) error
	PutPartFunc         func(ctx context.Context, uploadID string, partNumber int, r io.Reader) (*cache.ObjectInfo, error)
	AssemblePartsFunc   func(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (*cache.ObjectInfo, []string, error)
	DeleteUploadFunc    func(ctx context.Context, uploadID string) error
}

func (m *MockCache) Put(ctx context.Context, bucket, key string, r io.Reader) (*cache.ObjectInfo, error) {
	if m.PutFunc != nil {
		return m.PutFunc(ctx, bucket, key, r)
	}
	return nil, errors.New("MockCache.Put not configured")
}

func (m *MockCache) PutStaged(ctx context.Context, bucket, key string, r io.Reader) (*cache.StagedObject, error) {
	if m.PutStagedFunc != nil {
		return m.PutStagedFunc(ctx, bucket, key, r)
	}
	return nil, errors.New("MockCache.PutStaged not configured")
}

func (m *MockCache) Get(ctx context.Context, bucket, key string) (io.ReadCloser, *cache.ObjectInfo, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, bucket, key)
	}
	return nil, nil, errors.New("MockCache.Get not configured")
}

func (m *MockCache) Delete(ctx context.Context, bucket, key string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, bucket, key)
	}
	return nil
}

func (m *MockCache) Exists(ctx context.Context, bucket, key string) bool {
	if m.ExistsFunc != nil {
		return m.ExistsFunc(ctx, bucket, key)
	}
	return false
}

func (m *MockCache) UsedBytes() int64 {
	if m.UsedBytesFunc != nil {
		return m.UsedBytesFunc()
	}
	return 0
}

func (m *MockCache) CreateBucketDir(ctx context.Context, bucket string) error {
	if m.CreateBucketDirFunc != nil {
		return m.CreateBucketDirFunc(ctx, bucket)
	}
	return nil
}

func (m *MockCache) DeleteBucketDir(ctx context.Context, bucket string) error {
	if m.DeleteBucketDirFunc != nil {
		return m.DeleteBucketDirFunc(ctx, bucket)
	}
	return nil
}

func (m *MockCache) PutPart(ctx context.Context, uploadID string, partNumber int, r io.Reader) (*cache.ObjectInfo, error) {
	if m.PutPartFunc != nil {
		return m.PutPartFunc(ctx, uploadID, partNumber, r)
	}
	return nil, errors.New("MockCache.PutPart not configured")
}

func (m *MockCache) AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (*cache.ObjectInfo, []string, error) {
	if m.AssemblePartsFunc != nil {
		return m.AssemblePartsFunc(ctx, bucket, key, uploadID, partNumbers)
	}
	return nil, nil, errors.New("MockCache.AssembleParts not configured")
}

func (m *MockCache) DeleteUpload(ctx context.Context, uploadID string) error {
	if m.DeleteUploadFunc != nil {
		return m.DeleteUploadFunc(ctx, uploadID)
	}
	return nil
}
