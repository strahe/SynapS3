package testutil

import (
	"context"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

// Compile-time interface checks.
var (
	_ synapse.StorageClient = (*MockStorageClient)(nil)
	_ synapse.WalletQuerier = (*MockWalletQuerier)(nil)
	_ cache.Cache           = (*MockCache)(nil)
)

// MockStorageClient is a configurable test double for synapse.StorageClient.
type MockStorageClient struct {
	UploadFunc               func(ctx context.Context, r io.Reader, opts *storage.UploadOptions) (*storage.UploadResult, error)
	DownloadFunc             func(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error)
	PrepareUploadFunc        func(ctx context.Context, dataSize uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error)
	CreateContextsFunc       func(ctx context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error)
	CreateContextFunc        func(ctx context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error)
	CreateCleanupContextFunc func(ctx context.Context, opts *storage.CreateContextOptions) (synapse.CleanupContext, error)
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

func (m *MockStorageClient) PrepareUpload(ctx context.Context, dataSize uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error) {
	if m.PrepareUploadFunc != nil {
		return m.PrepareUploadFunc(ctx, dataSize, contexts)
	}
	return &storage.MultiContextCosts{Ready: true}, nil
}

func (m *MockStorageClient) CreateContexts(ctx context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
	if m.CreateContextsFunc != nil {
		return m.CreateContextsFunc(ctx, opts)
	}
	return nil, errors.New("MockStorageClient.CreateContexts not configured")
}

func (m *MockStorageClient) CreateContext(ctx context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
	if m.CreateContextFunc != nil {
		return m.CreateContextFunc(ctx, opts)
	}
	return NewMockUploadContext(opts), nil
}

func (m *MockStorageClient) CreateCleanupContext(ctx context.Context, opts *storage.CreateContextOptions) (synapse.CleanupContext, error) {
	if m.CreateCleanupContextFunc != nil {
		return m.CreateCleanupContextFunc(ctx, opts)
	}
	if m.CreateContextFunc != nil {
		ctx, err := m.CreateContextFunc(ctx, opts)
		if err != nil {
			return nil, err
		}
		cleanupCtx, ok := ctx.(synapse.CleanupContext)
		if !ok {
			return nil, errors.New("MockStorageClient.CreateContext did not return CleanupContext")
		}
		return cleanupCtx, nil
	}
	return nil, errors.New("MockStorageClient.CreateCleanupContext not configured")
}

// MockUploadContext is a minimal UploadContext for tests that only need
// provider metadata for upload preparation.
type MockUploadContext struct {
	ProviderIDValue sdktypes.BigInt
	DataSetIDValue  *sdktypes.BigInt
	ServiceURLValue string
	WithCDNValue    bool
}

func NewMockUploadContext(opts *storage.CreateContextOptions) *MockUploadContext {
	ctx := &MockUploadContext{ServiceURLValue: "https://provider.example"}
	if opts != nil {
		if opts.ProviderID != nil {
			ctx.ProviderIDValue = opts.ProviderID.Copy()
		}
		if opts.DataSetID != nil {
			id := opts.DataSetID.Copy()
			ctx.DataSetIDValue = &id
		}
		if opts.WithCDN != nil {
			ctx.WithCDNValue = *opts.WithCDN
		}
	}
	return ctx
}

func (m *MockUploadContext) ProviderID() sdktypes.BigInt { return m.ProviderIDValue.Copy() }

func (m *MockUploadContext) DataSetID() *sdktypes.BigInt {
	if m.DataSetIDValue == nil {
		return nil
	}
	id := m.DataSetIDValue.Copy()
	return &id
}

func (m *MockUploadContext) GetProviderInfo() storage.Provider {
	return storage.Provider{ID: m.ProviderID(), ServiceURL: m.ServiceURL()}
}

func (m *MockUploadContext) WithCDN() bool { return m.WithCDNValue }

func (m *MockUploadContext) PieceURL(piece cid.Cid) string {
	return m.ServiceURL() + "/piece/" + piece.String()
}

func (m *MockUploadContext) ServiceURL() string {
	if m.ServiceURLValue != "" {
		return m.ServiceURLValue
	}
	return "https://provider.example"
}

func (m *MockUploadContext) CreateDataSet(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	return nil, errors.New("MockUploadContext.CreateDataSet not configured")
}

func (m *MockUploadContext) WaitForDataSetCreated(context.Context, storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error) {
	return nil, errors.New("MockUploadContext.WaitForDataSetCreated not configured")
}

func (m *MockUploadContext) Store(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
	return nil, errors.New("MockUploadContext.Store not configured")
}

func (m *MockUploadContext) PresignForCommit(context.Context, []storage.PieceInput) ([]byte, error) {
	return nil, errors.New("MockUploadContext.PresignForCommit not configured")
}

func (m *MockUploadContext) Pull(context.Context, storage.PullRequest) (*storage.PullResult, error) {
	return nil, errors.New("MockUploadContext.Pull not configured")
}

func (m *MockUploadContext) Commit(context.Context, storage.CommitRequest) (*storage.CommitResult, error) {
	return nil, errors.New("MockUploadContext.Commit not configured")
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
