package testutil

import (
	"context"
	"errors"
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/synapse"
	sdkcosts "github.com/strahe/synapse-go/costs"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

// Compile-time interface checks.
var (
	_ synapse.StorageClient     = (*MockStorageClient)(nil)
	_ synapse.WalletQuerier     = (*MockWalletQuerier)(nil)
	_ synapse.ServiceTerminator = (*MockServiceTerminator)(nil)
	_ synapse.ChainEpochReader  = (*MockChainEpochReader)(nil)
	_ cache.Cache               = (*MockCache)(nil)
)

// MockStorageClient is a configurable test double for synapse.StorageClient.
type MockStorageClient struct {
	UploadFunc              func(ctx context.Context, r io.Reader, opts *storage.UploadOptions) (*storage.UploadResult, error)
	DownloadFunc            func(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error)
	PrepareUploadFunc       func(ctx context.Context, dataSize uint64, targets []synapse.StorageTarget) (*sdkcosts.MultiContextCosts, error)
	SelectUploadTargetsFunc func(ctx context.Context, opts storage.SelectUploadContextsOptions) ([]synapse.StorageTarget, error)
	OpenProviderTargetFunc  func(ctx context.Context, providerID sdktypes.BigInt, opts storage.NewProviderContextOptions) (synapse.ProviderTarget, error)
	OpenDataSetTargetFunc   func(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.DataSetTarget, error)
	OpenTargetFunc          func(ctx context.Context, opts *OpenTargetOptions) (synapse.StorageTarget, error)
	FindMatchingDataSetFunc func(ctx context.Context, providerID sdktypes.BigInt, metadata map[string]string, withCDN bool) (*storage.DataSetRef, error)
	OpenCleanupContextFunc  func(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.CleanupContext, error)
}

// OpenTargetOptions lets worker tests configure one callback for both
// immutable provider and data-set target opens.
type OpenTargetOptions struct {
	ProviderID      *sdktypes.BigInt
	DataSetID       *sdktypes.BigInt
	DataSetMetadata map[string]string
	WithCDN         *bool
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

func (m *MockStorageClient) PrepareUpload(ctx context.Context, dataSize uint64, targets []synapse.StorageTarget) (*sdkcosts.MultiContextCosts, error) {
	if m.PrepareUploadFunc != nil {
		return m.PrepareUploadFunc(ctx, dataSize, targets)
	}
	return &sdkcosts.MultiContextCosts{Ready: true}, nil
}

func (m *MockStorageClient) SelectUploadTargets(ctx context.Context, opts storage.SelectUploadContextsOptions) ([]synapse.StorageTarget, error) {
	if m.SelectUploadTargetsFunc != nil {
		return m.SelectUploadTargetsFunc(ctx, opts)
	}
	return nil, errors.New("MockStorageClient.SelectUploadTargets not configured")
}

func (m *MockStorageClient) OpenProviderTarget(ctx context.Context, providerID sdktypes.BigInt, opts storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
	if m.OpenProviderTargetFunc != nil {
		return m.OpenProviderTargetFunc(ctx, providerID, opts)
	}
	if m.OpenTargetFunc != nil {
		target, err := m.OpenTargetFunc(ctx, &OpenTargetOptions{
			ProviderID:      copySDKBigIntPtr(&providerID),
			DataSetMetadata: opts.DataSetMetadata,
			WithCDN:         opts.WithCDN,
		})
		if err != nil {
			return nil, err
		}
		providerTarget, ok := target.(synapse.ProviderTarget)
		if !ok {
			return nil, errors.New("MockStorageClient.OpenTargetFunc did not return ProviderTarget")
		}
		return providerTarget, nil
	}
	return NewMockProviderTarget(providerID, opts), nil
}

func (m *MockStorageClient) OpenDataSetTarget(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
	if m.OpenDataSetTargetFunc != nil {
		return m.OpenDataSetTargetFunc(ctx, dataSetID, opts)
	}
	if m.OpenTargetFunc != nil {
		target, err := m.OpenTargetFunc(ctx, &OpenTargetOptions{
			ProviderID: opts.ProviderID,
			DataSetID:  copySDKBigIntPtr(&dataSetID),
			WithCDN:    opts.WithCDN,
		})
		if err != nil {
			return nil, err
		}
		dataSetTarget, ok := target.(synapse.DataSetTarget)
		if !ok {
			return nil, errors.New("MockStorageClient.OpenTargetFunc did not return DataSetTarget")
		}
		if _, bound := dataSetTarget.DataSetRef(); !bound {
			clientDataSetID := dataSetID.Copy()
			if source, ok := target.(interface{ ClientDataSetID() sdktypes.BigInt }); ok {
				clientDataSetID = source.ClientDataSetID()
			}
			ref, refErr := storage.NewDataSetRef(target.ProviderID(), dataSetID, clientDataSetID)
			if refErr != nil {
				return nil, refErr
			}
			return dataSetTargetWithRef{DataSetTarget: dataSetTarget, ref: ref}, nil
		}
		return dataSetTarget, nil
	}
	providerID := sdktypes.BigInt{}
	if opts.ProviderID != nil {
		providerID = opts.ProviderID.Copy()
	}
	return NewMockDataSetTarget(providerID, dataSetID, opts.WithCDN), nil
}

type dataSetTargetWithRef struct {
	synapse.DataSetTarget
	ref storage.DataSetRef
}

func (c dataSetTargetWithRef) DataSetRef() (storage.DataSetRef, bool) { return c.ref, true }

func (m *MockStorageClient) FindMatchingDataSet(ctx context.Context, providerID sdktypes.BigInt, metadata map[string]string, withCDN bool) (*storage.DataSetRef, error) {
	if m.FindMatchingDataSetFunc != nil {
		return m.FindMatchingDataSetFunc(ctx, providerID, metadata, withCDN)
	}
	return nil, nil
}

func (m *MockStorageClient) OpenCleanupContext(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
	if m.OpenCleanupContextFunc != nil {
		return m.OpenCleanupContextFunc(ctx, dataSetID, opts)
	}
	if m.OpenDataSetTargetFunc != nil {
		dataSetCtx, err := m.OpenDataSetTargetFunc(ctx, dataSetID, opts)
		if err != nil {
			return nil, err
		}
		cleanupCtx, ok := dataSetCtx.(synapse.CleanupContext)
		if !ok {
			return nil, errors.New("MockStorageClient.OpenDataSetTarget did not return CleanupContext")
		}
		return cleanupCtx, nil
	}
	if m.OpenTargetFunc != nil {
		target, err := m.OpenTargetFunc(ctx, &OpenTargetOptions{
			ProviderID: opts.ProviderID,
			DataSetID:  copySDKBigIntPtr(&dataSetID),
			WithCDN:    opts.WithCDN,
		})
		if err != nil {
			return nil, err
		}
		cleanupCtx, ok := target.(synapse.CleanupContext)
		if !ok {
			return nil, errors.New("MockStorageClient.OpenTargetFunc did not return CleanupContext")
		}
		return cleanupCtx, nil
	}
	return nil, errors.New("MockStorageClient.OpenCleanupContext not configured")
}

// MockStorageTarget is a minimal immutable storage target for tests that only need
// provider metadata for upload preparation.
type MockStorageTarget struct {
	ProviderIDValue      sdktypes.BigInt
	DataSetIDValue       *sdktypes.BigInt
	ClientDataSetIDValue sdktypes.BigInt
	ServiceURLValue      string
	WithCDNValue         bool
	CreateDataSetFunc    func(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error)
	WaitDataSetFunc      func(context.Context, string, sdktypes.BigInt) (*storage.CreateDataSetResult, error)
	// ContextIdentityValue overrides DefaultContextIdentity.
	ContextIdentityValue      storage.ContextIdentity
	FindDataSetByClientIDFunc func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error)
	StoreFunc                 func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error)
	PresignForCommitFunc      func(context.Context, []storage.PieceInput) ([]byte, error)
	PullFunc                  func(context.Context, storage.PullRequest) (*storage.PullResult, error)
	SubmitCommitFunc          func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error)
	GetCommitStatusFunc       func(context.Context, string) (*storage.CommitStatus, error)
	PieceStatusFunc           func(context.Context, cid.Cid) (*storage.PieceStatus, error)
}

func NewMockProviderTarget(providerID sdktypes.BigInt, opts storage.NewProviderContextOptions) *MockStorageTarget {
	ctx := &MockStorageTarget{ProviderIDValue: providerID.Copy(), ServiceURLValue: "https://provider.example"}
	if opts.WithCDN != nil {
		ctx.WithCDNValue = *opts.WithCDN
	}
	return ctx
}

func NewMockDataSetTarget(providerID, dataSetID sdktypes.BigInt, withCDN *bool) *MockStorageTarget {
	ctx := &MockStorageTarget{
		ProviderIDValue: providerID.Copy(),
		DataSetIDValue:  copySDKBigIntPtr(&dataSetID),
		ServiceURLValue: "https://provider.example",
	}
	if withCDN != nil {
		ctx.WithCDNValue = *withCDN
	}
	return ctx
}

func (m *MockStorageTarget) ProviderID() sdktypes.BigInt { return m.ProviderIDValue.Copy() }

func (m *MockStorageTarget) DataSetRef() (storage.DataSetRef, bool) {
	if m.DataSetIDValue == nil {
		return storage.DataSetRef{}, false
	}
	ref, err := storage.NewDataSetRef(m.ProviderIDValue, *m.DataSetIDValue, m.ClientDataSetIDValue)
	return ref, err == nil
}

func (m *MockStorageTarget) ClientDataSetID() sdktypes.BigInt { return m.ClientDataSetIDValue.Copy() }

func (m *MockStorageTarget) GetProviderInfo() storage.Provider {
	return storage.Provider{ID: m.ProviderID(), ServiceURL: m.ServiceURL()}
}

func (m *MockStorageTarget) CDNEnabled() bool { return m.WithCDNValue }

func (m *MockStorageTarget) PieceURL(piece cid.Cid) string {
	return m.ServiceURL() + "/piece/" + piece.String()
}

func (m *MockStorageTarget) ServiceURL() string {
	if m.ServiceURLValue != "" {
		return m.ServiceURLValue
	}
	return "https://provider.example"
}

func (m *MockStorageTarget) CreateDataSet(ctx context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	if m.CreateDataSetFunc != nil {
		return m.CreateDataSetFunc(ctx, opts)
	}
	return nil, errors.New("MockStorageTarget.CreateDataSet not configured")
}

func (m *MockStorageTarget) WaitForDataSetCreated(ctx context.Context, statusURL string, clientDataSetID sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
	if m.WaitDataSetFunc != nil {
		return m.WaitDataSetFunc(ctx, statusURL, clientDataSetID)
	}
	return nil, errors.New("MockStorageTarget.WaitForDataSetCreated not configured")
}

// DefaultContextIdentity is the signing identity mock targets report unless a
// test sets ContextIdentityValue.
var DefaultContextIdentity = storage.ContextIdentity{
	Payer:        common.HexToAddress("0x00000000000000000000000000000000000000a1"),
	ChainID:      314159,
	RecordKeeper: common.HexToAddress("0x00000000000000000000000000000000000000b2"),
}

func (m *MockStorageTarget) ContextIdentity() storage.ContextIdentity {
	if m.ContextIdentityValue != (storage.ContextIdentity{}) {
		return m.ContextIdentityValue
	}
	return DefaultContextIdentity
}

func (m *MockStorageTarget) FindDataSetByClientDataSetID(ctx context.Context, clientDataSetID sdktypes.BigInt) (storage.DataSetRef, bool, error) {
	if m.FindDataSetByClientIDFunc != nil {
		return m.FindDataSetByClientIDFunc(ctx, clientDataSetID)
	}
	return storage.DataSetRef{}, false, errors.New("MockStorageTarget.FindDataSetByClientDataSetID not configured")
}

func (m *MockStorageTarget) Store(ctx context.Context, reader io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
	if m.StoreFunc != nil {
		return m.StoreFunc(ctx, reader, opts)
	}
	return nil, errors.New("MockStorageTarget.Store not configured")
}

func (m *MockStorageTarget) PresignForCommit(ctx context.Context, pieces []storage.PieceInput) ([]byte, error) {
	if m.PresignForCommitFunc != nil {
		return m.PresignForCommitFunc(ctx, pieces)
	}
	return nil, errors.New("MockStorageTarget.PresignForCommit not configured")
}

func (m *MockStorageTarget) Pull(ctx context.Context, request storage.PullRequest) (*storage.PullResult, error) {
	if m.PullFunc != nil {
		return m.PullFunc(ctx, request)
	}
	return nil, errors.New("MockStorageTarget.Pull not configured")
}

func (m *MockStorageTarget) SubmitCommit(ctx context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
	if m.SubmitCommitFunc != nil {
		return m.SubmitCommitFunc(ctx, request)
	}
	return nil, errors.New("MockStorageTarget.SubmitCommit not configured")
}

func (m *MockStorageTarget) GetCommitStatus(ctx context.Context, statusURL string) (*storage.CommitStatus, error) {
	if m.GetCommitStatusFunc != nil {
		return m.GetCommitStatusFunc(ctx, statusURL)
	}
	return nil, errors.New("MockStorageTarget.GetCommitStatus not configured")
}

func (m *MockStorageTarget) PieceStatus(ctx context.Context, pieceCID cid.Cid) (*storage.PieceStatus, error) {
	if m.PieceStatusFunc != nil {
		return m.PieceStatusFunc(ctx, pieceCID)
	}
	return nil, errors.New("MockStorageTarget.PieceStatus not configured")
}

func copySDKBigIntPtr(value *sdktypes.BigInt) *sdktypes.BigInt {
	if value == nil {
		return nil
	}
	copy := value.Copy()
	return &copy
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
	AssemblePartsFunc   func(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (*cache.StagedObject, []string, error)
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

func (m *MockCache) AssemblePartsStaged(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (*cache.StagedObject, []string, error) {
	if m.AssemblePartsFunc != nil {
		return m.AssemblePartsFunc(ctx, bucket, key, uploadID, partNumbers)
	}
	return nil, nil, errors.New("MockCache.AssemblePartsStaged not configured")
}

func (m *MockCache) DeleteUpload(ctx context.Context, uploadID string) error {
	if m.DeleteUploadFunc != nil {
		return m.DeleteUploadFunc(ctx, uploadID)
	}
	return nil
}

// MockServiceTerminator is a configurable test double for
// synapse.ServiceTerminator.
type MockServiceTerminator struct {
	TerminateServiceFunc func(ctx context.Context, dataSetID sdktypes.BigInt) (*synapse.TerminationResult, error)
	// VerifyServicePayerFunc defaults to accepting every data set.
	VerifyServicePayerFunc func(ctx context.Context, dataSetID sdktypes.BigInt) error
	// ContextIdentityValue overrides DefaultContextIdentity.
	ContextIdentityValue storage.ContextIdentity
}

// VerifyServicePayer calls VerifyServicePayerFunc, accepting every data set when
// no test sets it.
func (m *MockServiceTerminator) VerifyServicePayer(ctx context.Context, dataSetID sdktypes.BigInt) error {
	if m.VerifyServicePayerFunc != nil {
		return m.VerifyServicePayerFunc(ctx, dataSetID)
	}
	return nil
}

// ContextIdentity returns ContextIdentityValue, or DefaultContextIdentity when
// no test sets it.
func (m *MockServiceTerminator) ContextIdentity() storage.ContextIdentity {
	if m.ContextIdentityValue != (storage.ContextIdentity{}) {
		return m.ContextIdentityValue
	}
	return DefaultContextIdentity
}

func (m *MockServiceTerminator) TerminateService(ctx context.Context, dataSetID sdktypes.BigInt) (*synapse.TerminationResult, error) {
	if m.TerminateServiceFunc != nil {
		return m.TerminateServiceFunc(ctx, dataSetID)
	}
	return nil, errors.New("MockServiceTerminator.TerminateService not configured")
}

// MockChainEpochReader is a configurable test double for
// synapse.ChainEpochReader. An unconfigured reader reports epoch zero, which
// keeps a retirement gate waiting rather than letting it pass by accident.
type MockChainEpochReader struct {
	CurrentEpochFunc func(ctx context.Context) (int64, error)
}

func (m *MockChainEpochReader) CurrentEpoch(ctx context.Context) (int64, error) {
	if m.CurrentEpochFunc != nil {
		return m.CurrentEpochFunc(ctx)
	}
	return 0, nil
}
