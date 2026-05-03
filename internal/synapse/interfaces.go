package synapse

import (
	"context"
	"io"
	"math/big"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/storage"
	"github.com/strahe/synapse-go/types"
)

// UploadContext abstracts one provider-scoped SDK storage context.
type UploadContext interface {
	ProviderID() types.ProviderID
	DataSetID() *types.DataSetID
	PieceURL(cid.Cid) string
	ServiceURL() string
	CreateDataSet(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error)
	WaitForDataSetCreated(context.Context, storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error)
	Store(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error)
	PresignForCommit(context.Context, []storage.PieceInput) ([]byte, error)
	Pull(context.Context, storage.PullRequest) (*storage.PullResult, error)
	Commit(context.Context, storage.CommitRequest) (*storage.CommitResult, error)
}

// StorageClient abstracts the synapse-go storage service for upload/download
// plus staged provider operations.
type StorageClient interface {
	Upload(ctx context.Context, r io.Reader, opts *storage.UploadOptions) (*storage.UploadResult, error)
	Download(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error)
	CreateContexts(ctx context.Context, opts *storage.CreateContextsOptions) ([]UploadContext, error)
	CreateContext(ctx context.Context, opts *storage.CreateContextOptions) (UploadContext, error)
}

// WalletQuerier provides on-chain wallet state for the admin dashboard.
type WalletQuerier interface {
	GetWalletInfo(ctx context.Context) (*WalletInfo, error)
}

// WalletInfo holds a snapshot of the wallet's on-chain state.
// Fields are nil when the corresponding RPC call failed; see Errors for details.
type WalletInfo struct {
	Address         string
	Network         string
	ChainID         int64
	Nonce           *uint64
	PaymentsAddress string
	USDFCAddress    string
	USDFCDecimals   uint8
	FILBalance      *big.Int
	USDFCBalance    *big.Int
	FILAccount      *TokenAccountInfo
	USDFCAccount    *TokenAccountInfo
	Errors          map[string]string
}

// TokenAccountInfo holds the PDP payments contract account state for a single token.
type TokenAccountInfo struct {
	Funds               *big.Int
	AvailableFunds      *big.Int
	LockupCurrent       *big.Int
	LockupRate          *big.Int
	LockupLastSettledAt *big.Int
	FundedUntilEpoch    *big.Int
}
