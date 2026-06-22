package synapse

import (
	"context"
	"io"
	"math/big"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

// UploadContext abstracts one provider-scoped SDK storage context.
type UploadContext interface {
	ProviderID() sdktypes.BigInt
	DataSetID() *sdktypes.BigInt
	GetProviderInfo() storage.Provider
	WithCDN() bool
	PieceURL(cid.Cid) string
	ServiceURL() string
	CreateDataSet(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error)
	WaitForDataSetCreated(context.Context, storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error)
	Store(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error)
	PresignForCommit(context.Context, []storage.PieceInput) ([]byte, error)
	Pull(context.Context, storage.PullRequest) (*storage.PullResult, error)
	Commit(context.Context, storage.CommitRequest) (*storage.CommitResult, error)
}

// CleanupContext abstracts the SDK operations needed to remove PDP pieces.
type CleanupContext interface {
	DeletePieceByID(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error)
	PieceStatus(context.Context, cid.Cid) (*storage.PieceStatus, error)
}

// StorageClient abstracts the synapse-go storage service for download plus
// staged provider operations.
type StorageClient interface {
	Download(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error)
	PrepareUpload(ctx context.Context, dataSize uint64, contexts []UploadContext) (*storage.MultiContextCosts, error)
	CreateContexts(ctx context.Context, opts *storage.CreateContextsOptions) ([]UploadContext, error)
	CreateContext(ctx context.Context, opts *storage.CreateContextOptions) (UploadContext, error)
	CreateCleanupContext(ctx context.Context, opts *storage.CreateContextOptions) (CleanupContext, error)
}

// WalletQuerier provides on-chain wallet state for the admin dashboard.
type WalletQuerier interface {
	GetWalletInfo(ctx context.Context) (*WalletInfo, error)
}

// WalletOperator broadcasts wallet payment transactions.
type WalletOperator interface {
	FundUSDFC(ctx context.Context, amount *big.Int) (string, error)
	WithdrawUSDFC(ctx context.Context, amount *big.Int) (string, error)
	ApproveFWSS(ctx context.Context) (string, error)
}

// WalletInfo holds a snapshot of the wallet's on-chain state.
// Fields are nil when the corresponding RPC call failed; see Errors for details.
type WalletInfo struct {
	Address              string
	Network              string
	ChainID              int64
	Nonce                *uint64
	CurrentEpoch         *big.Int
	EpochDurationSeconds int64
	PaymentsAddress      string
	USDFCAddress         string
	USDFCDecimals        uint8
	FILGasBalance        *big.Int
	USDFCWalletBalance   *big.Int
	PaymentAccount       *PaymentAccountInfo
	Errors               map[string]string
}

// PaymentAccountInfo holds USDFC payment contract account state.
type PaymentAccountInfo struct {
	Funds               *big.Int
	AvailableFunds      *big.Int
	LockupCurrent       *big.Int
	LockupRate          *big.Int
	LockupLastSettledAt *big.Int
	FundedUntilEpoch    *big.Int
	FundedUntilTime     *time.Time
	RunwaySeconds       *int64
	LockupRatePerDay    *big.Int
	LockupRatePerMonth  *big.Int
	NoActiveSpend       bool
}
