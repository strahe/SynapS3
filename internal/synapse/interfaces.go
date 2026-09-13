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

// StorageTarget abstracts the immutable identity shared by provider and data
// set targets returned by synapse-go.
type StorageTarget interface {
	ProviderID() sdktypes.BigInt
	DataSetRef() (storage.DataSetRef, bool)
	GetProviderInfo() storage.Provider
	CDNEnabled() bool
	PieceURL(cid.Cid) string
	ServiceURL() string
}

// ProviderTarget is an unbound provider used to create or resume creation of
// a data set. Creating a data set does not mutate the target into a bound one.
type ProviderTarget interface {
	StorageTarget
	CreateDataSet(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error)
	WaitForDataSetCreated(context.Context, storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error)
	// ContextIdentity is the payer, chain, and record keeper the target signs
	// for. Client data set IDs are only unique within it.
	ContextIdentity() storage.ContextIdentity
	// FindDataSetByClientDataSetID reads the chain for the data set a create
	// request with this ID produced. Not found only means none is visible yet.
	FindDataSetByClientDataSetID(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error)
}

// DataSetTarget is an immutable existing data set used for piece operations.
type DataSetTarget interface {
	StorageTarget
	Store(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error)
	PresignForCommit(context.Context, []storage.PieceInput) ([]byte, error)
	Pull(context.Context, storage.PullRequest) (*storage.PullResult, error)
	SubmitCommit(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error)
	GetCommitStatus(context.Context, storage.CommitSubmission) (*storage.CommitStatus, error)
	PieceStatus(context.Context, cid.Cid) (*storage.PieceStatus, error)
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
	PrepareUpload(ctx context.Context, dataSize uint64, targets []StorageTarget) (*storage.MultiContextCosts, error)
	SelectUploadTargets(ctx context.Context, opts storage.SelectUploadContextsOptions) ([]StorageTarget, error)
	OpenProviderTarget(ctx context.Context, providerID sdktypes.BigInt, opts storage.NewProviderContextOptions) (ProviderTarget, error)
	OpenDataSetTarget(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (DataSetTarget, error)
	FindMatchingDataSet(ctx context.Context, providerID sdktypes.BigInt, metadata map[string]string, withCDN bool) (*storage.DataSetRef, error)
	OpenCleanupContext(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (CleanupContext, error)
}

// ParkedPieceState describes the provider-local state of a content-addressed
// piece before it has been committed on chain.
type ParkedPieceState string

const (
	ParkedPieceMissing    ParkedPieceState = "missing"
	ParkedPieceProcessing ParkedPieceState = "processing"
	ParkedPieceReady      ParkedPieceState = "parked"
)

// ParkedPieceChecker observes the provider's parked-piece endpoint without
// treating an on-chain lookup as evidence that an upload did not happen.
type ParkedPieceChecker interface {
	FindParkedPiece(context.Context, string, cid.Cid) (ParkedPieceState, error)
}

// ServiceTerminator is the destructive service-lifecycle boundary. It is used
// only after replacement cleanup authorization and the retirement safety gate
// have both passed.
type ServiceTerminator interface {
	TerminateService(ctx context.Context, dataSetID sdktypes.BigInt) (*TerminationResult, error)
	// VerifyServicePayer reads the data set's record and fails with
	// ErrServicePaidByAnother when this wallet does not pay for it; any other
	// error means the record could not be read. It sends nothing, so a caller can
	// check before committing to a termination.
	VerifyServicePayer(ctx context.Context, dataSetID sdktypes.BigInt) error
	// ContextIdentity reports the payer, chain, and record keeper this
	// terminator signs for, so a caller can tell that a recorded request was
	// made under another one.
	ContextIdentity() storage.ContextIdentity
}

// TerminationResult records what the chain agreed to. EndEpoch is the epoch at
// which the service actually stops, which is why retirement waits for the chain
// to reach it rather than trusting the call returning.
type TerminationResult struct {
	TxHash   string
	EndEpoch int64
}

// ChainEpochReader observes the chain head. Replacement uses it to decide when
// a terminated service has genuinely ended.
type ChainEpochReader interface {
	CurrentEpoch(ctx context.Context) (int64, error)
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
